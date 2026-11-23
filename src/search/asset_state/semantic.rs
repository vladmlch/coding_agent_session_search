use std::path::{Path, PathBuf};

use crate::search::ann_index::hnsw_index_path;
use crate::search::embedder::Embedder;
use crate::search::fastembed_embedder::FastEmbedder;
use crate::search::hash_embedder::HashEmbedder;
use crate::search::model_manager::{
    SemanticAvailability, probe_hash_semantic_availability, probe_semantic_availability,
};
use crate::search::policy::{
    CHUNKING_STRATEGY_VERSION, CliSemanticOverrides, SEMANTIC_SCHEMA_VERSION, SemanticPolicy,
};
use crate::search::semantic_manifest::{
    ArtifactRecord, BuildCheckpoint, SemanticManifest, SemanticShardManifest, SemanticShardRecord,
    TierKind, semantic_shard_artifact_path_is_safe,
};
use crate::search::vector_index::{SemanticProgressiveUnavailableReason, vector_index_path};
use crate::storage::sqlite::{FrankenStorage, SemanticIdentityTier};

use super::types::{
    SemanticAssetState, SemanticBacklogProgressState, SemanticCheckpointProgressState,
    SemanticPreference, SemanticPreferenceSurface, SemanticRuntimeInputs, SemanticRuntimeSurface,
    SemanticTierAssetState,
};

pub(super) fn semantic_state_not_inspected(
    data_dir: &Path,
    preference: SemanticPreference,
    current_db_fingerprint: Option<&str>,
) -> SemanticAssetState {
    let (fast_tier, quality_tier, backlog, checkpoint) =
        semantic_manifest_progress(data_dir, current_db_fingerprint);
    let preference_surface = semantic_preference_surface(data_dir, preference);

    SemanticAssetState {
        status: "not_inspected",
        availability: "not_inspected",
        summary: "semantic assets were not inspected for this fast path".to_string(),
        available: false,
        can_search: false,
        fallback_mode: Some("lexical"),
        preferred_backend: preference_surface.preferred_backend,
        embedder_id: None,
        vector_index_path: None,
        model_dir: preference_surface.model_dir,
        hnsw_path: None,
        hnsw_present: false,
        hnsw_state: "not_inspected",
        hnsw_ready: false,
        progressive_ready: false,
        progressive_reason_code: None,
        // The fast-path skip-DB-open lane doesn't have an
        // `availability` to consult, so we can't honestly call the
        // tiers queryable here — leave the sub-fix-3 flags false. The
        // caller upgrades to `semantic_state_from_availability` when
        // it needs an answer.
        quality_tier_published: false,
        semantic_only_search_available: false,
        hint: Some(
            "Use 'cass status --json' or 'cass models status --json' for semantic readiness."
                .to_string(),
        ),
        fast_tier,
        quality_tier,
        backlog,
        checkpoint,
    }
}

pub fn inspect_semantic_assets(
    data_dir: &Path,
    db_path: &Path,
    preference: SemanticPreference,
    current_db_fingerprint: Option<&str>,
    db_available: bool,
) -> SemanticAssetState {
    if !db_available {
        let availability = SemanticAvailability::DatabaseUnavailable {
            db_path: db_path.to_path_buf(),
            error: "database unavailable during asset inspection".to_string(),
        };
        return semantic_state_from_availability(
            data_dir,
            &availability,
            preference,
            current_db_fingerprint,
        );
    }

    let availability = match preference {
        SemanticPreference::DefaultModel => probe_semantic_availability(data_dir),
        SemanticPreference::HashFallback => probe_hash_semantic_availability(data_dir),
    };
    let availability = apply_semantic_identity_invalidation(db_path, availability, preference);
    semantic_state_from_availability(data_dir, &availability, preference, current_db_fingerprint)
}

pub(super) fn apply_semantic_identity_invalidation(
    db_path: &Path,
    availability: SemanticAvailability,
    preference: SemanticPreference,
) -> SemanticAvailability {
    if !availability.can_search() {
        return availability;
    }
    let identity_tier = match preference {
        SemanticPreference::HashFallback => SemanticIdentityTier::Fast,
        SemanticPreference::DefaultModel => SemanticIdentityTier::Quality,
    };
    let storage = match FrankenStorage::open_readonly(db_path) {
        Ok(storage) => storage,
        Err(err) => {
            // `db_available` is a point-in-time observation, not proof that
            // this later marker read succeeded. If the marker cannot be
            // checked, using the semantic index could expose stale
            // agent/source identity metadata after a Pi/OMP reclassification.
            // Mark semantic loading unavailable so hybrid search truthfully
            // falls back to lexical; explicit semantic search must not return
            // results from an index whose identity safety is unknown.
            return SemanticAvailability::LoadFailed {
                context: format!("checking semantic identity invalidation marker: {err}"),
            };
        }
    };
    match storage.semantic_identity_rebuild_required(identity_tier) {
        Ok(true) => SemanticAvailability::IndexStale {
            embedder_id: semantic_embedder_id(&availability, preference)
                .unwrap_or_else(|| "semantic".to_string()),
            reason: "canonical agent/source identity changed; run 'cass index --semantic' to rebuild semantic filter metadata"
                .to_string(),
        },
        Ok(false) => availability,
        Err(err) => SemanticAvailability::LoadFailed {
            context: format!("checking semantic identity invalidation marker: {err}"),
        },
    }
}

pub fn semantic_state_from_availability(
    data_dir: &Path,
    availability: &SemanticAvailability,
    preference: SemanticPreference,
    current_db_fingerprint: Option<&str>,
) -> SemanticAssetState {
    let (mut fast_tier, mut quality_tier, backlog, checkpoint) =
        semantic_manifest_progress(data_dir, current_db_fingerprint);
    let preference_surface = semantic_preference_surface(data_dir, preference);
    let base_embedder_id = semantic_embedder_id(availability, preference);
    if let (Some(db_fingerprint), Some(embedder_id)) =
        (current_db_fingerprint, base_embedder_id.as_deref())
    {
        promote_complete_shard_generation_state(
            data_dir,
            TierKind::Fast,
            embedder_id,
            db_fingerprint,
            &mut fast_tier,
        );
        promote_complete_shard_generation_state(
            data_dir,
            TierKind::Quality,
            embedder_id,
            db_fingerprint,
            &mut quality_tier,
        );
    }
    let base_vector_index_path = semantic_vector_index_path(data_dir, availability, preference);
    let base_model_dir = match availability {
        SemanticAvailability::ModelMissing { model_dir, .. } => Some(model_dir.clone()),
        _ => preference_surface.model_dir,
    };
    let base_hnsw_path = base_embedder_id
        .as_deref()
        .map(|embedder_id| hnsw_index_path(data_dir, embedder_id));
    let mut runtime = semantic_runtime_surface(SemanticRuntimeInputs {
        data_dir,
        availability,
        preference,
        fast_tier: &fast_tier,
        quality_tier: &quality_tier,
        backlog: &backlog,
        checkpoint: &checkpoint,
        base_embedder_id: base_embedder_id.clone(),
        base_vector_index_path: base_vector_index_path.clone(),
        base_model_dir: base_model_dir.clone(),
        base_hnsw_path: base_hnsw_path.clone(),
    });
    let (progressive_ready, progressive_reason) =
        semantic_progressive_topology(availability, &fast_tier, &quality_tier);
    let progressive_reason_code = progressive_reason.map(|reason| reason.code());
    if let Some(reason) = progressive_reason
        && runtime.can_search
    {
        runtime.summary = format!(
            "{}; progressive refinement unavailable ({})",
            runtime.summary,
            reason.code()
        );
    }
    let use_runtime_paths = runtime.embedder_id.is_some();
    let embedder_id = runtime.embedder_id.or(base_embedder_id);
    let vector_index_path = if use_runtime_paths {
        runtime.vector_index_path
    } else {
        runtime.vector_index_path.or(base_vector_index_path)
    };
    let model_dir = if use_runtime_paths {
        runtime.model_dir
    } else {
        runtime.model_dir.or(base_model_dir)
    };
    let hnsw_path = if use_runtime_paths {
        runtime.hnsw_path
    } else {
        runtime.hnsw_path.or(base_hnsw_path)
    };
    let (hnsw_present, hnsw_state) = bounded_ann_presence(hnsw_path.as_deref());

    // Sub-fix 3 for cass#257: report quality-tier readiness as a
    // first-class flag so operators querying `--mode semantic` can
    // tell when the quality index is usable even while the
    // progressive/hybrid stack remains incomplete.
    let quality_tier_published = semantic_tier_queryable(availability, &quality_tier);
    let fast_tier_queryable = semantic_tier_queryable(availability, &fast_tier);
    let semantic_only_search_available = quality_tier_published || fast_tier_queryable;

    SemanticAssetState {
        status: runtime.status,
        availability: runtime.availability,
        summary: runtime.summary,
        available: runtime.can_search,
        can_search: runtime.can_search,
        fallback_mode: runtime.fallback_mode,
        preferred_backend: preference_surface.preferred_backend,
        embedder_id,
        vector_index_path,
        model_dir,
        hnsw_path,
        hnsw_present,
        hnsw_state,
        // Only serving/deep admission against the retained exact owner may
        // advertise native readiness. A metadata entry alone proves nothing
        // about graph integrity, completeness, or source identity.
        hnsw_ready: false,
        progressive_ready,
        progressive_reason_code,
        quality_tier_published,
        semantic_only_search_available,
        hint: runtime.hint,
        fast_tier,
        quality_tier,
        backlog,
        checkpoint,
    }
}

pub(super) fn bounded_ann_presence(path: Option<&Path>) -> (bool, &'static str) {
    let Some(path) = path else {
        return (false, "absent");
    };
    match std::fs::symlink_metadata(path) {
        // Report even a dangling symlink as present, without following it or
        // claiming that its target is a usable native accelerator.
        Ok(_) => (true, "present_unverified"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (false, "absent"),
        Err(_) => (false, "inspection_failed"),
    }
}

pub(super) fn semantic_preference_surface(
    data_dir: &Path,
    preference: SemanticPreference,
) -> SemanticPreferenceSurface {
    match preference {
        SemanticPreference::DefaultModel => SemanticPreferenceSurface {
            preferred_backend: "fastembed",
            model_dir: active_policy_model_dir(data_dir),
        },
        SemanticPreference::HashFallback => SemanticPreferenceSurface {
            preferred_backend: "hash",
            model_dir: None,
        },
    }
}

fn semantic_runtime_surface(inputs: SemanticRuntimeInputs<'_>) -> SemanticRuntimeSurface {
    let SemanticRuntimeInputs {
        data_dir,
        availability,
        preference,
        fast_tier,
        quality_tier,
        backlog,
        checkpoint,
        base_embedder_id,
        base_vector_index_path,
        base_model_dir,
        base_hnsw_path,
    } = inputs;
    let base_status = semantic_status_from_availability(availability);
    let base_availability = semantic_availability_code(availability);
    let base_summary = availability.summary();
    let base_can_search = availability.can_search();
    let base_hint = semantic_hint(availability, preference);

    if matches!(
        availability,
        SemanticAvailability::Disabled { .. }
            | SemanticAvailability::DatabaseUnavailable { .. }
            | SemanticAvailability::LoadFailed { .. }
    ) {
        return SemanticRuntimeSurface {
            status: base_status,
            availability: base_availability,
            summary: base_summary,
            can_search: base_can_search,
            fallback_mode: (!base_can_search).then_some("lexical"),
            hint: base_hint,
            embedder_id: base_embedder_id,
            vector_index_path: base_vector_index_path,
            model_dir: base_model_dir,
            hnsw_path: base_hnsw_path,
        };
    }

    let quality_queryable = semantic_tier_queryable(availability, quality_tier);
    let fast_queryable = semantic_tier_queryable(availability, fast_tier);
    let checkpoint_active = checkpoint.active;
    let backlog_pending = backlog.pending_work;
    let manifest_assets_present = fast_tier.present || quality_tier.present;
    let backfill_active = checkpoint_active || backlog_pending;

    let effective_embedder_id = if quality_queryable {
        quality_tier.embedder_id.clone()
    } else if fast_queryable {
        fast_tier.embedder_id.clone()
    } else {
        None
    };
    let effective_vector_index_path = if quality_queryable {
        quality_tier.index_path.clone()
    } else if fast_queryable {
        fast_tier.index_path.clone()
    } else {
        None
    }
    .or_else(|| {
        effective_embedder_id
            .as_deref()
            .map(|embedder_id| vector_index_path(data_dir, embedder_id))
    });
    let effective_model_dir = effective_embedder_id.as_deref().and_then(|embedder_id| {
        (!semantic_embedder_is_hash(embedder_id))
            .then(|| model_dir_for_embedder_id(data_dir, embedder_id))
            .flatten()
    });
    let effective_hnsw_path = effective_embedder_id
        .as_deref()
        .map(|embedder_id| hnsw_index_path(data_dir, embedder_id));

    if quality_queryable || fast_queryable {
        let fully_ready = (quality_queryable || fast_queryable) && !backfill_active;
        let summary = if quality_queryable && backfill_active {
            "semantic quality tier is usable; residual semantic backfill is still finishing"
                .to_string()
        } else if quality_queryable {
            "semantic quality tier ready".to_string()
        } else if backfill_active {
            "semantic fast tier is usable; higher-quality semantic backfill is still in progress"
                .to_string()
        } else {
            "semantic fast tier ready".to_string()
        };
        let hint = if backfill_active {
            Some(
                "Semantic refinement is already usable; continue searching while higher-quality backfill finishes."
                    .to_string(),
            )
        } else {
            None
        };
        return SemanticRuntimeSurface {
            status: if fully_ready { "ready" } else { "building" },
            availability: if fully_ready {
                "ready"
            } else {
                "index_building"
            },
            summary,
            can_search: true,
            fallback_mode: None,
            hint,
            embedder_id: effective_embedder_id,
            vector_index_path: effective_vector_index_path,
            model_dir: effective_model_dir,
            hnsw_path: effective_hnsw_path,
        };
    }

    if backfill_active {
        return SemanticRuntimeSurface {
            status: "building",
            availability: "index_building",
            summary: "semantic backfill is in progress for the current database".to_string(),
            can_search: false,
            fallback_mode: Some("lexical"),
            hint: Some(
                "Run 'cass index --semantic' to finish backfilling current semantic assets; search will use lexical fallback until then."
                    .to_string(),
            ),
            embedder_id: base_embedder_id,
            vector_index_path: base_vector_index_path,
            model_dir: base_model_dir,
            hnsw_path: base_hnsw_path,
        };
    }

    if manifest_assets_present {
        return SemanticRuntimeSurface {
            status: "stale",
            availability: "index_stale",
            summary: "semantic artifacts exist but do not match the current database".to_string(),
            can_search: false,
            fallback_mode: Some("lexical"),
            hint: Some(
                "Run 'cass index --semantic' to refresh semantic assets for the current database; search will use lexical fallback until then."
                    .to_string(),
            ),
            embedder_id: base_embedder_id,
            vector_index_path: base_vector_index_path,
            model_dir: base_model_dir,
            hnsw_path: base_hnsw_path,
        };
    }

    SemanticRuntimeSurface {
        status: base_status,
        availability: base_availability,
        summary: base_summary,
        can_search: base_can_search,
        fallback_mode: (!base_can_search).then_some("lexical"),
        hint: base_hint,
        embedder_id: base_embedder_id,
        vector_index_path: base_vector_index_path,
        model_dir: base_model_dir,
        hnsw_path: base_hnsw_path,
    }
}

fn active_policy_model_dir(data_dir: &Path) -> Option<PathBuf> {
    let policy = SemanticPolicy::resolve(&CliSemanticOverrides::default());
    let embedder_name = FastEmbedder::canonical_name(&policy.quality_tier_embedder)?;
    FastEmbedder::runtime_model_dir_for(data_dir, embedder_name)
}

fn active_policy_embedder_id() -> Option<String> {
    let policy = SemanticPolicy::resolve(&CliSemanticOverrides::default());
    let embedder_name = FastEmbedder::canonical_name(&policy.quality_tier_embedder)?;
    FastEmbedder::config_for(embedder_name).map(|config| config.embedder_id)
}

fn model_dir_for_embedder_id(data_dir: &Path, embedder_id: &str) -> Option<PathBuf> {
    let embedder_name = FastEmbedder::canonical_name(embedder_id)?;
    FastEmbedder::runtime_model_dir_for(data_dir, embedder_name)
}

fn semantic_tier_queryable(
    availability: &SemanticAvailability,
    tier: &SemanticTierAssetState,
) -> bool {
    if !tier.ready || tier.current_db_matches == Some(false) {
        return false;
    }
    let Some(embedder_id) = tier.embedder_id.as_deref() else {
        return false;
    };
    if semantic_embedder_is_hash(embedder_id) {
        !matches!(
            availability,
            SemanticAvailability::Disabled { .. }
                | SemanticAvailability::DatabaseUnavailable { .. }
                | SemanticAvailability::LoadFailed { .. }
        )
    } else {
        matches!(
            availability,
            SemanticAvailability::Ready { .. }
                | SemanticAvailability::IndexBuilding { .. }
                | SemanticAvailability::IndexMissing { .. }
        )
    }
}

fn semantic_embedder_is_hash(embedder_id: &str) -> bool {
    embedder_id == HashEmbedder::default().id()
}

fn semantic_manifest_progress(
    data_dir: &Path,
    current_db_fingerprint: Option<&str>,
) -> (
    SemanticTierAssetState,
    SemanticTierAssetState,
    SemanticBacklogProgressState,
    SemanticCheckpointProgressState,
) {
    let manifest = SemanticManifest::load_or_default(data_dir).unwrap_or_default();
    let fast_tier = semantic_tier_asset_state(manifest.fast_tier.as_ref(), current_db_fingerprint);
    let quality_tier =
        semantic_tier_asset_state(manifest.quality_tier.as_ref(), current_db_fingerprint);
    let backlog = semantic_backlog_progress_state(&manifest, current_db_fingerprint);
    let checkpoint =
        semantic_checkpoint_progress_state(manifest.checkpoint.as_ref(), current_db_fingerprint);
    (fast_tier, quality_tier, backlog, checkpoint)
}

fn semantic_tier_asset_state(
    artifact: Option<&ArtifactRecord>,
    current_db_fingerprint: Option<&str>,
) -> SemanticTierAssetState {
    let Some(artifact) = artifact else {
        return SemanticTierAssetState::default();
    };

    SemanticTierAssetState {
        present: true,
        ready: artifact.ready,
        current_db_matches: current_db_fingerprint.map(|fp| artifact.db_fingerprint == fp),
        conversation_count: Some(artifact.conversation_count),
        doc_count: Some(artifact.doc_count),
        embedder_id: Some(artifact.embedder_id.clone()),
        model_revision: Some(artifact.model_revision.clone()),
        completed_at_ms: Some(artifact.completed_at_ms),
        size_bytes: Some(artifact.size_bytes),
        index_path: None,
        shard_count: Some(1),
        legacy_external_space: crate::search::semantic_manifest::is_external_identity(
            &artifact.embedder_id,
        ),
    }
}

fn resolve_semantic_artifact_path(data_dir: &Path, recorded_path: &str) -> Option<PathBuf> {
    semantic_shard_artifact_path_is_safe(recorded_path).then(|| data_dir.join(recorded_path))
}

fn complete_shard_records_for_state(
    data_dir: &Path,
    tier: TierKind,
    embedder_id: &str,
    db_fingerprint: &str,
) -> Option<Vec<SemanticShardRecord>> {
    let manifest = SemanticShardManifest::load(data_dir).ok().flatten()?;
    let summary = manifest.summary(tier, embedder_id, db_fingerprint);
    if !summary.complete {
        return None;
    }
    let mut records = manifest
        .shards
        .into_iter()
        .filter(|shard| shard.matches_generation(tier, embedder_id, db_fingerprint))
        .collect::<Vec<_>>();
    records.sort_by_key(|shard| shard.shard_index);
    if records.len() != usize::try_from(summary.shard_count).unwrap_or(usize::MAX) {
        return None;
    }
    let first = records.first()?;
    for (expected_index, shard) in records.iter().enumerate() {
        if shard.shard_index != u32::try_from(expected_index).unwrap_or(u32::MAX)
            || !shard.ready
            || !shard.mmap_ready
            || shard.model_revision != first.model_revision
            || shard.schema_version != SEMANTIC_SCHEMA_VERSION
            || shard.chunking_version != CHUNKING_STRATEGY_VERSION
            || shard.dimension == 0
            || shard.dimension != first.dimension
            || shard.total_conversations != first.total_conversations
        {
            return None;
        }
        let artifact_path = resolve_semantic_artifact_path(data_dir, &shard.index_path)?;
        if !artifact_path.is_file() {
            return None;
        }
    }
    Some(records)
}

fn promote_complete_shard_generation_state(
    data_dir: &Path,
    tier: TierKind,
    embedder_id: &str,
    db_fingerprint: &str,
    state: &mut SemanticTierAssetState,
) {
    if state.ready && state.current_db_matches == Some(true) {
        return;
    }
    let Some(records) =
        complete_shard_records_for_state(data_dir, tier, embedder_id, db_fingerprint)
    else {
        return;
    };
    let doc_count = records
        .iter()
        .map(|shard| shard.doc_count)
        .fold(0, u64::saturating_add);
    let size_bytes = records
        .iter()
        .map(|shard| shard.size_bytes)
        .fold(0, u64::saturating_add);
    let completed_at_ms = records
        .iter()
        .map(|shard| shard.completed_at_ms)
        .max()
        .unwrap_or(0);
    let first = &records[0];
    let Some(first_index_path) = resolve_semantic_artifact_path(data_dir, &first.index_path) else {
        return;
    };
    *state = SemanticTierAssetState {
        present: true,
        ready: true,
        current_db_matches: Some(true),
        conversation_count: Some(first.total_conversations),
        doc_count: Some(doc_count),
        embedder_id: Some(first.embedder_id.clone()),
        model_revision: Some(first.model_revision.clone()),
        completed_at_ms: Some(completed_at_ms),
        size_bytes: Some(size_bytes),
        index_path: Some(first_index_path),
        shard_count: u32::try_from(records.len()).ok(),
        legacy_external_space: crate::search::semantic_manifest::is_external_identity(
            &first.embedder_id,
        ),
    };
}

fn semantic_backlog_progress_state(
    manifest: &SemanticManifest,
    current_db_fingerprint: Option<&str>,
) -> SemanticBacklogProgressState {
    let backlog = &manifest.backlog;
    let current_db_matches = current_db_fingerprint.and_then(|fp| {
        (backlog.computed_at_ms > 0 || !backlog.db_fingerprint.is_empty())
            .then(|| backlog.is_current(fp))
    });

    SemanticBacklogProgressState {
        total_conversations: backlog.total_conversations,
        fast_tier_processed: backlog.fast_tier_processed,
        fast_tier_remaining: backlog.fast_tier_remaining(),
        quality_tier_processed: backlog.quality_tier_processed,
        quality_tier_remaining: backlog.quality_tier_remaining(),
        pending_work: backlog.has_pending_work() || manifest.checkpoint.is_some(),
        current_db_matches,
        computed_at_ms: (backlog.computed_at_ms > 0).then_some(backlog.computed_at_ms),
    }
}

fn semantic_checkpoint_progress_state(
    checkpoint: Option<&BuildCheckpoint>,
    current_db_fingerprint: Option<&str>,
) -> SemanticCheckpointProgressState {
    let Some(checkpoint) = checkpoint else {
        return SemanticCheckpointProgressState::default();
    };

    SemanticCheckpointProgressState {
        active: true,
        tier: Some(checkpoint.tier.as_str()),
        current_db_matches: current_db_fingerprint.map(|fp| checkpoint.is_valid(fp)),
        completed: Some(checkpoint.is_complete()),
        conversations_processed: Some(checkpoint.conversations_processed),
        total_conversations: Some(checkpoint.total_conversations),
        progress_pct: Some(checkpoint.progress_pct()),
        docs_embedded: Some(checkpoint.docs_embedded),
        last_offset: Some(checkpoint.last_offset),
        saved_at_ms: Some(checkpoint.saved_at_ms),
    }
}

fn semantic_embedder_id(
    availability: &SemanticAvailability,
    preference: SemanticPreference,
) -> Option<String> {
    match availability {
        SemanticAvailability::Ready { embedder_id }
        | SemanticAvailability::IndexBuilding { embedder_id, .. }
        | SemanticAvailability::IndexStale { embedder_id, .. }
        | SemanticAvailability::ModelMissing { embedder_id, .. } => Some(embedder_id.clone()),
        SemanticAvailability::HashFallback => Some(HashEmbedder::default().id().to_string()),
        _ => match preference {
            SemanticPreference::DefaultModel => active_policy_embedder_id(),
            SemanticPreference::HashFallback => Some(HashEmbedder::default().id().to_string()),
        },
    }
}

fn semantic_vector_index_path(
    data_dir: &Path,
    availability: &SemanticAvailability,
    preference: SemanticPreference,
) -> Option<PathBuf> {
    match availability {
        SemanticAvailability::IndexMissing { index_path } => Some(index_path.clone()),
        _ => semantic_embedder_id(availability, preference)
            .map(|embedder_id| vector_index_path(data_dir, &embedder_id)),
    }
}

fn semantic_progressive_topology(
    availability: &SemanticAvailability,
    fast_tier: &SemanticTierAssetState,
    quality_tier: &SemanticTierAssetState,
) -> (bool, Option<SemanticProgressiveUnavailableReason>) {
    if matches!(
        availability,
        SemanticAvailability::LoadFailed { context }
            if context
                == SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired.code()
    ) {
        return (
            false,
            Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired),
        );
    }

    let fast_queryable = semantic_tier_queryable(availability, fast_tier);
    let quality_queryable = semantic_tier_queryable(availability, quality_tier);
    if !availability.can_search() && !fast_queryable && !quality_queryable {
        return (false, None);
    }

    // Readiness must describe the strongest unsatisfied serving contract.
    // Every currently available progressive constructor reopens a pathname;
    // neither filename topology nor a successful prior exact open proves that
    // the reopened file is the same inode/generation. Keep all such lanes
    // unavailable until status and serving can share retained owners.
    (
        false,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired),
    )
}

fn semantic_availability_code(availability: &SemanticAvailability) -> &'static str {
    match availability {
        SemanticAvailability::Ready { .. } => "ready",
        SemanticAvailability::NotInstalled => "not_installed",
        SemanticAvailability::IndexBuilding { .. } => "index_building",
        SemanticAvailability::IndexStale { .. } => "index_stale",
        SemanticAvailability::HashFallback => "hash_fallback",
        SemanticAvailability::Disabled { .. } => "disabled",
        SemanticAvailability::ModelMissing { .. } => "model_missing",
        SemanticAvailability::IndexMissing { .. } => "index_missing",
        SemanticAvailability::DatabaseUnavailable { .. } => "database_unavailable",
        SemanticAvailability::LoadFailed { .. } => "load_failed",
    }
}

fn semantic_status_from_availability(availability: &SemanticAvailability) -> &'static str {
    match availability {
        SemanticAvailability::Ready { .. } => "ready",
        SemanticAvailability::HashFallback => "hash_fallback",
        SemanticAvailability::IndexBuilding { .. } => "building",
        SemanticAvailability::Disabled { .. } => "disabled",
        SemanticAvailability::IndexStale { .. } => "stale",
        SemanticAvailability::NotInstalled
        | SemanticAvailability::ModelMissing { .. }
        | SemanticAvailability::IndexMissing { .. } => "missing",
        SemanticAvailability::DatabaseUnavailable { .. }
        | SemanticAvailability::LoadFailed { .. } => "error",
    }
}

fn semantic_hint(
    availability: &SemanticAvailability,
    preference: SemanticPreference,
) -> Option<String> {
    let hint = match (preference, availability) {
        (SemanticPreference::HashFallback, SemanticAvailability::IndexMissing { .. }) => {
            "Run 'cass index --semantic --embedder hash' to build the hash vector index; lexical search remains available without semantic assets"
        }
        (SemanticPreference::HashFallback, SemanticAvailability::LoadFailed { .. })
        | (SemanticPreference::HashFallback, SemanticAvailability::DatabaseUnavailable { .. }) => {
            "Run 'cass index --semantic --embedder hash' after the database is healthy; lexical search remains available"
        }
        (SemanticPreference::HashFallback, _) => {
            "Run 'cass index --semantic --embedder hash' to build the hash vector index; lexical search remains available"
        }
        (_, SemanticAvailability::NotInstalled)
        | (_, SemanticAvailability::ModelMissing { .. }) => {
            "Run 'cass models install --from-file <DIR>' and then 'cass index --semantic'; lexical search remains available without the model"
        }
        (_, SemanticAvailability::IndexMissing { .. })
        | (_, SemanticAvailability::IndexStale { .. })
        | (_, SemanticAvailability::IndexBuilding { .. }) => {
            "Run 'cass index --semantic' to build or refresh vector assets; lexical search remains available"
        }
        (_, SemanticAvailability::Disabled { .. }) => {
            "Semantic search is disabled by policy; lexical search remains available, or re-enable semantic search"
        }
        (_, SemanticAvailability::DatabaseUnavailable { .. })
        | (_, SemanticAvailability::LoadFailed { .. }) => {
            "Restore the semantic assets and database; lexical search remains available when the archive database is healthy"
        }
        (_, SemanticAvailability::Ready { .. }) | (_, SemanticAvailability::HashFallback) => {
            return None;
        }
    };
    Some(hint.to_string())
}
