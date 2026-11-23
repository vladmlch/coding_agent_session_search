// Allow dead-code warnings for this module until downstream slices wire the
// manifest into the rebuild pipeline. The types, helpers, and unit tests here
// are the foundation ibuuh.30 needs in place before scratch-build integration
// and crash-resume lands.
#![allow(dead_code)]

//! Lexical generation manifests and publish-state vocabulary (bead ibuuh.30).
//!
//! The authoritative lexical rebuild pipeline from bead ibuuh.29 emits an
//! equivalence ledger proving what it ingested, but publish semantics are still
//! "one mutable `<data_dir>/index` directory". That leaves ordinary search
//! vulnerable to half-built artifacts during rebuild, crash, or parallel
//! experimentation.
//!
//! This module lands the *vocabulary* for the generation-based publish path:
//! a versioned manifest that describes a single lexical generation's
//! identity, build state, publish state, source fingerprint, and failure
//! history, plus atomic load / store helpers. It is intentionally isolated
//! from the rebuild pipeline in this slice; downstream slices will wire the
//! authoritative rebuild to produce these manifests in scratch directories,
//! promote them to `published`, and teach startup recovery to choose the
//! right generation.
//!
//! Invariants the type enforces:
//! - The schema version is explicit so future migrations can refuse or
//!   upgrade older manifests cleanly.
//! - Build state and publish state are independent enums so the lifecycle
//!   ("built but not yet validated", "validated but not yet published",
//!   "published but superseded") is representable without overloading a
//!   single state field.
//! - Failure history is an append-only log so crash-resume tooling can see
//!   why previous attempts were abandoned, including which attempt id, at
//!   which phase, and with what message.
//! - Counts and fingerprints live alongside state so a single manifest
//!   answers both "is this generation safe to serve?" and "does it
//!   correspond to the expected DB?".

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Current manifest format version. Bump whenever the struct layout changes
/// in a way that older or newer readers cannot ignore.
pub(crate) const LEXICAL_GENERATION_MANIFEST_VERSION: u32 = 3;

/// File name used inside a generation directory for the manifest artifact.
pub(crate) const LEXICAL_GENERATION_MANIFEST_FILE: &str = "lexical-generation-manifest.json";

mod cleanup;

pub(crate) use cleanup::{
    LexicalCleanupApplyBlocker, LexicalCleanupApplyGate, LexicalCleanupDisposition,
    LexicalCleanupDryRunPlan, LexicalCleanupReclaimCandidate, LexicalGenerationCleanupInventory,
};

/// Build-side lifecycle: what the rebuild has accomplished for this
/// generation so far.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LexicalGenerationBuildState {
    /// The generation directory exists but no docs have been written yet.
    Scratch,
    /// Docs are being written; the writer holds exclusive access.
    Building,
    /// Writer finished cleanly; artifacts are present but have not yet been
    /// validated against the equivalence ledger or schema expectations.
    Built,
    /// Validation is running (manifest fingerprint check, doc-count parity,
    /// golden-query digest check, Tantivy open probe, ...).
    Validating,
    /// Validation succeeded; the generation is a candidate for publish.
    Validated,
    /// Validation failed; the generation must not be served. The failure
    /// reason is recorded in `failure_history`.
    Failed,
}

/// Publish-side lifecycle: whether this generation is the live search
/// surface, has been superseded by a newer one, or has been quarantined for
/// forensic inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LexicalGenerationPublishState {
    /// Generation exists on disk but has not been offered to search yet.
    Staged,
    /// Generation is the current live search surface.
    Published,
    /// Generation was live at some point but a newer generation replaced it.
    Superseded,
    /// Generation is quarantined: keep the artifacts on disk for inspection
    /// but never serve them. Used for debugging failed rebuilds.
    Quarantined,
}

/// Per-shard lifecycle state. This is intentionally richer than the
/// generation-level state so recovery can reason from durable facts instead
/// of directory names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LexicalShardLifecycleState {
    /// Shard is planned but no output exists yet.
    Planned,
    /// Builder is actively writing the shard.
    Building,
    /// Output exists in a staged directory but has not been validated.
    Staged,
    /// Validation succeeded; the shard can be included in publish.
    Validated,
    /// Shard is part of a published generation.
    Published,
    /// Shard has staged output that recovery can safely continue.
    Resumable,
    /// Shard must be retained for inspection and excluded from serving.
    Quarantined,
    /// Shard is invalid or intentionally abandoned; rebuild from source.
    Abandoned,
}

/// Shard-plan identity for a generation. All shard manifests in a generation
/// must agree with this plan id before publish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LexicalGenerationShardPlan {
    pub plan_id: String,
    pub planner_version: u32,
    pub shard_count: u32,
    pub packet_contract_version: u32,
    pub source_db_fingerprint: String,
}

/// Effective build budget and controller context that shaped a generation.
/// This keeps postmortems explainable without dragging runtime-only planner
/// structs into the durable manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LexicalGenerationBuildBudget {
    pub policy_id: String,
    pub effective_settings_fingerprint: String,
    pub max_inflight_message_bytes: u64,
    pub producer_queue_pages: u64,
    pub batch_conversation_limit: u64,
    pub worker_threads: u64,
    pub controller_reason: Option<String>,
    #[serde(default)]
    pub extra_limits: BTreeMap<String, u64>,
}

/// Deferred merge/compaction lifecycle for a published shard generation.
///
/// Search-ready and fully consolidated are intentionally separate states: a
/// published generation can be safe to query while still carrying background
/// merge debt that cleanup/compaction workers may handle later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LexicalGenerationMergeDebtState {
    /// No deferred consolidation work is known for this generation.
    #[default]
    None,
    /// Consolidation is required but intentionally kept off the publish path.
    Pending,
    /// A background worker is currently consolidating this generation.
    Running,
    /// Work yielded to foreground pressure and can resume later.
    Paused,
    /// Work is blocked by policy, locks, or another explicit operator reason.
    Blocked,
    /// Deferred consolidation completed; generation is fully settled.
    Complete,
    /// Work was cancelled without invalidating the published generation.
    Cancelled,
}

/// Durable merge-debt accounting surfaced through the generation manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LexicalGenerationMergeDebt {
    pub state: LexicalGenerationMergeDebtState,
    pub updated_at_ms: Option<i64>,
    pub pending_shard_count: u64,
    pub pending_artifact_bytes: u64,
    pub reason: Option<String>,
    pub controller_reason: Option<String>,
}

impl Default for LexicalGenerationMergeDebt {
    fn default() -> Self {
        Self {
            state: LexicalGenerationMergeDebtState::None,
            updated_at_ms: None,
            pending_shard_count: 0,
            pending_artifact_bytes: 0,
            reason: None,
            controller_reason: None,
        }
    }
}

impl LexicalGenerationMergeDebt {
    pub(crate) fn has_pending_work(&self) -> bool {
        matches!(
            self.state,
            LexicalGenerationMergeDebtState::Pending
                | LexicalGenerationMergeDebtState::Running
                | LexicalGenerationMergeDebtState::Paused
                | LexicalGenerationMergeDebtState::Blocked
                | LexicalGenerationMergeDebtState::Cancelled
        )
    }

    pub(crate) fn is_fully_settled(&self) -> bool {
        matches!(
            self.state,
            LexicalGenerationMergeDebtState::None | LexicalGenerationMergeDebtState::Complete
        )
    }
}

/// Durable footprint and retention metadata for one shard artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LexicalShardManifest {
    pub shard_id: String,
    pub shard_ordinal: u32,
    pub state: LexicalShardLifecycleState,
    pub updated_at_ms: i64,
    pub indexed_doc_count: u64,
    pub message_count: u64,
    pub artifact_bytes: u64,
    pub stable_hash: Option<String>,
    pub reclaimable: bool,
    pub pinned: bool,
    pub recovery_reason: Option<String>,
    pub quarantine_reason: Option<String>,
}

impl LexicalShardManifest {
    pub(crate) fn planned(shard_id: impl Into<String>, shard_ordinal: u32, now_ms: i64) -> Self {
        Self {
            shard_id: shard_id.into(),
            shard_ordinal,
            state: LexicalShardLifecycleState::Planned,
            updated_at_ms: now_ms,
            indexed_doc_count: 0,
            message_count: 0,
            artifact_bytes: 0,
            stable_hash: None,
            reclaimable: true,
            pinned: false,
            recovery_reason: None,
            quarantine_reason: None,
        }
    }

    pub(crate) fn transition(&mut self, state: LexicalShardLifecycleState, now_ms: i64) {
        self.state = state;
        self.updated_at_ms = now_ms;
    }
}

/// Crash-startup decision derived only from manifest state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LexicalGenerationRecoveryAction {
    AttachPublished,
    PublishValidated,
    ResumeStaged,
    KeepQuarantined,
    DiscardAndRebuild,
    IgnoreSuperseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LexicalGenerationRecoveryDecision {
    pub action: LexicalGenerationRecoveryAction,
    pub reason: String,
    pub resumable_shards: Vec<String>,
    pub quarantined_shards: Vec<String>,
    pub abandoned_shards: Vec<String>,
}

/// Single entry in a generation's append-only failure log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LexicalGenerationFailure {
    /// Distinct attempt identity; different from `generation_id` because
    /// multiple attempts can be made before one succeeds.
    pub attempt_id: String,
    /// Unix ms at which the failure was observed.
    pub at_ms: i64,
    /// Coarse classification: "build", "validate", "publish", "recover".
    pub phase: String,
    /// Operator-readable message explaining the failure.
    pub message: String,
}

/// Canonical manifest describing one lexical rebuild generation.
///
/// Stored atomically at `<generation_dir>/lexical-generation-manifest.json`
/// via [`store_manifest`]. The entire manifest is re-serialized on every
/// state transition so crash-resume readers always see a consistent snapshot
/// rather than a partial update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LexicalGenerationManifest {
    /// Schema version for this manifest format.
    pub manifest_version: u32,
    /// Monotone, opaque generation identity. Recommended convention is a
    /// zero-padded sequence number combined with a short random suffix so
    /// simultaneous builds do not collide.
    pub generation_id: String,
    /// Attempt identity for the most recent build attempt that wrote this
    /// manifest. Rolls forward on retries while `generation_id` stays fixed
    /// for a logically planned generation.
    pub attempt_id: String,
    /// Unix ms at which the manifest was first created.
    pub created_at_ms: i64,
    /// Unix ms at which the manifest was last updated (build_state or
    /// publish_state transition, failure log append, etc.).
    pub updated_at_ms: i64,
    /// Source DB fingerprint the generation was built against. Kept aligned
    /// with the lexical-rebuild-state.json fingerprint so comparisons are
    /// trivial.
    pub source_db_fingerprint: String,
    /// Total conversations observed by the rebuild.
    pub conversation_count: u64,
    /// Total canonical messages observed by the rebuild.
    pub message_count: u64,
    /// Total indexed lexical documents committed to the generation.
    pub indexed_doc_count: u64,
    /// Optional pointer to the equivalence ledger fingerprint (bead
    /// ibuuh.29) so generation acceptance can cross-check the streaming
    /// accumulator digest.
    pub equivalence_manifest_fingerprint: Option<String>,
    /// Shard-plan identity, present for shard-farm generations.
    #[serde(default)]
    pub shard_plan: Option<LexicalGenerationShardPlan>,
    /// Build-budget and effective-settings context that governed this run.
    #[serde(default)]
    pub build_budget: Option<LexicalGenerationBuildBudget>,
    /// Durable per-shard state. Empty for legacy single-generation builds.
    #[serde(default)]
    pub shards: Vec<LexicalShardManifest>,
    /// Deferred merge/compaction debt that may be handled after publish.
    #[serde(default)]
    pub merge_debt: LexicalGenerationMergeDebt,
    pub build_state: LexicalGenerationBuildState,
    pub publish_state: LexicalGenerationPublishState,
    /// Append-only history of attempts that failed under this
    /// `generation_id`. Latest entry is the most recent failure.
    pub failure_history: Vec<LexicalGenerationFailure>,
}

impl LexicalGenerationManifest {
    /// Create a fresh manifest in Scratch/Staged state for the given
    /// generation id, attempt id, and source db fingerprint. Caller fills
    /// in counts as the build progresses.
    pub(crate) fn new_scratch(
        generation_id: impl Into<String>,
        attempt_id: impl Into<String>,
        source_db_fingerprint: impl Into<String>,
        now_ms: i64,
    ) -> Self {
        Self {
            manifest_version: LEXICAL_GENERATION_MANIFEST_VERSION,
            generation_id: generation_id.into(),
            attempt_id: attempt_id.into(),
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            source_db_fingerprint: source_db_fingerprint.into(),
            conversation_count: 0,
            message_count: 0,
            indexed_doc_count: 0,
            equivalence_manifest_fingerprint: None,
            shard_plan: None,
            build_budget: None,
            shards: Vec::new(),
            merge_debt: LexicalGenerationMergeDebt::default(),
            build_state: LexicalGenerationBuildState::Scratch,
            publish_state: LexicalGenerationPublishState::Staged,
            failure_history: Vec::new(),
        }
    }

    /// Attach shard-plan and budget context. The manifest records this once
    /// near generation creation so later recovery can explain which plan and
    /// controller limits produced the staged outputs.
    pub(crate) fn set_shard_plan_and_budget(
        &mut self,
        shard_plan: LexicalGenerationShardPlan,
        build_budget: LexicalGenerationBuildBudget,
        now_ms: i64,
    ) {
        self.shard_plan = Some(shard_plan);
        self.build_budget = Some(build_budget);
        self.updated_at_ms = now_ms;
    }

    /// Replace the durable shard list. Callers should provide one entry per
    /// planned shard in ordinal order.
    pub(crate) fn set_shards(&mut self, shards: Vec<LexicalShardManifest>, now_ms: i64) {
        self.shards = shards;
        self.updated_at_ms = now_ms;
    }

    /// Transition a known shard by id. Returns true when the shard existed.
    pub(crate) fn transition_shard(
        &mut self,
        shard_id: &str,
        state: LexicalShardLifecycleState,
        now_ms: i64,
        reason: Option<String>,
    ) -> bool {
        let Some(shard) = self
            .shards
            .iter_mut()
            .find(|candidate| candidate.shard_id == shard_id)
        else {
            return false;
        };
        shard.transition(state, now_ms);
        match state {
            LexicalShardLifecycleState::Quarantined => {
                shard.quarantine_reason = reason;
                shard.reclaimable = false;
            }
            LexicalShardLifecycleState::Resumable => {
                shard.recovery_reason = reason;
            }
            LexicalShardLifecycleState::Published => {
                shard.pinned = true;
                shard.reclaimable = false;
            }
            LexicalShardLifecycleState::Abandoned => {
                shard.recovery_reason = reason;
                shard.reclaimable = true;
            }
            _ => {}
        }
        self.updated_at_ms = now_ms;
        true
    }

    /// Record a build-state transition, bumping `updated_at_ms`.
    pub(crate) fn transition_build(&mut self, state: LexicalGenerationBuildState, now_ms: i64) {
        self.build_state = state;
        self.updated_at_ms = now_ms;
    }

    /// Record a publish-state transition, bumping `updated_at_ms`.
    pub(crate) fn transition_publish(&mut self, state: LexicalGenerationPublishState, now_ms: i64) {
        self.publish_state = state;
        self.updated_at_ms = now_ms;
    }

    /// Record or update deferred merge debt without changing serveability.
    pub(crate) fn record_merge_debt(
        &mut self,
        pending_shard_count: u64,
        pending_artifact_bytes: u64,
        reason: impl Into<String>,
        now_ms: i64,
    ) {
        self.merge_debt = LexicalGenerationMergeDebt {
            state: if pending_shard_count == 0 && pending_artifact_bytes == 0 {
                LexicalGenerationMergeDebtState::Complete
            } else {
                LexicalGenerationMergeDebtState::Pending
            },
            updated_at_ms: Some(now_ms),
            pending_shard_count,
            pending_artifact_bytes,
            reason: Some(reason.into()),
            controller_reason: None,
        };
        self.updated_at_ms = now_ms;
    }

    /// Move deferred merge work between background lifecycle states.
    pub(crate) fn transition_merge_debt(
        &mut self,
        state: LexicalGenerationMergeDebtState,
        now_ms: i64,
        reason: Option<String>,
        controller_reason: Option<String>,
    ) {
        self.merge_debt.state = state;
        self.merge_debt.updated_at_ms = Some(now_ms);
        self.merge_debt.reason = reason;
        self.merge_debt.controller_reason = controller_reason;
        if self.merge_debt.is_fully_settled() {
            self.merge_debt.pending_shard_count = 0;
            self.merge_debt.pending_artifact_bytes = 0;
        }
        self.updated_at_ms = now_ms;
    }

    /// Append a failure record and bump `updated_at_ms`. Callers should set
    /// `build_state` to [`LexicalGenerationBuildState::Failed`] separately
    /// when the failure is terminal for the attempt.
    pub(crate) fn record_failure(
        &mut self,
        attempt_id: impl Into<String>,
        phase: impl Into<String>,
        message: impl Into<String>,
        now_ms: i64,
    ) {
        self.failure_history.push(LexicalGenerationFailure {
            attempt_id: attempt_id.into(),
            at_ms: now_ms,
            phase: phase.into(),
            message: message.into(),
        });
        self.updated_at_ms = now_ms;
    }

    /// Whether this generation is safe to serve to ordinary search queries.
    pub(crate) fn is_serveable(&self) -> bool {
        matches!(self.build_state, LexicalGenerationBuildState::Validated)
            && matches!(self.publish_state, LexicalGenerationPublishState::Published)
    }

    /// Whether published artifacts have no known deferred merge debt.
    pub(crate) fn is_fully_consolidated(&self) -> bool {
        self.is_serveable() && self.merge_debt.is_fully_settled()
    }

    /// Derive the crash-startup action from durable manifest state. This is
    /// intentionally conservative: any quarantined or abandoned shard prevents
    /// partial shard sets from becoming visible to search.
    pub(crate) fn recovery_decision(&self) -> LexicalGenerationRecoveryDecision {
        let resumable_shards = self.shards_with_state(&[
            LexicalShardLifecycleState::Building,
            LexicalShardLifecycleState::Staged,
            LexicalShardLifecycleState::Resumable,
        ]);
        let quarantined_shards = self.shards_with_state(&[LexicalShardLifecycleState::Quarantined]);
        let abandoned_shards = self.shards_with_state(&[LexicalShardLifecycleState::Abandoned]);

        let (action, reason) = if matches!(
            self.publish_state,
            LexicalGenerationPublishState::Superseded
        ) {
            (
                LexicalGenerationRecoveryAction::IgnoreSuperseded,
                format!(
                    "generation {} was superseded by a newer publish",
                    self.generation_id
                ),
            )
        } else if !quarantined_shards.is_empty()
            || matches!(
                self.publish_state,
                LexicalGenerationPublishState::Quarantined
            )
        {
            (
                LexicalGenerationRecoveryAction::KeepQuarantined,
                format!(
                    "generation {} has quarantined shard state and must stay out of search",
                    self.generation_id
                ),
            )
        } else if !abandoned_shards.is_empty()
            || matches!(self.build_state, LexicalGenerationBuildState::Failed)
        {
            (
                LexicalGenerationRecoveryAction::DiscardAndRebuild,
                format!(
                    "generation {} has abandoned or failed state and must rebuild from source",
                    self.generation_id
                ),
            )
        } else if self.is_serveable() {
            (
                LexicalGenerationRecoveryAction::AttachPublished,
                format!(
                    "generation {} is validated and published",
                    self.generation_id
                ),
            )
        } else if matches!(self.build_state, LexicalGenerationBuildState::Validated)
            && self.all_shards_publish_ready()
        {
            (
                LexicalGenerationRecoveryAction::PublishValidated,
                format!(
                    "generation {} is validated with a complete publish-ready shard set",
                    self.generation_id
                ),
            )
        } else if !resumable_shards.is_empty()
            || matches!(
                self.build_state,
                LexicalGenerationBuildState::Scratch
                    | LexicalGenerationBuildState::Building
                    | LexicalGenerationBuildState::Built
                    | LexicalGenerationBuildState::Validating
            )
        {
            (
                LexicalGenerationRecoveryAction::ResumeStaged,
                format!(
                    "generation {} has staged or in-progress work that can be resumed",
                    self.generation_id
                ),
            )
        } else {
            (
                LexicalGenerationRecoveryAction::DiscardAndRebuild,
                format!(
                    "generation {} does not contain a safe publish or resume state",
                    self.generation_id
                ),
            )
        };

        LexicalGenerationRecoveryDecision {
            action,
            reason,
            resumable_shards,
            quarantined_shards,
            abandoned_shards,
        }
    }

    fn shards_with_state(&self, states: &[LexicalShardLifecycleState]) -> Vec<String> {
        self.shards
            .iter()
            .filter(|shard| states.contains(&shard.state))
            .map(|shard| shard.shard_id.clone())
            .collect()
    }

    fn all_shards_publish_ready(&self) -> bool {
        !self.shards.is_empty()
            && self.shards.iter().all(|shard| {
                matches!(
                    shard.state,
                    LexicalShardLifecycleState::Validated | LexicalShardLifecycleState::Published
                )
            })
            && match self.shard_plan.as_ref() {
                Some(plan) => usize::try_from(plan.shard_count) == Ok(self.shards.len()),
                None => true,
            }
    }
}

/// Canonical manifest path inside a generation directory.
pub(crate) fn manifest_path(generation_dir: &Path) -> PathBuf {
    generation_dir.join(LEXICAL_GENERATION_MANIFEST_FILE)
}

/// Current unix time in milliseconds, saturating on clock rollback.
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|delta| i64::try_from(delta.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Atomically write a manifest to `<generation_dir>/lexical-generation-manifest.json`.
///
/// Uses tmp-file + rename so partial writes are never observable to
/// readers. The parent directory is created if necessary.
pub(crate) fn store_manifest(
    generation_dir: &Path,
    manifest: &LexicalGenerationManifest,
) -> Result<()> {
    fs::create_dir_all(generation_dir).with_context(|| {
        format!(
            "creating lexical generation directory {}",
            generation_dir.display()
        )
    })?;
    let final_path = manifest_path(generation_dir);
    let tmp_path = unique_manifest_temp_path(generation_dir);
    let serialized =
        serde_json::to_vec_pretty(manifest).context("serializing lexical generation manifest")?;
    {
        let file = create_new_manifest_temp_file(&tmp_path).with_context(|| {
            format!(
                "creating scratch lexical generation manifest at {}",
                tmp_path.display()
            )
        })?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&serialized).with_context(|| {
            format!(
                "writing scratch lexical generation manifest at {}",
                tmp_path.display()
            )
        })?;
        writer.flush().with_context(|| {
            format!(
                "flushing scratch lexical generation manifest at {}",
                tmp_path.display()
            )
        })?;
        writer.get_ref().sync_all().with_context(|| {
            format!(
                "syncing scratch lexical generation manifest at {}",
                tmp_path.display()
            )
        })?;
    }
    fs::rename(&tmp_path, &final_path).with_context(|| {
        format!(
            "atomically publishing lexical generation manifest to {}",
            final_path.display()
        )
    })?;
    sync_parent_directory(&final_path)?;
    Ok(())
}

fn unique_manifest_temp_path(generation_dir: &Path) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_NONCE: AtomicU64 = AtomicU64::new(0);
    let nonce = NEXT_NONCE.fetch_add(1, Ordering::Relaxed);
    generation_dir.join(format!(
        "{}.tmp-{}.{}.{}",
        LEXICAL_GENERATION_MANIFEST_FILE,
        std::process::id(),
        now_ms(),
        nonce
    ))
}

fn create_new_manifest_temp_file(path: &Path) -> std::io::Result<fs::File> {
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
}

#[cfg(not(windows))]
fn sync_parent_directory(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let directory = fs::File::open(parent)
        .with_context(|| format!("opening parent directory {} for sync", parent.display()))?;
    directory
        .sync_all()
        .with_context(|| format!("syncing parent directory {}", parent.display()))
}

#[cfg(windows)]
fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}

/// Load a manifest from `<generation_dir>/lexical-generation-manifest.json`.
/// Returns `Ok(None)` when the file does not exist so callers can
/// distinguish "no manifest" from "corrupt manifest".
pub(crate) fn load_manifest(generation_dir: &Path) -> Result<Option<LexicalGenerationManifest>> {
    let path = manifest_path(generation_dir);
    match fs::read(&path) {
        Ok(bytes) => {
            let manifest: LexicalGenerationManifest =
                serde_json::from_slice(&bytes).with_context(|| {
                    format!("parsing lexical generation manifest at {}", path.display())
                })?;
            if manifest.manifest_version > LEXICAL_GENERATION_MANIFEST_VERSION {
                anyhow::bail!(
                    "lexical generation manifest at {} has future manifest_version {} (current runtime supports <= {})",
                    path.display(),
                    manifest.manifest_version,
                    LEXICAL_GENERATION_MANIFEST_VERSION,
                );
            }
            Ok(Some(manifest))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err)
            .with_context(|| format!("reading lexical generation manifest at {}", path.display())),
    }
}

#[cfg(test)]
mod tests;
