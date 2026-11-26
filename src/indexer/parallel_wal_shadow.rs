//! Parallel-WAL shadow observer (Card 1, `§15.4 Silo/Aether` in the alien
//! graveyard). **This module does NOT change commit semantics.** Per the
//! design in `tests/artifacts/perf/2026-04-21-profile-run/ALIEN-ARTIFACT-CARD1-SPEC.md`
//! §5.7, the shadow-run is the mandatory first rollout stage: we run the
//! existing serial batched persist path
//! unchanged, but instrument it so we can see what an epoch-ordered
//! group-commit path *would* do on the same workload.
//!
//! The goal at this stage is pure telemetry:
//!
//! * record when each writer chunk starts, ends, and how long it takes;
//! * note the "would-have-coalesced" boundaries where a parallel-WAL
//!   coordinator would have issued one combined epoch fsync instead of N;
//! * publish the numbers via `ParallelWalShadowTelemetry` so an
//!   operator can inspect them through `cass health --json`.
//!
//! Once we have 100+ consecutive shadow runs with stable numbers and no
//! surprises, the committing path can be written *on top of* this
//! observer — exactly the Shadow → Canary → Ramp → Default rollout the
//! spec demands. Until then, enabling this module costs only the shadow
//! counters' ~100 ns per chunk.
//!
//! Activation:
//! ```text
//! (unset)                             # DEFAULT: shadow observer ON
//! CASS_INDEXER_PARALLEL_WAL=shadow    # explicit shadow mode (same as default)
//! CASS_INDEXER_PARALLEL_WAL=off       # disable observer (zero overhead)
//! ```
//!
//! Any other value (including reserved `on` / `commit`) stays in shadow
//! mode at this revision — the committing path is deliberately not exposed
//! yet.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

/// One shadow-recorded chunk. Matches what the real parallel-WAL
/// coordinator would need to know: which chunk, who processed it, how
/// long it took, and whether any retry happened.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct ShadowChunkRecord {
    pub chunk_idx: usize,
    pub worker_slot: Option<usize>,
    pub base_conv_idx: usize,
    pub convs_in_chunk: usize,
    pub start_elapsed_micros: u64,
    pub finish_elapsed_micros: u64,
    pub wall_micros: u64,
    pub succeeded: bool,
}

/// One hypothetical Silo/Aether-style group-commit epoch derived from
/// shadow observations. This is evidence only; it does not drive writes.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct ShadowEpochManifest {
    pub epoch_id: u64,
    pub chunk_count: usize,
    pub worker_slots: Vec<usize>,
    pub conversation_count: usize,
    pub first_chunk_idx: usize,
    pub last_chunk_idx: usize,
    pub first_start_elapsed_micros: u64,
    pub last_finish_elapsed_micros: u64,
    pub max_chunk_wall_micros: u64,
    pub failed_chunks: usize,
    pub would_have_group_fsyncs: usize,
    pub fsyncs_saved_vs_per_chunk: usize,
}

/// Deterministic manifest for the current shadow window. It is the canary
/// contract future commit-mode work must satisfy before changing durability
/// semantics.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct ParallelWalShadowEpochPlan {
    pub schema_version: u32,
    pub mode: &'static str,
    pub epoch_micros: u64,
    pub commit_mode_allowed: bool,
    pub fallback_decision: &'static str,
    pub fallback_reason: &'static str,
    pub logical_digest: String,
    pub window_chunks: usize,
    pub total_chunks_observed: u64,
    pub successful_chunks: usize,
    pub failed_chunks: usize,
    pub total_conversations: usize,
    pub estimated_fsyncs_saved_vs_per_chunk: usize,
    pub planned_epochs: Vec<ShadowEpochManifest>,
    pub proof_obligations: Vec<&'static str>,
}

/// Aggregate shadow telemetry. This is the payload we expose to
/// operators via `cass health --json.parallel_wal_shadow`.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct ParallelWalShadowTelemetry {
    /// Most-recent run's chunk records (FIFO, bounded at
    /// `MAX_SHADOW_RECORDS` so the struct stays small enough for a
    /// health payload).
    pub recent_chunks: Vec<ShadowChunkRecord>,
    /// Monotone: total number of shadow chunks observed since startup.
    pub chunks_observed: u64,
    /// Monotone: total wall-clock across observed chunks, in
    /// microseconds.
    pub cumulative_wall_micros: u64,
    /// Monotone: chunks that returned an error (observed but didn't
    /// commit in the current code path).
    pub chunk_errors: u64,
    /// Whether shadow mode is currently active.
    pub active: bool,
    /// Hypothetical epoch/group-commit manifest for the current shadow
    /// window. This is intentionally shadow-only evidence.
    pub epoch_plan_manifest: ParallelWalShadowEpochPlan,
}

const MAX_SHADOW_RECORDS: usize = 64;
const SHADOW_EPOCH_MICROS: u64 = 40_000;

static PROCESS_START: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

struct ShadowObserverState {
    recent_chunks: std::collections::VecDeque<ShadowChunkRecord>,
    chunks_observed: u64,
    cumulative_wall_micros: u64,
    chunk_errors: u64,
}

impl ShadowObserverState {
    fn new() -> Self {
        Self {
            recent_chunks: std::collections::VecDeque::with_capacity(MAX_SHADOW_RECORDS),
            chunks_observed: 0,
            cumulative_wall_micros: 0,
            chunk_errors: 0,
        }
    }

    fn record(&mut self, record: ShadowChunkRecord) {
        if self.recent_chunks.len() >= MAX_SHADOW_RECORDS {
            self.recent_chunks.pop_front();
        }
        self.cumulative_wall_micros = self
            .cumulative_wall_micros
            .saturating_add(record.wall_micros);
        if !record.succeeded {
            self.chunk_errors = self.chunk_errors.saturating_add(1);
        }
        self.recent_chunks.push_back(record);
        self.chunks_observed = self.chunks_observed.saturating_add(1);
    }
}

static OBSERVER: std::sync::LazyLock<Mutex<ShadowObserverState>> =
    std::sync::LazyLock::new(|| Mutex::new(ShadowObserverState::new()));

/// Parse the env var. Explicit off-like values disable the observer;
/// everything else remains shadow-only, including reserved `on`/`commit`
/// values that are intentionally NOT wired up yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShadowMode {
    /// Observer is disabled; hot path is untouched.
    Off,
    /// Observer runs; per-chunk records are captured but no commit
    /// semantics change.
    Shadow,
}

pub(crate) fn mode_from_env() -> ShadowMode {
    // Default (env unset): Shadow — observer runs but no commit semantics
    // change. Explicit `off` disables it. `on` / `commit` are reserved
    // for a future revision that ships the committing path; they fall
    // back to Shadow so we never silently activate unbuilt code.
    match dotenvy::var("CASS_INDEXER_PARALLEL_WAL")
        .ok()
        .as_deref()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("off" | "0" | "false" | "no" | "disable" | "disabled") => ShadowMode::Off,
        _ => ShadowMode::Shadow,
    }
}

/// Per-chunk guard returned by [`start_chunk`]. Records wall-clock on
/// drop; caller reports success via [`finish_ok`]/[`finish_err`] before
/// dropping for clearer telemetry.
pub(crate) struct ShadowChunkGuard {
    chunk_idx: usize,
    worker_slot: Option<usize>,
    base_conv_idx: usize,
    convs_in_chunk: usize,
    start_elapsed_micros: u64,
    started_at: Instant,
    succeeded: Option<bool>,
}

impl ShadowChunkGuard {
    pub fn finish_ok(mut self) {
        self.succeeded = Some(true);
    }

    pub fn finish_err(mut self) {
        self.succeeded = Some(false);
    }
}

impl Drop for ShadowChunkGuard {
    fn drop(&mut self) {
        let wall = self.started_at.elapsed();
        let finish_elapsed_micros = elapsed_since_process_start_micros();
        let record = ShadowChunkRecord {
            chunk_idx: self.chunk_idx,
            worker_slot: self.worker_slot,
            base_conv_idx: self.base_conv_idx,
            convs_in_chunk: self.convs_in_chunk,
            start_elapsed_micros: self.start_elapsed_micros,
            finish_elapsed_micros,
            wall_micros: wall.as_micros().min(u64::MAX as u128) as u64,
            succeeded: self.succeeded.unwrap_or(false),
        };
        let mut state = OBSERVER.lock().unwrap_or_else(PoisonError::into_inner);
        state.record(record);
    }
}

/// Start a shadow chunk measurement. Cheap (one `Instant::now` +
/// struct init), and a no-op at the observer level when mode is `Off`.
pub(crate) fn start_chunk(
    chunk_idx: usize,
    base_conv_idx: usize,
    convs_in_chunk: usize,
) -> Option<ShadowChunkGuard> {
    if mode_from_env() == ShadowMode::Off {
        return None;
    }
    Some(ShadowChunkGuard {
        chunk_idx,
        worker_slot: rayon::current_thread_index(),
        base_conv_idx,
        convs_in_chunk,
        start_elapsed_micros: elapsed_since_process_start_micros(),
        started_at: Instant::now(),
        succeeded: None,
    })
}

/// Snapshot the current shadow telemetry. Clones the bounded ring
/// buffer under the observer lock. Safe to call from any thread.
pub(crate) fn telemetry_snapshot() -> ParallelWalShadowTelemetry {
    let state = OBSERVER.lock().unwrap_or_else(PoisonError::into_inner);
    let active = mode_from_env() == ShadowMode::Shadow;
    let recent_chunks: Vec<_> = state.recent_chunks.iter().cloned().collect();
    let epoch_plan_manifest = build_epoch_plan_manifest(
        active,
        &recent_chunks,
        state.chunks_observed,
        state.chunk_errors,
    );
    ParallelWalShadowTelemetry {
        recent_chunks,
        chunks_observed: state.chunks_observed,
        cumulative_wall_micros: state.cumulative_wall_micros,
        chunk_errors: state.chunk_errors,
        active,
        epoch_plan_manifest,
    }
}

fn elapsed_since_process_start_micros() -> u64 {
    PROCESS_START.elapsed().as_micros().min(u64::MAX as u128) as u64
}

fn build_epoch_plan_manifest(
    active: bool,
    recent_chunks: &[ShadowChunkRecord],
    total_chunks_observed: u64,
    total_chunk_errors: u64,
) -> ParallelWalShadowEpochPlan {
    let planned_epochs = build_epoch_manifests(recent_chunks);
    let successful_chunks = recent_chunks
        .iter()
        .filter(|record| record.succeeded)
        .count();
    let failed_chunks = recent_chunks.len().saturating_sub(successful_chunks);
    let total_conversations = recent_chunks
        .iter()
        .map(|record| record.convs_in_chunk)
        .sum();
    let estimated_fsyncs_saved_vs_per_chunk = planned_epochs
        .iter()
        .map(|epoch| epoch.fsyncs_saved_vs_per_chunk)
        .sum();
    let (fallback_decision, fallback_reason) = if !active {
        (
            "observer_disabled",
            "shadow observer is disabled; keep the existing begin-concurrent durability path",
        )
    } else if recent_chunks.is_empty() {
        (
            "collect_shadow_evidence",
            "no shadow chunks observed yet; commit-mode promotion has no evidence window",
        )
    } else if failed_chunks > 0 || total_chunk_errors > 0 {
        (
            "fallback_to_current_writer",
            "one or more shadow-observed chunks failed; commit-mode promotion remains blocked",
        )
    } else {
        (
            "shadow_only",
            "epoch plan is advisory evidence; commit mode remains blocked until equivalence and crash-replay gates pass",
        )
    };

    ParallelWalShadowEpochPlan {
        schema_version: 1,
        mode: "shadow_epoch_plan",
        epoch_micros: SHADOW_EPOCH_MICROS,
        commit_mode_allowed: false,
        fallback_decision,
        fallback_reason,
        logical_digest: logical_window_digest(recent_chunks),
        window_chunks: recent_chunks.len(),
        total_chunks_observed,
        successful_chunks,
        failed_chunks,
        total_conversations,
        estimated_fsyncs_saved_vs_per_chunk,
        planned_epochs,
        proof_obligations: vec![
            "shadow-vs-baseline persisted-row digest equality",
            "deterministic crash/replay at epoch flush checkpoints",
            "fallback to current begin-concurrent writer on any chunk or manifest validation error",
            "no commit-mode exposure while commit_mode_allowed is false",
        ],
    }
}

fn build_epoch_manifests(recent_chunks: &[ShadowChunkRecord]) -> Vec<ShadowEpochManifest> {
    #[derive(Default)]
    struct EpochAccumulator {
        chunk_count: usize,
        worker_slots: BTreeSet<usize>,
        conversation_count: usize,
        first_chunk_idx: Option<usize>,
        last_chunk_idx: Option<usize>,
        first_start_elapsed_micros: Option<u64>,
        last_finish_elapsed_micros: u64,
        max_chunk_wall_micros: u64,
        failed_chunks: usize,
    }

    let mut epochs: BTreeMap<u64, EpochAccumulator> = BTreeMap::new();
    for record in recent_chunks {
        let epoch_id = record.finish_elapsed_micros / SHADOW_EPOCH_MICROS;
        let acc = epochs.entry(epoch_id).or_default();
        acc.chunk_count += 1;
        if let Some(worker_slot) = record.worker_slot {
            acc.worker_slots.insert(worker_slot);
        }
        acc.conversation_count = acc.conversation_count.saturating_add(record.convs_in_chunk);
        acc.first_chunk_idx = Some(
            acc.first_chunk_idx
                .map_or(record.chunk_idx, |idx| idx.min(record.chunk_idx)),
        );
        acc.last_chunk_idx = Some(
            acc.last_chunk_idx
                .map_or(record.chunk_idx, |idx| idx.max(record.chunk_idx)),
        );
        acc.first_start_elapsed_micros = Some(
            acc.first_start_elapsed_micros
                .map_or(record.start_elapsed_micros, |micros| {
                    micros.min(record.start_elapsed_micros)
                }),
        );
        acc.last_finish_elapsed_micros = acc
            .last_finish_elapsed_micros
            .max(record.finish_elapsed_micros);
        acc.max_chunk_wall_micros = acc.max_chunk_wall_micros.max(record.wall_micros);
        if !record.succeeded {
            acc.failed_chunks += 1;
        }
    }

    epochs
        .into_iter()
        .map(|(epoch_id, acc)| {
            let successful_chunks = acc.chunk_count.saturating_sub(acc.failed_chunks);
            let would_have_group_fsyncs = usize::from(successful_chunks > 0);
            let fsyncs_saved_vs_per_chunk =
                successful_chunks.saturating_sub(would_have_group_fsyncs);
            ShadowEpochManifest {
                epoch_id,
                chunk_count: acc.chunk_count,
                worker_slots: acc.worker_slots.into_iter().collect(),
                conversation_count: acc.conversation_count,
                first_chunk_idx: acc.first_chunk_idx.unwrap_or(0),
                last_chunk_idx: acc.last_chunk_idx.unwrap_or(0),
                first_start_elapsed_micros: acc.first_start_elapsed_micros.unwrap_or(0),
                last_finish_elapsed_micros: acc.last_finish_elapsed_micros,
                max_chunk_wall_micros: acc.max_chunk_wall_micros,
                failed_chunks: acc.failed_chunks,
                would_have_group_fsyncs,
                fsyncs_saved_vs_per_chunk,
            }
        })
        .collect()
}

fn logical_window_digest(recent_chunks: &[ShadowChunkRecord]) -> String {
    let mut records = recent_chunks.to_vec();
    records.sort_by_key(|record| (record.chunk_idx, record.base_conv_idx));
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cass.parallel_wal_shadow.logical_window.v1");
    for record in records {
        hasher.update(&(record.chunk_idx as u64).to_le_bytes());
        hasher.update(
            &record
                .worker_slot
                .map(|slot| slot as u64)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(&(record.base_conv_idx as u64).to_le_bytes());
        hasher.update(&(record.convs_in_chunk as u64).to_le_bytes());
        hasher.update(&[u8::from(record.succeeded)]);
    }
    hasher.finalize().to_hex().to_string()
}

/// Mean wall-clock per chunk in the recent window; returns `None` when
/// fewer than 2 samples have been recorded so the caller can decide
/// whether the number is meaningful yet.
///
/// Currently unused in production. Kept as part of the public surface
/// because the Card 1 commit-path implementation (next session) will
/// feed it into the controller that decides whether to attempt the
/// group-commit coalescing. Removing and re-adding would just be churn.
#[allow(dead_code)]
pub(crate) fn mean_chunk_wall() -> Option<Duration> {
    let state = OBSERVER.lock().unwrap_or_else(PoisonError::into_inner);
    if state.recent_chunks.len() < 2 {
        return None;
    }
    let sum_us: u128 = state
        .recent_chunks
        .iter()
        .map(|r| r.wall_micros as u128)
        .sum();
    let mean_us = sum_us / state.recent_chunks.len() as u128;
    Some(Duration::from_micros(mean_us as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn reset_observer() {
        let mut state = OBSERVER.lock().unwrap_or_else(PoisonError::into_inner);
        *state = ShadowObserverState::new();
    }

    #[test]
    #[serial]
    fn mode_parses_shadow_and_off() {
        // SAFETY: test-local env mutation; restored at end.
        let prior = std::env::var("CASS_INDEXER_PARALLEL_WAL").ok();
        // Explicit shadow
        unsafe {
            std::env::set_var("CASS_INDEXER_PARALLEL_WAL", "shadow");
        }
        assert_eq!(mode_from_env(), ShadowMode::Shadow);
        unsafe {
            std::env::set_var("CASS_INDEXER_PARALLEL_WAL", "SHADOW");
        }
        assert_eq!(mode_from_env(), ShadowMode::Shadow);
        // Explicit off — multiple forms all recognised.
        for off_form in ["off", "0", "false", "no", "OFF", "Disable"] {
            unsafe {
                std::env::set_var("CASS_INDEXER_PARALLEL_WAL", off_form);
            }
            assert_eq!(
                mode_from_env(),
                ShadowMode::Off,
                "`{off_form}` should disable the observer"
            );
        }
        // `on` / `commit` are reserved — current revision has no
        // committing path, so they fall through to Shadow rather than
        // silently activating unbuilt code.
        unsafe {
            std::env::set_var("CASS_INDEXER_PARALLEL_WAL", "on");
        }
        assert_eq!(mode_from_env(), ShadowMode::Shadow);
        unsafe {
            std::env::set_var("CASS_INDEXER_PARALLEL_WAL", "commit");
        }
        assert_eq!(mode_from_env(), ShadowMode::Shadow);
        // Unset == default Shadow (post-flip).
        unsafe {
            std::env::remove_var("CASS_INDEXER_PARALLEL_WAL");
        }
        assert_eq!(mode_from_env(), ShadowMode::Shadow);
        if let Some(v) = prior {
            unsafe {
                std::env::set_var("CASS_INDEXER_PARALLEL_WAL", v);
            }
        }
    }

    #[test]
    #[serial]
    fn start_chunk_returns_some_by_default_post_flip() {
        let prior = std::env::var("CASS_INDEXER_PARALLEL_WAL").ok();
        // SAFETY: test-local env mutation.
        unsafe {
            std::env::remove_var("CASS_INDEXER_PARALLEL_WAL");
        }
        // After the default flip, an unset env = shadow mode on = guard
        // returned. Explicit off disables the observer and returns None.
        let guard = start_chunk(0, 0, 1);
        assert!(guard.is_some(), "unset env must default to shadow on");
        drop(guard);
        unsafe {
            std::env::set_var("CASS_INDEXER_PARALLEL_WAL", "off");
        }
        assert!(start_chunk(0, 0, 1).is_none());
        unsafe {
            std::env::remove_var("CASS_INDEXER_PARALLEL_WAL");
        }
        if let Some(v) = prior {
            unsafe {
                std::env::set_var("CASS_INDEXER_PARALLEL_WAL", v);
            }
        }
    }

    #[test]
    #[serial]
    fn start_chunk_records_on_drop_in_shadow_mode() {
        let prior = std::env::var("CASS_INDEXER_PARALLEL_WAL").ok();
        reset_observer();
        // SAFETY: test-local env mutation.
        unsafe {
            std::env::set_var("CASS_INDEXER_PARALLEL_WAL", "shadow");
        }
        {
            let guard = start_chunk(0, 0, 10).expect("guard returned in shadow mode");
            // Simulate a little work.
            std::thread::sleep(Duration::from_micros(50));
            guard.finish_ok();
        }
        let tele = telemetry_snapshot();
        assert!(tele.active);
        assert_eq!(tele.chunks_observed, 1);
        assert_eq!(tele.recent_chunks.len(), 1);
        assert!(tele.recent_chunks[0].succeeded);
        assert!(tele.recent_chunks[0].wall_micros > 0);
        unsafe {
            std::env::remove_var("CASS_INDEXER_PARALLEL_WAL");
        }
        if let Some(v) = prior {
            unsafe {
                std::env::set_var("CASS_INDEXER_PARALLEL_WAL", v);
            }
        }
    }

    #[test]
    #[serial]
    fn ring_buffer_bounded_at_max_shadow_records() {
        let prior = std::env::var("CASS_INDEXER_PARALLEL_WAL").ok();
        reset_observer();
        // SAFETY: test-local env mutation.
        unsafe {
            std::env::set_var("CASS_INDEXER_PARALLEL_WAL", "shadow");
        }
        for i in 0..(MAX_SHADOW_RECORDS + 20) {
            let g = start_chunk(i, i * 5, 5).unwrap();
            g.finish_ok();
        }
        let tele = telemetry_snapshot();
        assert_eq!(tele.recent_chunks.len(), MAX_SHADOW_RECORDS);
        assert_eq!(tele.chunks_observed, (MAX_SHADOW_RECORDS + 20) as u64);
        unsafe {
            std::env::remove_var("CASS_INDEXER_PARALLEL_WAL");
        }
        if let Some(v) = prior {
            unsafe {
                std::env::set_var("CASS_INDEXER_PARALLEL_WAL", v);
            }
        }
    }

    #[test]
    #[serial]
    fn telemetry_serializes_to_json_with_expected_keys() {
        let prior = std::env::var("CASS_INDEXER_PARALLEL_WAL").ok();
        reset_observer();
        unsafe {
            std::env::set_var("CASS_INDEXER_PARALLEL_WAL", "shadow");
        }
        let g = start_chunk(7, 100, 32).unwrap();
        g.finish_err();
        let tele = telemetry_snapshot();
        let json = serde_json::to_string(&tele).unwrap();
        for key in [
            "recent_chunks",
            "chunks_observed",
            "cumulative_wall_micros",
            "chunk_errors",
            "active",
            "epoch_plan_manifest",
            "chunk_idx",
            "worker_slot",
            "convs_in_chunk",
            "succeeded",
            "logical_digest",
            "fallback_decision",
        ] {
            assert!(
                json.contains(key),
                "expected JSON to contain `{key}`: {json}"
            );
        }
        assert_eq!(tele.chunk_errors, 1);
        unsafe {
            std::env::remove_var("CASS_INDEXER_PARALLEL_WAL");
        }
        if let Some(v) = prior {
            unsafe {
                std::env::set_var("CASS_INDEXER_PARALLEL_WAL", v);
            }
        }
    }

    fn synthetic_record(
        chunk_idx: usize,
        worker_slot: Option<usize>,
        base_conv_idx: usize,
        convs_in_chunk: usize,
        finish_elapsed_micros: u64,
        succeeded: bool,
    ) -> ShadowChunkRecord {
        ShadowChunkRecord {
            chunk_idx,
            worker_slot,
            base_conv_idx,
            convs_in_chunk,
            start_elapsed_micros: finish_elapsed_micros.saturating_sub(100),
            finish_elapsed_micros,
            wall_micros: 100,
            succeeded,
        }
    }

    #[test]
    fn epoch_plan_manifest_groups_chunks_by_shadow_epoch() {
        let records = vec![
            synthetic_record(0, Some(3), 0, 10, 1_000, true),
            synthetic_record(1, Some(4), 10, 8, 2_000, true),
            synthetic_record(2, Some(3), 18, 7, SHADOW_EPOCH_MICROS + 100, true),
        ];

        let manifest = build_epoch_plan_manifest(true, &records, records.len() as u64, 0);

        assert_eq!(manifest.schema_version, 1);
        assert!(!manifest.commit_mode_allowed);
        assert_eq!(manifest.fallback_decision, "shadow_only");
        assert_eq!(manifest.window_chunks, 3);
        assert_eq!(manifest.successful_chunks, 3);
        assert_eq!(manifest.total_conversations, 25);
        assert_eq!(manifest.planned_epochs.len(), 2);
        assert_eq!(manifest.planned_epochs[0].epoch_id, 0);
        assert_eq!(manifest.planned_epochs[0].worker_slots, vec![3, 4]);
        assert_eq!(manifest.planned_epochs[0].conversation_count, 18);
        assert_eq!(manifest.planned_epochs[0].would_have_group_fsyncs, 1);
        assert_eq!(manifest.planned_epochs[0].fsyncs_saved_vs_per_chunk, 1);
        assert_eq!(manifest.planned_epochs[1].epoch_id, 1);
        assert_eq!(manifest.estimated_fsyncs_saved_vs_per_chunk, 1);
        assert!(
            manifest
                .proof_obligations
                .iter()
                .any(|obligation| obligation.contains("crash/replay")),
            "manifest must carry the qhj9o.4 crash/replay gate"
        );
    }

    #[test]
    fn epoch_plan_digest_is_logical_not_timing_sensitive() {
        let records = vec![
            synthetic_record(1, Some(2), 8, 4, 1_000, true),
            synthetic_record(0, Some(1), 0, 8, 900, true),
        ];
        let mut retimed = records.clone();
        retimed[0].wall_micros = 9_999;
        retimed[0].start_elapsed_micros = 30_000;
        retimed[0].finish_elapsed_micros = 30_500;

        let original = build_epoch_plan_manifest(true, &records, records.len() as u64, 0);
        let retimed = build_epoch_plan_manifest(true, &retimed, records.len() as u64, 0);

        assert_eq!(
            original.logical_digest, retimed.logical_digest,
            "logical digest should identify committed chunk/worker/row intent, not timing noise"
        );
    }

    #[test]
    fn epoch_plan_manifest_blocks_commit_on_empty_or_error_windows() {
        let empty = build_epoch_plan_manifest(true, &[], 0, 0);
        assert_eq!(empty.fallback_decision, "collect_shadow_evidence");
        assert!(!empty.commit_mode_allowed);

        let failed = vec![synthetic_record(0, Some(0), 0, 10, 1_000, false)];
        let manifest = build_epoch_plan_manifest(true, &failed, 1, 1);
        assert_eq!(manifest.fallback_decision, "fallback_to_current_writer");
        assert_eq!(manifest.failed_chunks, 1);
        assert!(!manifest.commit_mode_allowed);
    }
}
