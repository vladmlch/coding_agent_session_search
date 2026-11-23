use std::path::{Path, PathBuf};

use crate::search::model_manager::SemanticAvailability;

use super::maintenance::SearchMaintenanceSnapshot;

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticPreference {
    DefaultModel,
    HashFallback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchAssetSnapshot {
    pub lexical: LexicalAssetState,
    pub semantic: SemanticAssetState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexicalAssetState {
    pub status: &'static str,
    pub exists: bool,
    /// b4uax (gh#382): the on-disk generation was written by the legacy
    /// Tantivy engine (`meta.json`, no Quill `MANIFEST`) and cannot be read
    /// by the current engine. It "exists" (so the flip is REBUILD, not
    /// "empty"), but it is not searchable until a full lexical rebuild runs.
    pub engine_incompatible: bool,
    pub fresh: bool,
    pub stale: bool,
    pub rebuilding: bool,
    /// `true` when the rebuild is nominally active but the indexing
    /// thread has not posted forward progress within the configured
    /// stall threshold. Driven by `last_progress_at_ms` on the lock
    /// file, NOT by `updated_at_ms` (which the heartbeat thread
    /// refreshes unconditionally). See issue #258 for the regression
    /// this guards against.
    pub stalled: bool,
    /// Wall-clock age of the most recent forward-progress event, when
    /// the indexer thread has posted one. `None` when no
    /// `last_progress_at_ms` is available (legacy lock file, or the
    /// rebuild is not active).
    pub last_progress_age_ms: Option<i64>,
    pub last_progress_at_ms: Option<i64>,
    pub watch_active: bool,
    pub last_indexed_at_ms: Option<i64>,
    pub age_seconds: Option<u64>,
    pub stale_threshold_seconds: u64,
    pub activity_at_ms: Option<i64>,
    pub pending_sessions: u64,
    pub processed_conversations: Option<u64>,
    pub total_conversations: Option<u64>,
    pub indexed_docs: Option<u64>,
    /// GH #457: the published generation serves far fewer documents than the
    /// completed rebuild checkpoint certified for it (see
    /// [`super::lexical_generation_hollow_verdict`]). Search still runs against a
    /// hollow generation and confidently answers "nothing found", so this is
    /// a rebuild-now condition, never "stale but searchable".
    pub hollow: bool,
    /// Live documents in the published lexical generation, from manifest
    /// metadata alone (`None` when no manifest decodes or the generation
    /// predates the Quill engine).
    pub live_docs: Option<u64>,
    /// The MANIFEST slot `live_docs` was read from (mknmt): anything but
    /// `Current` means readers serve an older generation than the newest
    /// publish. Same `None` cases as `live_docs`.
    pub manifest_source: Option<crate::search::quill_bridge::QuillManifestSource>,
    pub status_reason: Option<String>,
    pub fingerprint: LexicalFingerprintState,
    pub checkpoint: LexicalCheckpointState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexicalFingerprintState {
    pub current_db_fingerprint: Option<String>,
    pub checkpoint_fingerprint: Option<String>,
    pub matches_current_db_fingerprint: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexicalCheckpointState {
    pub present: bool,
    pub completed: Option<bool>,
    pub db_matches: Option<bool>,
    pub schema_matches: Option<bool>,
    pub page_size_matches: Option<bool>,
    pub page_size_compatible: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticAssetState {
    pub status: &'static str,
    pub availability: &'static str,
    pub summary: String,
    pub available: bool,
    pub can_search: bool,
    pub fallback_mode: Option<&'static str>,
    pub preferred_backend: &'static str,
    pub embedder_id: Option<String>,
    pub vector_index_path: Option<PathBuf>,
    pub model_dir: Option<PathBuf>,
    pub hnsw_path: Option<PathBuf>,
    /// Presence is not native admission. Bounded inspection never loads a graph.
    pub hnsw_present: bool,
    pub hnsw_state: &'static str,
    pub hnsw_ready: bool,
    pub progressive_ready: bool,
    pub progressive_reason_code: Option<&'static str>,
    /// Sub-fix 3 for cass#257: true when a quality-tier vector index is
    /// published, matches the current DB fingerprint, and could serve a
    /// `--mode semantic` search even if the progressive/hybrid stack is
    /// still building (e.g. the fast tier hasn't been backfilled yet).
    ///
    /// Distinct from `progressive_ready`, which only returns true when
    /// BOTH the fast and quality tier index files exist on disk —
    /// useful for the "hybrid stack is good to go" surface but
    /// misleading for operators who only run `--mode semantic`.
    pub quality_tier_published: bool,
    /// Sub-fix 3 for cass#257: true when at least one tier (fast OR
    /// quality) is queryable against the current DB. This collapses
    /// the per-tier readiness into a single flag suitable for the
    /// operator question "can I run `cass search --mode semantic`
    /// right now?". Mirrors `can_search` but is named so it survives
    /// future refactors of the can_search semantics.
    pub semantic_only_search_available: bool,
    pub hint: Option<String>,
    pub fast_tier: SemanticTierAssetState,
    pub quality_tier: SemanticTierAssetState,
    pub backlog: SemanticBacklogProgressState,
    pub checkpoint: SemanticCheckpointProgressState,
}

pub(super) struct SemanticRuntimeSurface {
    pub(super) status: &'static str,
    pub(super) availability: &'static str,
    pub(super) summary: String,
    pub(super) can_search: bool,
    pub(super) fallback_mode: Option<&'static str>,
    pub(super) hint: Option<String>,
    pub(super) embedder_id: Option<String>,
    pub(super) vector_index_path: Option<PathBuf>,
    pub(super) model_dir: Option<PathBuf>,
    pub(super) hnsw_path: Option<PathBuf>,
}

pub(super) struct SemanticRuntimeInputs<'a> {
    pub(super) data_dir: &'a Path,
    pub(super) availability: &'a SemanticAvailability,
    pub(super) preference: SemanticPreference,
    pub(super) fast_tier: &'a SemanticTierAssetState,
    pub(super) quality_tier: &'a SemanticTierAssetState,
    pub(super) backlog: &'a SemanticBacklogProgressState,
    pub(super) checkpoint: &'a SemanticCheckpointProgressState,
    pub(super) base_embedder_id: Option<String>,
    pub(super) base_vector_index_path: Option<PathBuf>,
    pub(super) base_model_dir: Option<PathBuf>,
    pub(super) base_hnsw_path: Option<PathBuf>,
}

pub(super) struct SemanticPreferenceSurface {
    pub(super) preferred_backend: &'static str,
    pub(super) model_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SemanticTierAssetState {
    pub present: bool,
    pub ready: bool,
    pub current_db_matches: Option<bool>,
    pub conversation_count: Option<u64>,
    pub doc_count: Option<u64>,
    pub embedder_id: Option<String>,
    pub model_revision: Option<String>,
    pub completed_at_ms: Option<i64>,
    pub size_bytes: Option<u64>,
    pub index_path: Option<PathBuf>,
    pub shard_count: Option<u32>,
    /// True when the recorded artifact belongs to the removed external HTTP
    /// embedding provider (an `external-v1-*` identity). The artifact is
    /// reported for provenance only — it is never loaded or served.
    pub legacy_external_space: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SemanticBacklogProgressState {
    pub total_conversations: u64,
    pub fast_tier_processed: u64,
    pub fast_tier_remaining: u64,
    pub quality_tier_processed: u64,
    pub quality_tier_remaining: u64,
    pub pending_work: bool,
    pub current_db_matches: Option<bool>,
    pub computed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SemanticCheckpointProgressState {
    pub active: bool,
    pub tier: Option<&'static str>,
    pub current_db_matches: Option<bool>,
    pub completed: Option<bool>,
    pub conversations_processed: Option<u64>,
    pub total_conversations: Option<u64>,
    pub progress_pct: Option<u8>,
    pub docs_embedded: Option<u64>,
    pub last_offset: Option<i64>,
    pub saved_at_ms: Option<i64>,
}

pub struct InspectSearchAssetsInput<'a> {
    pub data_dir: &'a Path,
    pub db_path: &'a Path,
    pub stale_threshold: u64,
    pub last_indexed_at_ms: Option<i64>,
    /// Full-precision (millisecond) wall clock used for stall-detection
    /// math against `last_progress_at_ms`. Callers should pass
    /// `FrankenStorage::now_millis()` here. F4 (cass tech debt): the
    /// previous shape only carried `now_secs`, which quantised the
    /// comparison to second resolution while `last_progress_at_ms` is
    /// stored at full ms. Down-stream `now_secs` is now derived from
    /// this value so the two clocks remain consistent.
    pub now_ms: i64,
    pub maintenance: SearchMaintenanceSnapshot,
    pub semantic_preference: SemanticPreference,
    pub db_available: bool,
    pub compute_lexical_fingerprint: bool,
    pub inspect_semantic: bool,
}
