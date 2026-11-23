use std::path::Path;

use anyhow::{Context, Result};

use crate::indexer::{
    LEXICAL_REBUILD_PAGE_SIZE_PUBLIC, LexicalRebuildCheckpoint,
    lexical_rebuild_page_size_is_compatible, lexical_storage_fingerprint_for_db,
    load_lexical_rebuild_checkpoint,
};
use crate::search::tantivy::SCHEMA_HASH;

use super::maintenance::{
    SearchMaintenanceMode, SearchMaintenanceSnapshot, maintenance_stall_age_ms,
};
use super::types::{LexicalAssetState, LexicalCheckpointState, LexicalFingerprintState};

const LEXICAL_STORAGE_FINGERPRINT_MTIME_TOLERANCE_MS: i64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParsedLexicalStorageFingerprint {
    db_len: u64,
    db_mtime_ms: i64,
    wal_len: u64,
    wal_mtime_ms: i64,
}

fn parse_lexical_storage_fingerprint(raw: &str) -> Option<ParsedLexicalStorageFingerprint> {
    let mut parts = raw.split(':');
    let fingerprint = ParsedLexicalStorageFingerprint {
        db_len: parts.next()?.parse().ok()?,
        db_mtime_ms: parts.next()?.parse().ok()?,
        wal_len: parts.next()?.parse().ok()?,
        wal_mtime_ms: parts.next()?.parse().ok()?,
    };
    if parts.next().is_some() {
        return None;
    }
    Some(fingerprint)
}

pub fn lexical_storage_fingerprints_match(current: &str, saved: &str) -> bool {
    match (
        parse_lexical_storage_fingerprint(current),
        parse_lexical_storage_fingerprint(saved),
    ) {
        (Some(current), Some(saved)) => {
            current.db_len == saved.db_len
                && current.wal_len == saved.wal_len
                && current.db_mtime_ms.abs_diff(saved.db_mtime_ms)
                    <= u64::try_from(LEXICAL_STORAGE_FINGERPRINT_MTIME_TOLERANCE_MS)
                        .unwrap_or(u64::MAX)
                && current.wal_mtime_ms.abs_diff(saved.wal_mtime_ms)
                    <= u64::try_from(LEXICAL_STORAGE_FINGERPRINT_MTIME_TOLERANCE_MS)
                        .unwrap_or(u64::MAX)
        }
        _ => current == saved,
    }
}

pub(super) struct InspectLexicalAssetsInput<'a> {
    pub(super) data_dir: &'a Path,
    pub(super) db_path: &'a Path,
    pub(super) stale_threshold: u64,
    pub(super) last_indexed_at_ms: Option<i64>,
    /// F4 (cass tech debt): full-precision wall clock; the legacy
    /// `now_secs` field was widening the second-precision comparison
    /// against ms-precision `last_progress_at_ms`. Derive `now_secs`
    /// locally from this when needed for age math.
    pub(super) now_ms: i64,
    pub(super) maintenance: SearchMaintenanceSnapshot,
    pub(super) db_available: bool,
    pub(super) compute_lexical_fingerprint: bool,
}

pub(super) fn inspect_lexical_assets(
    input: InspectLexicalAssetsInput<'_>,
) -> Result<LexicalAssetState> {
    let InspectLexicalAssetsInput {
        data_dir,
        db_path,
        stale_threshold,
        last_indexed_at_ms,
        now_ms,
        maintenance,
        db_available,
        compute_lexical_fingerprint,
    } = input;
    let index_path = crate::search::tantivy::expected_index_dir(data_dir);
    let checkpoint = load_lexical_rebuild_checkpoint(&index_path)
        .with_context(|| format!("loading lexical checkpoint from {}", index_path.display()))?;
    let current_db_fingerprint = if db_available && compute_lexical_fingerprint {
        let fingerprint = lexical_storage_fingerprint_for_db(db_path).with_context(|| {
            format!(
                "computing lexical storage fingerprint for {}",
                db_path.display()
            )
        })?;
        crate::indexer::write_archive_fingerprint_cache_best_effort(
            db_path,
            &index_path,
            &fingerprint,
        );
        Some(fingerprint)
    } else if db_available {
        // GH #353: the skip-open surfaces (health watermark lane, count-less
        // status) deliberately never open the archive, but they must still
        // compare the storage fingerprint `search` defers repair on instead
        // of leaving `matches_current_db_fingerprint` null while reporting
        // fresh. Serve the identity-keyed sidecar fingerprint — never an
        // open — and stay honestly null when the sidecar does not describe
        // the current archive identity.
        crate::indexer::lexical_storage_fingerprint_for_db_cached_readonly(db_path, &index_path)
    } else {
        None
    };

    Ok(lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path,
        stale_threshold,
        last_indexed_at_ms,
        now_ms,
        maintenance,
        checkpoint: checkpoint.as_ref(),
        current_db_fingerprint: current_db_fingerprint.as_deref(),
        live: crate::search::tantivy::searchable_index_live_docs(&index_path),
    }))
}

pub(super) struct LexicalObservationInput<'a> {
    pub(super) index_path: &'a Path,
    pub(super) db_path: &'a Path,
    pub(super) stale_threshold: u64,
    pub(super) last_indexed_at_ms: Option<i64>,
    /// Full-precision wall clock (F4); see [`super::InspectSearchAssetsInput::now_ms`].
    pub(super) now_ms: i64,
    pub(super) maintenance: SearchMaintenanceSnapshot,
    pub(super) checkpoint: Option<&'a LexicalRebuildCheckpoint>,
    pub(super) current_db_fingerprint: Option<&'a str>,
    /// Live documents the published generation serves and the MANIFEST slot
    /// they were read from, from manifest metadata alone (GH #457, mknmt);
    /// `None` when no manifest decodes.
    pub(super) live: Option<crate::search::tantivy::SearchableLiveDocs>,
}

/// GH #457: floor, as a percentage of the completed checkpoint's
/// `indexed_docs`, below which the published generation is HOLLOW.
///
/// Every rebuild proves `live == indexed_docs` before it certifies its
/// checkpoint, incremental ingest only adds documents, and an upsert replaces
/// one live document with one live document, so the served count never
/// legitimately drops below what the checkpoint certified; `cass forget`
/// rewrites the checkpoint along with the derived assets. Half is a generous
/// margin against any bookkeeping skew while still catching the reported
/// shape (3 live documents against ~1M certified) by orders of magnitude.
pub const LEXICAL_HOLLOW_LIVE_DOC_FLOOR_PERCENT: u64 = 50;

/// GH #457: a published lexical generation that serves far fewer documents
/// than its completed rebuild checkpoint certified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LexicalHollowVerdict {
    /// `indexed_docs` recorded by the completed checkpoint for this DB.
    pub expected_docs: u64,
    /// Documents the published generation currently serves.
    pub live_docs: u64,
}

impl LexicalHollowVerdict {
    /// Share of the certified documents the generation no longer serves.
    #[must_use]
    pub fn missing_percent(&self) -> u64 {
        if self.expected_docs == 0 {
            return 0;
        }
        let missing = u128::from(self.expected_docs.saturating_sub(self.live_docs));
        u64::try_from(missing * 100 / u128::from(self.expected_docs)).unwrap_or(100)
    }

    /// Operator-facing reason naming the gap and the only remedy that works.
    #[must_use]
    pub fn reason(&self) -> String {
        format!(
            "lexical index is HOLLOW: the published Quill generation serves {live} live document(s) \
             but the completed rebuild checkpoint certified {expected} indexed document(s) \
             ({missing}% missing); search silently answers from a near-empty index. \
             Run `cass index`: its pre-scan check rebuilds the lexical index from the canonical \
             database when the live index is sparse (`cass index --full` does the same after a \
             full rescan)",
            live = self.live_docs,
            expected = self.expected_docs,
            missing = self.missing_percent(),
        )
    }
}

/// GH #457: compare what the published generation serves against what the
/// completed checkpoint certified. `expected_docs` must already be gated on
/// a completed checkpoint for the current DB and lexical contract (a
/// mismatched or incomplete checkpoint is reported by its own signal);
/// `None` on either side means "no verdict", never "hollow".
#[must_use]
pub fn lexical_generation_hollow_verdict(
    expected_docs: Option<u64>,
    live_docs: Option<u64>,
) -> Option<LexicalHollowVerdict> {
    let expected_docs = expected_docs.filter(|&docs| docs > 0)?;
    let live_docs = live_docs?;
    // u128 so the comparison is exact at every scale (saturating u64 math
    // would collapse the ratio near the top of the range).
    let hollow = u128::from(live_docs) * 100
        < u128::from(expected_docs) * u128::from(LEXICAL_HOLLOW_LIVE_DOC_FLOOR_PERCENT);
    hollow.then_some(LexicalHollowVerdict {
        expected_docs,
        live_docs,
    })
}

/// `indexed_docs` certified by a COMPLETED lexical rebuild checkpoint that
/// describes `db_path` under the current lexical contract; `None` when no
/// such checkpoint exists (nothing certified means nothing to be hollow
/// against). Shared by readiness, `doctor` and the post-run indexer guard so
/// every surface applies the same gate (GH #457).
pub fn completed_lexical_checkpoint_indexed_docs(index_path: &Path, db_path: &Path) -> Option<u64> {
    let checkpoint = load_lexical_rebuild_checkpoint(index_path).ok().flatten()?;
    let certified = checkpoint.completed
        && crate::stored_path_identity_matches(&checkpoint.db_path, db_path)
        && checkpoint.schema_hash == SCHEMA_HASH
        && lexical_rebuild_page_size_is_compatible(checkpoint.page_size);
    certified.then_some(checkpoint.indexed_docs as u64)
}

/// b4uax: the exact operator contract for a legacy Tantivy generation. Names
/// the command and the expected cost so `status`/`triage` never leave an
/// established archive guessing after the engine flip.
pub const LEGACY_ENGINE_STATUS_REASON: &str = "lexical index was built by the legacy Tantivy engine and the current Quill engine cannot read it; search is unavailable until `cass index --full` completes a one-time full lexical rebuild (cost scales with message count: minutes on small archives, an hour-scale run on multi-million-message archives; `--force-rebuild` skips the filesystem rescan)";

/// mknmt: a corrupt current MANIFEST is read through `MANIFEST.prev`, so
/// search keeps answering from the generation before the newest publish.
pub const CORRUPT_MANIFEST_STATUS_REASON: &str = "the lexical index's current MANIFEST is corrupt, so search serves the previous generation (MANIFEST.prev), which lacks the newest publish; `cass index` rebuilds the generation from the canonical database";

pub(super) fn lexical_state_from_observations(
    input: LexicalObservationInput<'_>,
) -> LexicalAssetState {
    let LexicalObservationInput {
        index_path,
        db_path,
        stale_threshold,
        last_indexed_at_ms,
        now_ms,
        maintenance,
        checkpoint,
        current_db_fingerprint,
        live,
    } = input;
    let live_docs = live.map(|live| live.live_docs);
    let manifest_source = live.map(|live| live.manifest_source);
    let manifest_corrupt = manifest_source
        == Some(crate::search::quill_bridge::QuillManifestSource::PreviousAfterCorruptCurrent);
    let exists = crate::search::tantivy::searchable_index_exists(index_path);
    // b4uax: a Tantivy-era `meta.json` without a Quill `MANIFEST` (or a
    // federated manifest) is an index generation the current engine cannot
    // open. `exists` stays true; the state must say "rebuild required", not
    // "ready".
    let engine_incompatible = exists
        && !index_path
            .join(crate::search::quill_bridge::QUILL_INDEX_MARKER)
            .exists()
        && !crate::search::tantivy::federated_search_manifest_path(index_path).exists()
        && index_path.join("meta.json").exists();
    let checkpoint_db_matches =
        checkpoint.map(|state| crate::stored_path_identity_matches(&state.db_path, db_path));
    let schema_matches = checkpoint.map(|state| state.schema_hash == SCHEMA_HASH);
    let page_size_matches =
        checkpoint.map(|state| state.page_size == LEXICAL_REBUILD_PAGE_SIZE_PUBLIC);
    let page_size_compatible =
        checkpoint.map(|state| lexical_rebuild_page_size_is_compatible(state.page_size));
    let checkpoint_fingerprint = checkpoint.map(|state| state.storage_fingerprint.as_str());
    let fingerprint_matches = match (current_db_fingerprint, checkpoint_fingerprint) {
        (Some(current), Some(saved)) => Some(lexical_storage_fingerprints_match(current, saved)),
        _ => None,
    };
    let checkpoint_incomplete = checkpoint.is_some_and(|state| !state.completed);
    let checkpoint_db_mismatch = checkpoint_db_matches == Some(false);
    let contract_mismatch = schema_matches == Some(false) || page_size_compatible == Some(false);
    let fingerprint_mismatch = fingerprint_matches == Some(false);
    // GH #457: only a checkpoint that otherwise certifies this generation for
    // this DB can be contradicted by the served count; an incomplete, foreign
    // or contract-mismatched checkpoint already carries its own verdict.
    let certified_docs = checkpoint
        .filter(|state| {
            state.completed
                && checkpoint_db_matches == Some(true)
                && schema_matches == Some(true)
                && page_size_compatible == Some(true)
        })
        .map(|state| state.indexed_docs as u64);
    let hollow_verdict = lexical_generation_hollow_verdict(certified_docs, live_docs);
    let hollow = hollow_verdict.is_some();
    // F4 (cass tech debt): derive the (legacy) second-resolution clock
    // from `now_ms` rather than the other way around so the comparison
    // against ms-precision `last_progress_at_ms` below is no longer
    // forced into second-bin alignment. `as u64` is correct because
    // wall-clock millis fits well inside i63 (until year ~292477).
    let now_secs: u64 = now_ms.div_euclid(1000).max(0) as u64;
    let age_seconds = last_indexed_at_ms
        .and_then(|ts| (ts > 0).then(|| now_secs.saturating_sub((ts / 1000) as u64)));
    let age_exceeds_threshold = match age_seconds {
        Some(age) => age > stale_threshold,
        None => true,
    };
    // GH #452: a checkpoint fingerprint that still matches the live database
    // means no conversation and no message has been added since the last
    // successful index (`content-v1:<conversations>:<max_conversation_id>:
    // <max_message_id>`), so the index is not out of date — only old. Age alone
    // must not mark it stale and send every ordinary read into a refresh.
    //
    // Only genuine age is covered: an index with NO recorded `last_indexed_at`
    // is unproven rather than old, and stays stale. Every other staleness
    // signal (contract, engine, checkpoint, fingerprint mismatch) is untouched.
    let age_stale_covered_by_fingerprint =
        age_seconds.is_some() && fingerprint_matches == Some(true);
    let age_stale = age_exceeds_threshold && !age_stale_covered_by_fingerprint;
    let maintenance_targets_current_db = maintenance
        .db_path
        .as_ref()
        .is_none_or(|lock_db_path| crate::path_identities_match(lock_db_path, db_path));
    let watch_active = maintenance.active
        && maintenance_targets_current_db
        && maintenance
            .mode
            .is_some_and(SearchMaintenanceMode::watch_active);
    let rebuilding = maintenance.active
        && maintenance_targets_current_db
        && maintenance
            .mode
            .is_some_and(SearchMaintenanceMode::rebuild_active);
    let active_rebuild_progress = rebuilding;
    // Forward-progress liveness check (issue #258): when a rebuild is
    // active but the indexing thread has not posted progress within
    // `CASS_REBUILD_STALL_DETECT_SECS` (default 120 s), report
    // `stalled` rather than `rebuilding`. The lock file's
    // `updated_at_ms` is heartbeat-refreshed every ~1 s by a separate
    // thread, so it cannot be used as a "work is happening" signal —
    // only the indexer-thread-owned `last_progress_at_ms` can.
    //
    // `now_ms` is now passed in at full ms precision (F4); the old
    // shape derived it from `now_secs` and quantised the comparison.
    let stall_age_ms = if rebuilding && maintenance_targets_current_db {
        maintenance_stall_age_ms(&maintenance, now_ms)
    } else {
        None
    };
    let stalled = stall_age_ms.is_some();
    let last_progress_at_ms = maintenance
        .last_progress_at_ms
        .filter(|_| maintenance_targets_current_db);
    let last_progress_age_ms = last_progress_at_ms
        .filter(|_| rebuilding)
        .map(|ts| now_ms.saturating_sub(ts));
    let stale = if rebuilding {
        // A stalled rebuild leaves the on-disk index unchanged; if it
        // existed before the stall it is still searchable, so treat
        // `stalled` like the indexer just hadn't gotten there yet.
        !exists || contract_mismatch || engine_incompatible
    } else {
        exists
            && (engine_incompatible
                || age_stale
                || checkpoint_db_mismatch
                || checkpoint_incomplete
                || contract_mismatch
                || fingerprint_mismatch
                || hollow
                || manifest_corrupt)
    };
    let fresh = exists && !stale && !rebuilding;
    let status = if stalled {
        "stalled"
    } else if rebuilding {
        "building"
    } else if !exists {
        "missing"
    } else if engine_incompatible {
        "legacy_engine"
    } else if hollow {
        "hollow"
    } else if stale {
        "stale"
    } else {
        "ready"
    };
    let status_reason = if stalled {
        let secs = stall_age_ms.unwrap_or(0) / 1000;
        Some(format!(
            "indexing thread has not posted forward progress for {secs}s while the lock heartbeat keeps refreshing — see issue #258 for diagnostics (run `cass doctor check --json` and capture a stack trace)"
        ))
    } else if rebuilding {
        Some("lexical rebuild is in progress".to_string())
    } else if !exists {
        Some("lexical index metadata missing".to_string())
    } else if engine_incompatible {
        Some(LEGACY_ENGINE_STATUS_REASON.to_string())
    } else if let Some(verdict) = hollow_verdict {
        Some(verdict.reason())
    } else if manifest_corrupt {
        Some(CORRUPT_MANIFEST_STATUS_REASON.to_string())
    } else if checkpoint_db_mismatch {
        Some("lexical rebuild checkpoint points at a different database".to_string())
    } else if contract_mismatch {
        Some("lexical rebuild checkpoint no longer matches the active lexical contract".to_string())
    } else if fingerprint_mismatch {
        Some("database fingerprint changed since the last lexical checkpoint".to_string())
    } else if checkpoint_incomplete {
        Some("lexical rebuild checkpoint is incomplete".to_string())
    } else if age_stale {
        Some("lexical index is older than the stale threshold".to_string())
    } else {
        None
    };
    let checkpoint_progress_usable = checkpoint.is_some()
        && checkpoint_db_matches == Some(true)
        && schema_matches == Some(true)
        && page_size_compatible == Some(true)
        && if active_rebuild_progress {
            true
        } else {
            current_db_fingerprint.is_some() && fingerprint_matches == Some(true)
        };
    let pending_sessions = checkpoint
        .filter(|_| checkpoint_progress_usable)
        .map(|state| {
            state
                .total_conversations
                .saturating_sub(state.processed_conversations) as u64
        })
        .unwrap_or(0);
    let maintenance_activity_at_ms = maintenance_targets_current_db
        .then_some(())
        .and(maintenance.updated_at_ms.or(maintenance.started_at_ms));
    let checkpoint_activity_at_ms = checkpoint
        .filter(|_| checkpoint_progress_usable)
        .and_then(|state| (state.updated_at_ms > 0).then_some(state.updated_at_ms));
    let activity_at_ms = match (checkpoint_activity_at_ms, maintenance_activity_at_ms) {
        (Some(checkpoint_ts), Some(maintenance_ts)) => Some(checkpoint_ts.max(maintenance_ts)),
        (Some(checkpoint_ts), None) => Some(checkpoint_ts),
        (None, Some(maintenance_ts)) => Some(maintenance_ts),
        (None, None) => None,
    };

    LexicalAssetState {
        status,
        exists,
        engine_incompatible,
        fresh,
        stale,
        rebuilding,
        stalled,
        last_progress_age_ms,
        last_progress_at_ms,
        watch_active,
        last_indexed_at_ms,
        age_seconds,
        stale_threshold_seconds: stale_threshold,
        activity_at_ms,
        pending_sessions,
        processed_conversations: checkpoint
            .filter(|_| checkpoint_progress_usable)
            .map(|state| state.processed_conversations as u64),
        total_conversations: checkpoint
            .filter(|_| checkpoint_progress_usable)
            .map(|state| state.total_conversations as u64),
        indexed_docs: checkpoint
            .filter(|_| checkpoint_progress_usable)
            .map(|state| state.indexed_docs as u64),
        hollow,
        live_docs,
        manifest_source,
        status_reason,
        fingerprint: LexicalFingerprintState {
            current_db_fingerprint: current_db_fingerprint.map(ToOwned::to_owned),
            checkpoint_fingerprint: checkpoint.map(|state| state.storage_fingerprint.clone()),
            matches_current_db_fingerprint: fingerprint_matches,
        },
        checkpoint: LexicalCheckpointState {
            present: checkpoint.is_some(),
            completed: checkpoint.map(|state| state.completed),
            db_matches: checkpoint_db_matches,
            schema_matches,
            page_size_matches,
            page_size_compatible,
        },
    }
}
