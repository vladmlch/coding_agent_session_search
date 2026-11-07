use super::probe_state_db_strict_worker::{
    format_timestamp_millis_rfc3339, normalize_path_identity, probe_index_run_lock, probe_state_db,
    probe_state_db_modes,
};
use super::semantic_readiness_from_state::semantic_readiness_from_state;
use super::state_db_open_timeout::{
    STATE_DB_OPEN_TIMEOUT, STATUS_COUNT_SCAN_MAX_DB_BYTES, StateDbIntegrity, StateDbSnapshot,
};
use crate::cli::contract_version::DEFAULT_STALE_THRESHOLD_SECS;
use crate::cli::output::cli_error_json_payload::cass_not_initialized;
use std::path::Path;
use std::time::Duration;

pub(crate) fn state_meta_json(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
    allow_db_open: bool,
) -> serde_json::Value {
    state_meta_json_with_counts(data_dir, db_path, stale_threshold, allow_db_open, None)
}

/// `coding_agent_session_search-gi4oy` follow-up to d0rmo: the
/// COUNT(*) skip got cass health from p50=296ms to p50=132ms, but
/// the documented `<50ms` budget still demanded another ~80ms cut.
/// Profiling pointed at the second cold-path cost: opening the
/// FrankenStorage canonical DB twice per probe (once via
/// probe_state_db for `last_indexed_at`, once via
/// inspect_semantic_assets when a model is installed). Health's
/// readiness verdict only needs `db_exists` — corrupt-DB detection
/// is `cass doctor`'s job, not the fast-surface probe's.
///
/// `skip_db_open`: when true, probe_state_db is replaced with a
/// pure-fs StateDbSnapshot that reports `opened=true` if the file
/// exists (assumed-good) and surfaces the new `opened_skipped=true`
/// flag in the database envelope so callers know the open was
/// elided. status / diag pass false (they explicitly want the
/// open-success signal as a degraded-state diagnostic).
pub(crate) fn state_meta_json_with_counts(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
    allow_db_open: bool,
    include_counts_override: Option<bool>,
) -> serde_json::Value {
    state_meta_json_full(
        data_dir,
        db_path,
        stale_threshold,
        allow_db_open,
        include_counts_override,
        false,
        allow_db_open,
        false,
    )
}

/// `coding_agent_session_search-gi4oy`: most-general state_meta_json
/// variant exposing both the include_counts override (d0rmo) AND
/// the skip_db_open elision (gi4oy). Callers that already use
/// state_meta_json_with_counts keep working unchanged; new callers
/// (currently just run_health) opt in via state_meta_json_for_health.
#[allow(clippy::too_many_arguments)]
pub(crate) fn state_meta_json_full(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
    allow_db_open: bool,
    include_counts_override: Option<bool>,
    skip_db_open: bool,
    inspect_semantic: bool,
    passive_db_probe: bool,
) -> serde_json::Value {
    state_meta_json_inner(
        data_dir,
        db_path,
        stale_threshold,
        allow_db_open,
        include_counts_override,
        skip_db_open,
        inspect_semantic,
        passive_db_probe,
    )
}

/// Convenience wrapper for run_health: skip COUNT(*) AND skip the
/// DB open. Keep semantic asset probing enabled: it is filesystem-only
/// on this path, and health/status JSON must agree on realized fallback
/// state instead of reporting an unhelpful `not_inspected` placeholder.
pub(crate) fn state_meta_json_for_health(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
) -> serde_json::Value {
    state_meta_json_full(
        data_dir,
        db_path,
        stale_threshold,
        true,
        Some(false),
        true,
        true,
        false,
    )
}

pub(crate) fn state_meta_json_for_status(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
) -> serde_json::Value {
    state_meta_json_full(
        data_dir,
        db_path,
        stale_threshold,
        true,
        Some(false),
        false,
        true,
        false,
    )
}

/// Filesystem-only state metadata for `search --no-maintenance`. The database
/// is reported as deliberately unprobed; lexical checkpoint/coordination files
/// are inspected, while the state surface skips semantic DB fingerprint/filter
/// probes. This avoids the ordinary read opener's sanctioned dirty-WAL/schema
/// recovery writes and avoids scratch-copy diagnostics.
pub(crate) fn state_meta_json_for_strict_read(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
) -> serde_json::Value {
    state_meta_json_full(
        data_dir,
        db_path,
        stale_threshold,
        false,
        Some(false),
        false,
        false,
        true,
    )
}

/// Project the `.14.1` storage-integrity block onto a lightweight readiness
/// surface (`cass status --json`, `cass search --robot-meta`) from the
/// `state_meta` JSON it already built plus a cheap `db_path.exists()` check, so
/// every truth surface speaks the same `StorageState` vocabulary the doctor
/// does (bead qfswx, follow-on to vl1cj's doctor wiring). Returns the report as
/// a JSON value (`null` only if serialization somehow fails).
///
/// The shared db-open/index signals derive the coarse readiness state first;
/// bounded dedicated probes then refine typed contention, on-disk schema drift,
/// legacy layouts, and structurally suspect WAL/SHM sidecars. Generic
/// `CliError.retryable` is never used to infer busy/locked.
pub(crate) fn storage_integrity_value_from_state(
    data_dir: &Path,
    db_path: &Path,
    state: &serde_json::Value,
    not_initialized: bool,
) -> serde_json::Value {
    use crate::search::storage_integrity::{
        DoctorStorageSignals, apply_dedicated_storage_probe, build_readiness_storage_integrity,
        load_matching_integrity_attestation, probe_dedicated_storage_state,
    };
    let db_file_present = db_path.exists();
    let db_opened = state
        .pointer("/database/opened")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let open_skipped = state
        .pointer("/database/open_skipped")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let lexical_index_drifted = state
        .pointer("/index/empty_with_messages")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let signals = DoctorStorageSignals {
        // A deliberately skipped open proves only that a path is present, not
        // that the archive is readable. Feed the readiness classifier no
        // synthetic db-open signal so it emits unknown_deferred/not_checked
        // and records db_open as skipped instead of claiming a check ran.
        db_file_present: db_file_present && !open_skipped,
        not_initialized,
        // A present-but-unopened archive is an openread fault. status/search
        // open the DB for real (skip_db_open=false), so only claim the fault
        // when the open was attempted (never deliberately elided).
        db_open_failed: db_file_present && !db_opened && !open_skipped,
        probe_timed_out: false,
        integrity_failed: false,
        integrity_unverified: false,
        lexical_index_drifted,
    };
    // #331: consult the persisted deep-probe attestation so a successful
    // db_open alone projects `unchecked` (never a synthesized `ok`), while a
    // fingerprint-still-valid doctor/index quick_check verdict projects its
    // real pass/fail outcome with cached provenance.
    let attestation = load_matching_integrity_attestation(data_dir, db_path);
    let mut report = build_readiness_storage_integrity(signals, attestation.as_ref());
    if open_skipped
        && db_file_present
        && let Some(db_open) = report
            .checks_attempted
            .iter_mut()
            .find(|check| check.name == "db_open")
    {
        db_open.skipped_reason = Some("passive no-maintenance probe".to_string());
    }
    if open_skipped {
        return serde_json::to_value(report).unwrap_or(serde_json::Value::Null);
    }
    let dedicated = probe_dedicated_storage_state(
        db_path,
        crate::search::storage_integrity::dedicated_storage_probe_timeout(),
    );
    serde_json::to_value(apply_dedicated_storage_probe(report, dedicated))
        .unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
#[test]
pub(crate) fn readiness_projection_uses_only_fingerprint_matching_integrity_attestations() {
    use crate::search::storage_integrity::{
        IntegrityAttestationVerdict, store_integrity_attestation,
    };

    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"canonical archive bytes").expect("seed archive file");
    let opened_state = serde_json::json!({
        "database": { "opened": true, "open_skipped": false },
        "index": { "empty_with_messages": false }
    });

    let unchecked = storage_integrity_value_from_state(data_dir, &db_path, &opened_state, false);
    assert_eq!(unchecked["storage_state"], "unchecked");
    assert_eq!(unchecked["source_of_truth_risk"], "unknown");
    assert_eq!(unchecked["attestation_source"], "none");
    assert_eq!(unchecked["checks_not_attempted"][0]["name"], "quick_check");

    store_integrity_attestation(
        data_dir,
        &db_path,
        IntegrityAttestationVerdict::Fail,
        "quick_check",
        Some("fixture corruption".to_string()),
    )
    .expect("capture and persist fixture corruption attestation");
    let known_bad = storage_integrity_value_from_state(data_dir, &db_path, &opened_state, false);
    assert_eq!(known_bad["storage_state"], "integrity_failed");
    assert_eq!(known_bad["source_of_truth_risk"], "high");
    assert_eq!(known_bad["attestation_source"], "cached");

    // Any archive mutation invalidates the cached verdict. A successful open
    // without fresh structural evidence returns to the honest unchecked state.
    std::fs::write(&db_path, b"canonical archive bytes changed and longer")
        .expect("mutate archive fingerprint");
    let stale = storage_integrity_value_from_state(data_dir, &db_path, &opened_state, false);
    assert_eq!(stale["storage_state"], "unchecked");
    assert_eq!(stale["attestation_source"], "none");
}

#[cfg(test)]
#[test]
pub(crate) fn gh422_passive_search_metadata_never_claims_a_database_open() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"present but deliberately unprobed")
        .expect("seed passive archive path");
    let passive_state = serde_json::json!({
        "database": { "opened": false, "open_skipped": true },
        "index": { "empty_with_messages": false }
    });

    let report = storage_integrity_value_from_state(data_dir, &db_path, &passive_state, false);
    assert_eq!(report["storage_state"], "unknown_deferred");
    assert_eq!(report["archive_readability"], "not_checked");
    let db_open = report["checks_attempted"]
        .as_array()
        .and_then(|checks| checks.iter().find(|check| check["name"] == "db_open"))
        .expect("passive report must retain an explicit db_open provenance row");
    assert_eq!(
        db_open["skipped_reason"], "passive no-maintenance probe",
        "robot metadata must say the DB open was skipped, never that it ran"
    );
}

/// `coding_agent_session_search-d0rmo`: variant of `state_meta_json`
/// that lets the caller force-skip the COUNT(*) queries on the
/// canonical DB. Pre-fix the size-based heuristic (≤256 MB → run
/// counts) made `cass health --json` ~6× over its <50ms documented
/// budget on real corpora because COUNT(*) on conversations +
/// messages is a full table scan. Health doesn't need exact
/// counts to compute its readiness verdict (DB exists + opened +
/// index fresh + not rebuilding). Status still opens the DB to read cheap
/// meta watermarks, but now uses a bounded no-count/no-fingerprint default;
/// deep count/fingerprint probes belong to doctor/diag, not the routine
/// readiness surface on multi-GB archives.
///
/// `include_counts_override`:
/// - `None`         → existing size-based logic (≤256 MB → counts).
/// - `Some(false)`  → unconditionally skip; envelope reports 0/0
///   with `counts_skipped=true` (already in StateDbSnapshot).
/// - `Some(true)`   → unconditionally run counts (no caller uses
///   this today; reserved for future paths that MUST have totals regardless of cost).
#[allow(clippy::too_many_arguments)]
pub(crate) fn state_meta_json_inner(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
    allow_db_open: bool,
    include_counts_override: Option<bool>,
    skip_db_open: bool,
    inspect_semantic: bool,
    passive_db_probe: bool,
) -> serde_json::Value {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    let db_exists = db_path.exists();
    let index_run = probe_index_run_lock(data_dir, db_path, !passive_db_probe);

    // F4 (cass tech debt): capture the wall clock at full millisecond
    // precision so the stall-detection comparison against
    // `last_progress_at_ms` is no longer second-quantised inside
    // `asset_state::lexical_state_from_observations`. `now_secs` is
    // retained as a convenience for the existing age-in-seconds math
    // (the public status JSON surfaces `age_seconds`, not `age_ms`).
    let now_ms_i64: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    let now_secs = (now_ms_i64.max(0) as u64) / 1000;

    let db_metadata = fs::metadata(db_path).ok();
    let db_size_bytes = db_metadata.as_ref().map(|m| m.len());
    // WS-B.4a: the WAL sidecar is the archive's hidden cost — every opener
    // replays it, and an untruncated multi-hundred-MB WAL was invisible from
    // every observation surface until now. Read it from metadata only; the
    // observation surfaces never open or checkpoint anything here.
    let db_wal_bytes = fs::metadata(crate::storage::sqlite::database_sidecar_path(
        db_path, "-wal",
    ))
    .ok()
    .filter(|m| m.is_file())
    .map_or(0, |m| m.len());
    let db_shm_present = fs::metadata(crate::storage::sqlite::database_sidecar_path(
        db_path, "-shm",
    ))
    .ok()
    .is_some_and(|m| m.is_file());
    // [session-review-gi4oy] Capture is_file ALONGSIDE size from the
    // single metadata call — the gi4oy skip-open path cannot trust
    // db_path.exists() alone because exists() returns true for both
    // regular files AND directories. The corrupt-DB regression test
    // tests/cli_robot.rs:2725::health_json_reports_open_error_for_unopenable_db_path
    // creates a DIRECTORY at the DB path; without this guard, the
    // skip-open optimistic branch would report opened=true for a
    // directory and the contract regression would ship silently.
    let db_is_regular_file = db_metadata.as_ref().is_some_and(|m| m.is_file());
    let include_counts = include_counts_override.unwrap_or_else(|| {
        db_size_bytes
            .map(|size| size <= STATUS_COUNT_SCAN_MAX_DB_BYTES)
            .unwrap_or(false)
    });
    // [coding_agent_session_search-gi4oy + session-review-gi4oy]
    // Skip-open path: when skip_db_open is true AND the DB path is
    // a regular file (not a directory, not a broken symlink),
    // synthesize a StateDbSnapshot that reports opened=true
    // (assumed-good) + open_skipped=true. The expensive
    // FrankenStorage open is elided; callers reading the JSON
    // envelope branch on the new flag if they need to know the open
    // was assumed.
    //
    // If the DB path exists but is NOT a regular file (the corrupt-DB
    // detection scenario), fall through to the actual probe_state_db
    // — surfacing the open failure is more important than the perf
    // win in that case. probe_state_db will return opened=false +
    // open_error=Some(...) which downstream callers (run_health)
    // turn into a degraded status.
    //
    // counts_skipped MUST be true here regardless of `include_counts`
    // — we never opened the DB so we never ran COUNT(*); the snapshot
    // counts come from `..default()` (i.e. zero). Reporting
    // counts_skipped=false alongside message_count=0 would be a lie.
    let db_snapshot = if passive_db_probe && db_exists && db_is_regular_file {
        StateDbSnapshot {
            // Per the gi4oy contract above: opened=true (ASSUMED-good) +
            // open_skipped=true. Leaving `opened` at its false default made
            // the zero-touch doctor lane claim `database_unavailable` for
            // semantic assets on perfectly healthy archives (the semantic
            // inspection keys `db_available` off this field), regressing
            // doctor's model guidance to a dead end. Callers that need the
            // truth branch on `open_skipped`, exactly as documented.
            opened: true,
            counts_skipped: true,
            open_skipped: true,
            ..StateDbSnapshot::default()
        }
    } else if skip_db_open && db_exists && db_is_regular_file {
        // [coding_agent_session_search #301 truthfulness reconciliation]
        // Previously this branch synthesized a fully-empty snapshot
        // (last_scan_ts=None), which is exactly why `cass health`
        // could report `fresh`/`healthy` while `cass status` reported
        // `stale` on the SAME DB: the scan-ahead-of-projection check
        // below requires `last_scan_ts.is_some()`, and health never
        // read it. Now health does a watermarks-only probe — it opens
        // the DB but reads ONLY the two cheap single-row `meta`
        // watermarks (no COUNT(*) scans), so the staleness inputs are
        // IDENTICAL to status. The snapshot still reports
        // open_skipped=true + counts_skipped=true + zero counts, so the
        // gi4oy/d0rmo health contract (null counts, assumed-good open)
        // is preserved byte-for-byte.
        probe_state_db_modes(
            db_path,
            "state-meta-watermarks",
            STATE_DB_OPEN_TIMEOUT,
            false,
            true,
        )
    } else if allow_db_open && db_exists {
        probe_state_db(db_path, "state-meta", STATE_DB_OPEN_TIMEOUT, include_counts)
    } else {
        StateDbSnapshot::default()
    };
    let conversation_count = db_snapshot.conversation_count;
    let message_count = db_snapshot.message_count;
    let last_scan_ts = db_snapshot.last_scan_ts;
    let mut last_indexed_at = db_snapshot.last_indexed_at;
    let watch_lexical_replay_pending = db_snapshot.watch_lexical_replay_pending;
    let db_opened = db_snapshot.opened;
    let db_open_error = db_snapshot.open_error;
    let db_open_retryable = db_snapshot.open_retryable;
    let counts_skipped = db_snapshot.counts_skipped;
    let open_skipped = db_snapshot.open_skipped;
    // i6upe: distinct corruption signal, separate from staleness. "unknown"
    // covers both "probe not run" (health-contract watermark path, unopened
    // DB) and "probe could not classify" (busy archive).
    let db_integrity = match db_snapshot.integrity {
        Some(StateDbIntegrity::Ok) => "ok",
        Some(StateDbIntegrity::Corrupt) => "corrupt",
        None => "unknown",
    };
    let db_integrity_detail = db_snapshot.integrity_detail.clone();
    let db_integrity_hint = (db_snapshot.integrity == Some(StateDbIntegrity::Corrupt)).then(|| {
        "the archive's stored pages are malformed; do NOT run 'cass index' (it rebuilds on top of \
         the damage) — run 'cass doctor --check' and recover with sqlite3 '.recover' first"
            .to_owned()
    });
    let fallback_fts_repair_pending = db_snapshot.fallback_repair_pending.clone();
    let lexical_repair_deferred_runs = db_snapshot.lexical_repair_deferred_runs;
    let lexical_repair_deferred_reason = db_snapshot.lexical_repair_deferred_reason.clone();

    let index_path = crate::search::tantivy::expected_index_dir(data_dir);
    let lexical_index_initialized = crate::search::tantivy::searchable_index_exists(&index_path);
    if last_indexed_at.is_none() && lexical_index_initialized {
        last_indexed_at = crate::search::tantivy::searchable_index_modified_time(&index_path)
            .and_then(|m| m.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64);
    }
    let status_semantic_policy = crate::search::policy::SemanticPolicy::resolve(
        &crate::search::policy::CliSemanticOverrides::default(),
    );
    let semantic_preference = if status_semantic_policy
        .quality_tier_embedder
        .trim()
        .eq_ignore_ascii_case("hash")
    {
        crate::search::asset_state::SemanticPreference::HashFallback
    } else {
        crate::search::asset_state::SemanticPreference::DefaultModel
    };
    let mut assets = crate::search::asset_state::inspect_search_assets(
        crate::search::asset_state::InspectSearchAssetsInput {
            data_dir,
            db_path,
            stale_threshold,
            last_indexed_at_ms: last_indexed_at,
            now_ms: now_ms_i64,
            maintenance: index_run.clone(),
            semantic_preference,
            db_available: db_opened,
            // GH #368 defect 2: the lexical-fingerprint open is the operation
            // search actually performs, and it is what fails on a corrupt FTS5
            // structure. It was previously gated behind `include_counts` (DB
            // <= 256 MiB), so on large corpora (the 1.17 GB report) the
            // corruption was never probed and readiness reported "usable". The
            // fingerprint is cheap (readonly open + COUNT(conversations), not a
            // message scan), so attempt it on every real open — additively, so
            // tiny-DB and the health skip-open fast path (open_skipped=true)
            // are unchanged.
            compute_lexical_fingerprint: include_counts || (db_opened && !open_skipped),
            inspect_semantic,
        },
    )
    .unwrap_or_else(|err| {
        // `{err:#}` renders the full anyhow chain: the outer context alone
        // ("computing lexical storage fingerprint for ...") hides the root
        // cause (e.g. a corrupt FTS5 structure record), which is the only
        // actionable part of the failure (cass#368).
        let summary = format!("{err:#}");
        crate::search::asset_state::SearchAssetSnapshot {
            lexical: crate::search::asset_state::LexicalAssetState {
                status: "error",
                exists: lexical_index_initialized,
                engine_incompatible: false,
                fresh: false,
                stale: true,
                rebuilding: index_run
                    .mode
                    .is_some_and(crate::search::asset_state::SearchMaintenanceMode::rebuild_active)
                    && index_run.active,
                stalled: false,
                last_progress_age_ms: None,
                last_progress_at_ms: index_run.last_progress_at_ms,
                watch_active: index_run
                    .mode
                    .is_some_and(crate::search::asset_state::SearchMaintenanceMode::watch_active)
                    && index_run.active,
                last_indexed_at_ms: last_indexed_at,
                age_seconds: last_indexed_at
                    .and_then(|ts| (ts > 0).then(|| now_secs.saturating_sub((ts / 1000) as u64))),
                stale_threshold_seconds: stale_threshold,
                activity_at_ms: index_run.updated_at_ms.or(index_run.started_at_ms),
                pending_sessions: 0,
                processed_conversations: None,
                total_conversations: None,
                indexed_docs: None,
                hollow: false,
                live_docs: None,
                manifest_source: None,
                status_reason: Some(format!("asset inspection failed: {summary}")),
                fingerprint: crate::search::asset_state::LexicalFingerprintState {
                    current_db_fingerprint: None,
                    checkpoint_fingerprint: None,
                    matches_current_db_fingerprint: None,
                },
                checkpoint: crate::search::asset_state::LexicalCheckpointState {
                    present: false,
                    completed: None,
                    db_matches: None,
                    schema_matches: None,
                    page_size_matches: None,
                    page_size_compatible: None,
                },
            },
            semantic: crate::search::asset_state::SemanticAssetState {
                status: "error",
                availability: "load_failed",
                summary: format!("asset inspection failed: {summary}"),
                available: false,
                can_search: false,
                fallback_mode: Some("lexical"),
                preferred_backend: "fastembed",
                embedder_id: None,
                vector_index_path: None,
                model_dir: None,
                hnsw_path: None,
                hnsw_present: false,
                hnsw_state: "not_inspected",
                hnsw_ready: false,
                progressive_ready: false,
                progressive_reason_code: None,
                quality_tier_published: false,
                semantic_only_search_available: false,
                hint: Some(
                    "Repair semantic assets when convenient; lexical search remains available."
                        .to_string(),
                ),
                fast_tier: Default::default(),
                quality_tier: Default::default(),
                backlog: Default::default(),
                checkpoint: Default::default(),
            },
        }
    });
    if !assets.lexical.rebuilding
        && last_scan_ts.is_some_and(|scan_ts| {
            last_indexed_at
                .map(|indexed_at| scan_ts > indexed_at.saturating_add(1_000))
                .unwrap_or(true)
        })
    {
        assets.lexical.status = "stale";
        assets.lexical.fresh = false;
        assets.lexical.stale = true;
        assets.lexical.status_reason = Some(
            "last_scan_ts is newer than last_indexed_at; a prior scan advanced without a completed projection into the searchable index"
                .to_string(),
        );
    }
    // A lease heartbeat and a recent timestamp cannot certify publication
    // while source replay remains durable. An unsuccessful requested probe
    // also cannot certify its absence. Passive no-maintenance reads retain
    // their existing contract and expose null instead of pretending to probe.
    let watch_replay_unverified = watch_lexical_replay_pending.is_none()
        && db_exists
        && !passive_db_probe
        && (allow_db_open || skip_db_open);
    if assets.lexical.exists
        && (watch_lexical_replay_pending == Some(true) || watch_replay_unverified)
    {
        assets.lexical.fresh = false;
        assets.lexical.stale = true;
        // Do not replace stronger missing/corrupt/hollow/rebuilding verdicts.
        if matches!(assets.lexical.status, "ready" | "stale") {
            assets.lexical.status = "stale";
            assets.lexical.status_reason = Some(
                if watch_lexical_replay_pending == Some(true) {
                    "durable watch ingestion or lexical publication is pending; watcher activity and last_indexed_at do not establish that new content is searchable"
                } else {
                    "durable watch replay could not be checked; searchable publication is not verified"
                }
                .to_string(),
            );
        }
    }
    let not_initialized = cass_not_initialized(
        db_exists,
        lexical_index_initialized,
        assets.lexical.rebuilding,
    );
    if not_initialized {
        assets.semantic.status = "not_initialized";
        assets.semantic.availability = "not_initialized";
        assets.semantic.summary =
            "semantic search is optional and has not been initialized yet".to_string();
        assets.semantic.available = false;
        assets.semantic.can_search = false;
        assets.semantic.fallback_mode = Some("lexical");
        assets.semantic.embedder_id = None;
        assets.semantic.vector_index_path = None;
        assets.semantic.model_dir = None;
        assets.semantic.hnsw_path = None;
        assets.semantic.hnsw_present = false;
        assets.semantic.hnsw_state = "not_inspected";
        assets.semantic.hnsw_ready = false;
        assets.semantic.progressive_ready = false;
        assets.semantic.progressive_reason_code = None;
        assets.semantic.quality_tier_published = false;
        assets.semantic.semantic_only_search_available = false;
        assets.semantic.hint = Some(
            "Run 'cass index --full' first. Optional later: run 'cass models install --from-file <DIR>' and 'cass index --semantic'."
                .to_string(),
        );
        assets.semantic.fast_tier = Default::default();
        assets.semantic.quality_tier = Default::default();
        assets.semantic.backlog = Default::default();
        assets.semantic.checkpoint = Default::default();
    }
    let lexical = &assets.lexical;
    let semantic = &assets.semantic;
    let lexical_rebuild_pipeline = if skip_db_open {
        crate::indexer::lexical_rebuild_pipeline_settings_snapshot_passive()
    } else {
        crate::indexer::lexical_rebuild_pipeline_settings_snapshot()
    };
    let lexical_rebuild_pipeline_runtime = if lexical.rebuilding {
        crate::indexer::load_active_lexical_rebuild_pipeline_runtime(&index_path, db_path)
            .ok()
            .flatten()
    } else {
        None
    };
    let mut lexical_rebuild_pipeline_json =
        serde_json::to_value(&lexical_rebuild_pipeline).unwrap_or(serde_json::Value::Null);
    if let serde_json::Value::Object(ref mut pipeline) = lexical_rebuild_pipeline_json {
        let controller_loadavg_high_watermark_1m = pipeline
            .remove("controller_loadavg_high_watermark_1m_milli")
            .and_then(|value| value.as_u64())
            .map(|value| value as f64 / 1000.0);
        let controller_loadavg_low_watermark_1m = pipeline
            .remove("controller_loadavg_low_watermark_1m_milli")
            .and_then(|value| value.as_u64())
            .map(|value| value as f64 / 1000.0);
        pipeline.insert(
            "controller_loadavg_high_watermark_1m".to_string(),
            controller_loadavg_high_watermark_1m
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
        );
        pipeline.insert(
            "controller_loadavg_low_watermark_1m".to_string(),
            controller_loadavg_low_watermark_1m
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
        );
        // GH #496: the per-page byte budget page prep fetches against.
        let (startup_page_bytes, steady_page_bytes) =
            crate::indexer::lexical_rebuild_page_fetch_message_bytes(&lexical_rebuild_pipeline);
        pipeline.insert(
            "startup_page_fetch_message_bytes".to_string(),
            serde_json::json!(startup_page_bytes),
        );
        pipeline.insert(
            "steady_page_fetch_message_bytes".to_string(),
            serde_json::json!(steady_page_bytes),
        );
        let runtime_value = lexical_rebuild_pipeline_runtime
            .map(|runtime| {
                let queue_capacity = lexical_rebuild_pipeline.pipeline_channel_size;
                let inflight_message_bytes_limit = if runtime.max_message_bytes_in_flight > 0 {
                    runtime.max_message_bytes_in_flight
                } else {
                    lexical_rebuild_pipeline.pipeline_max_message_bytes_in_flight.max(1)
                };
                serde_json::json!({
                    "queue_depth": runtime.queue_depth,
                    "queue_capacity": queue_capacity,
                    "queue_headroom": queue_capacity.saturating_sub(runtime.queue_depth),
                    "inflight_message_bytes": runtime.inflight_message_bytes,
                    "max_message_bytes_in_flight": inflight_message_bytes_limit,
                    "inflight_message_bytes_headroom": inflight_message_bytes_limit
                        .saturating_sub(runtime.inflight_message_bytes),
                    "pending_batch_conversations": runtime.pending_batch_conversations,
                    "pending_batch_message_bytes": runtime.pending_batch_message_bytes,
                    "page_prep_workers": runtime.page_prep_workers,
                    "active_page_prep_jobs": runtime.active_page_prep_jobs,
                    "ordered_buffered_pages": runtime.ordered_buffered_pages,
                    "budget_generation": runtime.budget_generation,
                    "producer_budget_wait_count": runtime.producer_budget_wait_count,
                    "producer_budget_wait_ms": runtime.producer_budget_wait_ms,
                    "producer_handoff_wait_count": runtime.producer_handoff_wait_count,
                    "producer_handoff_wait_ms": runtime.producer_handoff_wait_ms,
                    "host_loadavg_1m": runtime.host_loadavg_1m_milli.map(|value| f64::from(value) / 1000.0),
                    "controller_mode": if runtime.controller_mode.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(runtime.controller_mode)
                    },
                    "controller_reason": if runtime.controller_reason.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(runtime.controller_reason)
                    },
                    "staged_merge_workers_max": runtime.staged_merge_workers_max,
                    "staged_merge_allowed_jobs": runtime.staged_merge_allowed_jobs,
                    "staged_merge_active_jobs": runtime.staged_merge_active_jobs,
                    "staged_merge_ready_artifacts": runtime.staged_merge_ready_artifacts,
                    "staged_merge_ready_groups": runtime.staged_merge_ready_groups,
                    "staged_merge_controller_reason": if runtime.staged_merge_controller_reason.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(runtime.staged_merge_controller_reason)
                    },
                    "staged_shard_build_workers_max": runtime.staged_shard_build_workers_max,
                    "staged_shard_build_allowed_jobs": runtime.staged_shard_build_allowed_jobs,
                    "staged_shard_build_active_jobs": runtime.staged_shard_build_active_jobs,
                    "staged_shard_build_pending_jobs": runtime.staged_shard_build_pending_jobs,
                    "staged_shard_build_controller_reason": if runtime.staged_shard_build_controller_reason.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(runtime.staged_shard_build_controller_reason)
                    },
                    "updated_at": format_timestamp_millis_rfc3339(runtime.updated_at_ms),
                })
            })
            .unwrap_or(serde_json::Value::Null);
        pipeline.insert("runtime".to_string(), runtime_value);
    }
    let semantic_policy = crate::search::policy::SemanticPolicy::resolve(
        &crate::search::policy::CliSemanticOverrides::default(),
    );
    let policy_registry = serde_json::to_value(crate::policy_registry::policy_registry_snapshot(
        &semantic_policy,
        semantic.available,
        semantic.fallback_mode,
        &lexical_rebuild_pipeline_json,
    ))
    .unwrap_or(serde_json::Value::Null);
    let ingest_quarantine_summary =
        crate::indexer::conversation_ingest_quarantine_summary(data_dir);
    let quarantined_conversations = ingest_quarantine_summary.quarantined_conversations;
    let ingest_quarantine_json =
        serde_json::to_value(&ingest_quarantine_summary).unwrap_or(serde_json::Value::Null);

    // Probe the live lexical document count when the DB has messages. Prefer
    // the published generation manifest: status only needs the durable count
    // for stale/empty diagnostics, while opening a large Tantivy reader here
    // fans out across every segment file on the hot robot path.
    // GH #457: prefer the count the engine SERVES (its MANIFEST, metadata
    // only) over the count a rebuild once RECORDED (the generation manifest):
    // a hollow generation keeps the recorded count while serving almost
    // nothing, which is exactly the gap that let `healthy: true` stand.
    let index_doc_count: Option<u64> = if let Some(live_docs) = lexical.live_docs {
        Some(live_docs)
    } else if db_opened && message_count > 0 && lexical.exists {
        lexical_manifest_indexed_doc_count(&index_path).or_else(|| {
            frankensearch::lexical_tantivy::cass_open_search_reader(
                &index_path,
                frankensearch::lexical_tantivy::ReloadPolicy::Manual,
            )
            .ok()
            .map(|(reader, _fields)| reader.searcher().num_docs())
        })
    } else {
        None
    };
    // #441 / WS-B.1a: how fragmented the published lexical generation is, from
    // directory metadata only (an upper bound on the live segment count; see
    // `quill_bridge::segment_file_count`). `null` when no Quill index exists.
    let lexical_segment_files: Option<u64> = if lexical.exists {
        crate::search::quill_bridge::segment_file_count(&index_path)
            .map(|count| u64::try_from(count).unwrap_or(u64::MAX))
    } else {
        None
    };
    let index_empty_with_messages = index_doc_count
        .map(|docs| docs == 0 && message_count > 0)
        .unwrap_or(false);

    let ts_str = chrono::DateTime::from_timestamp(now_secs as i64, 0)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339();

    // [coding_agent_session_search #301 partial-index marker] When
    // `cass index --full` is aborted by the stall watchdog (exit 70) or
    // otherwise interrupted, it leaves a SEARCHABLE-but-incomplete lexical
    // index: the checkpoint is present but never marked completed. Search
    // then queries it with results that silently omit a fraction of
    // conversations. Surface a dedicated `partial` boolean (and reason)
    // distinct from age-staleness so `cass search --robot-meta` can warn
    // that the index is incomplete, not merely old. The condition is: the
    // index exists, is not actively rebuilding right now, and its
    // checkpoint is present-but-not-completed (the durable abort/interrupt
    // signature) — OR the live scan watermark is ahead of the projected
    // index (scan advanced without a completed projection). Both are the
    // exact signatures #301 produces after an aborted full index.
    let index_partial = lexical.exists
        && !lexical.rebuilding
        && ((lexical.checkpoint.present && lexical.checkpoint.completed == Some(false))
            || watch_lexical_replay_pending == Some(true)
            || last_scan_ts.is_some_and(|scan_ts| {
                last_indexed_at
                    .map(|indexed_at| scan_ts > indexed_at.saturating_add(1_000))
                    .unwrap_or(true)
            }));
    let index_partial_reason: Option<&'static str> = if index_partial {
        if lexical.checkpoint.present && lexical.checkpoint.completed == Some(false) {
            Some(
                "lexical index is PARTIAL: a prior `cass index --full` was aborted or interrupted before its checkpoint completed; search results omit conversations indexed after the abort. Re-run `cass index --full` to complete it.",
            )
        } else if watch_lexical_replay_pending == Some(true) {
            Some(
                "lexical index is PARTIAL: durable watch ingestion or publication remains pending for one or more source roots; new content may not yet be searchable. The watcher will retry; restart `cass index --watch` if no watcher is running.",
            )
        } else {
            Some(
                "lexical index is PARTIAL: a scan advanced past the last completed projection (scan watermark ahead of indexed watermark); search results may omit recently scanned conversations. Re-run `cass index --full` to reconcile.",
            )
        }
    } else {
        None
    };

    let mut state = serde_json::json!({
        "index": {
            "exists": lexical.exists,
            "status": lexical.status,
            "reason": lexical.status_reason,
            "fresh": lexical.fresh,
            // [coding_agent_session_search #301] explicit incomplete-index
            // signal, distinct from age-staleness (`stale`).
            "partial": index_partial,
            "partial_reason": index_partial_reason,
            "last_indexed_at": last_indexed_at.map(|ts| {
                chrono::DateTime::from_timestamp_millis(ts)
                    .unwrap_or_else(chrono::Utc::now)
                    .to_rfc3339()
            }),
            "age_seconds": lexical.age_seconds,
            "stale": lexical.stale,
            "stale_threshold_seconds": stale_threshold,
            "rebuilding": lexical.rebuilding,
            "stalled": lexical.stalled,
            // b4uax: legacy Tantivy generation the current engine cannot read.
            "engine_incompatible": lexical.engine_incompatible,
            "activity_at": lexical.activity_at_ms.map(|ts| {
                chrono::DateTime::from_timestamp_millis(ts)
                    .unwrap_or_else(chrono::Utc::now)
                    .to_rfc3339()
            }),
            "documents": index_doc_count,
            // GH #457: what the published generation serves (manifest
            // metadata only) and whether that contradicts the completed
            // checkpoint. `reason` carries the operator-facing verdict.
            "live_documents": lexical.live_docs,
            "hollow": lexical.hollow,
            // mknmt: which MANIFEST slot readers serve; anything but
            // "current" is an older generation than the newest publish.
            "manifest_source": lexical
                .manifest_source
                .map(crate::search::quill_bridge::QuillManifestSource::as_str),
            // #441: upper bound on published Quill segments, metadata only.
            "segment_files": lexical_segment_files,
            "empty_with_messages": index_empty_with_messages,
            "quarantined_conversations": quarantined_conversations,
            "fingerprint": {
                "current_db_fingerprint": lexical.fingerprint.current_db_fingerprint,
                "checkpoint_fingerprint": lexical.fingerprint.checkpoint_fingerprint,
                "matches_current_db_fingerprint": lexical.fingerprint.matches_current_db_fingerprint,
            },
            "checkpoint": {
                "present": lexical.checkpoint.present,
                "completed": lexical.checkpoint.completed,
                "db_matches": lexical.checkpoint.db_matches,
                "schema_matches": lexical.checkpoint.schema_matches,
                "page_size_matches": lexical.checkpoint.page_size_matches,
                "page_size_compatible": lexical.checkpoint.page_size_compatible,
            }
        },
        "database": {
            "exists": db_exists,
            // WS-B.4a: physical footprint from metadata only (never an open).
            "db_bytes": db_size_bytes.unwrap_or(0),
            "wal_bytes": db_wal_bytes,
            "shm_present": db_shm_present,
            "opened": db_opened,
            "conversations": state_db_count_json(conversation_count, counts_skipped),
            "messages": state_db_count_json(message_count, counts_skipped),
            "open_error": db_open_error,
            "open_retryable": db_open_retryable,
            "counts_skipped": counts_skipped,
            "open_skipped": open_skipped,
            "integrity": db_integrity,
            "integrity_detail": db_integrity_detail,
            "integrity_hint": db_integrity_hint
        },
        "pending": {
            "sessions": lexical.pending_sessions,
            "lexical_replay": watch_lexical_replay_pending,
            "watch_active": lexical.watch_active,
            "orphaned": index_run.orphaned
        },
        "rebuild": {
            "active": lexical.rebuilding,
            // `stalled=true` means active=true but the indexing thread
            // has not made forward progress for at least
            // CASS_REBUILD_STALL_DETECT_SECS (default 120s), even though
            // the lock-file heartbeat is still being refreshed. Surface
            // this distinctly from `active` so operators (and the TUI)
            // can tell a slow-but-progressing rebuild apart from a
            // wedged one. Regression #258.
            "stalled": lexical.stalled,
            // b4uax: legacy Tantivy generation the current engine cannot read.
            "engine_incompatible": lexical.engine_incompatible,
            "last_progress_at": lexical.last_progress_at_ms.and_then(|ts| {
                chrono::DateTime::from_timestamp_millis(ts).map(|dt| dt.to_rfc3339())
            }),
            "last_progress_age_ms": lexical.last_progress_age_ms,
            "orphaned": index_run.orphaned,
            "pid": index_run.pid,
            "mode": index_run.mode.map(|mode| mode.as_lock_value()),
            "job_id": index_run.job_id,
            "job_kind": index_run.job_kind.map(|kind| kind.as_lock_value()),
            "phase": index_run.phase,
            "started_at": index_run.started_at_ms.map(|ts| {
                chrono::DateTime::from_timestamp_millis(ts)
                    .unwrap_or_else(chrono::Utc::now)
                    .to_rfc3339()
            }),
            "updated_at": lexical.activity_at_ms.map(|ts| {
                chrono::DateTime::from_timestamp_millis(ts)
                    .unwrap_or_else(chrono::Utc::now)
                    .to_rfc3339()
            }),
            "processed_conversations": lexical.processed_conversations,
            "total_conversations": lexical.total_conversations,
            "indexed_docs": lexical.indexed_docs,
            "pipeline": lexical_rebuild_pipeline_json,
            // GH #381 (ntxs1): where a running or interrupted full
            // expected-docs scan stands; the next index run resumes it
            // when the archive is unchanged or only grew. Null otherwise.
            "expected_docs_scan": crate::indexer::expected_lexical_docs_scan_position(&index_path, db_path)
                .map(|position| serde_json::json!({
                    "last_conversation_id": position.last_conversation_id,
                    "max_conversation_id": position.max_conversation_id,
                    "expected_docs_so_far": position.expected_docs_so_far,
                    "saved_at": position.saved_at_ms
                        .and_then(chrono::DateTime::from_timestamp_millis)
                        .map(|saved| saved.to_rfc3339()),
                }))
        },
        "semantic": {
            "status": semantic.status,
            "availability": semantic.availability,
            "summary": semantic.summary,
            "available": semantic.available,
            "can_search": semantic.can_search,
            "fallback_mode": semantic.fallback_mode,
            "preferred_backend": semantic.preferred_backend,
            "embedder_id": semantic.embedder_id,
            "vector_index_path": semantic.vector_index_path.as_ref().map(|path| path.display().to_string()),
            "model_dir": semantic.model_dir.as_ref().map(|path| path.display().to_string()),
            "hnsw_path": semantic.hnsw_path.as_ref().map(|path| path.display().to_string()),
            "hnsw_present": semantic.hnsw_present,
            "hnsw_state": semantic.hnsw_state,
            "hnsw_ready": semantic.hnsw_ready,
            "progressive_ready": semantic.progressive_ready,
            "progressive_reason_code": semantic.progressive_reason_code,
            // Always true since bead tg5o9 retired the `semantic` Cargo
            // feature: every build carries the pure-Rust
            // frankensearch/native backend (no ONNX, no `-baseline`
            // artifact). The key is kept for schema stability.
            "feature_compiled_in": true,
            // Sub-fix 3 for cass#257 — additive fields that report
            // quality-tier readiness independently of the
            // progressive/hybrid stack so `--mode semantic` consumers
            // see "yes, you can search" even while the fast tier or
            // lexical surface is still building.
            "quality_tier_published": semantic.quality_tier_published,
            "semantic_only_search_available": semantic.semantic_only_search_available,
            "hint": semantic.hint,
            "fast_tier": {
                "present": semantic.fast_tier.present,
                "ready": semantic.fast_tier.ready,
                "current_db_matches": semantic.fast_tier.current_db_matches,
                "conversation_count": semantic.fast_tier.conversation_count,
                "doc_count": semantic.fast_tier.doc_count,
                "embedder_id": semantic.fast_tier.embedder_id,
                "model_revision": semantic.fast_tier.model_revision,
                "completed_at": semantic
                    .fast_tier
                    .completed_at_ms
                    .and_then(format_timestamp_millis_rfc3339),
                "size_bytes": semantic.fast_tier.size_bytes,
                "shard_count": semantic.fast_tier.shard_count,
                "legacy_external_embedding_space": semantic.fast_tier.legacy_external_space,
            },
            "quality_tier": {
                "present": semantic.quality_tier.present,
                "ready": semantic.quality_tier.ready,
                "current_db_matches": semantic.quality_tier.current_db_matches,
                "conversation_count": semantic.quality_tier.conversation_count,
                "doc_count": semantic.quality_tier.doc_count,
                "embedder_id": semantic.quality_tier.embedder_id,
                "model_revision": semantic.quality_tier.model_revision,
                "completed_at": semantic
                    .quality_tier
                    .completed_at_ms
                    .and_then(format_timestamp_millis_rfc3339),
                "size_bytes": semantic.quality_tier.size_bytes,
                "shard_count": semantic.quality_tier.shard_count,
                "legacy_external_embedding_space": semantic.quality_tier.legacy_external_space,
            },
            "backlog": {
                "total_conversations": semantic.backlog.total_conversations,
                "fast_tier_processed": semantic.backlog.fast_tier_processed,
                "fast_tier_remaining": semantic.backlog.fast_tier_remaining,
                "quality_tier_processed": semantic.backlog.quality_tier_processed,
                "quality_tier_remaining": semantic.backlog.quality_tier_remaining,
                "pending_work": semantic.backlog.pending_work,
                "current_db_matches": semantic.backlog.current_db_matches,
                "computed_at": semantic
                    .backlog
                    .computed_at_ms
                    .and_then(format_timestamp_millis_rfc3339),
            },
            "checkpoint": {
                "active": semantic.checkpoint.active,
                "tier": semantic.checkpoint.tier,
                "current_db_matches": semantic.checkpoint.current_db_matches,
                "completed": semantic.checkpoint.completed,
                "conversations_processed": semantic.checkpoint.conversations_processed,
                "total_conversations": semantic.checkpoint.total_conversations,
                "progress_pct": semantic.checkpoint.progress_pct,
                "docs_embedded": semantic.checkpoint.docs_embedded,
                "last_offset": semantic.checkpoint.last_offset,
                "saved_at": semantic
                    .checkpoint
                    .saved_at_ms
                    .and_then(format_timestamp_millis_rfc3339),
            },
        },
        "ingest_quarantine": ingest_quarantine_json,
        "policy_registry": policy_registry,
        "_meta": {
            "timestamp": ts_str,
            "data_dir": data_dir.display().to_string(),
            "db_path": db_path.display().to_string()
        }
    });
    // zn1xn: emit the "canonical FTS shadow may be half-rebuilt" signal
    // additively — present only when the last full index run left the shadow
    // unrepaired — so existing status/index JSON is byte-identical otherwise.
    if let Some(detail) = fallback_fts_repair_pending
        && let Some(index) = state.get_mut("index").and_then(|v| v.as_object_mut())
    {
        // GH #495: a shadow retired as not viable over the message bound keeps
        // this marker to say why it is absent, but that is a settled state:
        // `doctor --rebuild-canonical-fts` plans nothing for it. Only other
        // markers describe a repair that is still pending.
        let retired =
            crate::storage::sqlite::error_message_indicates_fts_shadow_not_viable(&detail);
        index.insert(
            "fallback_fts_repair".to_string(),
            serde_json::json!({
                "pending": !retired,
                "retired_not_viable": retired,
                "detail": detail,
            }),
        );
    }
    // zn1xn F4: surface a persistent lexical-repair deferral streak additively —
    // present only when the last non-watch run deferred inline lexical updates,
    // so existing JSON is byte-identical otherwise.
    if let Some(runs) = lexical_repair_deferred_runs
        && let Some(index) = state.get_mut("index").and_then(|v| v.as_object_mut())
    {
        index.insert(
            "lexical_repair_deferred".to_string(),
            serde_json::json!({
                "consecutive_runs": runs,
                "reason": lexical_repair_deferred_reason,
            }),
        );
    }
    state
}

pub(crate) fn lexical_manifest_indexed_doc_count(index_path: &Path) -> Option<u64> {
    use crate::indexer::lexical_generation::{LexicalGenerationPublishState, load_manifest};

    match load_manifest(index_path) {
        Ok(Some(manifest))
            if matches!(
                manifest.publish_state,
                LexicalGenerationPublishState::Published
            ) =>
        {
            Some(manifest.indexed_doc_count)
        }
        _ => None,
    }
}

/// Stale-on-read catch-up (see `indexer::background_refresh`).
///
/// Read paths (search, pack, TUI launch) call this *after* they have what
/// they need from the current index. If the index is stale/partial/behind,
/// a detached low-priority `cass index --background` is spawned so the next
/// read is fresh; the current read is never blocked or altered.
///
/// `precomputed` lets `--robot-meta` callers reuse the freshness block they
/// already built; otherwise the health-grade probe (no DB open) is used.
/// Returns the outcome JSON for robot metadata, or `None` when nothing was
/// attempted (policy off, scratch dir, headless test, index fresh).
pub(crate) fn maybe_auto_refresh_index_after_read(
    data_dir: &Path,
    db_path: &Path,
    precomputed: Option<&serde_json::Value>,
) -> Option<serde_json::Value> {
    use crate::indexer::background_refresh;

    if !background_refresh::AutoRefreshPolicy::from_env().enabled {
        return None;
    }
    // Scratch/test data dirs are ephemeral by definition; a detached child
    // indexing a tempdir after the test that created it has moved on is
    // pure noise (and a flake generator). Headless TUI harnesses likewise.
    if auto_refresh_is_scratch_data_dir(data_dir) || dotenvy::var("TUI_HEADLESS").is_ok() {
        return None;
    }
    let owned;
    let base_freshness = match precomputed {
        Some(value) => value,
        None => {
            let meta = state_meta_json_for_health(data_dir, db_path, DEFAULT_STALE_THRESHOLD_SECS);
            owned = state_index_freshness(&meta)?;
            &owned
        }
    };
    let mut trigger = background_refresh::catch_up_reason(base_freshness)?;
    let enriched;
    let freshness = if trigger == "source-scan-overdue"
        && let Some(with_scan) =
            background_refresh::enrich_freshness_with_source_scan_watermark(base_freshness, db_path)
    {
        enriched = with_scan;
        trigger = background_refresh::catch_up_reason(&enriched)?;
        &enriched
    } else {
        base_freshness
    };
    let outcome = background_refresh::maybe_spawn_background_index_refresh(
        data_dir,
        db_path,
        trigger,
        background_refresh::last_indexed_at_ms_from_freshness(freshness),
    );
    let mut value = serde_json::to_value(&outcome).unwrap_or(serde_json::Value::Null);
    if let serde_json::Value::Object(map) = &mut value {
        map.insert(
            "trigger".to_string(),
            serde_json::Value::String(trigger.to_string()),
        );
    }
    Some(value)
}

/// True when `data_dir` lives under the platform temp dir (or `CASS_DATA_DIR`
/// explicitly points at one). Auto-refresh never fires there, and neither
/// does the TUI's first-run index.
pub(crate) fn auto_refresh_is_scratch_data_dir(data_dir: &Path) -> bool {
    // Both sides resolve through their nearest existing ancestor, so a data
    // dir that does not exist yet shares the temp dir's canonical spelling
    // (`\\?\C:\...` on Windows, `/private/var/folders/...` on macOS).
    let canon_temp = normalize_path_identity(&std::env::temp_dir());
    let canon_dir = normalize_path_identity(data_dir);
    // `/tmp` is a symlink to `/private/tmp` on macOS, so a canonicalized
    // Linux-style `/tmp/...` data dir lands under `/private/tmp` there.
    canon_dir.starts_with(&canon_temp)
        || canon_dir.starts_with("/tmp")
        || canon_dir.starts_with("/private/tmp")
        || canon_dir.starts_with("/var/tmp")
        || canon_dir.starts_with("/private/var/tmp")
}

pub(crate) fn state_index_freshness(state: &serde_json::Value) -> Option<serde_json::Value> {
    let index = state.get("index")?;
    let pending = state.get("pending");
    Some(serde_json::json!({
        "exists": index.get("exists"),
        "status": index.get("status"),
        "reason": index.get("reason"),
        "fresh": index.get("fresh"),
        "last_indexed_at": index.get("last_indexed_at"),
        "age_seconds": index.get("age_seconds"),
        "stale": index.get("stale"),
        "stale_threshold_seconds": index.get("stale_threshold_seconds"),
        // b4uax: legacy Tantivy generation the current engine cannot read.
        "engine_incompatible": index.get("engine_incompatible"),
        // mknmt: readers serving MANIFEST.prev, not the newest publish.
        "manifest_source": index.get("manifest_source"),
        "rebuilding": index.get("rebuilding"),
        // [coding_agent_session_search #301] propagate the incomplete-index
        // markers so `cass search --robot-meta` can warn that the index is
        // PARTIAL (aborted/interrupted full index), not merely stale.
        "partial": index.get("partial"),
        "partial_reason": index.get("partial_reason"),
        "pending_sessions": pending.and_then(|p| p.get("sessions"))
    }))
}

pub(crate) fn rebuild_progress_summary_json(state: &serde_json::Value) -> serde_json::Value {
    let rebuild = state.get("rebuild").unwrap_or(&serde_json::Value::Null);
    let runtime = rebuild
        .get("pipeline")
        .and_then(|pipeline| pipeline.get("runtime"))
        .unwrap_or(&serde_json::Value::Null);
    let value_or_null =
        |value: Option<&serde_json::Value>| value.cloned().unwrap_or(serde_json::Value::Null);

    let active = rebuild
        .get("active")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let stalled = rebuild
        .get("stalled")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let last_progress_at = rebuild
        .get("last_progress_at")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let last_progress_age_ms = rebuild
        .get("last_progress_age_ms")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let processed = rebuild
        .get("processed_conversations")
        .and_then(|value| value.as_u64());
    let total = rebuild
        .get("total_conversations")
        .and_then(|value| value.as_u64());
    let remaining = total.map(|count| count.saturating_sub(processed.unwrap_or(0)));
    let completion_ratio = match (processed, total) {
        (Some(done), Some(total_conversations)) if total_conversations > 0 => {
            Some((done as f64 / total_conversations as f64).clamp(0.0, 1.0))
        }
        _ => None,
    };
    let updated_at = runtime
        .get("updated_at")
        .cloned()
        .or_else(|| rebuild.get("updated_at").cloned())
        .unwrap_or(serde_json::Value::Null);

    serde_json::json!({
        "active": active,
        // Issue #258: surface `stalled` distinctly so health/status
        // consumers can tell a wedged indexer apart from a slow rebuild.
        "stalled": stalled,
        "last_progress_at": last_progress_at,
        "last_progress_age_ms": last_progress_age_ms,
        "mode": value_or_null(rebuild.get("mode")),
        "phase": value_or_null(rebuild.get("phase")),
        "processed_conversations": processed,
        "total_conversations": total,
        "remaining_conversations": remaining,
        "completion_ratio": completion_ratio,
        "indexed_docs": value_or_null(rebuild.get("indexed_docs")),
        "runtime_available": runtime.is_object(),
        "queue_depth": value_or_null(runtime.get("queue_depth")),
        "queue_capacity": value_or_null(runtime.get("queue_capacity")),
        "queue_headroom": value_or_null(runtime.get("queue_headroom")),
        "pending_batch_conversations": value_or_null(runtime.get("pending_batch_conversations")),
        "pending_batch_message_bytes": value_or_null(runtime.get("pending_batch_message_bytes")),
        "inflight_message_bytes": value_or_null(runtime.get("inflight_message_bytes")),
        "max_message_bytes_in_flight": value_or_null(runtime.get("max_message_bytes_in_flight")),
        "inflight_message_bytes_headroom": value_or_null(runtime.get("inflight_message_bytes_headroom")),
        "controller_mode": value_or_null(runtime.get("controller_mode")),
        "controller_reason": value_or_null(runtime.get("controller_reason")),
        "updated_at": updated_at,
    })
}

pub(crate) fn state_db_count_json(count: i64, counts_skipped: bool) -> serde_json::Value {
    if counts_skipped {
        serde_json::Value::Null
    } else {
        serde_json::Value::from(count)
    }
}

pub(crate) fn state_database_counts_need_refresh(
    state: &serde_json::Value,
    db_path: &Path,
) -> bool {
    let current_opened = state
        .get("database")
        .and_then(|db| db.get("opened"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let current_counts_skipped = state
        .get("database")
        .and_then(|db| db.get("counts_skipped"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let current_conversations = state
        .get("database")
        .and_then(|db| db.get("conversations"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let current_messages = state
        .get("database")
        .and_then(|db| db.get("messages"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);

    let count_refresh_allowed = std::fs::metadata(db_path).ok().is_some_and(|metadata| {
        metadata.is_file() && metadata.len() <= STATUS_COUNT_SCAN_MAX_DB_BYTES
    });
    ((!current_counts_skipped || count_refresh_allowed)
        && (!current_opened || current_conversations <= 0 || current_messages <= 0))
        && db_path.exists()
}

pub(crate) fn apply_state_database_count_snapshot(
    state: &mut serde_json::Value,
    refreshed: StateDbSnapshot,
) {
    let current_opened = state
        .get("database")
        .and_then(|db| db.get("opened"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let current_counts_skipped = state
        .get("database")
        .and_then(|db| db.get("counts_skipped"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let current_conversations = state
        .get("database")
        .and_then(|db| db.get("conversations"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let current_messages = state
        .get("database")
        .and_then(|db| db.get("messages"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);

    let improved = (!current_opened && refreshed.opened)
        || (current_counts_skipped && !refreshed.counts_skipped)
        || (current_conversations <= 0 && refreshed.conversation_count > 0)
        || (current_messages <= 0 && refreshed.message_count > 0);
    if !improved {
        return;
    }

    let Some(database) = state.get_mut("database").and_then(|db| db.as_object_mut()) else {
        return;
    };
    database.insert(
        "opened".to_string(),
        serde_json::Value::Bool(refreshed.opened),
    );
    database.insert(
        "conversations".to_string(),
        state_db_count_json(refreshed.conversation_count, refreshed.counts_skipped),
    );
    database.insert(
        "messages".to_string(),
        state_db_count_json(refreshed.message_count, refreshed.counts_skipped),
    );
    database.insert(
        "open_error".to_string(),
        refreshed
            .open_error
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    database.insert(
        "open_retryable".to_string(),
        serde_json::Value::Bool(refreshed.open_retryable),
    );
    database.insert(
        "counts_skipped".to_string(),
        serde_json::Value::Bool(refreshed.counts_skipped),
    );
    // i6upe: a classified integrity verdict upgrades prior unknown state. A
    // transient refresh result never downgrades an existing verdict.
    if let Some(integrity) = refreshed.integrity {
        database.insert(
            "integrity".to_string(),
            serde_json::Value::String(
                match integrity {
                    StateDbIntegrity::Ok => "ok",
                    StateDbIntegrity::Corrupt => "corrupt",
                }
                .to_string(),
            ),
        );
        database.insert(
            "integrity_detail".to_string(),
            refreshed
                .integrity_detail
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
        );
    }
}

pub(crate) fn refresh_state_database_counts_if_needed(
    state: &mut serde_json::Value,
    db_path: &Path,
    reason: &str,
) {
    if !state_database_counts_need_refresh(state, db_path) {
        return;
    }

    let refreshed = probe_state_db(db_path, reason, Duration::from_secs(30), true);
    apply_state_database_count_snapshot(state, refreshed);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundedCountRefresh {
    NotNeeded,
    Completed,
    TimedOut,
}

pub(crate) fn maybe_test_triage_worker_delay(variable: &str) {
    if let Ok(raw) = dotenvy::var(variable)
        && let Ok(milliseconds) = raw.parse::<u64>()
        && milliseconds > 0
    {
        std::thread::sleep(Duration::from_millis(milliseconds));
    }
}

pub(crate) fn refresh_state_database_counts_with_hard_timeout(
    state: &mut serde_json::Value,
    db_path: &Path,
    reason: &str,
    timeout: Duration,
) -> BoundedCountRefresh {
    if !state_database_counts_need_refresh(state, db_path) {
        return BoundedCountRefresh::NotNeeded;
    }

    let path = db_path.to_path_buf();
    let reason = reason.to_string();
    let (tx, rx) = std::sync::mpsc::channel();
    let Ok(_count_worker) = std::thread::Builder::new()
        .name("cass-triage-count-probe".to_string())
        .spawn(move || {
            maybe_test_triage_worker_delay("CASS_TEST_TRIAGE_COUNT_SLOW_MS");
            let snapshot = probe_state_db(&path, &reason, timeout, true);
            let _ = tx.send(snapshot);
        })
    else {
        return BoundedCountRefresh::TimedOut;
    };

    match rx.recv_timeout(timeout) {
        Ok(snapshot) => {
            apply_state_database_count_snapshot(state, snapshot);
            BoundedCountRefresh::Completed
        }
        Err(
            std::sync::mpsc::RecvTimeoutError::Timeout
            | std::sync::mpsc::RecvTimeoutError::Disconnected,
        ) => BoundedCountRefresh::TimedOut,
    }
}

pub(crate) fn readiness_snapshot_from_state(
    state: &serde_json::Value,
    not_initialized: bool,
) -> crate::search::readiness::ReadinessSnapshot {
    use crate::search::readiness::ReadinessSnapshot;

    let lexical = lexical_readiness_from_state(state, not_initialized);
    let semantic = semantic_readiness_from_state(state);
    ReadinessSnapshot::new(lexical, semantic)
}

/// Live bridge (bead `cass-fleet-resilience-20260608-uojcg.envu1`): build the
/// canonical typed `DerivedAssetTruthTable` for `data_dir` from the same
/// `state_meta_json` evidence `run_status`/`run_doctor` already read.
///
/// Today the typed truth table is only constructed in fixtures; the live
/// readiness surfaces work off a parallel `serde_json::Value` snapshot. This
/// builder maps that snapshot into the typed table so the typed projections
/// (`readiness_projection::project`, `next_command_envelope`,
/// `QuarantineSummary`) can be reused across surfaces instead of each
/// re-deriving the JSON. Staged ahead of its callers (the recovery-support
/// bundle wiring `6f1lm` and the human readiness summary wiring `v6vuz`),
/// which become thin assemblers over this one builder.
pub(crate) fn build_truth_table_for_data_dir(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
) -> crate::search::readiness::DerivedAssetTruthTable {
    // allow_db_open=true: derive from the real DB-open signal (status-like),
    // so OpenFailed is distinguished from a present-but-elided open.
    let state = state_meta_json(data_dir, db_path, stale_threshold, true);
    build_truth_table_from_state(&state)
}

/// Variant of [`build_truth_table_for_data_dir`] for callers that already
/// hold a `state_meta_json` snapshot (status/doctor compute it once), so the
/// data dir is not re-probed.
pub(crate) fn build_truth_table_from_state(
    state: &serde_json::Value,
) -> crate::search::readiness::DerivedAssetTruthTable {
    let db_exists = state
        .pointer("/database/exists")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let lexical_index_initialized = state
        .pointer("/index/exists")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let rebuild_active = state
        .pointer("/rebuild/active")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let not_initialized =
        cass_not_initialized(db_exists, lexical_index_initialized, rebuild_active);
    let readiness = readiness_snapshot_from_state(state, not_initialized);
    crate::search::readiness::truth_table_from_state(state, readiness)
}

/// Render the bounded human readiness summary (bead
/// `coding_agent_session_search-v6vuz`) as a CLI section, from a live
/// `state_meta_json` snapshot the surface already computed.
///
/// This is the live counterpart of the `.13.2` contract
/// (`crate::search::human_readiness_summary`): every human (non-`--json`)
/// readiness surface emits the *same* projection the robot JSON serializes, so
/// the operator-facing copy can never drift from the machine contract. The
/// projection is pure over the already-built snapshot (no extra I/O), so even
/// the `<50ms` health surface stays fast. Nothing is printed if the projection
/// would be unsafe to surface — a structural backstop on top of the curated
/// safe-next-command, so a human surface can never recommend a destructive or
/// archive-risking action.
pub(crate) fn print_human_readiness_section(
    state: &serde_json::Value,
    surface: crate::search::readiness_projection::SurfaceKind,
) {
    let table = build_truth_table_from_state(state);
    let summary = crate::search::human_readiness_summary::project_human_summary(&table, surface);
    if !summary.is_safe(table.archive_risk) {
        return;
    }
    println!();
    println!("Readiness:");
    for line in summary.render_lines() {
        println!("{line}");
    }
}

pub(crate) fn lexical_readiness_from_state(
    state: &serde_json::Value,
    not_initialized: bool,
) -> crate::search::readiness::LexicalReadinessState {
    use crate::search::readiness::LexicalReadinessState;

    if not_initialized {
        return LexicalReadinessState::Missing;
    }

    let index = state.get("index");
    let rebuild_active = state
        .get("rebuild")
        .and_then(|rebuild| rebuild.get("active"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    if rebuild_active {
        return LexicalReadinessState::Repairing;
    }

    let exists = index
        .and_then(|idx| idx.get("exists"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    if !exists {
        return LexicalReadinessState::Missing;
    }

    let status = index
        .and_then(|idx| idx.get("status"))
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    let reason = index
        .and_then(|idx| idx.get("reason"))
        .and_then(|value| value.as_str())
        .unwrap_or("");

    if status == "error" && reason.contains("quarantin") {
        return LexicalReadinessState::CorruptQuarantined;
    }
    // GH #368 defect 2: any other asset-inspection error means search actually
    // hard-fails — most importantly the lexical-fingerprint open failing on a
    // corrupt FTS5 structure ("computing lexical storage fingerprint ... fts5:
    // corrupt %_data record ..."). That error path (state_meta_json_inner's
    // unwrap_or_else fallback) sets status="error" AND fresh=false, and the
    // fresh=false branch below previously classified it StaleButSearchable —
    // reporting "Search usable now: yes" while every query errored. An errored
    // index is corrupt/unusable (non-searchable, inspect first), not stale.
    if status == "error" {
        return LexicalReadinessState::CorruptQuarantined;
    }
    // b4uax (gh#382): a legacy Tantivy generation exists on disk but no
    // reader can open it. Search is unavailable until the rebuild runs, so it
    // is the Missing class (repair now), never "stale but searchable".
    if status == "legacy_engine"
        || index
            .and_then(|idx| idx.get("engine_incompatible"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
    {
        return LexicalReadinessState::Missing;
    }
    // GH #457: a hollow generation serves a small fraction of the corpus while
    // every other signal says ready. Search technically runs, but its answers
    // are wrong ("no prior work found"), so it is the rebuild-now class —
    // never "stale but searchable" (which promises correct answers for what
    // is already indexed).
    if status == "hollow"
        || index
            .and_then(|idx| idx.get("hollow"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
    {
        return LexicalReadinessState::Missing;
    }
    if status == "stale"
        || index
            .and_then(|idx| idx.get("fresh"))
            .and_then(|value| value.as_bool())
            == Some(false)
    {
        return LexicalReadinessState::StaleButSearchable;
    }

    LexicalReadinessState::Ready
}
