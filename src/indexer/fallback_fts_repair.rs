use super::*;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildStartupOptions {
    pub(in crate::indexer) defer_initial_content_fingerprint: bool,
}

pub(in crate::indexer) fn should_evaluate_incremental_canonical_lexical_repair(
    context: &IncrementalCanonicalLexicalRepairContext,
) -> bool {
    !context.full_refresh
        && !context.force_rebuild
        && !context.resume_lexical_rebuild
        && !context.targeted_watch_once_only
        && context.salvage_messages_imported == 0
}

pub(in crate::indexer) fn choose_incremental_canonical_lexical_repair_plan(
    context: IncrementalCanonicalLexicalRepairContext,
) -> Option<IncrementalCanonicalLexicalRepairPlan> {
    if context.full_refresh
        || context.force_rebuild
        || context.resume_lexical_rebuild
        || context.targeted_watch_once_only
        || context.salvage_messages_imported > 0
        || context.canonical_messages == 0
    {
        return None;
    }

    if context.tantivy_requires_rebuild {
        return Some(IncrementalCanonicalLexicalRepairPlan {
            canonical_messages: context.canonical_messages,
            observed_tantivy_docs: context.observed_tantivy_docs,
            reason: "incremental_index_repairs_missing_or_invalid_tantivy_from_authoritative_canonical_db_before_scan",
        });
    }

    let observed_tantivy_docs = context.observed_tantivy_docs?;
    // Task 5: live documents include converged search-region documents
    // alongside canonical message documents — subtract the persisted
    // bound so the sparse gate compares the canonical-message share, not
    // a region-inflated total. The bound counts persisted doc-backed rows
    // plus queued stale deletes, so it can only UNDER-state the share
    // (repair too eagerly — conservative, never mask a sparse index).
    let observed_message_docs = observed_tantivy_docs.saturating_sub(context.region_doc_bound);
    // cass#317: compare against the noise-adjusted expected doc count, not the
    // raw `canonical_messages`. The sink drops hard-noise messages (empty /
    // tool-acks), so a healthy live index holds `expected_lexical_docs` docs
    // (<= canonical_messages). Using the raw message count made any corpus with
    // tool-ack traffic look permanently sparse and rebuilt every run.
    if observed_message_docs < context.expected_lexical_docs {
        if context.published_index_validated_for_current_data {
            tracing::warn!(
                canonical_messages = context.canonical_messages,
                expected_lexical_docs = context.expected_lexical_docs,
                observed_tantivy_docs,
                "completed lexical checkpoint matches the canonical DB, but the live lexical index is sparse below the noise-adjusted expectation; repairing derived search assets from SQLite"
            );
        }
        return Some(IncrementalCanonicalLexicalRepairPlan {
            canonical_messages: context.canonical_messages,
            observed_tantivy_docs: Some(observed_tantivy_docs),
            reason: "incremental_index_repairs_sparse_tantivy_from_authoritative_canonical_db_before_scan",
        });
    }

    None
}

pub(in crate::indexer) fn should_salvage_historical_databases(
    canonical_storage_rebuilt: bool,
    canonical_sessions_before_salvage: usize,
    has_pending_historical_bundles: bool,
    canonical_only_full_rebuild: bool,
) -> bool {
    if canonical_only_full_rebuild {
        return false;
    }
    canonical_storage_rebuilt
        || canonical_sessions_before_salvage == 0
        || has_pending_historical_bundles
}

pub(in crate::indexer) fn should_probe_pending_historical_bundles(
    full_rebuild: bool,
    canonical_storage_rebuilt: bool,
    canonical_sessions_before_salvage: usize,
    canonical_only_full_rebuild: bool,
    operator_requested_discovery: bool,
) -> bool {
    if canonical_only_full_rebuild {
        return false;
    }
    full_rebuild
        || canonical_storage_rebuilt
        || canonical_sessions_before_salvage == 0
        || operator_requested_discovery
}

pub(in crate::indexer) fn should_run_targeted_watch_once_only(
    has_watch_once_paths: bool,
    _watch_enabled: bool,
    full_rebuild: bool,
    _needs_rebuild: bool,
    _canonical_sessions_before_salvage: usize,
) -> bool {
    // GH #350: an explicit targeted `--watch-once <paths>` ALWAYS bounds work to
    // the supplied paths — the path-scoped run ingests the changed sessions and
    // applies their inline lexical updates, deferring any broader authoritative
    // rebuild (exactly as the plain-index path defers it) instead of scanning the
    // whole archive. Previously `!needs_rebuild || canonical_sessions == 0`
    // flipped a populated archive whose derived index looked stale into a
    // full-corpus authoritative rebuild — the reported "scans large archive for
    // one changed session". `needs_rebuild` is true for essentially any change a
    // watch-once exists to ingest, so gating the targeted path on it defeated the
    // feature; the size-based deferral now keeps the run path-bounded.
    // watch_sources returns after these explicit paths even with --watch.
    // Startup must obey the same scope instead of broad-scanning neighbors.
    has_watch_once_paths && !full_rebuild
}

pub(in crate::indexer) fn should_skip_absent_explicit_watch_once_paths(
    opts: &IndexOptions,
) -> bool {
    if opts.watch || opts.full || opts.force_rebuild || opts.semantic || opts.build_hnsw {
        return false;
    }

    let Some(paths) = opts
        .watch_once_paths
        .as_ref()
        .filter(|paths| !paths.is_empty())
    else {
        return false;
    };

    paths
        .iter()
        .all(|path| matches!(path.try_exists(), Ok(false)))
}

pub(in crate::indexer) fn can_skip_absent_explicit_watch_once_index_run(
    opts: &IndexOptions,
) -> bool {
    if !should_skip_absent_explicit_watch_once_paths(opts) {
        return false;
    }
    let Ok(storage) = FrankenStorage::open_readonly(&opts.db_path) else {
        return false;
    };
    let db_schema_current = matches!(
        storage.schema_version(),
        Ok(crate::storage::sqlite::CURRENT_SCHEMA_VERSION)
    );
    let _ = storage.close();
    if !db_schema_current {
        return false;
    }

    let index_path = crate::search::tantivy::expected_index_dir(&opts.data_dir);
    let schema_hash_path = index_path.join("schema_hash.json");
    let schema_matches = schema_hash_path.exists()
        && std::fs::read_to_string(&schema_hash_path)
            .ok()
            .and_then(|content| serde_json::from_str::<serde_json::Value>(&content).ok())
            .and_then(|json| {
                json.get("schema_hash")
                    .and_then(|value| value.as_str())
                    .map(schema_hash_matches)
            })
            .unwrap_or(false);
    if !schema_matches {
        return false;
    }

    matches!(
        crate::search::tantivy::searchable_index_summary(&index_path),
        Ok(Some(_))
    )
}

pub(in crate::indexer) fn current_searchable_index_summary_available(index_path: &Path) -> bool {
    let schema_hash_path = index_path.join("schema_hash.json");
    let schema_matches = schema_hash_path.exists()
        && std::fs::read_to_string(&schema_hash_path)
            .ok()
            .and_then(|content| serde_json::from_str::<serde_json::Value>(&content).ok())
            .and_then(|json| {
                json.get("schema_hash")
                    .and_then(|value| value.as_str())
                    .map(schema_hash_matches)
            })
            .unwrap_or(false);
    schema_matches
        && matches!(
            crate::search::tantivy::searchable_index_summary(index_path),
            Ok(Some(_))
        )
}

pub(in crate::indexer) fn should_skip_unchanged_explicit_watch_once_paths(
    opts: &IndexOptions,
    storage: &FrankenStorage,
    roots: &[(ConnectorKind, ScanRoot)],
) -> Result<bool> {
    if opts.watch || opts.full || opts.force_rebuild || opts.semantic || opts.build_hnsw {
        return Ok(false);
    }

    let Some(paths) = opts
        .watch_once_paths
        .as_ref()
        .filter(|paths| !paths.is_empty())
    else {
        return Ok(false);
    };

    let triggers = classify_paths(paths.clone(), roots, true);
    if triggers.is_empty() {
        // The run ends here without reindexing, so this is the only place
        // that can say why nothing was indexed.
        warn_unclaimed_explicit_watch_once_paths(paths, roots);
        return Ok(true);
    }

    for (kind, root, _, _) in triggers {
        if !explicit_watch_once_root_unchanged_after_last_index(storage, kind, &root)? {
            return Ok(false);
        }
    }

    Ok(true)
}

pub(in crate::indexer) fn can_skip_unchanged_explicit_watch_once_index_run(
    opts: &IndexOptions,
    storage: &FrankenStorage,
    index_path: &Path,
    lexical_rebuild_required: bool,
    analytics_rebuild_required: bool,
) -> Result<bool> {
    if lexical_rebuild_required
        || analytics_rebuild_required
        || opts.watch
        || opts.full
        || opts.force_rebuild
        || opts.semantic
        || opts.build_hnsw
    {
        return Ok(false);
    }
    if opts
        .watch_once_paths
        .as_ref()
        .is_none_or(|paths| paths.is_empty())
    {
        return Ok(false);
    }

    let additional_scan_roots =
        additional_scan_roots_for_scan_or_watch(storage, &opts.data_dir, false);
    let watch_roots = build_watch_roots(additional_scan_roots);
    if !should_skip_unchanged_explicit_watch_once_paths(opts, storage, &watch_roots)? {
        return Ok(false);
    }

    Ok(current_searchable_index_summary_available(index_path))
}

pub(in crate::indexer) fn should_skip_broad_scan_after_watch_once_authoritative_repair(
    has_watch_once_paths: bool,
    _watch_enabled: bool,
    full_rebuild: bool,
    repaired_from_authoritative_canonical_db: bool,
) -> bool {
    has_watch_once_paths && !full_rebuild && repaired_from_authoritative_canonical_db
}

pub(in crate::indexer) fn should_repair_fallback_fts_after_full_index_run(
    full_rebuild: bool,
    canonical_only_full_rebuild: bool,
) -> bool {
    full_rebuild && !canonical_only_full_rebuild
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer) enum FallbackFtsRepairOutcome {
    SkippedKnownHealthyForFingerprint {
        archive_fingerprint: String,
    },
    SkippedUnsupportedPopulatedShadowReload {
        detail: String,
    },
    SkippedCorruptDerivedIndex {
        detail: String,
    },
    /// #329/#345: the optional DB-resident fallback FTS repair failed for a
    /// reason outside the enumerated nonfatal classes (e.g. an internal
    /// frankensqlite allocation failure on a multi-million-message shadow
    /// rebuild). The canonical archive and the published Tantivy index are
    /// already good at this point, so the failure is reported instead of
    /// failing the whole `index --full` run with exit 9 after hours of
    /// successful work; doctor surfaces the shadow state on its next check.
    SkippedRepairFailed {
        detail: String,
    },
    /// GH #413 follow-up (iify0): the shadow was dropped for size and the
    /// corpus still exceeds the bound; there is nothing to repair until it
    /// fits again. Quill serves lexical search; doctor carries the detail.
    SkippedNotViable {
        detail: String,
    },
    Repaired(FtsConsistencyRepair),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer) enum DailyStatsRepairOutcome {
    SkippedKnownHealthyForFingerprint {
        archive_fingerprint: String,
    },
    AlreadyHealthy,
    Rebuilt {
        rows_created: i64,
        total_sessions: i64,
    },
}

pub(in crate::indexer) fn nonfatal_fallback_fts_repair_outcome(
    detail: String,
) -> Option<FallbackFtsRepairOutcome> {
    if crate::storage::sqlite::error_message_indicates_fts_shadow_not_viable(&detail) {
        return Some(FallbackFtsRepairOutcome::SkippedNotViable { detail });
    }
    if crate::storage::sqlite::error_message_indicates_populated_fts_shadow_without_rowid_reload(
        &detail,
    ) {
        return Some(FallbackFtsRepairOutcome::SkippedUnsupportedPopulatedShadowReload { detail });
    }
    if crate::storage::sqlite::fts_messages_integrity_error_from_message(&detail).is_some() {
        return Some(FallbackFtsRepairOutcome::SkippedCorruptDerivedIndex { detail });
    }
    None
}

pub(in crate::indexer) fn repair_fallback_fts_after_full_index_run(
    _storage: &FrankenStorage,
    db_path: &Path,
    full_rebuild: bool,
    canonical_only_full_rebuild: bool,
    known_archive_fingerprint: Option<&str>,
    heartbeat: Option<Box<dyn Fn() + Send + Sync>>,
) -> Result<Option<FallbackFtsRepairOutcome>> {
    if !should_repair_fallback_fts_after_full_index_run(full_rebuild, canonical_only_full_rebuild) {
        return Ok(None);
    }

    // Use a short-lived, freshly-opened FrankenStorage for *every* step
    // of the repair — the fingerprint probe, the consistency check, and
    // the marker write.  The long-running indexer connection accumulates
    // per-thread cursor cancellation state across the rayon worker pool
    // (see `observe_cursor_cancellation` in fsqlite-btree), which can
    // surface on the FIRST subsequent query as `SQLITE_ABORT`
    // ("callback requested query abort").  That failure was making
    // every full-rebuild `run_index` call in the bench harness return
    // an Err after ~1 s of abort bookkeeping, swamping the actual
    // indexing wall-clock and quantizing measurements to 1 s bins.
    //
    // The repair work is bounded (at most one `COUNT(*)` probe, an
    // optional batched rebuild, and a single meta write) and strictly
    // idempotent on the disk state, so a fresh connection is both
    // sufficient and correct.  The caller's `_storage` is intentionally
    // *not* consulted: an earlier version of this function did the
    // fingerprint probe on it and was still vulnerable to the exact
    // abort this fix is supposed to prevent.  The `_storage` parameter
    // is kept for API stability (callers pass the same long-running
    // connection they already have) but is not touched.
    let fresh_storage = match crate::storage::sqlite::open_franken_storage_with_timeout(
        db_path,
        std::time::Duration::from_secs(10),
    ) {
        Ok(storage) => storage,
        Err(err) => {
            let detail = format!("{err:#}");
            if let Some(outcome) = nonfatal_fallback_fts_repair_outcome(detail.clone()) {
                return Ok(Some(outcome));
            }
            // #329: the canonical archive and Tantivy publish already
            // succeeded; an unopenable connection for the *optional* derived
            // repair must not fail the whole run.
            return Ok(Some(FallbackFtsRepairOutcome::SkippedRepairFailed {
                detail: format!(
                    "opening fresh frankensqlite connection for fallback FTS repair: {detail}"
                ),
            }));
        }
    };
    // #439: stream-page liveness for the stall watchdog (see the call site).
    fresh_storage.set_fts_maintenance_heartbeat(heartbeat);

    let outcome: FallbackFtsRepairOutcome = 'compute: {
        // GH #495: an absent shadow carrying the durable size-retirement
        // marker is a terminal derived-asset state, not a failed repair to
        // retry on every full index. Re-evaluate viability so lowering/removing
        // the bound still lets the normal recreation path resume.
        if let Ok(Some(detail)) = fresh_storage.fts_shadow_not_viable_marker()
            && matches!(
                fresh_storage.fts_shadow_viability(),
                Ok(crate::storage::sqlite::FtsShadowViability::NotViable { .. })
            )
        {
            break 'compute FallbackFtsRepairOutcome::SkippedNotViable { detail };
        }
        if let Some(archive_fingerprint) = known_archive_fingerprint {
            match fresh_storage
                .fallback_fts_is_known_healthy_for_archive_fingerprint(archive_fingerprint)
            {
                Ok(true) => {
                    break 'compute FallbackFtsRepairOutcome::SkippedKnownHealthyForFingerprint {
                        archive_fingerprint: archive_fingerprint.to_string(),
                    };
                }
                Ok(false) => {}
                // #329: same policy as the repair itself — a failed probe of the
                // optional derived asset must not fail the completed run.
                Err(err) => {
                    break 'compute FallbackFtsRepairOutcome::SkippedRepairFailed {
                        detail: format!("probing fallback FTS archive-fingerprint health: {err:#}"),
                    };
                }
            }
        }

        let repair = match fresh_storage.ensure_search_fallback_fts_consistency() {
            Ok(repair) => repair,
            Err(err) => {
                let detail = format!("{err:#}");
                if let Some(outcome) = nonfatal_fallback_fts_repair_outcome(detail.clone()) {
                    break 'compute outcome;
                }
                // #329/#345: internal repair failures (frankensqlite "out of
                // memory" on huge shadow rebuilds being the reported case) no
                // longer fail an `index --full` whose canonical + Tantivy work
                // already completed. Lexical search is served by Tantivy and
                // doctor treats the shadow state honestly on its next check.
                break 'compute FallbackFtsRepairOutcome::SkippedRepairFailed { detail };
            }
        };
        if let Some(archive_fingerprint) = known_archive_fingerprint
            && let Err(err) =
                fresh_storage.record_search_fallback_fts_archive_fingerprint(archive_fingerprint)
        {
            // The repair itself SUCCEEDED — only the memo that lets the next run
            // skip re-probing failed to persist, so the worst case is one
            // redundant probe/repair later. Report the repair honestly instead
            // of discarding it as SkippedRepairFailed, which logged "repair
            // failed" for a fully repaired shadow (adversarial-review F5).
            tracing::warn!(
                error = %format!("{err:#}"),
                "fallback FTS repair succeeded but recording its archive fingerprint failed; the next run will re-probe"
            );
        }
        FallbackFtsRepairOutcome::Repaired(repair)
    };

    // zn1xn (adversarial-review F5): persist a "canonical FTS shadow may be
    // half-rebuilt" marker when the repair was skipped/failed, or clear it once
    // the shadow is repaired/known-healthy, so `cass index --json` / `status`
    // surface the half-built shadow immediately. Best-effort: a marker write
    // failure must never fail a run whose canonical + Tantivy work succeeded.
    if let Err(err) = fresh_storage
        .record_fallback_fts_repair_pending(fallback_fts_repair_pending_detail(&outcome))
    {
        tracing::debug!(
            error = %format!("{err:#}"),
            "recording fallback FTS repair-pending marker failed (non-fatal)"
        );
    }
    Ok(Some(outcome))
}

/// #434 defect 3: consecutive `cass index` runs whose optional derived FTS
/// repair failed IDENTICALLY before escalation. Below the threshold the
/// failure stays a warning (canonical rows and the Tantivy index are good, so
/// exit 0 is honest for the run's own work); at the threshold a non-watch
/// `cass index` exits non-zero so cron/automation notices the persistent
/// shadow corruption instead of logging the same warning forever — the #434
/// report ate a month of identical daily warnings before a version bump turned
/// the latent damage into a hard refusal.
pub(in crate::indexer) const FTS_REPAIR_FAILURE_ESCALATION_RUNS: u64 = 5;

/// Cap on the failure detail persisted in the streak state file (identity
/// comparison and the escalation message both use the bounded form).
pub(in crate::indexer) const FTS_REPAIR_FAILURE_STREAK_DETAIL_MAX_CHARS: usize = 2048;

/// Persisted consecutive-identical-failure counter for the optional derived
/// FTS repair (#434 defect 3). Lives as a dotfile in the index state dir like
/// `.lexical-rebuild-state.json`; the DB is deliberately NOT used because one
/// failure class is "cannot open a fresh connection at all".
pub(in crate::indexer) fn fts_repair_failure_streak_path(index_path: &Path) -> PathBuf {
    index_path.join(".fts-repair-failure-streak.json")
}

pub(in crate::indexer) fn read_fts_repair_failure_streak(
    index_path: &Path,
) -> Option<(u64, String)> {
    let raw = std::fs::read_to_string(fts_repair_failure_streak_path(index_path)).ok()?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let streak = value.get("consecutive_identical_failures")?.as_u64()?;
    if streak == 0 {
        return None;
    }
    let detail = value.get("detail")?.as_str()?.to_string();
    Some((streak, detail))
}

/// Record one more derived-FTS repair failure and return the new streak
/// length: incremented when `detail` matches the persisted failure exactly
/// (bounded form), reset to 1 when the failure changed. Best-effort — a state
/// write failure never fails a run whose canonical + Tantivy work succeeded.
pub(in crate::indexer) fn record_fts_repair_failure_streak(index_path: &Path, detail: &str) -> u64 {
    let bounded: String = detail
        .chars()
        .take(FTS_REPAIR_FAILURE_STREAK_DETAIL_MAX_CHARS)
        .collect();
    let streak = match read_fts_repair_failure_streak(index_path) {
        Some((prior, prior_detail)) if prior_detail == bounded => prior.saturating_add(1),
        _ => 1,
    };
    let payload = serde_json::json!({
        "schema_version": 1,
        "consecutive_identical_failures": streak,
        "escalation_threshold_runs": FTS_REPAIR_FAILURE_ESCALATION_RUNS,
        "detail": bounded,
        "updated_at_ms": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
    });
    let _ = std::fs::create_dir_all(index_path);
    if let Err(err) =
        write_json_pretty_atomically(&fts_repair_failure_streak_path(index_path), &payload)
    {
        tracing::debug!(
            error = %err,
            "persisting derived-FTS repair failure streak failed (non-fatal)"
        );
    }
    streak
}

/// Reset the streak after any repair attempt that did not hit the corruption
/// class (repaired, already healthy, known-healthy fingerprint, or a different
/// nonfatal class — a changed failure breaks the "identical" chain anyway).
/// Writes a zeroed record rather than deleting: cass never deletes.
pub(in crate::indexer) fn clear_fts_repair_failure_streak(index_path: &Path) {
    let path = fts_repair_failure_streak_path(index_path);
    if !path.exists() {
        return;
    }
    let payload = serde_json::json!({
        "schema_version": 1,
        "consecutive_identical_failures": 0,
        "escalation_threshold_runs": FTS_REPAIR_FAILURE_ESCALATION_RUNS,
        "detail": serde_json::Value::Null,
        "updated_at_ms": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
    });
    if let Err(err) = write_json_pretty_atomically(&path, &payload) {
        tracing::debug!(
            error = %err,
            "clearing derived-FTS repair failure streak failed (non-fatal)"
        );
    }
}

/// The failure detail to persist as the "shadow may be half-rebuilt" marker for
/// an outcome — `Some` for the skipped/failed cases, `None` (clear) when the
/// shadow is repaired or known-healthy (zn1xn).
pub(in crate::indexer) fn fallback_fts_repair_pending_detail(
    outcome: &FallbackFtsRepairOutcome,
) -> Option<&str> {
    match outcome {
        FallbackFtsRepairOutcome::SkippedRepairFailed { detail }
        | FallbackFtsRepairOutcome::SkippedCorruptDerivedIndex { detail }
        | FallbackFtsRepairOutcome::SkippedUnsupportedPopulatedShadowReload { detail }
        | FallbackFtsRepairOutcome::SkippedNotViable { detail } => Some(detail.as_str()),
        FallbackFtsRepairOutcome::Repaired(_)
        | FallbackFtsRepairOutcome::SkippedKnownHealthyForFingerprint { .. } => None,
    }
}

pub(in crate::indexer) fn full_rebuild_requires_historical_restart(
    _storage: &FrankenStorage,
    _db_path: &Path,
    _canonical_sessions_before_salvage: usize,
) -> Result<bool> {
    // Historical-bundle restart probing was intentionally retired. The old
    // code always returned `false`, but still walked candidate bundle roots and
    // inspected meta markers before deciding not to restart. That work is now
    // handled by the later salvage decision path when it actually matters.
    Ok(false)
}

pub(in crate::indexer) fn lexical_rebuild_db_state_with_total_conversations(
    storage: &FrankenStorage,
    db_path: &Path,
    total_conversations: usize,
) -> Result<LexicalRebuildDbState> {
    let prep_profile = std::env::var_os("CASS_PREP_PROFILE").is_some();
    let normalize_started = Instant::now();
    let normalized_db_path = crate::normalize_path_identity(db_path)
        .to_string_lossy()
        .into_owned();
    if prep_profile {
        eprintln!(
            "CASS_PREP_PROFILE step=normalize_db_path step_ms={}",
            normalize_started.elapsed().as_millis()
        );
    }
    let fingerprint_started = Instant::now();
    let storage_fingerprint = lexical_rebuild_content_fingerprint(storage, total_conversations)?;
    if prep_profile {
        eprintln!(
            "CASS_PREP_PROFILE step=compute_storage_fingerprint step_ms={}",
            fingerprint_started.elapsed().as_millis()
        );
    }
    Ok(lexical_rebuild_db_state_from_storage_fingerprint(
        &normalized_db_path,
        total_conversations,
        0,
        storage_fingerprint,
    ))
}

pub(in crate::indexer) fn lexical_rebuild_db_state_from_storage_fingerprint(
    normalized_db_path: &str,
    total_conversations: usize,
    total_messages: usize,
    storage_fingerprint: String,
) -> LexicalRebuildDbState {
    LexicalRebuildDbState {
        db_path: normalized_db_path.to_string(),
        total_conversations,
        total_messages,
        storage_fingerprint,
    }
}

pub(in crate::indexer) fn lexical_rebuild_db_state_with_exact_totals(
    storage: &FrankenStorage,
    db_path: &Path,
    total_conversations: usize,
    total_messages: usize,
) -> Result<LexicalRebuildDbState> {
    let normalized_db_path = crate::normalize_path_identity(db_path)
        .to_string_lossy()
        .into_owned();
    Ok(lexical_rebuild_db_state_from_storage_fingerprint(
        &normalized_db_path,
        total_conversations,
        total_messages,
        lexical_rebuild_content_fingerprint(storage, total_conversations)?,
    ))
}

pub(in crate::indexer) fn deferred_lexical_rebuild_db_state(
    db_path: &Path,
    total_conversations: usize,
) -> LexicalRebuildDbState {
    LexicalRebuildDbState {
        db_path: crate::normalize_path_identity(db_path)
            .to_string_lossy()
            .into_owned(),
        total_conversations,
        total_messages: 0,
        storage_fingerprint: lexical_rebuild_deferred_content_fingerprint(total_conversations),
    }
}

#[cfg(test)]
pub(in crate::indexer) fn lexical_rebuild_db_state(
    storage: &FrankenStorage,
    db_path: &Path,
) -> Result<LexicalRebuildDbState> {
    let total_conversations = count_total_conversations_exact(storage)?;
    lexical_rebuild_db_state_with_total_conversations(storage, db_path, total_conversations)
}
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct LexicalRebuildSnapshot {
    pub db_path: String,
    pub total_conversations: usize,
    pub storage_fingerprint: String,
    pub committed_offset: i64,
    pub committed_conversation_id: Option<i64>,
    pub processed_conversations: usize,
    pub indexed_docs: usize,
    pub completed: bool,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct LexicalRebuildCheckpoint {
    pub db_path: String,
    pub total_conversations: usize,
    pub storage_fingerprint: String,
    pub committed_offset: i64,
    pub committed_conversation_id: Option<i64>,
    pub processed_conversations: usize,
    pub indexed_docs: usize,
    /// Task 5: the share of `indexed_docs` that is search-region
    /// documents; `indexed_docs - indexed_region_docs` is the canonical
    /// message count.
    pub indexed_region_docs: usize,
    pub schema_hash: String,
    pub page_size: i64,
    pub completed: bool,
    pub updated_at_ms: i64,
}

#[cfg(test)]
pub(in crate::indexer) mod fallback_fts_repair_pending_tests {
    use super::{FallbackFtsRepairOutcome, fallback_fts_repair_pending_detail};
    use crate::storage::sqlite::FtsConsistencyRepair;

    #[test]
    fn only_broken_outcomes_mark_the_shadow_pending() {
        assert_eq!(
            fallback_fts_repair_pending_detail(&FallbackFtsRepairOutcome::SkippedRepairFailed {
                detail: "boom".to_string()
            }),
            Some("boom")
        );
        assert_eq!(
            fallback_fts_repair_pending_detail(
                &FallbackFtsRepairOutcome::SkippedCorruptDerivedIndex {
                    detail: "corrupt".to_string()
                }
            ),
            Some("corrupt")
        );
        assert_eq!(
            fallback_fts_repair_pending_detail(
                &FallbackFtsRepairOutcome::SkippedUnsupportedPopulatedShadowReload {
                    detail: "unsupported".to_string()
                }
            ),
            Some("unsupported")
        );
        // Repaired / known-healthy outcomes clear the marker (None).
        assert_eq!(
            fallback_fts_repair_pending_detail(
                &FallbackFtsRepairOutcome::SkippedKnownHealthyForFingerprint {
                    archive_fingerprint: "fp".to_string()
                }
            ),
            None
        );
        assert_eq!(
            fallback_fts_repair_pending_detail(&FallbackFtsRepairOutcome::Repaired(
                FtsConsistencyRepair::AlreadyHealthy { rows: 3 }
            )),
            None
        );
    }
}

/// #434 defect 3: the persisted consecutive-identical-failure counter behind
/// the warn-forever → non-zero-exit escalation of `cass index`.
#[cfg(test)]
pub(in crate::indexer) mod fts_repair_failure_streak_tests {
    use super::{
        FTS_REPAIR_FAILURE_ESCALATION_RUNS, clear_fts_repair_failure_streak,
        fts_repair_failure_streak_path, read_fts_repair_failure_streak,
        record_fts_repair_failure_streak,
    };

    #[test]
    fn identical_failures_increment_and_changed_failures_reset() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index_path = tmp.path().join("index");

        assert_eq!(read_fts_repair_failure_streak(&index_path), None);
        assert_eq!(record_fts_repair_failure_streak(&index_path, "boom"), 1);
        assert_eq!(record_fts_repair_failure_streak(&index_path, "boom"), 2);
        // A different failure breaks the "identical" chain.
        assert_eq!(record_fts_repair_failure_streak(&index_path, "other"), 1);
        for expected in 2..=FTS_REPAIR_FAILURE_ESCALATION_RUNS {
            assert_eq!(
                record_fts_repair_failure_streak(&index_path, "other"),
                expected
            );
        }
        let (streak, detail) =
            read_fts_repair_failure_streak(&index_path).expect("streak persisted");
        assert_eq!(streak, FTS_REPAIR_FAILURE_ESCALATION_RUNS);
        assert_eq!(detail, "other");
    }

    #[test]
    fn clearing_zeroes_the_counter_without_deleting_the_state_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index_path = tmp.path().join("index");
        assert_eq!(record_fts_repair_failure_streak(&index_path, "boom"), 1);

        clear_fts_repair_failure_streak(&index_path);
        assert_eq!(
            read_fts_repair_failure_streak(&index_path),
            None,
            "a cleared streak must read back as no streak"
        );
        assert!(
            fts_repair_failure_streak_path(&index_path).exists(),
            "clearing zeroes the record in place — cass never deletes"
        );
        // The next failure restarts from 1.
        assert_eq!(record_fts_repair_failure_streak(&index_path, "boom"), 1);
    }

    #[test]
    fn clearing_a_never_written_streak_is_a_no_op() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index_path = tmp.path().join("index");
        clear_fts_repair_failure_streak(&index_path);
        assert!(!fts_repair_failure_streak_path(&index_path).exists());
        assert_eq!(read_fts_repair_failure_streak(&index_path), None);
    }
}
