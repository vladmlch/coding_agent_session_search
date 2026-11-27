use std::path::{Path, PathBuf};

use crate::storage::sqlite::{
    FrankenStorage, FtsConsistencyRepair, FtsDryRunParity, FtsShadowParity, FtsShadowParityStatus,
};
use crate::{CliError, CliResult, RobotFormat, default_data_dir};

use super::{
    ENGINE_REPAIRS_MISSING_FTS5_SHADOW_AUTOINDEX, fts_dry_run_rowid_comparison_cap,
    fts5_oversized_leaf_shadow_error, is_fts_shadow_schema_level_open_failure,
    is_fts5_oversized_leaf_error, is_fts5_shadow_open_corruption_error,
    is_missing_fts5_shadow_autoindex_failure, legacy_fts_shadow_catalog_unrepairable_error,
    print_json, resolve_db_path, storage_error,
};

fn fts_parity_json(parity: &FtsShadowParity) -> serde_json::Value {
    serde_json::json!({
        "status": parity.status.as_str(),
        "canonical_messages": parity.canonical_messages,
        "indexable_messages": parity.indexable_messages,
        "indexed_messages": parity.indexed_messages,
        "detail": parity.detail,
    })
}

fn planned_fts_repair(status: FtsShadowParityStatus) -> &'static str {
    match status {
        FtsShadowParityStatus::Absent => "failure_atomic_recreate",
        FtsShadowParityStatus::Residue => "drop_residue_and_recreate_from_canonical",
        FtsShadowParityStatus::Healthy => "verify_and_record_generation",
        FtsShadowParityStatus::Partial => "resumable_incremental_catch_up",
        FtsShadowParityStatus::Excess | FtsShadowParityStatus::Divergent => {
            "refuse_unsafe_destructive_rebuild"
        }
        FtsShadowParityStatus::Unqueryable => "refuse_unqueryable_preserve_bundle",
    }
}

fn fts_repair_is_applicable(status: FtsShadowParityStatus) -> bool {
    match status {
        FtsShadowParityStatus::Absent
        | FtsShadowParityStatus::Residue
        | FtsShadowParityStatus::Healthy
        | FtsShadowParityStatus::Partial => true,
        FtsShadowParityStatus::Excess
        | FtsShadowParityStatus::Divergent
        | FtsShadowParityStatus::Unqueryable => false,
    }
}

fn fts_dry_run_parity_json(parity: &FtsDryRunParity) -> serde_json::Value {
    serde_json::json!({
        "status": parity.status_as_str(),
        "canonical_messages": parity.canonical_messages,
        "indexable_messages": parity.indexable_messages,
        "indexed_messages": parity.indexed_messages,
        "inspection_complete": parity.inspection_complete,
        "comparison_cap": parity.comparison_cap,
        "canonical_ids_examined": parity.canonical_ids_examined,
        "indexed_ids_examined": parity.indexed_ids_examined,
        "observed_missing_canonical_rowids_at_least": parity.observed_missing_canonical_rowids_at_least,
        "observed_excess_fts_rowids_at_least": parity.observed_excess_fts_rowids_at_least,
        "divergent_rowids_at_least": parity.divergent_rowids_at_least(),
        "detail": parity.detail,
    })
}

/// `retirement_recorded` is [`FrankenStorage::fts_shadow_retirement_is_current`]:
/// no registration, no leftover shadow table, and both retirement markers
/// already describing the current corpus and bound.
pub(super) fn fts_rebuild_dry_run_envelope(
    db_path: &Path,
    parity: &FtsDryRunParity,
    retirement_recorded: bool,
) -> serde_json::Value {
    let exceeds_bound = crate::storage::sqlite::fts_shadow_max_messages()
        .is_some_and(|bound| u64::try_from(parity.indexable_messages).unwrap_or(0) > bound);
    // GH #497 follow-up: an oversized corpus whose shadow is already retired
    // is settled. The apply would only re-report the retirement, so the plan
    // must not claim a mutation.
    let settled_retirement = exceeds_bound
        && retirement_recorded
        && parity.exact_status == Some(FtsShadowParityStatus::Absent);
    let applicable = if settled_retirement {
        Some(false)
    } else {
        parity.exact_status.map(fts_repair_is_applicable)
    };
    let planned_action = match parity.exact_status {
        _ if settled_retirement => "none_shadow_retired_not_viable",
        Some(FtsShadowParityStatus::Residue) if exceeds_bound => "drop_residue_and_mark_not_viable",
        Some(FtsShadowParityStatus::Absent) if exceeds_bound => "record_not_viable_retirement",
        Some(status) => planned_fts_repair(status),
        None => "exact_parity_inspection_deferred_to_apply",
    };
    let apply_command = match applicable {
        Some(true) | None => Some("cass doctor --rebuild-canonical-fts --yes --json"),
        Some(false) => None,
    };
    serde_json::json!({
        "schema_version": 1,
        "doctor_contract_version": 1,
        "kind": "rebuild_canonical_fts_dry_run",
        "dry_run": true,
        "db_path": db_path.display().to_string(),
        "parity": fts_dry_run_parity_json(parity),
        "planned_action": planned_action,
        "would_mutate": applicable,
        "shadow_retired_not_viable": settled_retirement,
        "canonical_rows_modified": false,
        "apply_command": apply_command,
        "note": "Read-only bounded inspection only; an indeterminate result never means healthy. The --yes path performs exact parity validation before any mutation, and --yes never overrides --dry-run.",
    })
}

fn emit_corrupt_shadow_dry_run(
    detail: String,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    // A dry-run must stay read-only and non-locking: report the planned repair
    // straight from the diagnostic without mutating the archive.
    let envelope = serde_json::json!({
        "surface": "doctor_rebuild_canonical_fts_dry_run",
        "status": "shadow_structure_corrupt",
        "planned_action": "drop_recreate_rebuild_from_canonical",
        "detail": detail,
    });
    if structured_format.is_some() {
        print_json(&envelope)?;
    } else {
        println!(
            "Canonical FTS5 dry-run: status=shadow_structure_corrupt, planned_action=drop_recreate_rebuild_from_canonical; re-run with --yes to drop, recreate, and rebuild the corrupt shadow from canonical"
        );
    }
    Ok(())
}

fn rebuild_from_corrupt_shadow(
    storage: &FrankenStorage,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let inserted = storage
        .rebuild_fts_shadow_via_drop_recreate()
        .map_err(|e| {
            storage_error(
                format!("rebuilding corrupt canonical FTS5 shadow via drop+recreate: {e:#}"),
                Some("Preserve the archive bundle and run 'cass doctor check --json'."),
            )
        })?;
    let envelope = serde_json::json!({
        "surface": "doctor_rebuild_canonical_fts",
        "status": "rebuilt_from_corrupt_shadow",
        "method": "drop_recreate_rebuild_from_canonical",
        "inserted_messages": inserted,
    });
    if structured_format.is_some() {
        print_json(&envelope)?;
    } else {
        println!(
            "Canonical FTS5 shadow was structurally corrupt; dropped, recreated, and rebuilt {inserted} message(s) from canonical rows."
        );
    }
    Ok(())
}

/// Verify and safely repair the canonical FTS5 shadow tables in place.
///
/// Queryable partial shadows are retained and caught up in bounded, resumable
/// batches. An absent shadow is created in a transaction so interruption
/// cannot publish a partial table. Unqueryable or divergent artifacts are
/// preserved for bundle-level recovery rather than destroyed in place. Exact
/// canonical/indexable/FTS parity is required before success.
pub fn run_doctor_rebuild_canonical_fts(
    data_dir_override: Option<PathBuf>,
    db_override: Option<PathBuf>,
    dry_run: bool,
    yes: bool,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let db_path = resolve_db_path(&data_dir, db_override.as_deref());

    if !dry_run && !yes {
        return Err(CliError {
            code: 4,
            kind: "refused-unsafe",
            message: "`cass doctor --rebuild-canonical-fts` mutates the canonical archive's derived FTS5 shadow and requires `--yes`".to_string(),
            hint: Some(
                "Inspect first with `--rebuild-canonical-fts --dry-run --json`, then re-run with `--rebuild-canonical-fts --yes` only when the plan is applicable. Queryable partial shadows are caught up in place and absent shadows are created failure-atomically; unqueryable/divergent artifacts are preserved for bundle-level recovery. Canonical rows are never modified.".to_string(),
            ),
            retryable: false,
        });
    }

    if !db_path.exists() {
        return Err(storage_error(
            format!("canonical archive {} does not exist", db_path.display()),
            Some("Recover the source tree with 'cass doctor --recover-from-archive <DIR>' first."),
        ));
    }

    // GH #495: every mutating FTS repair shares the authoritative index-run
    // lock with index/watch. This prevents residue cleanup from racing a
    // watcher that still has the old virtual-table shape loaded.
    let _mutation_guard = if dry_run {
        None
    } else {
        Some(
            crate::indexer::acquire_search_maintenance_mutation_lock(
                &data_dir,
                &db_path,
                crate::search::asset_state::SearchMaintenanceJobKind::LexicalRefresh,
            )
            .map_err(|err| {
                storage_error(
                    format!(
                        "acquiring exclusive index-run lock for canonical FTS5 repair: {err:#}"
                    ),
                    Some("Stop the active cass index/watch process, then retry the repair."),
                )
            })?,
        )
    };

    let storage_open = if dry_run {
        FrankenStorage::open_readonly(&db_path)
    } else {
        FrankenStorage::open_existing_schema_only_for_fts_repair(&db_path)
    };
    let storage = match storage_open {
        Ok(storage) => storage,
        // #368 defect 3 / #434 defect 2: the FTS5 shadow is corrupt enough that
        // the archive cannot be opened normally (the schema reload decodes the
        // corrupt %_data, or refuses a shadow object's sqlite_master state).
        // Open with FTS5 validation DEFERRED or rebuild the shadow by dropping
        // + recreating it from canonical rows — the shadow is fully derived
        // and canonical rows are never touched.
        Err(open_err)
            if is_fts5_shadow_open_corruption_error(&open_err)
                || is_fts_shadow_schema_level_open_failure(&open_err) =>
        {
            if !ENGINE_REPAIRS_MISSING_FTS5_SHADOW_AUTOINDEX
                && is_missing_fts5_shadow_autoindex_failure(&open_err)
            {
                return Err(legacy_fts_shadow_catalog_unrepairable_error(
                    &db_path, &open_err,
                ));
            }
            if dry_run {
                return emit_corrupt_shadow_dry_run(format!("{open_err:#}"), structured_format);
            }
            let deferred = FrankenStorage::open_deferred_fts5_for_repair(&db_path).map_err(|e| {
                // #434 defect 2: BOTH structured opens failed. Say so
                // explicitly instead of repeating the refusal, and route to
                // the checksum-verified raw-mirror recovery path.
                storage_error(
                    format!(
                        "no structured open of {} is possible: the ordinary open failed ({open_err:#}) and the deferred-FTS5 repair open also failed ({e:#})",
                        db_path.display()
                    ),
                    Some(
                        "The archive's schema state is damaged beyond what the deferred-FTS5 repair open tolerates, so doctor cannot repair the derived shadow in place. Preserve the complete archive bundle (db + -wal + -shm + sidecars) and recover instead: 'cass doctor check --json' ranks the recovery authorities, 'cass doctor --fix --json' verifies the checksum-verified raw mirror and stages an isolated reconstruct candidate, and 'cass doctor --recover-from-archive <DIR>' rebuilds the source tree from the archive's preserved events when the archive is still readable read-only.",
                    ),
                )
            })?;
            return rebuild_from_corrupt_shadow(&deferred, structured_format);
        }
        Err(open_err) => {
            return Err(storage_error(
                format!(
                    "could not open canonical archive {} for FTS5 inspection: {open_err:#}",
                    db_path.display()
                ),
                Some(
                    // #434 defect 1: an open failure proves a schema-level open
                    // failure, NOT that canonical rows are unreadable (stock
                    // readers served every canonical row of the #434 archive).
                    "The archive failed to open at the schema level; that does not by itself mean \
                     the canonical rows are unreadable. Run 'cass doctor check --json' to see what \
                     is actually readable and which recovery authority doctor ranks first. If \
                     doctor confirms the canonical rows are unreadable, 'cass doctor \
                     --recover-from-archive <DIR>' rebuilds the source tree from the archive's \
                     preserved events.",
                ),
            ));
        }
    };
    if let Err(probe_err) = storage
        .raw()
        .query("SELECT rowid FROM fts_messages WHERE fts_messages MATCH 'probe' LIMIT 1")
    {
        let probe_err = anyhow::Error::new(probe_err);
        if is_fts5_shadow_open_corruption_error(&probe_err) {
            if dry_run {
                return emit_corrupt_shadow_dry_run(format!("{probe_err:#}"), structured_format);
            }
            return rebuild_from_corrupt_shadow(&storage, structured_format);
        }
    }
    if dry_run {
        let before = storage
            .inspect_search_fallback_fts_parity_dry_run(fts_dry_run_rowid_comparison_cap())
            .map_err(|e| {
                storage_error(
                    format!("performing bounded canonical/FTS5 row-parity inspection: {e:#}"),
                    Some(
                        "Preserve the canonical archive bundle and run 'cass doctor check --json' before retrying.",
                    ),
                )
            })?;
        let retirement_recorded = storage.fts_shadow_retirement_is_current().map_err(|e| {
            storage_error(
                format!("inspecting the fallback FTS shadow retirement markers: {e:#}"),
                Some(
                    "Preserve the canonical archive bundle and run 'cass doctor check --json' before retrying.",
                ),
            )
        })?;
        let envelope = fts_rebuild_dry_run_envelope(&db_path, &before, retirement_recorded);
        if structured_format.is_some() {
            print_json(&envelope)?;
        } else {
            println!(
                "Canonical FTS5 dry-run: status={}, inspection_complete={}, canonical={}, indexable={}, indexed={:?}, rowid_cap={}, divergent>={}{}",
                before.status_as_str(),
                before.inspection_complete,
                before.canonical_messages,
                before.indexable_messages,
                before.indexed_messages,
                before.comparison_cap,
                before.divergent_rowids_at_least(),
                if before.inspection_complete {
                    ""
                } else {
                    " (capped; a divergence floor, not an exact count — raise CASS_FTS_DRYRUN_CAP or run --yes for exact parity)"
                },
            );
        }
        return Ok(());
    }

    let before = storage.inspect_search_fallback_fts_parity().map_err(|e| {
        storage_error(
            format!("inspecting exact canonical/FTS5 row parity before repair: {e:#}"),
            Some(
                "Preserve the canonical archive bundle and run 'cass doctor check --json' before retrying.",
            ),
        )
    })?;

    let retirement_recorded_before = storage.fts_shadow_retirement_is_current().map_err(|e| {
        storage_error(
            format!("inspecting the fallback FTS shadow retirement markers before repair: {e:#}"),
            Some(
                "Preserve the canonical archive bundle and run 'cass doctor check --json' before retrying.",
            ),
        )
    })?;
    let repair = match storage.ensure_search_fallback_fts_consistency() {
        Ok(repair) => repair,
        Err(e) if is_fts5_shadow_open_corruption_error(&e) => {
            return rebuild_from_corrupt_shadow(&storage, structured_format);
        }
        Err(e) if fts_shadow_not_viable_detail_in(&e).is_some() => {
            return report_fts_shadow_retired(
                &storage,
                &db_path,
                &before,
                retirement_recorded_before,
                &e,
                structured_format,
            );
        }
        Err(e) => {
            return Err(if is_fts5_oversized_leaf_error(&e) {
                // GH #369: a known, content-dependent engine limitation — not
                // archive corruption. Surface a distinct, reassuring diagnostic
                // instead of the generic storage wall so operators do not treat
                // a working (Tantivy-served) search as broken.
                fts5_oversized_leaf_shadow_error(&db_path)
            } else {
                storage_error(
                    format!("safely repairing canonical FTS5 shadow tables: {e:#}"),
                    Some(
                        "Preserve the complete database bundle. Re-run the dry-run to inspect exact current parity before any retry.",
                    ),
                )
            });
        }
    };
    // GH #438: an explicit repair of a shadow whose rows already match must
    // still rewrite segments written by an older engine; queryable rows do not
    // prove a valid segment format. Optimize rewrites them with the current
    // writer (a no-op on an already optimized index) and the FTS5
    // integrity-check then validates the result.
    let segments_optimized = if matches!(repair, FtsConsistencyRepair::AlreadyHealthy { .. }) {
        if let Err(e) = storage
            .optimize_fts_messages_segments()
            .and_then(|()| storage.validate_fts_messages_integrity())
        {
            if is_fts5_shadow_open_corruption_error(&e) {
                return rebuild_from_corrupt_shadow(&storage, structured_format);
            }
            return Err(storage_error(
                format!("rewriting existing FTS5 segments of a parity-healthy shadow: {e:#}"),
                Some(
                    "Canonical rows are unchanged. Re-run the dry-run; if segment rewriting keeps failing, remove the derived shadow with `cass index --full` after preserving the database bundle.",
                ),
            ));
        }
        true
    } else {
        false
    };
    let after = storage.inspect_search_fallback_fts_parity().map_err(|e| {
        storage_error(
            format!("validating canonical/FTS5 parity after repair: {e:#}"),
            Some("Repair is not complete until exact parity validation succeeds."),
        )
    })?;
    if after.status != FtsShadowParityStatus::Healthy {
        return Err(storage_error(
            format!(
                "canonical FTS5 repair did not reach exact parity: status={}, indexable={}, indexed={:?}",
                after.status.as_str(),
                after.indexable_messages,
                after.indexed_messages
            ),
            Some("Re-run the dry-run; do not treat this repair as complete."),
        ));
    }
    // GH #497 follow-up: exact parity is verified, so the "shadow may be
    // half-rebuilt" marker (left by a failed/skipped index repair or by a
    // size retirement) no longer describes this archive. Clear it here, or
    // `cass status` keeps reporting `fallback_fts_repair.pending` until the
    // next full index run.
    let repair_pending_cleared = match storage.read_fallback_fts_repair_pending() {
        Ok(Some(_)) => {
            storage
                .record_fallback_fts_repair_pending(None)
                .map_err(|e| {
                    storage_error(
                        format!(
                            "clearing the fallback FTS repair-pending marker after a verified repair: {e:#}"
                        ),
                        Some(
                            "The shadow itself was repaired and verified; re-run the same command to clear the stale marker.",
                        ),
                    )
                })?;
            true
        }
        Ok(None) => false,
        Err(e) => {
            return Err(storage_error(
                format!("reading the fallback FTS repair-pending marker after repair: {e:#}"),
                Some(
                    "The shadow itself was repaired and verified; re-run the same command to clear the stale marker.",
                ),
            ));
        }
    };
    let (repair_kind, inserted_rows) = match repair {
        FtsConsistencyRepair::AlreadyHealthy { .. } => ("already_healthy", 0),
        FtsConsistencyRepair::IncrementalCatchUp { inserted_rows, .. } => {
            ("resumable_incremental_catch_up", inserted_rows)
        }
        FtsConsistencyRepair::Rebuilt { inserted_rows } => {
            ("failure_atomic_recreate", inserted_rows)
        }
    };

    let envelope = serde_json::json!({
        "schema_version": 1,
        "doctor_contract_version": 1,
        "kind": "rebuild_canonical_fts",
        "db_path": db_path.display().to_string(),
        "repair_kind": repair_kind,
        "inserted_rows": inserted_rows,
        "segments_optimized": segments_optimized,
        "repair_pending_marker_cleared": repair_pending_cleared,
        "parity_before": fts_parity_json(&before),
        "parity_after": fts_parity_json(&after),
        "mutated_asset_class": "canonical_fts5_shadow",
        "canonical_rows_modified": false,
        "note": "Queryable shadows are preserved and caught up resumably; recreation is transactionally published only after exact parity validation. Canonical rows are never modified.",
    });

    if structured_format.is_some() {
        print_json(&envelope)?;
    } else {
        println!(
            "Canonical FTS5 repair complete ({repair_kind}{}, {inserted_rows} rows inserted, {} rows indexed) in {}",
            if segments_optimized {
                ", existing segments rewritten with the current writer"
            } else {
                ""
            },
            after.indexable_messages,
            db_path.display()
        );
    }
    Ok(())
}

/// The not-viable detail carried by a repair error, if the repair stopped
/// because the corpus is over `CASS_FTS_SHADOW_MAX_MESSAGES`.
fn fts_shadow_not_viable_detail_in(error: &anyhow::Error) -> Option<String> {
    error.chain().map(ToString::to_string).find(|message| {
        crate::storage::sqlite::error_message_indicates_fts_shadow_not_viable(message)
    })
}

/// GH #497 follow-up: the repair stopped because the corpus is over the
/// shadow bound. When the archive now holds the current retirement (no
/// registration, no shadow table, both markers describing this corpus and
/// bound), that is the converged state
/// for this corpus, so report it as a completed repair (exit 0,
/// `repair_kind = retired_not_viable`) instead of a storage failure.
/// `shadow_mutated` says whether this run retired the shadow or found the
/// retirement already recorded.
fn report_fts_shadow_retired(
    storage: &FrankenStorage,
    db_path: &Path,
    before: &FtsShadowParity,
    retirement_recorded_before: bool,
    error: &anyhow::Error,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let detail = fts_shadow_not_viable_detail_in(error).unwrap_or_else(|| format!("{error:#}"));
    let retired = storage.fts_shadow_retirement_is_current().map_err(|e| {
        storage_error(
            format!("verifying the fallback FTS shadow retirement after repair: {e:#}"),
            Some("Repair is not complete until the retirement is verified."),
        )
    })?;
    if !retired {
        return Err(storage_error(
            format!(
                "safely repairing canonical FTS5 shadow tables: {detail}; the retirement was not \
                 recorded (a derived FTS object remains, or a retirement marker is missing or \
                 does not describe the current corpus)"
            ),
            Some(
                "Preserve the complete database bundle. Re-run the dry-run to inspect exact current parity before any retry.",
            ),
        ));
    }
    let after = storage.inspect_search_fallback_fts_parity().map_err(|e| {
        storage_error(
            format!("validating the retired canonical FTS5 shadow: {e:#}"),
            Some("Repair is not complete until the retirement is verified."),
        )
    })?;
    let shadow_mutated = !retirement_recorded_before;
    let envelope = serde_json::json!({
        "schema_version": 1,
        "doctor_contract_version": 1,
        "kind": "rebuild_canonical_fts",
        "db_path": db_path.display().to_string(),
        "repair_kind": "retired_not_viable",
        "inserted_rows": 0,
        "segments_optimized": false,
        "shadow_retired": true,
        "shadow_mutated": shadow_mutated,
        "detail": detail,
        "parity_before": fts_parity_json(before),
        "parity_after": fts_parity_json(&after),
        "mutated_asset_class": shadow_mutated.then_some("canonical_fts5_shadow"),
        "canonical_rows_modified": false,
        "note": "The canonical corpus is over CASS_FTS_SHADOW_MAX_MESSAGES, so the derived SQL-fallback FTS shadow is retired rather than rebuilt; this is the settled state for this archive, not a failure. Quill lexical search is unaffected. Canonical rows are never modified.",
    });
    if structured_format.is_some() {
        print_json(&envelope)?;
    } else if shadow_mutated {
        println!(
            "Canonical FTS5 shadow retired (not viable for this corpus) in {}: {detail}",
            db_path.display()
        );
    } else {
        println!(
            "Canonical FTS5 shadow is already retired (not viable for this corpus); nothing to do in {}: {detail}",
            db_path.display()
        );
    }
    Ok(())
}
