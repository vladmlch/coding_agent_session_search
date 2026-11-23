use super::*;

pub(in crate::indexer) struct RunIndexLexicalOutcome {
    pub(in crate::indexer) t_index: Option<TantivyIndex>,
    pub(in crate::indexer) performed_scan: bool,
    pub(in crate::indexer) scan_canonical_mutations: CanonicalMutationCounts,
    pub(in crate::indexer) scan_lexical_update_deferred: bool,
    pub(in crate::indexer) scanned_connectors: BTreeSet<String>,
    pub(in crate::indexer) scan_had_errors: bool,
    pub(in crate::indexer) stale_index_ingest_quarantine_retry_attempted: bool,
    pub(in crate::indexer) deferred_watch_sources: BTreeSet<PathBuf>,
    pub(in crate::indexer) exact_completed_lexical_checkpoint: bool,
    pub(in crate::indexer) exact_completed_lexical_checkpoint_predates_scan: bool,
    pub(in crate::indexer) skipped_noop_full_scan_authoritative_rebuild: bool,
    pub(in crate::indexer) targeted_watch_once_only_run: bool,
}

pub(in crate::indexer) fn run_index_lexical_and_scan(
    state: RunIndexPreflightOutcome,
    local_connector_roots: LocalConnectorRootsOverride,
) -> Result<(RunIndexPreflightOutcome, RunIndexLexicalOutcome)> {
    let RunIndexPreflightOutcome {
        mut ctx,
        mut storage,
        index_path,
        defer_checkpoints,
        canonical_storage_rebuilt,
        opened_fresh_for_full,
        reopened_after_writable_preflight,
        mut legacy_omp_upgrade,
        initial_canonical_sessions_before_salvage,
        canonical_only_full_rebuild,
        has_explicit_watch_once_paths,
        targeted_semantic_watch_once,
        resume_lexical_rebuild,
        restart_pending_lexical_rebuild_from_zero,
        initial_matching_lexical_checkpoint,
        reconcile_nightly_lexical,
        checked_daily_stats_pre_scan,
        tantivy_requires_rebuild,
        observed_tantivy_docs,
        mut needs_rebuild,
        initial_needs_rebuild,
        large_incremental_authoritative_lexical_repair_probe_deferred,
        deferred_incremental_authoritative_lexical_repair,
        empty_archive_lexical_rebuild,
        scan_start_ts,
        keep_tantivy_open_after_rebuild,
    } = state;
    let mut performed_scan = false;
    let mut scan_canonical_mutations = CanonicalMutationCounts::default();
    let mut scan_lexical_update_deferred = false;
    let mut scanned_connectors = BTreeSet::new();
    let mut scan_had_errors = false;
    let mut stale_index_ingest_quarantine_retry_attempted = false;
    let mut deferred_watch_sources = BTreeSet::new();
    let mut exact_completed_lexical_checkpoint = false;
    // GH #457 follow-on: set when an authoritative rebuild persisted its exact
    // completed checkpoint BEFORE this run's incremental source scan — i.e. by
    // the pre-scan sparse/invalid repair. The canonical-only full rebuild runs
    // no scan at all and the post-scan rebuilds run after ingest, so that path
    // is the only one whose checkpoint (and whose exact row counts recorded in
    // progress) can be invalidated by the rest of its own run.
    let mut exact_completed_lexical_checkpoint_predates_scan = false;
    let mut skipped_noop_full_scan_authoritative_rebuild = false;
    let mut targeted_watch_once_only_run = false;
    let t_index = if resume_lexical_rebuild {
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            "resuming incomplete lexical rebuild from canonical database checkpoint"
        );
        record_lexical_population_strategy(
            ctx.opts.progress.as_ref(),
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            "resume_incomplete_authoritative_db_rebuild_from_checkpoint",
        );
        tracing::info!(
            strategy = LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild.as_str(),
            reason = "resume_incomplete_authoritative_db_rebuild_from_checkpoint",
            "selected_lexical_population_strategy"
        );
        ensure_authoritative_lexical_rebuild_storage_headroom(
            &ctx.opts.data_dir,
            &ctx.opts.db_path,
        )?;
        ctx.enter_work_phase("lexical:rebuild")?;
        let rebuild = if restart_pending_lexical_rebuild_from_zero {
            rebuild_tantivy_from_db_deferred_startup_with_progress_bump(
                &ctx.opts.db_path,
                &ctx.opts.data_dir,
                initial_canonical_sessions_before_salvage,
                ctx.opts.progress.clone(),
                Arc::clone(&ctx.progress_bump),
            )?
        } else {
            rebuild_tantivy_from_db_with_progress_bump(
                &ctx.opts.db_path,
                &ctx.opts.data_dir,
                initial_canonical_sessions_before_salvage,
                ctx.opts.progress.clone(),
                Arc::clone(&ctx.progress_bump),
            )?
        };
        exact_completed_lexical_checkpoint = rebuild.exact_checkpoint_persisted;
        // Populate stats for resumed lexical rebuild path
        if let Some(p) = &ctx.opts.progress
            && let Ok(mut stats) = p.stats.lock()
        {
            stats.total_conversations = initial_canonical_sessions_before_salvage;
        }
        if let Some(observed_messages) = rebuild.observed_messages {
            record_exact_total_counts_in_progress(
                ctx.opts.progress.as_ref(),
                initial_canonical_sessions_before_salvage,
                observed_messages,
            );
        }
        if keep_tantivy_open_after_rebuild {
            Some(open_lexical_index_with_heartbeat(
                &index_path,
                ctx.opts.progress.as_ref(),
            )?)
        } else {
            None
        }
    } else {
        let mut t_index: Option<TantivyIndex> = None;

        if ctx.opts.full && !opened_fresh_for_full && initial_canonical_sessions_before_salvage == 0
        {
            // NOTE: We deliberately do NOT reset either SQLite or Tantivy here.
            // An empty canonical archive can be populated in place, and the
            // Tantivy index will be atomically replaced by rebuild_tantivy_from_db()
            // at the end of a successful --full rebuild. Eagerly deleting or
            // replacing anything is both redundant and dangerous if the scan or
            // rebuild OOMs / hits a constraint error. (CASS #164)
        } else if ctx.opts.full {
            // Same rationale — skip eager delete_all(); rebuild_tantivy_from_db()
            // handles starting fresh after the scan succeeds.  (CASS #164)
        }

        let canonical_sessions_before_salvage = initial_canonical_sessions_before_salvage;
        // See CASS #153: plain --full must always rescan the filesystem.
        // --force-rebuild with existing sessions skips rescan (fast path).
        //
        // cass#272 performance follow-up: routine incremental refreshes on a
        // populated canonical archive should not probe historical bundles at
        // startup. Discovery opens candidate backup/snapshot DBs and can wedge
        // in fsqlite page-cache eviction before real indexing starts. Keep the
        // automatic path for explicit full/rebuilt/empty recovery, and expose
        // the old populated-incremental discovery behavior behind
        // CASS_PREFLIGHT_HISTORICAL_SALVAGE_DISCOVERY=1.
        let probe_pending_historical_bundles = should_probe_pending_historical_bundles(
            ctx.opts.full,
            canonical_storage_rebuilt,
            canonical_sessions_before_salvage,
            canonical_only_full_rebuild,
            preflight_historical_salvage_discovery_enabled(),
        );
        let mut has_pending_historical_bundles = if probe_pending_historical_bundles {
            storage.has_pending_historical_bundles(&ctx.opts.db_path)?
        } else {
            tracing::debug!(
                target: "cass::historical_salvage",
                db_path = %ctx.opts.db_path.display(),
                canonical_sessions_before_salvage,
                canonical_storage_rebuilt,
                canonical_only_full_rebuild,
                "skipping opt-in historical bundle discovery during populated incremental index startup"
            );
            false
        };
        let targeted_watch_once_only = should_run_targeted_watch_once_only(
            has_explicit_watch_once_paths,
            ctx.opts.watch,
            ctx.opts.full,
            needs_rebuild,
            canonical_sessions_before_salvage,
        );
        targeted_watch_once_only_run = targeted_watch_once_only;
        let should_salvage_historical = !targeted_watch_once_only
            && should_salvage_historical_databases(
                canonical_storage_rebuilt,
                canonical_sessions_before_salvage,
                has_pending_historical_bundles,
                canonical_only_full_rebuild,
            );
        tracing::warn!(
            db_path = %ctx.opts.db_path.display(),
            canonical_storage_rebuilt,
            reopened_after_writable_preflight,
            opened_fresh_for_full,
            canonical_sessions_before_salvage,
            has_pending_historical_bundles,
            probe_pending_historical_bundles,
            canonical_only_full_rebuild,
            targeted_watch_once_only,
            should_salvage_historical,
            "historical salvage decision"
        );
        ctx.enter_preflight_phase("watch_startup:historical_salvage")?;
        let historical_salvage: HistoricalSalvageOutcome = if targeted_watch_once_only {
            tracing::info!(
                db_path = %ctx.opts.db_path.display(),
                "skipping historical salvage because targeted watch-once paths were supplied"
            );
            HistoricalSalvageOutcome::default()
        } else if should_salvage_historical {
            let mut outcome = HistoricalSalvageOutcome::default();
            if canonical_sessions_before_salvage == 0 {
                let (reopened_storage, seed_outcome) =
                    maybe_seed_empty_canonical_from_historical_bundle(storage, &ctx.opts.db_path)?;
                storage = reopened_storage;
                persist::apply_index_writer_busy_timeout(&storage);
                persist::apply_index_writer_checkpoint_policy(&storage, defer_checkpoints);
                if let Some(seed_outcome) = seed_outcome {
                    outcome.accumulate(seed_outcome);
                    has_pending_historical_bundles =
                        storage.has_pending_historical_bundles(&ctx.opts.db_path)?;
                }
            }
            if has_pending_historical_bundles {
                outcome.accumulate(storage.salvage_historical_databases(&ctx.opts.db_path)?);
            } else {
                tracing::info!(
                    db_path = %ctx.opts.db_path.display(),
                    "skipping incremental historical salvage because all discoverable historical bundles are already recorded in the canonical database"
                );
            }
            outcome
        } else {
            tracing::info!(
                db_path = %ctx.opts.db_path.display(),
                conversations = canonical_sessions_before_salvage,
                pending_historical_bundles = has_pending_historical_bundles,
                "skipping historical salvage because canonical database is already populated and no additional historical bundles are pending"
            );
            HistoricalSalvageOutcome::default()
        };
        ctx.complete_preflight_phase()?;
        if historical_salvage.messages_imported > 0 {
            tracing::info!(
                bundles_imported = historical_salvage.bundles_imported,
                conversations_imported = historical_salvage.conversations_imported,
                messages_imported = historical_salvage.messages_imported,
                "historical cass bundles merged into canonical database before scan"
            );
            let imported_omp_upgrade = storage
                .reclassify_legacy_omp_conversations_after_historical_import()
                .with_context(
                    || "reclassifying legacy Pi-labeled OMP rows imported from historical bundles",
                )?;
            if imported_omp_upgrade.conversations_reclassified > 0 {
                tracing::info!(
                    conversations = imported_omp_upgrade.conversations_reclassified,
                    "reclassified legacy OMP conversations imported from historical bundles"
                );
            }
            legacy_omp_upgrade.conversations_reclassified = legacy_omp_upgrade
                .conversations_reclassified
                .saturating_add(imported_omp_upgrade.conversations_reclassified);
            legacy_omp_upgrade.lexical_rebuild_required |=
                imported_omp_upgrade.lexical_rebuild_required;
            legacy_omp_upgrade.analytics_rebuild_required |=
                imported_omp_upgrade.analytics_rebuild_required;
            needs_rebuild |= imported_omp_upgrade.lexical_rebuild_required;
        }
        let rebuild_from_canonical_only =
            canonical_only_full_rebuild && historical_salvage.conversations_imported == 0;
        let repair_context = IncrementalCanonicalLexicalRepairContext {
            full_refresh: ctx.opts.full && !reconcile_nightly_lexical,
            force_rebuild: ctx.opts.force_rebuild,
            resume_lexical_rebuild,
            targeted_watch_once_only,
            salvage_messages_imported: historical_salvage.messages_imported,
            canonical_messages: 0,
            // Placeholder: the noise-adjusted expectation is a per-conversation
            // content scan, computed lazily below only when the live index looks
            // sparse against the cheap raw message-count upper bound (cass#317).
            expected_lexical_docs: 0,
            tantivy_requires_rebuild,
            observed_tantivy_docs,
            // Placeholder: the persisted region-doc bound is a marks-table
            // query, computed lazily below only when the sparse branch runs.
            region_doc_bound: 0,
            // Placeholder: validating the published index opens the DB read-only
            // and runs COUNT scans, so it is computed lazily below — only when the
            // live index actually looks sparse — to keep normal index/watch runs
            // cheap (#248, coding_agent_session_search-raoug).
            published_index_validated_for_current_data: false,
        };
        let incremental_canonical_lexical_repair = if canonical_sessions_before_salvage > 0
            && should_evaluate_incremental_canonical_lexical_repair(&repair_context)
            && large_incremental_authoritative_lexical_repair_probe_deferred.is_none()
            // cass#265: `CASS_SKIP_PREFLIGHT_COUNT_TOTAL_MESSAGES=1`
            // short-circuits the entire incremental-canonical-lexical-
            // repair branch — `count_total_messages_exact` issues a
            // `SELECT COUNT(*) FROM messages` which, on the cass#265
            // reporter's 417k-row DB, can route through the same
            // fsqlite full-table-scan path that wedges
            // `cleanup_orphan_fk_rows`. Skipping leaves the planner
            // in the "no repair plan" branch, which falls through to
            // the normal scan loop instead of the lexical-repair
            // shortcut.
            && !preflight_skip("watch_startup:count_total_messages")
        {
            ctx.enter_preflight_phase("watch_startup:count_total_messages")?;
            let canonical_messages = count_total_messages_exact(&storage)?;
            // Task 5: live documents include converged search-region
            // documents alongside canonical message documents — the
            // persisted bound splits the observed total into its
            // canonical-message share so region docs can never mask a
            // genuinely sparse index.
            let region_doc_bound =
                crate::viewer_extensions::search_regions::live_region_doc_bound(storage.raw())
                    .unwrap_or(0) as usize;
            let observed_message_docs =
                observed_tantivy_docs.map(|docs| docs.saturating_sub(region_doc_bound));
            // cass#317: the true sparseness threshold is the noise-adjusted
            // expected doc count (reachable messages the sink actually indexes),
            // not the raw `canonical_messages`. Only pay for that per-conversation
            // content scan when the index looks sparse against the cheap raw upper
            // bound — if `observed >= canonical_messages` it certainly covers the
            // smaller noise-adjusted expectation, so no repair and no scan needed.
            // Computed under the same `count_total_messages` preflight phase so
            // the scan stays watchdog-covered without a new taxonomy sub-phase.
            let expected_lexical_docs =
                if observed_message_docs.is_some_and(|docs| docs < canonical_messages) {
                    expected_live_lexical_doc_count_cached(
                        &storage,
                        &index_path,
                        &ctx.opts.db_path.to_string_lossy(),
                        canonical_messages,
                        ctx.opts.progress.as_deref(),
                    )?
                } else {
                    canonical_messages
                };
            ctx.complete_preflight_phase()?;
            // #248 (coding_agent_session_search-raoug): only pay for the
            // published-index validation (a read-only DB open + fingerprint COUNT
            // scans) when the live index actually looks sparse — i.e. below the
            // noise-adjusted expectation, when it would otherwise trigger a
            // from-scratch rebuild. A genuinely missing/invalid index rebuilds
            // regardless (handled in
            // choose_incremental_canonical_lexical_repair_plan via
            // tantivy_requires_rebuild), so skip the check then.
            let published_index_validated_for_current_data = !tantivy_requires_rebuild
                && observed_message_docs.is_some_and(|docs| docs < expected_lexical_docs)
                && !preflight_skip("watch_startup:published_index_validate")
                && {
                    ctx.enter_preflight_phase("watch_startup:published_index_validate")?;
                    let validated = published_lexical_index_validated_for_current_data(
                        &index_path,
                        &ctx.opts.db_path,
                    );
                    ctx.complete_preflight_phase()?;
                    validated
                };
            choose_incremental_canonical_lexical_repair_plan(
                IncrementalCanonicalLexicalRepairContext {
                    canonical_messages,
                    expected_lexical_docs,
                    region_doc_bound,
                    published_index_validated_for_current_data,
                    ..repair_context
                },
            )
        } else {
            None
        };

        if should_repair_daily_stats_after_historical_salvage(
            checked_daily_stats_pre_scan,
            ctx.opts.full,
            rebuild_from_canonical_only,
            historical_salvage.messages_imported,
        ) {
            repair_daily_stats_if_drifted(&storage, &ctx.opts.db_path, None)?;
        }

        if rebuild_from_canonical_only {
            tracing::info!(
                db_path = %ctx.opts.db_path.display(),
                conversations = initial_canonical_sessions_before_salvage,
                "skipping raw source rescan during full rebuild because the canonical database is already populated"
            );
            record_lexical_population_strategy(
                ctx.opts.progress.as_ref(),
                LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
                "full_rebuild_uses_authoritative_canonical_db_rebuild_only",
            );
            tracing::info!(
                strategy = LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild.as_str(),
                reason = "full_rebuild_uses_authoritative_canonical_db_rebuild_only",
                "selected_lexical_population_strategy"
            );
        }

        if rebuild_from_canonical_only {
            drop(t_index.take());
            ensure_authoritative_lexical_rebuild_storage_headroom(
                &ctx.opts.data_dir,
                &ctx.opts.db_path,
            )?;
            let rebuild_start = std::time::Instant::now();
            let rebuild_convs = canonical_sessions_before_salvage;
            ctx.enter_work_phase("lexical:rebuild")?;
            let rebuild = rebuild_tantivy_from_db_deferred_startup_with_progress_bump(
                &ctx.opts.db_path,
                &ctx.opts.data_dir,
                rebuild_convs,
                ctx.opts.progress.clone(),
                Arc::clone(&ctx.progress_bump),
            )?;
            exact_completed_lexical_checkpoint = rebuild.exact_checkpoint_persisted;
            let rebuild_ms = rebuild_start.elapsed().as_millis() as u64;
            // Populate stats for canonical-only rebuild path (no scan occurs).
            // Without this, indexing_stats in JSON output would be all zeros
            // because the scan/batch code paths that normally populate stats
            // are bypassed entirely.
            if let Some(p) = &ctx.opts.progress
                && let Ok(mut stats) = p.stats.lock()
            {
                stats.scan_ms = 0; // no scan phase in canonical-only rebuild
                stats.index_ms = rebuild_ms;
                stats.total_conversations = rebuild_convs;
            }
            if let Some(observed_messages) = rebuild.observed_messages {
                record_exact_total_counts_in_progress(
                    ctx.opts.progress.as_ref(),
                    rebuild_convs,
                    observed_messages,
                );
            }
            if keep_tantivy_open_after_rebuild {
                t_index = Some(open_lexical_index_with_heartbeat(
                    &index_path,
                    ctx.opts.progress.as_ref(),
                )?);
            }
        } else {
            let followup_scan_after_authoritative_repair =
                incremental_canonical_lexical_repair.is_some();
            if let Some(repair_plan) = incremental_canonical_lexical_repair {
                tracing::info!(
                    db_path = %ctx.opts.db_path.display(),
                    canonical_conversations = canonical_sessions_before_salvage,
                    canonical_messages = repair_plan.canonical_messages,
                    observed_tantivy_docs = repair_plan.observed_tantivy_docs,
                    reason = repair_plan.reason,
                    "repairing Tantivy from the authoritative canonical database before incremental source scan"
                );
                record_lexical_population_strategy(
                    ctx.opts.progress.as_ref(),
                    LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
                    repair_plan.reason,
                );
                record_incremental_canonical_lexical_repair(
                    ctx.opts.progress.as_ref(),
                    &repair_plan,
                    canonical_sessions_before_salvage,
                );
                tracing::info!(
                    strategy = LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild.as_str(),
                    reason = repair_plan.reason,
                    "selected_lexical_population_strategy"
                );

                drop(t_index.take());
                ensure_authoritative_lexical_rebuild_storage_headroom(
                    &ctx.opts.data_dir,
                    &ctx.opts.db_path,
                )?;
                let rebuild_convs = count_total_conversations_exact(&storage)?;
                ctx.enter_work_phase("lexical:rebuild")?;
                let rebuild = rebuild_tantivy_from_db_deferred_startup_with_progress_bump(
                    &ctx.opts.db_path,
                    &ctx.opts.data_dir,
                    rebuild_convs,
                    ctx.opts.progress.clone(),
                    Arc::clone(&ctx.progress_bump),
                )?;
                exact_completed_lexical_checkpoint = rebuild.exact_checkpoint_persisted;
                // GH #457 follow-on: this repair runs BEFORE the incremental
                // source scan below, so whatever it just certified describes
                // the pre-scan database.
                exact_completed_lexical_checkpoint_predates_scan =
                    exact_completed_lexical_checkpoint;
                if let Some(observed_messages) = rebuild.observed_messages {
                    record_exact_total_counts_in_progress(
                        ctx.opts.progress.as_ref(),
                        rebuild_convs,
                        observed_messages,
                    );
                }
                t_index = Some(open_lexical_index_with_heartbeat(
                    &index_path,
                    ctx.opts.progress.as_ref(),
                )?);
                needs_rebuild = false;
            }

            if targeted_watch_once_only {
                tracing::info!(
                    db_path = %ctx.opts.db_path.display(),
                    "skipping broad incremental scan because targeted watch-once paths were supplied"
                );
            } else if should_skip_broad_scan_after_watch_once_authoritative_repair(
                has_explicit_watch_once_paths,
                ctx.opts.watch,
                ctx.opts.full,
                followup_scan_after_authoritative_repair,
            ) {
                tracing::info!(
                    db_path = %ctx.opts.db_path.display(),
                    "skipping broad incremental scan because targeted watch-once paths were supplied after authoritative lexical repair"
                );
            } else {
                let defer_empty_archive_rebuild = deferred_incremental_authoritative_lexical_repair
                    .is_none()
                    && !legacy_omp_upgrade.lexical_rebuild_required
                    && empty_archive_lexical_rebuild
                    && needs_rebuild
                    && historical_salvage.messages_imported == 0;
                let (lexical_strategy, lexical_strategy_reason) =
                    if let Some(deferred) = &deferred_incremental_authoritative_lexical_repair {
                        (
                            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
                            deferred.reason,
                        )
                    } else if legacy_omp_upgrade.lexical_rebuild_required {
                        (
                            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
                            "legacy_omp_identity_upgrade_requires_authoritative_db_rebuild",
                        )
                    } else if defer_empty_archive_rebuild {
                        (
                            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
                            EMPTY_ARCHIVE_LEXICAL_REBUILD_REASON,
                        )
                    } else {
                        resolve_lexical_population_strategy(
                            needs_rebuild,
                            ctx.opts.full && (!reconcile_nightly_lexical || needs_rebuild),
                            historical_salvage.messages_imported,
                        )
                    };
                record_lexical_population_strategy_if_unset(
                    ctx.opts.progress.as_ref(),
                    lexical_strategy,
                    lexical_strategy_reason,
                );
                let stale_index_ingest_quarantine_retry =
                    if targeted_watch_once_only || canonical_only_full_rebuild {
                        None
                    } else {
                        stale_index_ingest_quarantine_version_retry(&ctx.opts.data_dir)?
                    };
                if let Some(retry) = &stale_index_ingest_quarantine_retry {
                    tracing::warn!(
                        stale_records = retry.stale_records,
                        legacy_records = retry.legacy_records,
                        previous_versions = ?retry.previous_versions,
                        current_version = current_cass_version(),
                        "retrying stale index-ingest quarantine records after cass version change"
                    );
                }
                if followup_scan_after_authoritative_repair {
                    tracing::info!(
                        strategy = lexical_strategy.as_str(),
                        reason = lexical_strategy_reason,
                        full = ctx.opts.full,
                        needs_rebuild,
                        salvage_messages_imported = historical_salvage.messages_imported,
                        "selected_followup_scan_lexical_strategy_after_authoritative_repair"
                    );
                } else {
                    tracing::info!(
                        strategy = lexical_strategy.as_str(),
                        reason = lexical_strategy_reason,
                        full = ctx.opts.full,
                        needs_rebuild,
                        salvage_messages_imported = historical_salvage.messages_imported,
                        "selected_lexical_population_strategy"
                    );
                }

                // Get last scan timestamp for incremental indexing.
                // If full rebuild or force_rebuild, scan everything (since_ts = None).
                // Otherwise, only scan files modified since last successful scan.
                let last_scan_ts = storage.get_last_scan_ts().unwrap_or(None);
                let bootstrap_missing_scan_watermark =
                    should_bootstrap_missing_incremental_scan_watermark(
                        &ctx.opts,
                        canonical_storage_rebuilt,
                        canonical_sessions_before_salvage,
                        needs_rebuild,
                        stale_index_ingest_quarantine_retry.is_some(),
                        last_scan_ts,
                    );
                let since_ts = if let Some(bootstrap) = &bootstrap_missing_scan_watermark {
                    tracing::warn!(
                        db_path = %ctx.opts.db_path.display(),
                        canonical_conversations = bootstrap.canonical_conversations,
                        db_size_bytes = bootstrap.db_size_bytes,
                        max_automatic_full_scan_db_size_bytes =
                            bootstrap.max_automatic_full_scan_db_size_bytes,
                        reason = bootstrap.reason,
                        "bootstrapping missing incremental scan watermark on a large populated archive; use --full or CASS_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN=1 for deliberate historical backfill"
                    );
                    Some(scan_start_ts.saturating_sub(1).max(0))
                } else {
                    non_watch_scan_since_ts(
                        ctx.opts.full,
                        needs_rebuild,
                        stale_index_ingest_quarantine_retry.is_some(),
                        last_scan_ts,
                    )
                };

                if since_ts.is_some() {
                    tracing::info!(since_ts = ?since_ts, "incremental_scan: using last_scan_ts");
                } else {
                    tracing::info!("full_scan: no last_scan_ts or rebuild requested");
                }

                // qu81y closed-world override: sources.toml resolution is
                // HOME/XDG-scoped, so an env-free run must not consult it —
                // the override map is the complete scan universe.
                let additional_scan_roots = if local_connector_roots.is_some() {
                    Vec::new()
                } else {
                    additional_scan_roots_for_scan_or_watch(&storage, &ctx.opts.data_dir, false)
                };
                // #372: drop remote mirror roots whose on-disk fingerprint is
                // unchanged since the last error-free scan (they would otherwise
                // be fully re-scanned every run), and capture fingerprints to
                // persist for the roots we still scan.
                let (additional_scan_roots, mirror_fingerprints_to_store) =
                    plan_remote_mirror_scan_skips(&storage, &ctx.opts, additional_scan_roots);
                let scan_watermark = Some(scan_start_ts);
                let scan_requires_tantivy =
                    lexical_population_strategy_requires_inline_tantivy(lexical_strategy);

                // Choose between streaming indexing (Opt 8.2) and batch indexing
                if scan_requires_tantivy && t_index.is_none() {
                    t_index = Some(open_lexical_index_with_heartbeat(
                        &index_path,
                        ctx.opts.progress.as_ref(),
                    )?);
                } else if !scan_requires_tantivy {
                    tracing::info!(
                        strategy = lexical_strategy.as_str(),
                        "scan phase is deferring Tantivy writer open/commit until the authoritative rebuild"
                    );
                }
                // A whole-archive region republish (REGION_VERSION bump) must
                // not converge per persist batch — one segment publish per
                // batch is the quadratic this run would otherwise spend hours
                // in. Latch the deferral once, before the first batch, when
                // the stale-version marks say this scan re-projects more
                // sessions than the deferral threshold. Watch keeps per-event
                // convergence (its chunks have no final drain), and the
                // rebuild lanes are excluded by the strategy check.
                if scan_requires_tantivy
                    && !ctx.opts.watch
                    && ctx.opts.watch_once_paths.is_none()
                    && lexical_strategy == LexicalPopulationStrategy::IncrementalInline
                    && let Some(index) = t_index.as_mut()
                {
                    persist::maybe_defer_region_doc_convergence_for_republish(
                        storage.raw(),
                        index,
                    )?;
                }
                ctx.enter_preflight_phase("watch_startup:scan_entry")?;
                ctx.complete_preflight_phase()?;
                ctx.enter_work_phase("index:scan")?;
                if streaming_index_enabled() {
                    tracing::info!("using streaming indexing (Opt 8.2)");
                    let scan_outcome = run_streaming_index(
                        &storage,
                        t_index.as_mut(),
                        &ctx.opts,
                        since_ts,
                        lexical_strategy,
                        additional_scan_roots.clone(),
                        local_connector_roots.clone(),
                        scan_watermark,
                        Some(&ctx.progress_bump),
                    )?;
                    // F4 (cass tech debt): a completed scan is a real
                    // forward-progress boundary; bump the atomic so the
                    // heartbeat folds the timestamp into the lock file
                    // on its next tick and a long single-mode scan does
                    // not false-positive as `status: "stalled"`
                    // (cass#258 follow-up).
                    bump_index_run_lock_progress_atomic(&ctx.progress_bump);
                    scan_canonical_mutations =
                        scan_canonical_mutations.accumulate(scan_outcome.canonical_mutations);
                    scan_lexical_update_deferred |= scan_outcome.lexical_update_deferred;
                    scanned_connectors.extend(scan_outcome.scanned_connectors);
                    scan_had_errors |= scan_outcome.scan_had_errors;
                    deferred_watch_sources.extend(scan_outcome.deferred_sources);
                } else {
                    tracing::info!(
                        "using batch indexing (streaming disabled via CASS_STREAMING_INDEX=0)"
                    );
                    let scan_outcome = run_batch_index(
                        &storage,
                        t_index.as_mut(),
                        &ctx.opts,
                        since_ts,
                        lexical_strategy,
                        additional_scan_roots.clone(),
                        local_connector_roots.clone(),
                        scan_watermark,
                        Some(&ctx.progress_bump),
                    )?;
                    bump_index_run_lock_progress_atomic(&ctx.progress_bump);
                    scan_canonical_mutations =
                        scan_canonical_mutations.accumulate(scan_outcome.canonical_mutations);
                    scan_lexical_update_deferred |= scan_outcome.lexical_update_deferred;
                    scanned_connectors.extend(scan_outcome.scanned_connectors);
                    scan_had_errors |= scan_outcome.scan_had_errors;
                    deferred_watch_sources.extend(scan_outcome.deferred_sources);
                }
                // #372: persist remote mirror fingerprints only after an
                // error-free scan (the same signal that gates last_scan_ts), so
                // a scan with errors never memoizes a fingerprint that would
                // skip re-scanning incompletely-ingested mirror data.
                if !scan_had_errors {
                    for (source_id, fingerprint) in &mirror_fingerprints_to_store {
                        if let Err(error) =
                            storage.record_mirror_scan_fingerprint(source_id, fingerprint)
                        {
                            tracing::warn!(
                                source_id = %source_id,
                                error = %error,
                                "failed to record remote mirror scan fingerprint (#372)"
                            );
                        }
                    }
                }
                performed_scan = true;
                stale_index_ingest_quarantine_retry_attempted =
                    stale_index_ingest_quarantine_retry.is_some();

                // Extension-version re-projection lane: replay the stale
                // set from the PERSISTED archive (no source re-scan), then
                // converge the replayed sessions' region docs through
                // the same batched drain the scan lanes use. Under a
                // deferred lexical strategy `t_index` is `None` here, so
                // the lane's DOCS_PENDING marks converge through the
                // deferred rebuild or the next run's drain — crash-honest
                // either way. A lane failure never fails the run (the
                // scan converged what it could), but the stats object is
                // omitted rather than reporting fabricated numbers.
                match run_region_reprojection_lane(&storage, &ctx.opts) {
                    Ok(Some(stats)) => {
                        if stats.reprojected > 0
                            && let Some(index) = t_index.as_mut()
                        {
                            index.converge_pending_search_region_docs(storage.raw(), i64::MAX)?;
                            index.commit_with_region_doc_stamps(storage.raw())?;
                        }
                        if let Some(progress) = ctx.opts.progress.as_ref()
                            && let Ok(mut progress_stats) = progress.stats.lock()
                        {
                            progress_stats.region_reproject = Some(stats);
                        }
                    }
                    Ok(None) => {}
                    Err(error) => tracing::warn!(
                        error = %format!("{error:#}"),
                        "region re-projection lane failed; the run's stats omit region_reproject"
                    ),
                }

                if scan_lexical_update_deferred {
                    tracing::warn!(
                        db_path = %ctx.opts.db_path.display(),
                        inserted_conversations = scan_canonical_mutations.inserted_conversations,
                        inserted_messages = scan_canonical_mutations.inserted_messages,
                        "inline lexical updates were deferred during non-watch scan; rebuilding lexical assets from canonical SQLite"
                    );
                    drop(t_index.take());
                    ensure_authoritative_lexical_rebuild_storage_headroom(
                        &ctx.opts.data_dir,
                        &ctx.opts.db_path,
                    )?;
                    let rebuild_convs = count_total_conversations_exact(&storage)?;
                    let rebuild = rebuild_tantivy_from_db_deferred_startup_with_progress_bump(
                        &ctx.opts.db_path,
                        &ctx.opts.data_dir,
                        rebuild_convs,
                        ctx.opts.progress.clone(),
                        Arc::clone(&ctx.progress_bump),
                    )?;
                    exact_completed_lexical_checkpoint = rebuild.exact_checkpoint_persisted;
                    if let Some(observed_messages) = rebuild.observed_messages {
                        record_exact_total_counts_in_progress(
                            ctx.opts.progress.as_ref(),
                            rebuild_convs,
                            observed_messages,
                        );
                    }
                    if keep_tantivy_open_after_rebuild {
                        t_index = Some(open_lexical_index_with_heartbeat(
                            &index_path,
                            ctx.opts.progress.as_ref(),
                        )?);
                    }
                } else if scan_requires_tantivy {
                    let t_index = t_index
                        .as_mut()
                        .expect("tantivy index must remain open for lexical commit");
                    // Whole-archive republish deferral: drain the pending
                    // region documents the per-batch prelude skipped before
                    // this run's final seal. For the streaming flavor the
                    // consumer already drained at its own final commit, so
                    // this pass finds nothing pending; for the batch flavor
                    // this IS the drain. No-op when the latch was never set.
                    if t_index.region_doc_convergence_deferred_to_final_drain() {
                        t_index.converge_pending_search_region_docs(storage.raw(), i64::MAX)?;
                    }
                    // Seals any region documents staged during the run;
                    // their queued marks stamps land `published` only
                    // after the commit succeeds.
                    t_index.commit_with_region_doc_stamps(storage.raw())?;
                    // #441: an append-only archive never trips Quill's
                    // tombstone-driven compaction, and its width-tiered merge
                    // cannot fold the sparse per-session docid leases, so
                    // every incremental run used to add segments that only a
                    // full rebuild removed. Apply the bounded size-tiered
                    // policy here (threshold + cooldown gated), the same hook
                    // the watch loop already runs. The merge is post-publish
                    // work with parked counters, so it runs under the
                    // finalize-class grace like the WAL checkpoint does.
                    if (scan_canonical_mutations.inserted_messages > 0
                        || scan_canonical_mutations.inserted_conversations > 0)
                        && !lexical_post_run_maintenance_skipped_for_test()
                    {
                        if let Some(progress) = ctx.opts.progress.as_ref() {
                            progress.finalizing.store(true, Ordering::Relaxed);
                            progress.tick_activity();
                        }
                        match t_index.optimize_if_idle() {
                            Ok(true) => tracing::info!(
                                segments = t_index.segment_count(),
                                "folded lexical segments after incremental index run (#441)"
                            ),
                            Ok(false) => {}
                            Err(err) => tracing::warn!(
                                error = %format!("{err:#}"),
                                "segment merge after incremental index run failed; continuing with the unmerged generation"
                            ),
                        }
                        if let Some(progress) = ctx.opts.progress.as_ref() {
                            progress.tick_activity();
                            progress.finalizing.store(false, Ordering::Relaxed);
                        }
                    }
                }

                if !scan_lexical_update_deferred
                    && (ctx.opts.full
                        || historical_salvage.messages_imported > 0
                        || legacy_omp_upgrade.lexical_rebuild_required
                        || defer_empty_archive_rebuild)
                {
                    let post_scan_observed_tantivy_docs =
                        observed_tantivy_docs_for_post_full_scan_skip(
                            &index_path,
                            ctx.opts.full,
                            initial_needs_rebuild,
                            historical_salvage.messages_imported,
                            &initial_matching_lexical_checkpoint,
                            scan_canonical_mutations,
                            observed_tantivy_docs,
                        )?;
                    if should_skip_post_full_scan_authoritative_rebuild(
                        ctx.opts.full,
                        initial_needs_rebuild,
                        historical_salvage.messages_imported,
                        &initial_matching_lexical_checkpoint,
                        scan_canonical_mutations,
                        post_scan_observed_tantivy_docs,
                    ) {
                        tracing::info!(
                            db_path = %ctx.opts.db_path.display(),
                            observed_tantivy_docs = post_scan_observed_tantivy_docs,
                            completed_indexed_docs = initial_matching_lexical_checkpoint
                                .completed_indexed_docs,
                            inserted_conversations = scan_canonical_mutations.inserted_conversations,
                            inserted_messages = scan_canonical_mutations.inserted_messages,
                            "skipping post-scan authoritative lexical rebuild because the full scan found no canonical changes and the live Tantivy index still matches the completed checkpoint"
                        );
                        let (exact_total_conversations, exact_total_messages) =
                            initial_matching_lexical_checkpoint
                                .completed_exact_totals
                                .unwrap_or((
                                    count_total_conversations_exact(&storage)?,
                                    count_total_messages_exact(&storage)?,
                                ));
                        record_exact_total_counts_in_progress(
                            ctx.opts.progress.as_ref(),
                            exact_total_conversations,
                            exact_total_messages,
                        );
                        skipped_noop_full_scan_authoritative_rebuild = true;
                    } else if !reconcile_nightly_lexical
                        || matches!(
                            lexical_strategy,
                            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild
                        )
                    {
                        drop(t_index.take());
                        ensure_authoritative_lexical_rebuild_storage_headroom(
                            &ctx.opts.data_dir,
                            &ctx.opts.db_path,
                        )?;
                        let rebuild_convs = count_total_conversations_exact(&storage)?;
                        let rebuild = rebuild_tantivy_from_db_deferred_startup_with_progress_bump(
                            &ctx.opts.db_path,
                            &ctx.opts.data_dir,
                            rebuild_convs,
                            ctx.opts.progress.clone(),
                            Arc::clone(&ctx.progress_bump),
                        )?;
                        exact_completed_lexical_checkpoint = rebuild.exact_checkpoint_persisted;
                        // Update stats to reflect the authoritative rebuild
                        // totals. The scan-phase stats tracked only what the
                        // connectors discovered; the DB rebuild is the source of
                        // truth for full-index runs.
                        if let Some(observed_messages) = rebuild.observed_messages {
                            record_exact_total_counts_in_progress(
                                ctx.opts.progress.as_ref(),
                                rebuild_convs,
                                observed_messages,
                            );
                        }
                        if keep_tantivy_open_after_rebuild {
                            t_index = Some(open_lexical_index_with_heartbeat(
                                &index_path,
                                ctx.opts.progress.as_ref(),
                            )?);
                        }
                    }
                }
            }
        }

        t_index
    };
    let preflight = RunIndexPreflightOutcome {
        storage,
        ctx,
        index_path,
        defer_checkpoints,
        canonical_storage_rebuilt,
        opened_fresh_for_full,
        reopened_after_writable_preflight,
        legacy_omp_upgrade,
        initial_canonical_sessions_before_salvage,
        canonical_only_full_rebuild,
        has_explicit_watch_once_paths,
        targeted_semantic_watch_once,
        resume_lexical_rebuild,
        restart_pending_lexical_rebuild_from_zero,
        initial_matching_lexical_checkpoint,
        reconcile_nightly_lexical,
        checked_daily_stats_pre_scan,
        tantivy_requires_rebuild,
        observed_tantivy_docs,
        needs_rebuild,
        initial_needs_rebuild,
        large_incremental_authoritative_lexical_repair_probe_deferred,
        deferred_incremental_authoritative_lexical_repair,
        empty_archive_lexical_rebuild,
        scan_start_ts,
        keep_tantivy_open_after_rebuild,
    };
    let lexical = RunIndexLexicalOutcome {
        t_index,
        performed_scan,
        scan_canonical_mutations,
        scan_lexical_update_deferred,
        scanned_connectors,
        scan_had_errors,
        stale_index_ingest_quarantine_retry_attempted,
        deferred_watch_sources,
        exact_completed_lexical_checkpoint,
        exact_completed_lexical_checkpoint_predates_scan,
        skipped_noop_full_scan_authoritative_rebuild,
        targeted_watch_once_only_run,
    };
    Ok((preflight, lexical))
}
