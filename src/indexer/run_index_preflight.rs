use super::*;
use std::ops::ControlFlow;

pub(in crate::indexer) struct RunIndexPhaseContext {
    pub(in crate::indexer) opts: IndexOptions,
    pub(in crate::indexer) initial_lock_mode: SearchMaintenanceMode,
    pub(in crate::indexer) progress_bump: Arc<AtomicI64>,
    pub(in crate::indexer) preflight_state:
        Arc<watch_startup_preflight::WatchStartupPreflightState>,
    // Declare RAII guards in reverse of local-variable acquisition order so
    // Rust drops struct fields top-to-bottom in the exact original sequence:
    // `_preflight_watchdog` -> `_index_run_lock_heartbeat` -> `index_run_lock`
    // -> `_defer_analytics_guard` -> `_progress_reset` ->
    // `storage_cancellation_scope` -> `_raw_mirror_io`.
    _preflight_watchdog: Option<watch_startup_preflight::WatchStartupPreflightWatchdog>,
    _index_run_lock_heartbeat: IndexRunLockHeartbeat,
    pub(in crate::indexer) index_run_lock: IndexRunLockGuard,
    _defer_analytics_guard: crate::storage::sqlite::DeferAnalyticsUpdatesGuard,
    _progress_reset: RunIndexProgressReset,
    pub(in crate::indexer) storage_cancellation_scope:
        Option<crate::franken_sync::CancellationScope>,
    _raw_mirror_io: Option<crate::raw_mirror::RawMirrorIoScope>,
}

impl RunIndexPhaseContext {
    pub(in crate::indexer) fn enter_preflight_phase(&mut self, phase: &'static str) -> Result<()> {
        if let Some(progress) = self.opts.progress.as_ref() {
            progress.check_stop()?;
            if self.storage_cancellation_scope.is_none() {
                self.storage_cancellation_scope = Some(progress.enter_storage_cancellation());
            }
        }
        let phase_visible = preflight_breadcrumb_visible(self.initial_lock_mode, phase);
        let timeout_enforced = self.initial_lock_mode == SearchMaintenanceMode::WatchStartup;
        if timeout_enforced {
            // Notify the watchdog of the new active step BEFORE writing
            // the lock-file breadcrumb. If the write itself wedges, the
            // watchdog can still kill the process with the right
            // `<step>_TIMEOUT` phase.
            if let Some(step_idx) = watch_startup_step_idx(phase) {
                self.preflight_state
                    .enter(step_idx, FrankenStorage::now_millis());
            } else {
                // Coding error: an undocumented timeout-enforced phase
                // reached the macro.
                debug_assert!(
                    false,
                    "undocumented watch_startup sub-phase `{phase}`; add it to WATCH_STARTUP_SUB_PHASE_TAXONOMY",
                );
            }
        }
        if phase_visible
            && let Err(err) = self.index_run_lock.set_phase(self.initial_lock_mode, phase)
        {
            tracing::debug!(
                sub_phase = phase,
                error = %err,
                "index preflight phase breadcrumb write failed (continuing)"
            );
        }
        Ok(())
    }

    pub(in crate::indexer) fn complete_preflight_phase(&self) -> Result<()> {
        self.preflight_state.exit();
        if let Some(progress) = self.opts.progress.as_ref() {
            progress.check_stop()?;
        }
        bump_index_run_lock_progress_atomic(&self.progress_bump);
        Ok(())
    }

    // The long stages after preflight (the scan, the authoritative lexical
    // rebuild) name themselves in the lock. Otherwise the last preflight
    // breadcrumb persists for the whole run, and an operator reading the lock
    // is told a step that finished minutes ago is still active (#483/#497).
    // These are not watch-startup steps, so the preflight watchdog ignores them.
    pub(in crate::indexer) fn enter_work_phase(&mut self, phase: &'static str) -> Result<()> {
        drop(self.storage_cancellation_scope.take());
        if let Some(progress) = self.opts.progress.as_ref() {
            progress.check_stop()?;
        }
        if let Err(err) = self.index_run_lock.set_phase(self.initial_lock_mode, phase) {
            tracing::debug!(
                phase = phase,
                error = %err,
                "index work phase breadcrumb write failed (continuing)"
            );
        }
        Ok(())
    }
}

pub(in crate::indexer) struct RunIndexPreflightOutcome {
    // Declare `storage` BEFORE `ctx` so `storage` drops before `ctx` guards on unwind.
    pub(in crate::indexer) storage: FrankenStorage,
    pub(in crate::indexer) ctx: RunIndexPhaseContext,
    pub(in crate::indexer) index_path: PathBuf,
    pub(in crate::indexer) defer_checkpoints: bool,
    pub(in crate::indexer) canonical_storage_rebuilt: bool,
    pub(in crate::indexer) opened_fresh_for_full: bool,
    pub(in crate::indexer) reopened_after_writable_preflight: bool,
    pub(in crate::indexer) legacy_omp_upgrade:
        crate::storage::sqlite::LegacyOmpReclassificationResult,
    pub(in crate::indexer) initial_canonical_sessions_before_salvage: usize,
    pub(in crate::indexer) canonical_only_full_rebuild: bool,
    pub(in crate::indexer) has_explicit_watch_once_paths: bool,
    pub(in crate::indexer) targeted_semantic_watch_once: bool,
    pub(in crate::indexer) resume_lexical_rebuild: bool,
    pub(in crate::indexer) restart_pending_lexical_rebuild_from_zero: bool,
    pub(in crate::indexer) initial_matching_lexical_checkpoint: MatchingLexicalRebuildStateStatus,
    pub(in crate::indexer) reconcile_nightly_lexical: bool,
    pub(in crate::indexer) checked_daily_stats_pre_scan: bool,
    pub(in crate::indexer) tantivy_requires_rebuild: bool,
    pub(in crate::indexer) observed_tantivy_docs: Option<usize>,
    pub(in crate::indexer) needs_rebuild: bool,
    pub(in crate::indexer) initial_needs_rebuild: bool,
    pub(in crate::indexer) large_incremental_authoritative_lexical_repair_probe_deferred:
        Option<DeferredIncrementalCanonicalLexicalRepair>,
    pub(in crate::indexer) deferred_incremental_authoritative_lexical_repair:
        Option<DeferredIncrementalCanonicalLexicalRepair>,
    pub(in crate::indexer) empty_archive_lexical_rebuild: bool,
    pub(in crate::indexer) scan_start_ts: i64,
    pub(in crate::indexer) keep_tantivy_open_after_rebuild: bool,
}

pub(in crate::indexer) fn run_index_preflight_and_open(
    opts: IndexOptions,
    maintain_raw_mirror: bool,
) -> Result<ControlFlow<(), RunIndexPreflightOutcome>> {
    let _raw_mirror_io = observe_index_raw_mirror_io(opts.progress.as_ref());
    // Reject malformed configuration before opening storage or capturing any
    // source evidence. Every persistence entry also validates the policy.
    WorkspaceExclusions::from_env()?;
    if let Some(progress) = opts.progress.as_ref() {
        progress.check_stop()?;
    }
    // Preparation includes SQL between the named breadcrumbs as well as
    // inside them. Keep one scope until actual scan/rebuild work is admitted;
    // that work retains the existing finish-current-batch stop contract.
    let mut storage_cancellation_scope = opts
        .progress
        .as_ref()
        .map(|progress| progress.enter_storage_cancellation());
    ACTIVE_SESSION_SOURCE_SKIP_OBSERVED.store(false, Ordering::Relaxed);
    if let Some(warning) = dotenvy::var("CASS_EXCLUDE_PATHS")
        .ok()
        .and_then(|value| colon_separated_exclusion_warning(&value))
    {
        tracing::warn!("{warning}");
    }
    let _progress_reset = RunIndexProgressReset::new(opts.progress.clone());
    // Analytics tables are derived assets and can be rebuilt by doctor/rebuild
    // flows. Keep routine indexing focused on the canonical conversation store
    // and lexical assets; set CASS_INLINE_ANALYTICS_UPDATES=1 to restore the
    // old inline analytics writes for targeted maintenance runs.
    let _defer_analytics_guard = crate::storage::sqlite::defer_analytics_updates_guard();
    set_progress_last_error(opts.progress.as_ref(), None);
    let initial_lock_mode = if opts.watch {
        SearchMaintenanceMode::WatchStartup
    } else if opts
        .watch_once_paths
        .as_ref()
        .is_some_and(|paths| !paths.is_empty())
    {
        SearchMaintenanceMode::WatchOnce
    } else {
        SearchMaintenanceMode::Index
    };
    let job_kind = if opts.semantic || opts.build_hnsw {
        SearchMaintenanceJobKind::SemanticRebuild
    } else {
        maintenance_job_kind_for_mode(initial_lock_mode)
    };
    // Ordinary writers must own both the data dir and canonical archive
    // before even an integrity probe can pin a snapshot. The narrow readonly
    // force-rebuild path can consume an unwritable archive; it acquires the
    // archive lease before any recovery write, its optional watermark write,
    // or a writable preparation fallback for an empty archive.
    let archive_admission = if should_try_readonly_canonical_force_rebuild(&opts) {
        ArchiveAdmission::DataDirOnly
    } else {
        ArchiveAdmission::Writable
    };
    let mut index_run_lock = acquire_index_run_lock_with_archive_admission(
        &opts.data_dir,
        &opts.db_path,
        initial_lock_mode,
        job_kind,
        archive_admission,
    )?;
    let require_canonical_rebuild = canonical_rebuild_required_from_env()?;
    validate_canonical_rebuild_scope(&opts, require_canonical_rebuild)?;
    let _index_run_lock_heartbeat = IndexRunLockHeartbeat::start(
        opts.data_dir.clone(),
        index_run_lock_heartbeat_interval(),
        Arc::clone(&index_run_lock.metadata_write_lock),
        Arc::clone(&index_run_lock.last_progress_at_ms_atomic),
    );
    // i6upe: refuse to rebuild ON TOP of a malformed canonical archive.
    // Every index mode reads the canonical DB (`--force-rebuild` performs a
    // READONLY authoritative rebuild from it), so indexing a corrupt archive
    // at best fails confusingly and at worst rewrites over recoverable
    // pages. The probe always validates the fixed SQLite header; only archives
    // below its explicit physical bundle-size ceiling also run the stronger
    // real-column projections. A busy or missing DB never trips it.
    // CASS_INDEX_ALLOW_MALFORMED_DB=1 bypasses for emergency flows that
    // knowingly operate on a damaged archive. The refusal is the shared
    // unhealthy-archive one (zp0fp, GH #374): typed as data corruption, not
    // retryable, and routed to the doctor recovery that can rebuild from a
    // verified raw mirror, rather than to an external sqlite3 '.recover'.
    if !dotenvy::var("CASS_INDEX_ALLOW_MALFORMED_DB").is_ok_and(|v| v == "1")
        && let Some(detail) =
            crate::bounded_canonical_db_corruption_probe(&opts.db_path, "index-preflight")
    {
        return Err(canonical_archive_unhealthy_for_index_error(
            &opts.db_path,
            &format!("the archive is malformed ({detail})"),
        ));
    }
    // GH#324: reclaim staging debris stranded by previous hard-crashed runs
    // now that we hold the exclusive index-run lock, and BEFORE the
    // disk-headroom preflight below — otherwise a crash/restart loop's own
    // debris trips the headroom guard and wedges indexing hard-down on a
    // full disk it could have freed itself.
    staging_reclaim::reclaim_orphaned_staging_dirs_for_data_dir(&opts.data_dir, SystemTime::now())
        .log();
    // F4: clone the atomic so deeply nested batch loops can bump progress
    // without holding a `&mut IndexRunLockGuard`. The clone is cheap
    // (single Arc bump) and lives only inside this function.
    let progress_bump = Arc::clone(&index_run_lock.last_progress_at_ms_atomic);
    // The atomic-store bump runs once per "interesting" batch boundary
    // — currently used at the per-conversation completion ingest loop
    // and the lexical commit boundary; see `bump_index_run_lock_progress_atomic`.

    if can_skip_absent_explicit_watch_once_index_run(&opts) {
        let path_count = opts
            .watch_once_paths
            .as_ref()
            .map(std::vec::Vec::len)
            .unwrap_or_default();
        tracing::info!(
            db_path = %opts.db_path.display(),
            data_dir = %opts.data_dir.display(),
            path_count,
            "skipping watch-once index because all explicit paths are absent"
        );
        return Ok(ControlFlow::Break(()));
    }

    // cass#265 / GH #413: each preflight step gets its own sub-phase
    // breadcrumb so operators investigating a watch-startup wedge can see
    // WHICH step stalled instead of an opaque top-level phase for the entire
    // pre-pipeline block. The historical `watch_startup:` namespace remains a
    // compatibility contract.
    //
    // GH #443: the breadcrumb is written in EVERY index mode, not only watch
    // startup. Plain incremental runs used to expose just the (sidecar-only,
    // millisecond) `classify_nonresumable_checkpoint` step and then go dark:
    // the lock file kept naming that step while the process was actually
    // inside the writable frankensqlite open (`open_storage`) or a later
    // preflight, so a wedge there was reported — by `cass status`, the stall
    // event and the reporter — against a function that had already returned.
    // A stale breadcrumb is worse than none; see
    // `preflight_breadcrumb_visible`. The hard per-step abort watchdog
    // remains scoped to watch startup.
    // Each `set_phase`
    // call bumps `last_progress_at_ms` (in both the atomic and the on-disk
    // field) and emits a new `phase=` string that `IndexStallWatchdog` treats
    // as a phase transition (resets its `last_progress_advance` timer). The
    // macro keeps the call sites compact.
    //
    // v0.6.7 extension: the macro also notifies the
    // `WatchStartupPreflightState` so the watchdog thread can detect
    // a wedge that exceeds `CASS_PREFLIGHT_OP_TIMEOUT_SECS` (default
    // 180 s) and abort the process with a `<step>_TIMEOUT` breadcrumb
    // instead of hanging forever.
    let preflight_state = WatchStartupPreflightState::new();
    let preflight_watchdog_enabled = initial_lock_mode == SearchMaintenanceMode::WatchStartup;
    let _preflight_watchdog = preflight_watchdog_enabled.then(|| {
        WatchStartupPreflightWatchdog::start(
            Arc::clone(&preflight_state),
            WATCH_STARTUP_SUB_PHASE_TAXONOMY,
            index_run_lock._path.clone(),
            opts.data_dir.clone(),
            watch_startup_preflight::op_timeout(),
            std::time::Duration::from_secs(5),
            PreflightTimeoutPolicy::Abort,
            // INV3: pass the shared write lock so the watchdog's
            // `_TIMEOUT` rewrite is serialised with the heartbeat.
            Arc::clone(&index_run_lock.metadata_write_lock),
        )
    });
    let mut ctx = RunIndexPhaseContext {
        opts,
        initial_lock_mode,
        progress_bump,
        preflight_state,
        _preflight_watchdog,
        _index_run_lock_heartbeat,
        index_run_lock,
        _defer_analytics_guard,
        _progress_reset,
        storage_cancellation_scope,
        _raw_mirror_io,
    };
    // Retention used to run in main before tracing and the watchdog existed.
    // Reuse the held index lease and the normal monitored preflight instead.
    if maintain_raw_mirror {
        ctx.enter_preflight_phase("watch_startup:raw_mirror_maintenance")?;
        crate::raw_mirror::maintain_for_index(
            &ctx.opts.data_dir,
            &ctx.opts.db_path,
            &ctx.index_run_lock,
        );
        ctx.complete_preflight_phase()?;
    }
    ctx.enter_preflight_phase("watch_startup:ensure_index_dir")?;
    let index_path = index_dir(&ctx.opts.data_dir)?;
    ctx.complete_preflight_phase()?;
    ctx.enter_preflight_phase("watch_startup:ensure_storage_headroom")?;
    ensure_index_startup_storage_headroom(&ctx.opts)?;
    ctx.complete_preflight_phase()?;
    if should_try_readonly_nonresumable_lexical_resume(&ctx.opts) {
        // GH #413: classify the small staged-checkpoint sidecar before opening
        // the canonical DB, then restart the non-resumable build without an
        // exact COUNT(*). The packet producer enumerates the authoritative DB
        // to EOF and therefore learns the exact total as part of useful Quill
        // ingest work. The persisted count is only a progress hint: it may be
        // stale after an interrupted run and must never authorize an early
        // return or become the completed generation's fingerprint.
        let pending_state = if preflight_skip("watch_startup:classify_nonresumable_checkpoint") {
            Ok(None)
        } else {
            ctx.enter_preflight_phase("watch_startup:classify_nonresumable_checkpoint")?;
            let result =
                nonresumable_pending_lexical_rebuild_state_for_db(&index_path, &ctx.opts.db_path);
            ctx.complete_preflight_phase()?;
            result
        };
        match pending_state {
            Ok(Some(state)) => {
                let total_conversations_hint = state.db.total_conversations;
                tracing::info!(
                    db_path = %ctx.opts.db_path.display(),
                    total_conversations_hint,
                    "restarting non-resumable lexical rebuild from a readonly canonical DB without an exact startup count"
                );
                record_lexical_population_strategy(
                    ctx.opts.progress.as_ref(),
                    LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
                    "readonly_fast_resume_incomplete_nonresumable_lexical_rebuild",
                );
                tracing::info!(
                    strategy = LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild.as_str(),
                    reason = "readonly_fast_resume_incomplete_nonresumable_lexical_rebuild",
                    "selected_lexical_population_strategy"
                );
                ensure_authoritative_lexical_rebuild_storage_headroom(
                    &ctx.opts.data_dir,
                    &ctx.opts.db_path,
                )?;
                // The legacy restart skips the ordinary writable-open path.
                // Apply its derived-shadow bound before any readonly reader
                // can hydrate the oversized FTS index (GH #413).
                ctx.enter_preflight_phase("watch_startup:fts_shadow_viability")?;
                preflight_fts_shadow_before_lexical_readers(&ctx.opts.db_path)?;
                ctx.complete_preflight_phase()?;
                ctx.enter_work_phase("lexical:rebuild")?;
                let rebuild = rebuild_tantivy_from_db_deferred_startup_with_progress_bump(
                    &ctx.opts.db_path,
                    &ctx.opts.data_dir,
                    total_conversations_hint,
                    ctx.opts.progress.clone(),
                    Arc::clone(&ctx.progress_bump),
                )?;
                if let Some(p) = &ctx.opts.progress
                    && let Ok(mut stats) = p.stats.lock()
                {
                    stats.scan_ms = 0;
                    stats.total_conversations = rebuild.observed_conversations;
                }
                if let Some(observed_messages) = rebuild.observed_messages {
                    record_exact_total_counts_in_progress(
                        ctx.opts.progress.as_ref(),
                        rebuild.observed_conversations,
                        observed_messages,
                    );
                }
                return Ok(ControlFlow::Break(()));
            }
            Ok(None) => {}
            Err(err) => {
                tracing::debug!(
                    db_path = %ctx.opts.db_path.display(),
                    error = %err,
                    "readonly lexical resume preflight failed; falling back to writable storage open"
                );
            }
        }
    }
    let readonly_opts = ctx.opts.clone();
    let readonly_progress_bump = Arc::clone(&ctx.progress_bump);
    if try_readonly_canonical_force_rebuild(
        &readonly_opts,
        &readonly_progress_bump,
        require_canonical_rebuild,
        |boundary| {
            match boundary {
                ReadonlyRebuildBoundary::Rebuild => ctx.enter_work_phase("lexical:rebuild")?,
                ReadonlyRebuildBoundary::ArchiveWrite => {
                    if let Some(progress) = ctx.opts.progress.as_ref() {
                        progress.check_stop()?;
                    }
                    ctx.index_run_lock.acquire_archive_write_lease()?;
                }
            }
            Ok(())
        },
    )? {
        return Ok(ControlFlow::Break(()));
    }

    // An empty canonical archive makes the readonly force-rebuild probe
    // decline. Its writable fallback must not bypass cross-data-dir admission.
    ctx.index_run_lock.acquire_archive_write_lease()?;
    ctx.enter_preflight_phase("watch_startup:open_storage")?;
    // GH #374: a full rebuild must classify an existing canonical archive on
    // a strictly read-only connection before `open_storage_for_index`. That
    // writable open may repair transition/schema objects even on the current
    // schema, so checking immediately afterward would already be too late.
    if ctx.opts.full
        && let Some(reason) = full_rebuild_existing_archive_integrity_preflight(&ctx.opts.db_path)?
    {
        tracing::error!(
            db_path = %ctx.opts.db_path.display(),
            reason = %reason,
            "full rebuild detected an unhealthy current-schema canonical db; refusing to replace the canonical archive"
        );
        return Err(canonical_archive_unhealthy_for_index_error(
            &ctx.opts.db_path,
            &reason,
        ));
    }

    // GH #450: this writable open is where frankensqlite runs its one-time
    // migration repair, which copies and rewrites the whole archive and took
    // ~25 minutes on a 2.6 GiB bundle. It posts no progress of its own, so the
    // #258 stall detector saw `last_progress_at_ms` frozen and reported
    // `status: "stalled"` at 0/N conversations while the process was doing
    // sustained IO. Tick the same honest liveness signal the Quill sink uses
    // (#446): progress is posted only in an interval where the database bundle
    // (including the migration backup being written) grew or the process burned
    // CPU, so a genuinely wedged open still reports `stalled`.
    let open_liveness_targets = storage_open_liveness_targets(&ctx.opts.db_path);
    let open_liveness_heartbeat: crate::search::quill_bridge::EngineHeartbeat = {
        let progress = ctx.opts.progress.clone();
        let progress_bump = Arc::clone(&ctx.progress_bump);
        Arc::new(move || {
            if let Some(progress) = progress.as_ref() {
                progress.tick_activity();
            }
            bump_index_run_lock_progress_atomic(&progress_bump);
        })
    };
    let storage_open_started = Instant::now();
    let (mut storage, canonical_storage_rebuilt, opened_fresh_for_full) =
        crate::search::quill_bridge::run_with_liveness_ticks(
            open_liveness_targets,
            open_liveness_heartbeat,
            || open_storage_for_index(&ctx.opts.db_path, ctx.opts.full),
        )?;
    let storage_open_elapsed = storage_open_started.elapsed();
    if storage_open_elapsed >= SLOW_STORAGE_OPEN_WARN_THRESHOLD {
        // GH #450: name the cost. An open this long is a schema migration or
        // frankensqlite's one-time migration repair, and the operator otherwise
        // sees only a long silence before the first indexing phase.
        tracing::warn!(
            db_path = %ctx.opts.db_path.display(),
            elapsed_secs = storage_open_elapsed.as_secs_f64(),
            bundle_bytes = database_bundle_size_bytes(&ctx.opts.db_path),
            "opening the canonical archive for writing took a long time; this phase covers schema \
             migration and frankensqlite's one-time migration repair, and its cost scales with the \
             archive size"
        );
    }
    ctx.complete_preflight_phase()?;

    ctx.enter_preflight_phase("watch_startup:fts_shadow_viability")?;
    // GH #413 follow-up (iify0): decide BEFORE the first write on this
    // connection — the moment fsqlite rebuilds the whole in-memory index of a
    // populated shadow (20 GB and minutes on a 10 GB archive) — whether the
    // derived shadow may exist at all. The drop goes through a deferred-FTS5
    // connection so it never hydrates what it removes.
    match storage.fts_shadow_viability() {
        Ok(crate::storage::sqlite::FtsShadowViability::NotViable { .. }) => {
            storage.close_best_effort_in_place();
            preflight_fts_shadow_before_lexical_readers(&ctx.opts.db_path)?;
            storage = crate::storage::sqlite::open_franken_storage_with_timeout(
                &ctx.opts.db_path,
                Duration::from_secs(10),
            )
            .with_context(|| {
                format!(
                    "reopening storage after dropping the oversized fallback FTS shadow in {}",
                    ctx.opts.db_path.display()
                )
            })?;
        }
        Ok(crate::storage::sqlite::FtsShadowViability::Viable { corpus_messages }) => {
            storage.note_fts_shadow_corpus_messages(corpus_messages);
        }
        Ok(crate::storage::sqlite::FtsShadowViability::Absent) => {}
        Err(err) => {
            tracing::warn!(
                db_path = %ctx.opts.db_path.display(),
                error = %format!("{err:#}"),
                "could not assess the fallback FTS shadow's viability; leaving it as it is"
            );
        }
    }
    ctx.complete_preflight_phase()?;

    let defer_checkpoints = !ctx.opts.watch;
    let mut reopened_after_writable_preflight = false;

    // GH #344: an interrupted fallback-FTS rebuild can leave a queryable but
    // partial contentless shadow. Inserting one targeted conversation into
    // that state makes frankensqlite repeatedly re-encode the large partial
    // table before progress reaches 1/1. Detect exact parity before even the
    // writable no-op below and fail with the resumable doctor repair command.
    if let Some(reason) = targeted_watch_once_fts_parity_problem(&storage, &ctx.opts)? {
        storage.close_best_effort_in_place();
        return Err(targeted_watch_once_unhealthy_fts_error(
            &ctx.opts.db_path,
            &reason,
        ));
    }

    ctx.enter_preflight_phase("watch_startup:writable_preflight")?;
    // CASS #162 item 2: Verify the connection is writable early, before the
    // code reaches deep batch-insert paths where a readonly failure is hard
    // to diagnose.  A benign no-op UPDATE catches "attempt to write a readonly
    // database" from frankensqlite.
    if let Err(err) = storage
        .raw()
        .execute("UPDATE meta SET value = value WHERE key = 'schema_version'")
    {
        tracing::warn!(
            db_path = %ctx.opts.db_path.display(),
            error = %err,
            "primary storage connection failed writable preflight; \
             attempting to close and reopen"
        );
        storage.close_best_effort_in_place();
        storage = crate::storage::sqlite::open_franken_storage_with_timeout(
            &ctx.opts.db_path,
            Duration::from_secs(10),
        )
        .with_context(|| {
            format!(
                "reopening storage after writable preflight failure: {}. \
                     If this persists, check that no other cass process holds \
                     an exclusive lock on the database.",
                ctx.opts.db_path.display()
            )
        })?;
        // Reopening repairs handle availability only; it does not mean the
        // canonical database content was reset or replaced.
        reopened_after_writable_preflight = true;
    }

    persist::apply_index_writer_busy_timeout(&storage);
    persist::apply_index_writer_checkpoint_policy(&storage, defer_checkpoints);
    ctx.complete_preflight_phase()?;

    ctx.enter_preflight_phase("watch_startup:validate_fts_messages")?;
    // cass#265/cass#272 follow-up: this derived fallback-FTS repair
    // probe can be multi-minute on large databases before any real
    // indexing work starts. Routine indexing now skips it by default;
    // operators investigating fallback FTS corruption can restore the
    // old preflight with CASS_PREFLIGHT_VALIDATE_FTS_MESSAGES=1, and
    // can still bypass that explicit probe with
    // CASS_SKIP_PREFLIGHT_VALIDATE_FTS_MESSAGES=1.
    let validate_fts_requested = preflight_validate_fts_messages_enabled();
    let validate_fts_skipped =
        validate_fts_requested && preflight_skip("watch_startup:validate_fts_messages");
    if !validate_fts_requested {
        tracing::debug!(
            target: "cass::indexer::preflight",
            db_path = %ctx.opts.db_path.display(),
            "skipping opt-in derived fallback FTS preflight validation before indexing"
        );
    } else if validate_fts_skipped {
        tracing::debug!(
            target: "cass::indexer::preflight",
            db_path = %ctx.opts.db_path.display(),
            "operator skip suppressed opt-in derived fallback FTS preflight validation before indexing"
        );
    } else if let Err(err) = storage.validate_fts_messages_integrity() {
        let detail = format!("{err:#}");
        if crate::storage::sqlite::error_message_indicates_populated_fts_shadow_without_rowid_reload(
            &detail,
        ) {
            tracing::warn!(
                db_path = %ctx.opts.db_path.display(),
                error = %detail,
                "skipping derived fallback FTS validation repair because frankensqlite cannot yet reload populated FTS shadow WITHOUT ROWID tables; canonical indexing will continue"
            );
        } else {
            tracing::warn!(
                db_path = %ctx.opts.db_path.display(),
                error = %err,
                "derived fallback FTS metadata is inconsistent; repairing before index pipeline"
            );
            storage.close_best_effort_in_place();
            let mut repair_storage = crate::storage::sqlite::open_franken_storage_with_timeout(
                &ctx.opts.db_path,
                Duration::from_secs(10),
            )
            .with_context(|| {
                format!(
                    "opening fresh storage for fallback FTS repair before indexing {}",
                    ctx.opts.db_path.display()
                )
            })?;
            let repair = repair_storage.ensure_search_fallback_fts_consistency();
            repair_storage.close_best_effort_in_place();
            let repair = repair.with_context(|| {
                format!(
                    "repairing derived fallback FTS before indexing {}",
                    ctx.opts.db_path.display()
                )
            })?;
            tracing::info!(
                db_path = %ctx.opts.db_path.display(),
                ?repair,
                "derived fallback FTS repair completed before index pipeline"
            );
            storage = crate::storage::sqlite::open_franken_storage_with_timeout(
                &ctx.opts.db_path,
                Duration::from_secs(10),
            )
            .with_context(|| {
                format!(
                    "reopening storage after fallback FTS repair before indexing {}",
                    ctx.opts.db_path.display()
                )
            })?;
            persist::apply_index_writer_busy_timeout(&storage);
            persist::apply_index_writer_checkpoint_policy(&storage, defer_checkpoints);
        }
    }
    ctx.complete_preflight_phase()?;

    ctx.enter_preflight_phase("watch_startup:reclassify_legacy_omp")?;
    let mut legacy_omp_upgrade = storage
        .reclassify_legacy_omp_conversations()
        .with_context(|| "reclassifying legacy Pi-labeled OMP archive rows")?;
    if legacy_omp_upgrade.conversations_reclassified > 0 {
        tracing::info!(
            conversations = legacy_omp_upgrade.conversations_reclassified,
            "reclassified legacy Pi-labeled OMP conversations and scheduled derived-asset rebuilds"
        );
    }
    ctx.complete_preflight_phase()?;

    if can_skip_unchanged_explicit_watch_once_index_run(
        &ctx.opts,
        &storage,
        &index_path,
        legacy_omp_upgrade.lexical_rebuild_required,
        legacy_omp_upgrade.analytics_rebuild_required,
    )? {
        let now_ms = FrankenStorage::now_millis();
        persist_final_index_run_metadata(&storage, &ctx.opts.db_path, false, now_ms, now_ms)?;
        record_lexical_population_strategy_if_unset(
            ctx.opts.progress.as_ref(),
            LexicalPopulationStrategy::IncrementalInline,
            "watch_once_targeted_reindex_applies_inline_lexical_updates_for_changed_paths",
        );
        reset_progress_to_idle(ctx.opts.progress.as_ref());
        let path_count = ctx
            .opts
            .watch_once_paths
            .as_ref()
            .map(std::vec::Vec::len)
            .unwrap_or_default();
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            path_count,
            "skipping unchanged explicit watch-once index run before startup maintenance"
        );
        close_storage_after_index(storage, &ctx.opts.db_path, "watch-once no-op index run")?;
        prime_archive_fingerprint_cache_from_completed_checkpoint(&ctx.opts.db_path, &index_path);
        return Ok(ControlFlow::Break(()));
    }

    ctx.enter_preflight_phase("watch_startup:cleanup_orphan_fk_rows")?;
    // cass#202 self-heal: a Connection dropped mid-transaction can
    // leave child rows persisted without a matching parent. The sweep
    // remains available as an explicit maintenance preflight, but it
    // is not a safe default on large archives: it walks broad message
    // ranges before any real indexing work starts.
    //
    // cass#265/cass#272 follow-up: both external reports and the
    // local profile of the production archive show this preflight can
    // peg CPU inside `collect_orphan_message_ids` / fsqlite page-cache
    // eviction for minutes. Routine indexing skips it by default; run
    // with CASS_PREFLIGHT_CLEANUP_ORPHAN_FK_ROWS=1 to force the old
    // repair behavior, or combine that with
    // CASS_SKIP_PREFLIGHT_CLEANUP_ORPHAN_FK_ROWS=1 to suppress an
    // explicit operator opt-in.
    let cleanup_orphans_requested = preflight_cleanup_orphan_fk_rows_enabled();
    let cleanup_orphans_skipped =
        cleanup_orphans_requested && preflight_skip("watch_startup:cleanup_orphan_fk_rows");
    if !cleanup_orphans_requested {
        tracing::debug!(
            target: "cass::fk_repair",
            db_path = %ctx.opts.db_path.display(),
            "skipping opt-in orphan-FK cleanup preflight before indexing"
        );
    } else if cleanup_orphans_skipped {
        tracing::warn!(
            target: "cass::fk_repair",
            db_path = %ctx.opts.db_path.display(),
            "cass#265: skipping cleanup_orphan_fk_rows preflight by operator request \
             (CASS_SKIP_PREFLIGHT_CLEANUP_ORPHAN_FK_ROWS=1). cass#202 orphan FK \
             self-heal is bypassed for this run only; a future run without the \
             env var will re-attempt the sweep."
        );
    } else if let Err(err) = storage.cleanup_orphan_fk_rows() {
        tracing::warn!(
            target: "cass::fk_repair",
            db_path = %ctx.opts.db_path.display(),
            error = %err,
            "cass#202: orphan FK self-heal failed; aborting index run before further writes"
        );
        storage.close_best_effort_in_place();
        return Err(orphan_fk_cleanup_failed_index_error(
            &ctx.opts.db_path,
            &err,
        ));
    }
    ctx.complete_preflight_phase()?;

    ctx.enter_preflight_phase("watch_startup:count_total_conversations")?;
    let initial_canonical_sessions_before_salvage = count_total_conversations_exact(&storage)?;
    ctx.complete_preflight_phase()?;
    if ctx.opts.full
        && !opened_fresh_for_full
        && full_rebuild_requires_historical_restart(
            &storage,
            &ctx.opts.db_path,
            initial_canonical_sessions_before_salvage,
        )?
    {
        tracing::error!(
            db_path = %ctx.opts.db_path.display(),
            conversations = initial_canonical_sessions_before_salvage,
            "full rebuild detected incomplete historical salvage state; refusing to replace the canonical archive"
        );
        storage.close_best_effort_in_place();
        return Err(canonical_archive_unhealthy_for_index_error(
            &ctx.opts.db_path,
            "historical salvage restart would require replacing canonical SQLite",
        ));
    }
    // canonical_only_full_rebuild: when --force-rebuild is set and we already
    // have canonical sessions in the DB, skip the expensive filesystem rescan
    // and go straight to rebuild_tantivy_from_db().  Plain --full continues to
    // rescan as expected (preserving the #153 fix).
    let canonical_only_full_rebuild =
        ctx.opts.force_rebuild && initial_canonical_sessions_before_salvage > 0;
    let has_explicit_watch_once_paths = ctx
        .opts
        .watch_once_paths
        .as_ref()
        .is_some_and(|paths| !paths.is_empty());
    let targeted_semantic_watch_once = should_run_targeted_semantic_watch_once(&ctx.opts);
    let populated_explicit_watch_once_only = has_explicit_watch_once_paths
        && !ctx.opts.watch
        && !ctx.opts.full
        && !ctx.opts.force_rebuild
        && !ctx.opts.semantic
        && !ctx.opts.build_hnsw
        && initial_canonical_sessions_before_salvage > 0;
    ctx.enter_preflight_phase("watch_startup:probe_lexical_checkpoint")?;
    let mut initial_matching_lexical_checkpoint = MatchingLexicalRebuildStateStatus::default();
    let mut restart_pending_lexical_rebuild_from_zero = false;
    let resume_lexical_rebuild = if ctx.opts.force_rebuild {
        false
    } else if initial_canonical_sessions_before_salvage > 0 {
        if let Some(status) = nonresumable_pending_lexical_rebuild_status_without_fingerprint(
            &index_path,
            &ctx.opts.db_path,
            initial_canonical_sessions_before_salvage,
        )? {
            if let Some(deferred) = should_defer_incremental_authoritative_lexical_repair(
                &ctx.opts,
                canonical_storage_rebuilt,
                initial_canonical_sessions_before_salvage,
                true,
                None,
            ) {
                tracing::warn!(
                    db_path = %ctx.opts.db_path.display(),
                    canonical_conversations = deferred.canonical_conversations,
                    db_size_bytes = deferred.db_size_bytes,
                    max_automatic_repair_db_size_bytes =
                        deferred.max_automatic_repair_db_size_bytes,
                    reason = deferred.reason,
                    "deferring pending lexical rebuild resume during routine incremental index"
                );
                initial_matching_lexical_checkpoint = MatchingLexicalRebuildStateStatus {
                    has_pending_resume: false,
                    ..status
                };
                record_deferred_incremental_canonical_lexical_repair(
                    ctx.opts.progress.as_ref(),
                    &deferred,
                );
            } else {
                initial_matching_lexical_checkpoint = status;
                restart_pending_lexical_rebuild_from_zero = true;
            }
        } else if populated_explicit_watch_once_only {
            if let Some(status) =
                matching_completed_lexical_rebuild_state_status_without_fingerprint(
                    &index_path,
                    &ctx.opts.db_path,
                    initial_canonical_sessions_before_salvage,
                )?
            {
                initial_matching_lexical_checkpoint = status;
            } else {
                initial_matching_lexical_checkpoint =
                    matching_lexical_rebuild_state_status_if_present(&index_path, || {
                        lexical_rebuild_db_state_with_total_conversations(
                            &storage,
                            &ctx.opts.db_path,
                            initial_canonical_sessions_before_salvage,
                        )
                    })?;
            }
        } else {
            initial_matching_lexical_checkpoint =
                matching_lexical_rebuild_state_status_if_present(&index_path, || {
                    lexical_rebuild_db_state_with_total_conversations(
                        &storage,
                        &ctx.opts.db_path,
                        initial_canonical_sessions_before_salvage,
                    )
                })?;
        }
        initial_matching_lexical_checkpoint.has_pending_resume
    } else {
        false
    };
    // GH #472: nightly still needs the full source census and archive safety
    // preflight. Only a matching completed checkpoint admits incremental
    // lexical publication. Missing evidence and explicit repair/deferral
    // requests retain the ordinary full rebuild path.
    let reconcile_nightly_lexical = ctx.opts.full
        && !ctx.opts.force_rebuild
        && !resume_lexical_rebuild
        && dotenvy_truthy("CASS_NIGHTLY_RECONCILIATION")
        && initial_matching_lexical_checkpoint.has_completed_checkpoint
        && !persist::defer_lexical_updates_enabled()
        && !preflight_skip("watch_startup:count_total_messages");
    let preserve_matching_completed_checkpoint_during_full_scan =
        should_preserve_matching_completed_lexical_checkpoint_during_full_scan(
            ctx.opts.full,
            resume_lexical_rebuild,
            canonical_only_full_rebuild,
            &initial_matching_lexical_checkpoint,
        );
    if ctx.opts.full
        && !resume_lexical_rebuild
        && !preserve_matching_completed_checkpoint_during_full_scan
    {
        clear_lexical_rebuild_state(&index_path)?;
    } else if preserve_matching_completed_checkpoint_during_full_scan {
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            completed_indexed_docs = initial_matching_lexical_checkpoint.completed_indexed_docs,
            "preserving matching completed lexical checkpoint during full scan until canonical mutations require a rebuild"
        );
    }
    ctx.complete_preflight_phase()?;
    let pre_scan_daily_stats_archive_fingerprint =
        preserve_matching_completed_checkpoint_during_full_scan
            .then_some(
                initial_matching_lexical_checkpoint
                    .completed_storage_fingerprint
                    .as_deref(),
            )
            .flatten();
    let mut checked_daily_stats_pre_scan = false;
    if ctx.opts.full && !canonical_only_full_rebuild {
        ctx.enter_preflight_phase("watch_startup:repair_daily_stats")?;
        if let DailyStatsRepairOutcome::SkippedKnownHealthyForFingerprint {
            archive_fingerprint,
        } = repair_daily_stats_if_drifted(
            &storage,
            &ctx.opts.db_path,
            pre_scan_daily_stats_archive_fingerprint,
        )? {
            tracing::info!(
                db_path = %ctx.opts.db_path.display(),
                archive_fingerprint,
                "skipping pre-scan daily_stats health probe because this full run preserved an archive fingerprint already known to be healthy"
            );
        }
        ctx.complete_preflight_phase()?;
        checked_daily_stats_pre_scan = true;
    } else if canonical_only_full_rebuild {
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            conversations = initial_canonical_sessions_before_salvage,
            "deferring daily_stats repair because full rebuild is reindexing an already-populated canonical database"
        );
    }

    let mut tantivy_requires_rebuild = false;
    let mut observed_tantivy_docs = None;
    ctx.enter_preflight_phase("watch_startup:tantivy_reader_preflight")?;
    if should_preflight_existing_tantivy_reader(
        resume_lexical_rebuild,
        ctx.opts.full && !reconcile_nightly_lexical,
    ) {
        // Detect if we are rebuilding due to missing meta/schema mismatch/index corruption.
        // IMPORTANT: This must stay aligned with TantivyIndex::open_or_create() rebuild triggers.
        let schema_hash_path = index_path.join("schema_hash.json");
        let schema_matches = schema_hash_path.exists()
            && std::fs::read_to_string(&schema_hash_path)
                .ok()
                .and_then(|content| serde_json::from_str::<serde_json::Value>(&content).ok())
                .and_then(|json| {
                    json.get("schema_hash")
                        .and_then(|v| v.as_str())
                        .map(schema_hash_matches)
                })
                .unwrap_or(false);

        // Treat missing schema hash as rebuild (open_or_create will wipe/recreate).
        tantivy_requires_rebuild = ctx.opts.force_rebuild
            || !crate::search::tantivy::searchable_index_exists(&index_path)
            || !schema_matches;

        // Preflight open: if the cass-compatible Tantivy reader can't open, force a
        // rebuild so we do a full scan and reindex messages into the new index
        // (SQLite is incremental-only by default).
        if !tantivy_requires_rebuild {
            // One validated open supplies both the contract check and the
            // live doc count; each Quill open hashes every live segment.
            match crate::search::tantivy::validated_searchable_index_summary(&index_path) {
                Ok(summary) => {
                    observed_tantivy_docs = Some(summary.docs);
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        path = %index_path.display(),
                        "tantivy open/contract preflight failed; forcing rebuild"
                    );
                    tantivy_requires_rebuild = true;
                }
            }
        }
    } else if resume_lexical_rebuild {
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            "skipping live Tantivy schema/reader preflight because checkpoint resume will rebuild directly from the canonical database"
        );
    } else if ctx.opts.full {
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            "deferring live Tantivy reader/doc-count preflight until after the full scan proves the canonical archive is unchanged"
        );
    } else {
        tracing::info!(db_path = %ctx.opts.db_path.display(), "skipping live Tantivy reader preflight");
    }
    // mknmt: a corrupt current MANIFEST opens through MANIFEST.prev, so the
    // reader preflight succeeds on the generation before the newest publish,
    // and a full run's post-scan skip compares only document counts. cass
    // builds Quill without durability sidecars, so only the canonical DB
    // restores that publish. Metadata only; a resumed rebuild replaces it.
    if !tantivy_requires_rebuild
        && !resume_lexical_rebuild
        && crate::search::tantivy::searchable_index_live_docs(&index_path).is_some_and(|live| {
            live.manifest_source
                == crate::search::quill_bridge::QuillManifestSource::PreviousAfterCorruptCurrent
        })
    {
        tracing::warn!(
            path = %index_path.display(),
            "lexical MANIFEST is corrupt and readers serve MANIFEST.prev; rebuilding the generation from the canonical database"
        );
        tantivy_requires_rebuild = true;
    }
    // 2l1b0.79: rows deleted outside the indexer (a forget, dedup or agent
    // purge whose own lexical rebuild failed or was interrupted) keep their
    // documents in the published generation, and the incremental scan never
    // revisits them. The indexer never deletes conversations, so fewer
    // canonical conversations than the completed checkpoint was certified
    // against means deleted text is still live: rebuild from canonical rows.
    if !tantivy_requires_rebuild
        && let Some(checkpoint_conversations) =
            completed_lexical_checkpoint_conversations(&index_path, &ctx.opts.db_path)
        && initial_canonical_sessions_before_salvage < checkpoint_conversations
    {
        tracing::warn!(
            db_path = %ctx.opts.db_path.display(),
            canonical_conversations = initial_canonical_sessions_before_salvage,
            checkpoint_conversations,
            "canonical conversations were deleted since the lexical generation was certified; rebuilding it"
        );
        tantivy_requires_rebuild = true;
    }
    ctx.complete_preflight_phase()?;
    let mut needs_rebuild = legacy_omp_upgrade.lexical_rebuild_required
        || should_force_authoritative_rebuild(canonical_storage_rebuilt, tantivy_requires_rebuild);
    // #289: pass the real `tantivy_requires_rebuild` instead of hardcoding
    // `authoritative_rebuild_required: true`. The hardcoded value made every
    // plain populated incremental over the automatic-repair DB-size cap
    // (default 1 GiB) report a deferred repair probe even when no
    // authoritative rebuild was required, which suppressed the end-of-run
    // lexical checkpoint refresh — including the cheap metadata-only
    // bootstrap that rewrites a missing `.lexical-rebuild-state.json` — so a
    // lost checkpoint could never converge on large archives.
    let large_incremental_authoritative_lexical_repair_probe_deferred =
        if legacy_omp_upgrade.lexical_rebuild_required {
            None
        } else {
            should_defer_incremental_authoritative_lexical_repair(
                &ctx.opts,
                canonical_storage_rebuilt,
                initial_canonical_sessions_before_salvage,
                tantivy_requires_rebuild,
                observed_tantivy_docs,
            )
        };
    let deferred_incremental_authoritative_lexical_repair =
        large_incremental_authoritative_lexical_repair_probe_deferred;
    if let Some(deferred) = &deferred_incremental_authoritative_lexical_repair {
        tracing::warn!(
            db_path = %ctx.opts.db_path.display(),
            canonical_conversations = deferred.canonical_conversations,
            db_size_bytes = deferred.db_size_bytes,
            max_automatic_repair_db_size_bytes =
                deferred.max_automatic_repair_db_size_bytes,
            observed_tantivy_docs = deferred.observed_tantivy_docs,
            reason = deferred.reason,
            "deferring heavyweight authoritative lexical repair during routine incremental index"
        );
        record_deferred_incremental_canonical_lexical_repair(ctx.opts.progress.as_ref(), deferred);
        needs_rebuild = false;
    }
    let initial_needs_rebuild = needs_rebuild;
    // at3us: a lexical rebuild on an archive that holds no conversations yet
    // (a first plain `cass index`) has nothing to preserve, so it takes the
    // path --full takes: ingest canonically, then rebuild the index once from
    // the database. Rebuilding inline from the scan commits Quill at every
    // batch, and every publish re-verifies every segment, so that path is
    // quadratic in index size (10,252 Codex sessions: 62 min against 7). A
    // populated archive that needs a rebuild is repaired from the database
    // before its scan instead, so only an empty archive reached the inline
    // rebuild here.
    let empty_archive_lexical_rebuild =
        !ctx.opts.full && initial_needs_rebuild && initial_canonical_sessions_before_salvage == 0;

    if needs_rebuild && let Some(p) = &ctx.opts.progress {
        p.is_rebuilding.store(true, Ordering::Relaxed);
    }

    // Keep the published lexical generation live until the authoritative
    // rebuild has produced and validated its replacement. Fresh non-empty
    // rebuilds already use `publish_staged_lexical_index`, whose atomic swap
    // retains the prior generation only at commit time and rolls it back if
    // publication fails. Eagerly renaming the live tree here created a much
    // larger failure window: a killed or failed semantic run could strand the
    // old generation at `*.bak` and leave lexical search empty or partial.
    // The rebuild implementation below remains responsible for clearing only
    // an unpublished/incomplete target when staged publication is impossible.
    // Record scan start time before scanning
    let scan_start_ts = FrankenStorage::now_millis();

    let keep_tantivy_open_after_rebuild = ctx.opts.watch
        || ctx
            .opts
            .watch_once_paths
            .as_ref()
            .is_some_and(|paths| !paths.is_empty());

    Ok(ControlFlow::Continue(RunIndexPreflightOutcome {
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
    }))
}
