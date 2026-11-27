use super::*;

pub(in crate::indexer) fn rebuild_tantivy_from_db_with_options(
    db_path: &Path,
    data_dir: &Path,
    total_conversations: usize,
    progress: Option<Arc<IndexingProgress>>,
    options: LexicalRebuildStartupOptions,
    progress_bump: Option<Arc<AtomicI64>>,
) -> Result<LexicalRebuildOutcome> {
    match rebuild_tantivy_from_db_once(
        db_path,
        data_dir,
        total_conversations,
        progress.clone(),
        options,
        progress_bump.clone(),
    ) {
        // GH #494: the retained candidate's checkpoint now starts at zero, so
        // this pass is not resumed and cannot be retried again.
        Err(error)
            if error
                .downcast_ref::<ResumedLexicalCandidateRetained>()
                .is_some() =>
        {
            rebuild_tantivy_from_db_once(
                db_path,
                data_dir,
                total_conversations,
                progress,
                options,
                progress_bump,
            )
        }
        outcome => outcome,
    }
}

pub(in crate::indexer) fn rebuild_tantivy_from_db_once(
    db_path: &Path,
    data_dir: &Path,
    total_conversations: usize,
    progress: Option<Arc<IndexingProgress>>,
    options: LexicalRebuildStartupOptions,
    progress_bump: Option<Arc<AtomicI64>>,
) -> Result<LexicalRebuildOutcome> {
    let prep_profile = std::env::var_os("CASS_PREP_PROFILE").is_some();
    let prep_started = Instant::now();
    let mut prep_step_started = Instant::now();
    let prep_progress = progress.clone();
    let log_prep_step = move |step: &str, step_started: &mut Instant| {
        // #366: each completed prep step (readonly open, checkpoint load,
        // shard planning, ...) is forward progress. Tick the watchdog's
        // activity channel unconditionally so the pre-pipeline hand-off
        // window is not misread as a finalize wedge and aborted.
        if let Some(p) = &prep_progress {
            p.tick_activity();
        }
        let step_ms = step_started.elapsed().as_millis() as u64;
        let total_ms = prep_started.elapsed().as_millis() as u64;
        if prep_profile {
            eprintln!(
                "CASS_PREP_PROFILE step={step} step_ms={} total_ms={}",
                step_ms, total_ms
            );
            tracing::info!(
                component = "main",
                step,
                step_ms,
                total_ms,
                "lexical rebuild prep profile"
            );
        }
        *step_started = Instant::now();
    };

    let storage = FrankenStorage::open_strict_readonly(db_path).with_context(|| {
        format!(
            "opening database for Tantivy rebuild: {}",
            db_path.display()
        )
    })?;

    log_prep_step("open_readonly", &mut prep_step_started);

    // Task 8: capture the publication coverage floor at BUILD START —
    // before the streaming loop stages a single document. A canonical
    // commit landing mid-build then lands ABOVE the floor, so the
    // certification this build's terminal publication writes can never
    // claim revisions the generation does not contain. `None` on an
    // unextended archive — certification binds current revisions there
    // anyway.
    let build_start_floor =
        crate::viewer_extensions::changes::current_journal_revisions(storage.raw())
            .context("capturing the lexical build-start coverage floor")?;

    let index_path = index_dir(data_dir)?;
    let db_state = if options.defer_initial_content_fingerprint {
        deferred_lexical_rebuild_db_state(db_path, total_conversations)
    } else {
        lexical_rebuild_db_state_with_total_conversations(&storage, db_path, total_conversations)?
    };

    log_prep_step(
        if options.defer_initial_content_fingerprint {
            "prepare_db_state_deferred_fingerprint"
        } else {
            "compute_db_state_fingerprint"
        },
        &mut prep_step_started,
    );

    let mut rebuild_state = if options.defer_initial_content_fingerprint {
        LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE)
    } else {
        match load_lexical_rebuild_state(&index_path)? {
            Some(state) if state.matches_run(&db_state, LEXICAL_REBUILD_PAGE_SIZE) => {
                if lexical_rebuild_bound_candidate_is_missing(&index_path, &state)?
                    || legacy_lexical_resume_lost_its_generation(&index_path, &state)?
                {
                    let quarantine =
                        retain_missing_lexical_candidate_checkpoint(&index_path, &state)?;
                    tracing::warn!(
                        checkpoint_path = %lexical_rebuild_state_path(&quarantine).display(),
                        missing_candidate = %staged_lexical_rebuild_scratch_path(&index_path).display(),
                        processed_conversations = state.reported_processed_conversations(),
                        indexed_docs = state.reported_indexed_docs(),
                        "lexical resume generation is missing or ambiguous; retained its checkpoint and restarting off-live, not adopting the prior live generation"
                    );
                    LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE)
                } else if state.is_incomplete() && state.requires_restart_from_zero_on_resume() {
                    tracing::info!(
                        db_path = %db_path.display(),
                        execution_mode = state
                            .execution_mode
                            .map_or("legacy_unspecified", LexicalRebuildExecutionMode::as_str),
                        processed_conversations = state.reported_processed_conversations(),
                    "discarding non-resumable lexical rebuild checkpoint and restarting from zero"
                    );
                    LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE)
                } else {
                    let mut state = reconcile_pending_lexical_commit(&index_path, state)?;
                    normalize_lexical_rebuild_state_for_current_run(&index_path, &mut state)?;
                    upgrade_lexical_rebuild_state_resume_cursor_if_needed(
                        &storage,
                        &index_path,
                        &mut state,
                    )?;
                    prepare_lexical_rebuild_state_for_active_run(&index_path, &mut state)?;
                    state
                }
            }
            Some(state) => {
                tracing::info!(
                    db_path = %db_path.display(),
                    existing_db_path = %state.db.db_path,
                    existing_total_conversations = state.db.total_conversations,
                    existing_storage_fingerprint = %state.db.storage_fingerprint,
                    "discarding incompatible lexical rebuild checkpoint and restarting from zero"
                );
                LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE)
            }
            None => LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE),
        }
    };

    log_prep_step("load_checkpoint_state", &mut prep_step_started);

    // GH #494: EOF is an ingest cursor, NOT a publication receipt. An
    // interrupted/refused terminal publish leaves all rows committed in the
    // scratch generation but completed=false. It must pass through finalization
    // below, including the swap and completed checkpoint, even with no rows left.
    // This also lets a genuinely empty archive publish its first readable index.
    if rebuild_state.completed {
        lexical_publish::Finalization::new(
            &index_path,
            &index_path,
            rebuild_state.indexed_docs,
            rebuild_state.processed_conversations,
        )
        .run("reuse_completed_generation", || {
            crate::search::tantivy::validate_searchable_index_contract(&index_path)?;
            verify_published_lexical_doc_count(
                &index_path,
                rebuild_state.indexed_docs,
                "completed",
            )?;
            // A crash after the swap may leave the old generation parked.
            // Retain it without reopening the rebuild or swapping it back.
            recover_or_finalize_interrupted_lexical_publish_backup(&index_path)
        })?;
        storage.close_without_checkpoint().with_context(|| {
            format!(
                "closing readonly database after confirming completed Tantivy rebuild without checkpoint: {}",
                db_path.display()
            )
        })?;
        if let Some(p) = &progress {
            p.phase.store(0, Ordering::Relaxed);
            p.is_rebuilding.store(false, Ordering::Relaxed);
        }
        return Ok(LexicalRebuildOutcome {
            observed_conversations: rebuild_state.processed_conversations,
            indexed_docs: rebuild_state.indexed_docs,
            indexed_region_docs: rebuild_state.indexed_region_docs,
            observed_messages: Some(
                rebuild_state.db.total_messages.max(
                    // Task 5: `indexed_docs` also counts converged
                    // search-region documents; the canonical-message
                    // share is the total minus the persisted region
                    // share, so reused generations never report region
                    // docs as messages.
                    rebuild_state
                        .indexed_docs
                        .saturating_sub(rebuild_state.indexed_region_docs),
                ),
            ),
            exact_checkpoint_persisted: false,
            equivalence: None,
        });
    }

    if !options.defer_initial_content_fingerprint
        && rebuild_state.processed_conversations >= total_conversations
    {
        tracing::info!(
            processed_conversations = rebuild_state.processed_conversations,
            total_conversations,
            indexed_docs = rebuild_state.indexed_docs,
            index_path = %index_path.display(),
            "lexical rebuild cursor reached EOF but publication is incomplete; resuming finalization"
        );
    }
    let mut restart_from_zero =
        rebuild_state.processed_conversations == 0 && rebuild_state.pending.is_none();

    // Plan staged shards BEFORE deciding whether to pre-wipe the live index.
    // Bead 9ct8r: when the rebuild will go through
    // `rebuild_tantivy_from_db_via_staged_shards`, the build happens in a
    // scratch tmpdir and `publish_staged_lexical_index` atomically swaps it
    // into the live path at commit time. Pre-wiping the live index in that
    // case is both redundant AND actively harmful — it exposes a multi-
    // second 0-doc window to any concurrent reader (tests/atomic_swap_publish_crash_window.rs
    // currently tolerates this window with an `Ok(Some(0))` carve-out; the
    // carve-out's bead-0k0sk follow-up is this fix).
    let page_size = LEXICAL_REBUILD_PAGE_SIZE;
    let pipeline_settings = lexical_rebuild_pipeline_settings_snapshot();
    // CASS->Quill flip: staged sharding exists ONLY to parallelize Tantivy
    // ingest — it builds N independent index directories and merges them with
    // `merge_compatible_index_directories`, which opens Tantivy directories and
    // merges Tantivy segments. Quill has no N-directory merge (its
    // `concat_merge` joins segments WITHIN one manifest), and building one would
    // mean remapping Q1 global docid spaces and rebuilding IDMAP across
    // independent indexes.
    //
    // It is not needed: measured Quill CASS ingest is ~9.8k docs/s, so a
    // 442k-message corpus streams into ONE index in minutes. The single-index
    // path below is the same code that already runs whenever no shard plan is
    // produced, so this is a strategy choice, not a new code path.
    let staged_shard_plan: Option<LexicalShardPlan> = None;
    let _ = (restart_from_zero, total_conversations, &pipeline_settings);
    if staged_shard_plan.is_some() {
        log_prep_step("plan_lexical_shards", &mut prep_step_started);
    }
    // Any fresh non-empty shard plan can rebuild off-live and publish
    // atomically, even when the plan collapses to a single shard. The
    // final staged artifact already supports the single-input fast-path,
    // and forcing single-shard fresh rebuilds through that path closes
    // bead 9ct8r's remaining 0-doc crash window for concurrent readers.
    let will_use_atomic_staged_publish = staged_shard_plan.is_some();

    // Atomic publish is INDEPENDENT of sharding. Bead 9ct8r's invariant is that a
    // concurrent reader never observes an intermediate doc count during a
    // from-zero rebuild; the staged-shards path delivered that by building
    // off-live and swapping, but the swap is what matters, not the N shards.
    // With sharding disabled for Quill, the rebuild still builds into a scratch
    // sibling and publishes atomically — otherwise pre-wiping the live index
    // would reopen the multi-second 0-doc window that bead closed.
    //
    // IMPORTANT: only the INDEX CONTENT goes to the scratch directory. The
    // rebuild state file stays on the LIVE path, because that is where the next
    // run looks for it (`load_lexical_rebuild_state(&index_path)` above). Moving
    // the state into the scratch dir would make every interrupted from-zero
    // rebuild restart from zero — the exact failure #380 reports.
    let scratch_path = staged_lexical_rebuild_scratch_path(&index_path);
    let mut staged_build_path = if restart_from_zero && !will_use_atomic_staged_publish {
        // Fresh staged build: discard any scratch left by an abandoned run.
        if let Err(err) = fs::remove_dir_all(&scratch_path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            return Err(err).with_context(|| {
                format!(
                    "clearing stale staged rebuild scratch {}",
                    scratch_path.display()
                )
            });
        }
        fs::create_dir_all(&scratch_path).with_context(|| {
            format!(
                "creating staged rebuild scratch directory {}",
                scratch_path.display()
            )
        })?;
        rebuild_state = LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE);
        Some(scratch_path.clone())
    } else if !will_use_atomic_staged_publish
        && rebuild_state.is_incomplete()
        && lexical_rebuild_resume_content_path(&index_path, &rebuild_state)? != index_path
    {
        // Resuming a staged build that was interrupted mid-flight: the partial
        // index lives in the scratch dir, so continue there rather than in the
        // live index.
        //
        // `is_incomplete()` is load-bearing, not defensive. A scratch directory
        // can also survive a crash in the narrow window between the publish
        // RENAME_EXCHANGE and the rename that parks the prior-live index as a
        // backup. In that state the scratch holds the PRIOR-LIVE index while
        // `index_path` already holds the newer one, and the checkpoint is
        // `completed`. Resuming into it would publish the older index back over
        // the newer one and then record that as a completed rebuild. Requiring
        // an incomplete checkpoint keeps that debris out of the publish path;
        // it is discarded by the next from-zero rebuild's wipe above.
        Some(scratch_path.clone())
    } else {
        None
    };
    // Where the index CONTENT is built. Equals the live path for an ordinary
    // in-place incremental run; the scratch path for a staged full rebuild.
    let mut build_path = staged_build_path
        .clone()
        .unwrap_or_else(|| index_path.clone());
    if restart_from_zero && will_use_atomic_staged_publish {
        // Staged-shards path will atomically swap the new index into
        // `index_path` via `publish_staged_lexical_index`. The live
        // index stays intact for concurrent readers until the swap —
        // that's the whole point. We still need to reset the in-memory
        // rebuild_state so the rebuild starts from pristine accounting;
        // the staged-shards delegation writes a fresh state file to
        // the old live path (which gets moved to retained-backups on
        // publish) and a completed state file to the new live path
        // after publish succeeds.
        rebuild_state = LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE);
    }

    // GH #494: a saved cursor is only usable while its committed prefix still
    // exists. Check BEFORE open_or_create: a missing MANIFEST creates an empty
    // writer, and a schema mismatch can wipe the candidate. Neither may keep
    // the old counters/cursor. Do this before constructing producer/controller
    // state so a restart uses one coherent zero-based pipeline, not a new
    // checkpoint with the previous run's accounting still captured in locals.
    if !restart_from_zero && !will_use_atomic_staged_publish {
        let resume_check = (|| -> Result<()> {
            // GH #530: pending commit reconciliation above accounts for an
            // exactly landed commit before this check. Any remaining count gap
            // makes this prefix ambiguous, even when its cursor is short of EOF.
            // Upserting the suffix cannot remove an uncounted prefix identity or
            // a foreign document, and waiting for terminal certification wastes
            // another pass through the suffix on every interrupted retry.
            // Inspect one validated reader, without opening a writer or changing
            // the expected count to whatever the candidate happens to contain.
            let summary = crate::search::tantivy::validated_searchable_index_summary(&build_path)?;
            anyhow::ensure!(
                summary.docs == rebuild_state.indexed_docs,
                "resumed lexical candidate contains {} documents but its reconciled checkpoint \
                 requires exactly {}; refusing ambiguous prefix before replay",
                summary.docs,
                rebuild_state.indexed_docs,
            );
            Ok(())
        })();
        if let Err(error) = resume_check {
            let cause = format!("{error:#}");
            if staged_build_path.is_some() {
                let quarantine = lexical_publish::quarantine_incomplete_candidate(&build_path)
                    .with_context(|| {
                        format!("cannot retain unusable lexical candidate: {cause}")
                    })?;
                // Keep the exact cursor/accounting that could no longer be
                // resumed alongside the untouched failed candidate files.
                persist_lexical_rebuild_state(&quarantine, &rebuild_state)?;
                tracing::warn!(
                    error = %cause,
                    quarantine_path = %quarantine.display(),
                    "retained unusable staged lexical candidate; restarting canonical replay"
                );
            } else {
                // An older checkpoint can refer to in-place work, or staging
                // can have disappeared. Never wipe or move the prior live tree
                // just to restart: build its replacement off-live instead.
                tracing::warn!(
                    error = %cause,
                    index_path = %index_path.display(),
                    "lexical resume prefix is unusable; preserving live tree and restarting off-live"
                );
            }
            fs::create_dir(&scratch_path).with_context(|| {
                format!(
                    "creating replacement lexical candidate {}",
                    scratch_path.display()
                )
            })?;
            build_path = scratch_path.clone();
            staged_build_path = Some(scratch_path.clone());
            rebuild_state = LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE);
            restart_from_zero = true;
        }
        log_prep_step("validate_resume_prefix", &mut prep_step_started);
    }

    // Persist this location before the first commit/heartbeat. A later process
    // must not infer which generation owns the cursor from directory existence.
    rebuild_state.set_execution_mode(if staged_build_path.is_some() {
        LexicalRebuildExecutionMode::StagedSingleIndex
    } else {
        LexicalRebuildExecutionMode::LiveSingleIndex
    });
    let resumed_from_checkpoint = rebuild_state.processed_conversations > 0;
    log_prep_step("restart_from_zero_reset", &mut prep_step_started);
    let batch_conversation_limit = lexical_rebuild_batch_fetch_conversation_limit(page_size);
    let initial_batch_conversation_limit =
        lexical_rebuild_initial_batch_fetch_conversation_limit(batch_conversation_limit);
    let lexical_rebuild_worker_pool = build_lexical_rebuild_worker_pool()?;
    let mut commit_interval_conversations;
    let mut commit_interval_messages;
    let mut commit_interval_message_bytes;
    let progress_heartbeat_interval_conversations =
        lexical_rebuild_progress_heartbeat_interval_conversations();
    let progress_heartbeat_interval = lexical_rebuild_progress_heartbeat_interval();
    let mut indexed_docs = rebuild_state.indexed_docs;
    let mut observed_messages = rebuild_state.indexed_docs;
    let mut processed_conversations = rebuild_state.processed_conversations;
    let mut last_processed_conversation_id = rebuild_state.committed_conversation_id;
    let mut conversations_since_commit = 0usize;
    let mut messages_since_commit = 0usize;
    let mut message_bytes_since_commit = 0usize;
    let mut conversations_since_progress_persist = 0usize;
    let mut last_progress_persist = Instant::now();
    let mut pending_batch: Vec<LexicalRebuildConversationPacket> = Vec::with_capacity(
        batch_conversation_limit
            .max(initial_batch_conversation_limit)
            .max(1),
    );
    let mut pending_batch_message_count = 0usize;
    let mut pending_batch_message_bytes = 0usize;
    let mut latest_pipeline_runtime = LexicalRebuildPipelineRuntimeSnapshot::default();
    let mut perf_profile = LexicalRebuildPerfProfile::from_env();
    let rebuild_profile_started = perf_profile.as_ref().map(|_| Instant::now());

    let pipeline_channel_size = lexical_rebuild_pipeline_channel_size();
    let startup_commit_interval_conversations =
        lexical_rebuild_initial_commit_interval_conversations()
            .min(lexical_rebuild_commit_interval_conversations());
    let startup_commit_interval_messages = lexical_rebuild_initial_commit_interval_messages()
        .min(lexical_rebuild_commit_interval_messages());
    let startup_commit_interval_message_bytes =
        lexical_rebuild_initial_commit_interval_message_bytes()
            .min(lexical_rebuild_commit_interval_message_bytes());
    let steady_commit_interval_conversations = lexical_rebuild_commit_interval_conversations();
    let steady_commit_interval_messages = lexical_rebuild_commit_interval_messages();
    let steady_commit_interval_message_bytes = lexical_rebuild_commit_interval_message_bytes();
    let startup_pipeline_budget = lexical_rebuild_runtime_pipeline_budget_snapshot(
        initial_batch_conversation_limit,
        startup_commit_interval_messages,
        startup_commit_interval_message_bytes,
        pipeline_channel_size,
        startup_commit_interval_conversations,
        startup_commit_interval_messages,
        startup_commit_interval_message_bytes,
    );
    let steady_pipeline_budget = lexical_rebuild_runtime_pipeline_budget_snapshot(
        batch_conversation_limit,
        steady_commit_interval_messages,
        steady_commit_interval_message_bytes,
        pipeline_channel_size,
        steady_commit_interval_conversations,
        steady_commit_interval_messages,
        steady_commit_interval_message_bytes,
    );
    let controller_policy = lexical_rebuild_responsiveness_policy();
    let available_parallelism = pipeline_settings.available_parallelism;
    let reserved_cores = pipeline_settings.reserved_cores;
    let controller_loadavg_high_watermark_1m_milli =
        lexical_rebuild_controller_loadavg_high_watermark_1m_milli_for_available_and_reserved(
            available_parallelism,
            reserved_cores,
        );
    let controller_loadavg_low_watermark_1m_milli =
        lexical_rebuild_controller_loadavg_low_watermark_1m_milli_from_high(
            controller_loadavg_high_watermark_1m_milli,
        );
    let start_conservative = match controller_policy {
        LexicalRebuildResponsivenessPolicy::Auto => rebuild_state.processed_conversations == 0,
        LexicalRebuildResponsivenessPolicy::Steady => false,
        LexicalRebuildResponsivenessPolicy::Conservative => true,
    };
    let mut responsiveness_controller = LexicalRebuildResponsivenessController::new(
        controller_policy,
        startup_pipeline_budget,
        steady_pipeline_budget,
        pipeline_channel_size,
        start_conservative,
        controller_loadavg_high_watermark_1m_milli,
        controller_loadavg_low_watermark_1m_milli,
    );
    let mut current_batch_conversation_limit = responsiveness_controller
        .current_budget()
        .page_conversation_limit;
    let current_budget = responsiveness_controller.current_budget();
    commit_interval_conversations = current_budget.commit_interval_conversations;
    commit_interval_messages = current_budget.commit_interval_messages;
    commit_interval_message_bytes = current_budget.commit_interval_message_bytes;
    let lexical_rebuild_flow_limiter = Arc::new(StreamingByteLimiter::new(
        responsiveness_controller
            .current_budget()
            .max_message_bytes_in_flight,
    ));
    let pipeline_budget_controller = Arc::new(LexicalRebuildPipelineBudgetController::new(
        responsiveness_controller.current_budget(),
    ));
    let first_budget_promotion_wait = responsiveness_controller
        .waits_for_first_durable_commit()
        .then_some(LexicalRebuildFirstBudgetPromotionWait {
            generation: pipeline_budget_controller.generation(),
            commit_interval_conversations,
            commit_interval_messages,
            commit_interval_message_bytes,
        });
    let producer_telemetry = Arc::new(LexicalRebuildProducerTelemetry::default());
    refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
        &mut latest_pipeline_runtime,
        progress.as_ref(),
        lexical_rebuild_flow_limiter.as_ref(),
        Some(producer_telemetry.as_ref()),
        &mut responsiveness_controller,
        pipeline_budget_controller.as_ref(),
        &mut current_batch_conversation_limit,
        Some((
            &mut commit_interval_conversations,
            &mut commit_interval_messages,
            &mut commit_interval_message_bytes,
        )),
        LexicalRebuildPipelineSinkRuntimeSnapshot::new(0, 0, 0),
    );
    let lexical_rebuild_worker_pool = lexical_rebuild_worker_pool.map(Arc::new);
    let (pipeline_tx, pipeline_rx) =
        bounded::<LexicalRebuildPipelineMessage>(pipeline_channel_size);
    let mut pipeline_tx = Some(pipeline_tx);
    let mut pipeline_rx = Some(pipeline_rx);
    let mut producer_handle = None;
    if restart_from_zero {
        producer_handle = Some(spawn_lexical_rebuild_packet_producer(
            db_path.to_path_buf(),
            rebuild_state.committed_conversation_id,
            staged_shard_plan.clone(),
            page_size,
            pipeline_channel_size,
            first_budget_promotion_wait,
            pipeline_budget_controller.clone(),
            pipeline_tx
                .take()
                .expect("packet producer sender missing before startup overlap"),
            lexical_rebuild_flow_limiter.clone(),
            lexical_rebuild_worker_pool.clone(),
            producer_telemetry.clone(),
        ));
        log_prep_step("start_packet_producer", &mut prep_step_started);
    }

    if let Some(ref shard_plan) = staged_shard_plan {
        rebuild_state.set_execution_mode(LexicalRebuildExecutionMode::StagedShardBuild);
        let pipeline_rx = pipeline_rx
            .take()
            .expect("staged lexical rebuild pipeline receiver missing before consume loop");
        let producer_handle = producer_handle
            .take()
            .expect("staged lexical rebuild packet producer handle missing before consume loop");
        return rebuild_tantivy_from_db_via_staged_shards(
            db_path,
            &index_path,
            total_conversations,
            progress,
            options,
            prep_profile.then_some(prep_started),
            storage,
            build_start_floor,
            rebuild_state,
            shard_plan.clone(),
            pipeline_settings,
            progress_heartbeat_interval_conversations,
            progress_heartbeat_interval,
            latest_pipeline_runtime,
            responsiveness_controller,
            pipeline_budget_controller,
            current_batch_conversation_limit,
            lexical_rebuild_flow_limiter,
            producer_telemetry,
            lexical_rebuild_worker_pool,
            pipeline_rx,
            producer_handle,
            perf_profile,
            rebuild_profile_started,
            progress_bump,
        );
    }

    let mut t_index = match (|| -> Result<TantivyIndex> {
        // A remaining writer-open error (locks, permissions, or a race after
        // preflight) is not permission to erase the candidate. Leave it and its
        // checkpoint intact; a retry revalidates before initializing counters.
        let mut t_index = TantivyIndex::open_or_create(&build_path).with_context(|| {
            format!(
                "opening lexical rebuild writer {}; candidate retained",
                build_path.display()
            )
        })?;
        log_prep_step("open_tantivy", &mut prep_step_started);

        // GH #446: the sink side of the pipeline (accumulate / commit /
        // merge) is where a large rebuild spends its longest silent
        // stretches; let that work tick the watchdog's `activity` signal.
        install_lexical_index_heartbeat(&mut t_index, progress.as_ref());
        t_index.configure_bulk_load_merge_policy();

        // Keep the persisted checkpoint aligned with the in-memory active-run
        // state before any producer heartbeat arrives. This closes attach/resume
        // windows where we discarded or reset stale state in memory but would
        // otherwise leave the old checkpoint file visible until the first
        // heartbeat or durable commit.
        persist_lexical_rebuild_state_for_active_run_start(&index_path, &rebuild_state)?;

        log_prep_step("persist_initial_checkpoint", &mut prep_step_started);

        Ok(t_index)
    })() {
        Ok(index) => index,
        Err(err) => {
            if let Some(handle) = producer_handle.take() {
                lexical_rebuild_flow_limiter.close();
                drop(pipeline_rx.take());
                if let Err(payload) = handle.join() {
                    let panic_message = panic_payload_message(payload);
                    tracing::warn!(
                        panic_message = %panic_message,
                        "lexical rebuild packet producer panicked during startup cleanup"
                    );
                }
            }
            return Err(err);
        }
    };

    // Task 8: arm the coverage floor captured at build start — the lazy
    // stage-time captures below must never move it forward past a commit
    // this generation cannot contain.
    t_index.arm_publication_floor(build_start_floor);

    // #440: fence the resume cursor against the published authority. The
    // checkpoint says how many documents the last durable commit covered;
    // the index says how many are actually live. Anything live beyond the
    // checkpoint was published without a matching checkpoint advance (a
    // pre-fix engine visibility publish, or an upsert-triggered commit inside
    // an earlier reconcile window) and replaying it with plain adds is
    // refused as a duplicate identity. Route that many leading documents —
    // plus a small margin — through the identity-idempotent upsert path so
    // the replay converges instead of failing. A resume that builds in the
    // LIVE directory (no staged scratch) may be sitting on a complete prior
    // generation whose identities are anywhere in the replay, so it upserts
    // everything.
    let resume_reconcile_docs = resume_reconcile_upsert_budget(
        staged_build_path.is_none(),
        rebuild_state.processed_conversations > 0 || rebuild_state.pending.is_some(),
        rebuild_state.indexed_docs,
        usize::try_from(t_index.doc_count().unwrap_or(0)).unwrap_or(usize::MAX),
    );
    if resume_reconcile_docs > 0 {
        tracing::warn!(
            checkpoint_indexed_docs = rebuild_state.indexed_docs,
            build_path = %build_path.display(),
            reconcile_docs = resume_reconcile_docs,
            "published lexical authority is ahead of the rebuild checkpoint; resuming through identity-idempotent upserts (#440)"
        );
        t_index.arm_resume_reconcile(resume_reconcile_docs);
    }

    if let Some(p) = &progress {
        p.phase.store(2, Ordering::Relaxed);
        p.is_rebuilding.store(true, Ordering::Relaxed);
        p.total.store(total_conversations, Ordering::Relaxed);
        p.current
            .store(rebuild_state.processed_conversations, Ordering::Relaxed);
        p.total_is_final.store(
            !options.defer_initial_content_fingerprint,
            Ordering::Relaxed,
        );
        p.discovered_agents.store(0, Ordering::Relaxed);
    }

    if producer_handle.is_none() {
        producer_handle = Some(spawn_lexical_rebuild_packet_producer(
            db_path.to_path_buf(),
            rebuild_state.committed_conversation_id,
            staged_shard_plan.clone(),
            page_size,
            pipeline_channel_size,
            first_budget_promotion_wait,
            pipeline_budget_controller.clone(),
            pipeline_tx
                .take()
                .expect("packet producer sender missing before normal startup"),
            lexical_rebuild_flow_limiter.clone(),
            lexical_rebuild_worker_pool.clone(),
            producer_telemetry.clone(),
        ));
        log_prep_step("start_packet_producer", &mut prep_step_started);
    }

    let pipeline_rx = pipeline_rx
        .take()
        .expect("lexical rebuild pipeline receiver missing before consume loop");
    let producer_handle = producer_handle
        .take()
        .expect("lexical rebuild packet producer handle missing before consume loop");

    {
        let step_ms = prep_step_started.elapsed().as_millis() as u64;
        let total_ms = prep_started.elapsed().as_millis() as u64;
        if prep_profile {
            eprintln!(
                "CASS_PREP_PROFILE step=ready_to_index step_ms={} total_ms={}",
                step_ms, total_ms
            );
        }
        tracing::info!(
            component = "main",
            step = "ready_to_index",
            total_prep_ms = total_ms,
            total_conversations,
            restart_from_zero,
            "lexical rebuild startup complete, entering consumer loop"
        );
    }
    let lexical_rebuild_started = Instant::now();
    let mut max_conversation_id = 0i64;
    let mut max_message_id = 0i64;
    let mut equivalence_accumulator = LexicalRebuildEquivalenceAccumulator::new();

    {
        macro_rules! finish_conversation {
            ($packet:expr) => {{
                let packet = $packet;
                let conversation_id = packet.identity.conversation_id.ok_or_else(|| {
                    anyhow::anyhow!(
                        "authoritative lexical rebuild packet missing stable conversation id at processed_conversations={}",
                        processed_conversations
                    )
                })?;
                let message_count = packet.message_count;
                let message_bytes = packet.message_bytes;
                observed_messages = observed_messages.saturating_add(message_count);
                pending_batch_message_count =
                    pending_batch_message_count.saturating_add(message_count);
                pending_batch_message_bytes =
                    pending_batch_message_bytes.saturating_add(message_bytes);
                pending_batch.push(packet);

                last_processed_conversation_id = Some(conversation_id);
                processed_conversations = processed_conversations.saturating_add(1);
                conversations_since_commit = conversations_since_commit.saturating_add(1);
                conversations_since_progress_persist =
                    conversations_since_progress_persist.saturating_add(1);
                if let Some(p) = &progress {
                    p.current.fetch_add(1, Ordering::Relaxed);
                    if options.defer_initial_content_fingerprint {
                        p.total.fetch_max(processed_conversations, Ordering::Relaxed);
                    }
                }
                bump_index_run_lock_progress_if_present(progress_bump.as_ref());
                refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
                    &mut latest_pipeline_runtime,
                    progress.as_ref(),
                    lexical_rebuild_flow_limiter.as_ref(),
                    Some(producer_telemetry.as_ref()),
                    &mut responsiveness_controller,
                    pipeline_budget_controller.as_ref(),
                    &mut current_batch_conversation_limit,
                    Some((
                        &mut commit_interval_conversations,
                        &mut commit_interval_messages,
                        &mut commit_interval_message_bytes,
                    )),
                    LexicalRebuildPipelineSinkRuntimeSnapshot::new(
                        pipeline_rx.len(),
                        pending_batch.len(),
                        pending_batch_message_bytes,
                    ),
                );

                if pending_lexical_rebuild_batch_reached_runtime_limit(
                    pending_batch.len(),
                    current_batch_conversation_limit,
                ) {
                    flush_streamed_lexical_rebuild_batch(
                        &mut pending_batch,
                        &mut pending_batch_message_count,
                        &mut pending_batch_message_bytes,
                        Some(lexical_rebuild_flow_limiter.as_ref()),
                        lexical_rebuild_worker_pool.as_deref(),
                        &mut t_index,
                        &mut indexed_docs,
                        &mut messages_since_commit,
                        &mut message_bytes_since_commit,
                        current_batch_conversation_limit,
                        page_size,
                        perf_profile.as_mut(),
                    )?;
                    refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
                        &mut latest_pipeline_runtime,
                        progress.as_ref(),
                        lexical_rebuild_flow_limiter.as_ref(),
                        Some(producer_telemetry.as_ref()),
                        &mut responsiveness_controller,
                        pipeline_budget_controller.as_ref(),
                        &mut current_batch_conversation_limit,
                        Some((
                            &mut commit_interval_conversations,
                            &mut commit_interval_messages,
                            &mut commit_interval_message_bytes,
                        )),
                        LexicalRebuildPipelineSinkRuntimeSnapshot::new(
                            pipeline_rx.len(),
                            pending_batch.len(),
                            pending_batch_message_bytes,
                        ),
                    );
                }

                if should_commit_lexical_rebuild(
                    conversations_since_commit,
                    messages_since_commit.saturating_add(pending_batch_message_count),
                    message_bytes_since_commit.saturating_add(pending_batch_message_bytes),
                    commit_interval_conversations,
                    commit_interval_messages,
                    commit_interval_message_bytes,
                ) {
                    ensure_lexical_rebuild_free_space(&build_path)?;
                    flush_streamed_lexical_rebuild_batch(
                        &mut pending_batch,
                        &mut pending_batch_message_count,
                        &mut pending_batch_message_bytes,
                        Some(lexical_rebuild_flow_limiter.as_ref()),
                        lexical_rebuild_worker_pool.as_deref(),
                        &mut t_index,
                        &mut indexed_docs,
                        &mut messages_since_commit,
                        &mut message_bytes_since_commit,
                        current_batch_conversation_limit,
                        page_size,
                        perf_profile.as_mut(),
                    )?;
                    refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
                        &mut latest_pipeline_runtime,
                        progress.as_ref(),
                        lexical_rebuild_flow_limiter.as_ref(),
                        Some(producer_telemetry.as_ref()),
                        &mut responsiveness_controller,
                        pipeline_budget_controller.as_ref(),
                        &mut current_batch_conversation_limit,
                        Some((
                            &mut commit_interval_conversations,
                            &mut commit_interval_messages,
                            &mut commit_interval_message_bytes,
                        )),
                        LexicalRebuildPipelineSinkRuntimeSnapshot::new(
                            pipeline_rx.len(),
                            pending_batch.len(),
                            pending_batch_message_bytes,
                        ),
                    );
                    commit_lexical_rebuild_progress(
                        &index_path,
                        &build_path,
                        &mut rebuild_state,
                        last_processed_conversation_id,
                        processed_conversations,
                        indexed_docs,
                        &latest_pipeline_runtime,
                        &mut t_index,
                        true,
                        perf_profile.as_mut(),
                        0,
                    )?;
                    conversations_since_commit = 0;
                    messages_since_commit = 0;
                    message_bytes_since_commit = 0;
                    conversations_since_progress_persist = 0;
                    last_progress_persist = Instant::now();
                    (
                        commit_interval_conversations,
                        commit_interval_messages,
                        commit_interval_message_bytes,
                    ) = lexical_rebuild_commit_intervals_for_state(&rebuild_state);
                    if let Some(transition) =
                        responsiveness_controller.record_first_durable_commit()
                    {
                        apply_lexical_rebuild_budget_transition(
                            transition,
                            lexical_rebuild_flow_limiter.as_ref(),
                            pipeline_budget_controller.as_ref(),
                            &mut current_batch_conversation_limit,
                            Some((
                                &mut commit_interval_conversations,
                                &mut commit_interval_messages,
                                &mut commit_interval_message_bytes,
                            )),
                        );
                        refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
                            &mut latest_pipeline_runtime,
                            progress.as_ref(),
                            lexical_rebuild_flow_limiter.as_ref(),
                            Some(producer_telemetry.as_ref()),
                            &mut responsiveness_controller,
                            pipeline_budget_controller.as_ref(),
                            &mut current_batch_conversation_limit,
                            Some((
                                &mut commit_interval_conversations,
                                &mut commit_interval_messages,
                                &mut commit_interval_message_bytes,
                            )),
                            LexicalRebuildPipelineSinkRuntimeSnapshot::new(
                                pipeline_rx.len(),
                                pending_batch.len(),
                                pending_batch_message_bytes,
                            ),
                        );
                    }
                } else if should_persist_lexical_rebuild_progress(
                    conversations_since_progress_persist,
                    progress_heartbeat_interval_conversations,
                    last_progress_persist.elapsed(),
                    progress_heartbeat_interval,
                ) {
                    let heartbeat_progress_started = perf_profile.as_ref().map(|_| Instant::now());
                    refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
                        &mut latest_pipeline_runtime,
                        progress.as_ref(),
                        lexical_rebuild_flow_limiter.as_ref(),
                        Some(producer_telemetry.as_ref()),
                        &mut responsiveness_controller,
                        pipeline_budget_controller.as_ref(),
                        &mut current_batch_conversation_limit,
                        Some((
                            &mut commit_interval_conversations,
                            &mut commit_interval_messages,
                            &mut commit_interval_message_bytes,
                        )),
                        LexicalRebuildPipelineSinkRuntimeSnapshot::new(
                            pipeline_rx.len(),
                            pending_batch.len(),
                            pending_batch_message_bytes,
                        ),
                    );
                    ensure_lexical_rebuild_free_space(&build_path)?;
                    persist_pending_lexical_rebuild_progress(
                        &index_path,
                        &build_path,
                        &mut rebuild_state,
                        last_processed_conversation_id,
                        processed_conversations,
                        indexed_docs,
                        &latest_pipeline_runtime,
                        0,
                    )?;
                    if let (Some(profile), Some(started)) =
                        (perf_profile.as_mut(), heartbeat_progress_started)
                    {
                        profile.heartbeat_persist_count =
                            profile.heartbeat_persist_count.saturating_add(1);
                        profile.heartbeat_progress_duration += started.elapsed();
                    }
                    conversations_since_progress_persist = 0;
                    last_progress_persist = Instant::now();
                }

                Ok::<(), anyhow::Error>(())
            }};
        }

        let pipeline_result: Result<()> = (|| {
            loop {
                match pipeline_rx.recv_timeout(Duration::from_millis(250)) {
                    Ok(LexicalRebuildPipelineMessage::Batch(prepared_page)) => {
                        if let Some(profile) = perf_profile.as_mut() {
                            profile.conversation_list_duration +=
                                prepared_page.conversation_list_duration;
                            profile.message_stream_duration += prepared_page.message_fetch_duration;
                            profile.finish_conversation_duration +=
                                prepared_page.packet_prepare_duration;
                        }
                        let first_packet_fingerprint = prepared_page
                            .packets
                            .first()
                            .map(LexicalRebuildConversationPacket::fingerprint_input);
                        tracing::debug!(
                            queue_depth = pipeline_rx.len(),
                            inflight_message_bytes = lexical_rebuild_flow_limiter.bytes_in_flight(),
                            page_conversations = prepared_page.packets.len(),
                            page_message_bytes = prepared_page
                                .packets
                                .iter()
                                .map(|packet| packet.message_bytes)
                                .sum::<usize>(),
                            page_last_conversation_id = prepared_page.page_last_conversation_id,
                            planned_shard_index = prepared_page.planned_shard_index,
                            finishes_planned_shard = prepared_page.finishes_planned_shard,
                            first_packet_source_id = first_packet_fingerprint
                                .as_ref()
                                .map(|fingerprint| fingerprint.source_id)
                                .unwrap_or(""),
                            first_packet_origin_kind = first_packet_fingerprint
                                .as_ref()
                                .map(|fingerprint| fingerprint.origin_kind)
                                .unwrap_or(""),
                            first_packet_message_count = first_packet_fingerprint
                                .as_ref()
                                .map(|fingerprint| fingerprint.message_count)
                                .unwrap_or(0),
                            first_packet_message_bytes = first_packet_fingerprint
                                .as_ref()
                                .map(|fingerprint| fingerprint.message_bytes)
                                .unwrap_or(0),
                            "lexical rebuild pipeline received prepared page"
                        );
                        if options.defer_initial_content_fingerprint {
                            max_conversation_id =
                                max_conversation_id.max(prepared_page.page_last_conversation_id);
                        }
                        let LexicalRebuildPreparedPage {
                            packets,
                            page_last_conversation_id: _page_last_conversation_id,
                            planned_shard_index,
                            finishes_planned_shard,
                            conversation_list_duration: _conversation_list_duration,
                            message_fetch_duration: _message_fetch_duration,
                            packet_prepare_duration: _packet_prepare_duration,
                        } = prepared_page;
                        for packet in packets {
                            if options.defer_initial_content_fingerprint
                                && let Some(last_message_id) = packet.last_message_id
                            {
                                max_message_id = max_message_id.max(last_message_id);
                            }
                            equivalence_accumulator.absorb_packet(&packet);
                            if let Some(gate) = packet
                                .chunk
                                .as_ref()
                                .filter(|chunk| !chunk.is_final)
                                .map(|chunk| Arc::clone(&chunk.gate))
                            {
                                // A non-final chunk of a long conversation
                                // (bgn6s): flush it into the index now, freeing
                                // its text so the worker reads the next chunk.
                                // The conversation is counted, and can be
                                // committed, only at its last chunk.
                                observed_messages =
                                    observed_messages.saturating_add(packet.message_count);
                                pending_batch_message_count = pending_batch_message_count
                                    .saturating_add(packet.message_count);
                                pending_batch_message_bytes = pending_batch_message_bytes
                                    .saturating_add(packet.message_bytes);
                                pending_batch.push(packet);
                                flush_streamed_lexical_rebuild_batch(
                                    &mut pending_batch,
                                    &mut pending_batch_message_count,
                                    &mut pending_batch_message_bytes,
                                    Some(lexical_rebuild_flow_limiter.as_ref()),
                                    lexical_rebuild_worker_pool.as_deref(),
                                    &mut t_index,
                                    &mut indexed_docs,
                                    &mut messages_since_commit,
                                    &mut message_bytes_since_commit,
                                    current_batch_conversation_limit,
                                    page_size,
                                    perf_profile.as_mut(),
                                )?;
                                gate.flushed();
                                bump_index_run_lock_progress_if_present(progress_bump.as_ref());
                                continue;
                            }
                            finish_conversation!(packet)?;
                        }
                        if flush_streamed_lexical_rebuild_batch_for_planned_shard_boundary(
                            planned_shard_index,
                            finishes_planned_shard,
                            &mut pending_batch,
                            &mut pending_batch_message_count,
                            &mut pending_batch_message_bytes,
                            Some(lexical_rebuild_flow_limiter.as_ref()),
                            lexical_rebuild_worker_pool.as_deref(),
                            &mut t_index,
                            &mut indexed_docs,
                            &mut messages_since_commit,
                            &mut message_bytes_since_commit,
                            current_batch_conversation_limit,
                            page_size,
                            perf_profile.as_mut(),
                        )? {
                            refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
                                &mut latest_pipeline_runtime,
                                progress.as_ref(),
                                lexical_rebuild_flow_limiter.as_ref(),
                                Some(producer_telemetry.as_ref()),
                                &mut responsiveness_controller,
                                pipeline_budget_controller.as_ref(),
                                &mut current_batch_conversation_limit,
                                Some((
                                    &mut commit_interval_conversations,
                                    &mut commit_interval_messages,
                                    &mut commit_interval_message_bytes,
                                )),
                                LexicalRebuildPipelineSinkRuntimeSnapshot::new(
                                    pipeline_rx.len(),
                                    pending_batch.len(),
                                    pending_batch_message_bytes,
                                ),
                            );
                        }
                    }
                    Ok(LexicalRebuildPipelineMessage::Error(error)) => {
                        return Err(error);
                    }
                    Ok(LexicalRebuildPipelineMessage::Done) => break,
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
                            &mut latest_pipeline_runtime,
                            progress.as_ref(),
                            lexical_rebuild_flow_limiter.as_ref(),
                            Some(producer_telemetry.as_ref()),
                            &mut responsiveness_controller,
                            pipeline_budget_controller.as_ref(),
                            &mut current_batch_conversation_limit,
                            Some((
                                &mut commit_interval_conversations,
                                &mut commit_interval_messages,
                                &mut commit_interval_message_bytes,
                            )),
                            LexicalRebuildPipelineSinkRuntimeSnapshot::new(
                                pipeline_rx.len(),
                                pending_batch.len(),
                                pending_batch_message_bytes,
                            ),
                        );
                        // A responsiveness transition can lower the live sink
                        // threshold while the sink already holds more packets
                        // than the new limit. No packet may arrive afterward:
                        // page-prep can be waiting for the retained bytes in
                        // this very batch. Re-check on the idle tick, not only
                        // inside `finish_conversation!`, so the transition
                        // itself cannot strand an over-limit batch (GH#413).
                        if !pending_batch.is_empty()
                            && pending_lexical_rebuild_batch_reached_runtime_limit(
                                pending_batch.len(),
                                current_batch_conversation_limit,
                            )
                        {
                            tracing::info!(
                                pending_conversations = pending_batch.len(),
                                current_batch_conversation_limit,
                                pending_message_bytes = pending_batch_message_bytes,
                                "lexical rebuild sink exceeded its live batch limit after a runtime budget transition; flushing retained packets (GH#413)"
                            );
                            flush_streamed_lexical_rebuild_batch(
                                &mut pending_batch,
                                &mut pending_batch_message_count,
                                &mut pending_batch_message_bytes,
                                Some(lexical_rebuild_flow_limiter.as_ref()),
                                lexical_rebuild_worker_pool.as_deref(),
                                &mut t_index,
                                &mut indexed_docs,
                                &mut messages_since_commit,
                                &mut message_bytes_since_commit,
                                current_batch_conversation_limit,
                                page_size,
                                perf_profile.as_mut(),
                            )?;
                        }
                        // GH#413: the sink retains the byte reservations of
                        // every packet in `pending_batch` until a flush, but
                        // the only flush triggers were conversation COUNT and
                        // planned-shard boundaries — never bytes. A page of
                        // few-but-huge conversations can therefore park a
                        // page-prep worker on `acquire_with_wait` while the
                        // bytes it needs sit here in an idle sink, and the
                        // pipeline deadlocks: the worker holds the ordered
                        // reservation turn, so no later page can reserve, so
                        // this channel stays empty forever. Break the cycle at
                        // the only safe point — when the sink is starved (its
                        // channel is empty) AND someone is parked on the byte
                        // budget, flush the pending batch, releasing its
                        // reservations. Batching efficiency is preserved: an
                        // idle tick with no budget waiter never force-flushes.
                        if !pending_batch.is_empty()
                            && pipeline_rx.is_empty()
                            && lexical_rebuild_flow_limiter.has_active_waiters()
                        {
                            tracing::info!(
                                pending_conversations = pending_batch.len(),
                                pending_message_bytes = pending_batch_message_bytes,
                                inflight_message_bytes =
                                    lexical_rebuild_flow_limiter.bytes_in_flight(),
                                "lexical rebuild sink starved while a page-prep worker waits for byte budget; flushing pending batch to release its reservations (GH#413)"
                            );
                            flush_streamed_lexical_rebuild_batch(
                                &mut pending_batch,
                                &mut pending_batch_message_count,
                                &mut pending_batch_message_bytes,
                                Some(lexical_rebuild_flow_limiter.as_ref()),
                                lexical_rebuild_worker_pool.as_deref(),
                                &mut t_index,
                                &mut indexed_docs,
                                &mut messages_since_commit,
                                &mut message_bytes_since_commit,
                                current_batch_conversation_limit,
                                page_size,
                                perf_profile.as_mut(),
                            )?;
                        }
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                        return Err(anyhow::anyhow!(
                            "lexical rebuild pipeline channel closed before producer completion"
                        ));
                    }
                }
            }
            Ok(())
        })();
        if pipeline_result.is_err() {
            lexical_rebuild_flow_limiter.close();
        }
        drop(pipeline_rx);
        match producer_handle.join() {
            Ok(()) => {}
            Err(payload) => {
                let panic_message = panic_payload_message(payload);
                if pipeline_result.is_ok() {
                    return Err(anyhow::anyhow!(
                        "lexical rebuild packet producer panicked: {}",
                        panic_message
                    ));
                }
                tracing::warn!(
                    error = %panic_message,
                    "lexical rebuild packet producer panicked while the consumer was already failing"
                );
            }
        }
        pipeline_result?;
    }

    flush_streamed_lexical_rebuild_batch(
        &mut pending_batch,
        &mut pending_batch_message_count,
        &mut pending_batch_message_bytes,
        Some(lexical_rebuild_flow_limiter.as_ref()),
        lexical_rebuild_worker_pool.as_deref(),
        &mut t_index,
        &mut indexed_docs,
        &mut messages_since_commit,
        &mut message_bytes_since_commit,
        current_batch_conversation_limit,
        page_size,
        perf_profile.as_mut(),
    )?;
    refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
        &mut latest_pipeline_runtime,
        progress.as_ref(),
        lexical_rebuild_flow_limiter.as_ref(),
        Some(producer_telemetry.as_ref()),
        &mut responsiveness_controller,
        pipeline_budget_controller.as_ref(),
        &mut current_batch_conversation_limit,
        Some((
            &mut commit_interval_conversations,
            &mut commit_interval_messages,
            &mut commit_interval_message_bytes,
        )),
        LexicalRebuildPipelineSinkRuntimeSnapshot::new(
            0,
            pending_batch.len(),
            pending_batch_message_bytes,
        ),
    );

    // #441: publish everything staged, then fold the freshly built generation
    // into one segment BEFORE the terminal checkpoint records the manifest
    // fingerprint. A from-scratch build seals one segment per ingest shard
    // per accumulation budget, and Quill's query planner probes every sealed
    // dictionary for every lowered segment, so segment count is the dominant
    // query cost on a large archive; a published generation should start at
    // one. The merge is a Q1-preserving concat behind an atomic MANIFEST
    // publish, so a failure leaves the (valid, merely unmerged) generation in
    // place and is logged rather than failing a completed rebuild.
    ensure_lexical_rebuild_free_space(&build_path)?;
    let publication = lexical_publish::Finalization::new(
        &index_path,
        &build_path,
        indexed_docs,
        processed_conversations,
    );
    // Task 5: the authoritative rebuild is the deferred-lexical lane's
    // convergence point. A from-scratch candidate owns NO region
    // documents, so EVERY session carrying a marks row re-materializes
    // here (not only `pending`) — otherwise `published` marks would
    // certify docs this generation never wrote. The marks stamp needs a
    // writable handle; the rebuild's page reader stays strictly
    // read-only by design.
    // A resumed terminal re-converges every marked session. The region share an
    // interrupted run already recorded is inside `indexed_docs`, and the
    // re-upserts report it again, so that share is taken out before this pass
    // counts its own region documents.
    indexed_docs = indexed_docs.saturating_sub(rebuild_state.indexed_region_docs);
    publication.run("close_canonical_reader", || {
        storage.close_without_checkpoint().with_context(|| {
            format!(
                "closing readonly database after Tantivy rebuild without checkpoint: {}",
                db_path.display()
            )
        })
    })?;
    let (region_docs, marks_storage) = {
        let marks_storage = FrankenStorage::open_writer(db_path).with_context(|| {
            format!(
                "opening writable database to stamp search-region docs published: {}",
                db_path.display()
            )
        })?;
        let (_sessions, docs) =
            t_index.converge_all_search_region_docs(marks_storage.raw(), i64::MAX)?;
        indexed_docs += docs;
        (docs, marks_storage)
    };
    // The terminal commit follows the streaming protocol: the exact counts it
    // lands are recorded as a pending commit BEFORE the candidate commit can
    // become durable. Without this record, a kill after the commit left a
    // candidate ahead of its checkpoint, and the resume check quarantined a
    // fully indexed generation whose cursor already reached the end.
    publication.run("checkpoint_terminal_pending", || {
        persist_pending_lexical_rebuild_progress(
            &index_path,
            &build_path,
            &mut rebuild_state,
            last_processed_conversation_id,
            processed_conversations,
            indexed_docs,
            &latest_pipeline_runtime,
            region_docs,
        )
    })?;
    #[cfg(test)]
    lexical_terminal_failpoint("terminal_pending_recorded")?;
    publication.run("commit_candidate", || t_index.commit())?;
    #[cfg(test)]
    lexical_terminal_failpoint("candidate_committed")?;
    // Two-phase region-doc publication: only NOW — after the candidate
    // commit sealed the staged documents — do the marks rows move to
    // `published`. A failed commit or crash above leaves them pending so
    // the next run re-materializes instead of certifying documents that
    // never shipped. Each stamp is a compare-and-swap on the stage-time
    // marks snapshot, so a concurrent republish/meta-edit is refused
    // rather than overwritten.
    //
    // Task 8: the marks stamps land WITHOUT certification here — the
    // post-stamp fold and the terminal checkpoint commit below still
    // rewrite this candidate's MANIFEST, so the generation sealed at this
    // point is an intermediate the published manifest supersedes. The
    // writer's armed coverage floor survives for the deferred
    // `certify_published_manifest` call after the swap validates.
    t_index.replay_region_doc_stamps(marks_storage.raw())?;
    if let Some(p) = &progress {
        p.tick_activity();
    }
    let merge_started = Instant::now();
    if lexical_post_run_maintenance_skipped_for_test() {
        tracing::warn!(
            segments = t_index.segment_count(),
            "post-rebuild segment merge skipped by CASS_TEST_SKIP_POST_RUN_LEXICAL_MAINTENANCE (test hook)"
        );
    } else if let Some(shortfall) = lexical_rebuild_fold_shortfall(&build_path) {
        // nw61n: the fold rewrites the whole generation while its inputs stay
        // on disk. Without room for that, publish the unmerged generation, as
        // a failed fold already does, instead of filling the filesystem.
        tracing::warn!(
            segments = t_index.segment_count(),
            shortfall = %shortfall,
            "skipping the post-rebuild segment fold for lack of disk space; publishing the unmerged generation (#441)"
        );
    } else {
        match t_index.force_merge() {
            Ok(()) => tracing::info!(
                merge_ms = merge_started.elapsed().as_millis() as u64,
                segments = t_index.segment_count(),
                "folded the rebuilt lexical generation before publish (#441)"
            ),
            Err(err) => tracing::warn!(
                error = %format!("{err:#}"),
                "segment merge after lexical rebuild failed; publishing the unmerged generation"
            ),
        }
    }
    if let Some(p) = &progress {
        p.tick_activity();
    }
    // The pending record written before the candidate commit covers every
    // kill point from that commit through this one, so restart reconciliation
    // lands the commit or reverts it. This final commit records and commits the
    // post-stamp, post-merge manifest, and it runs unconditionally: the merge
    // above changed the manifest, and the fingerprint the completed checkpoint
    // carries must describe the generation that is actually published — a stale
    // fingerprint reads as "index changed underneath the checkpoint" and forces
    // the next run into another full rebuild.
    #[cfg(test)]
    lexical_terminal_failpoint("before_terminal_checkpoint")?;
    publication.run("checkpoint_after_fold", || {
        commit_lexical_rebuild_progress(
            &index_path,
            &build_path,
            &mut rebuild_state,
            last_processed_conversation_id,
            processed_conversations,
            indexed_docs,
            &latest_pipeline_runtime,
            &mut t_index,
            false,
            perf_profile.as_mut(),
            region_docs,
        )
    })?;

    // Task 8: certification must wait for the candidate to be validated
    // and — for a staged build — swapped live and re-verified at the
    // published path. A receipt written here would durably survive a
    // refusal below and certify a manifest that never went live. Disarm
    // the build-start coverage floor across the writer drop so the
    // post-publish call can bind it to the manifest readers actually
    // open; `marks_storage` stays open for that call.
    let certification_floor = t_index.take_publication_floor();

    // Release the bulk writer before opening a fresh validation reader. Large
    // archives must not retain both writer and reader working sets here.
    publication.run("release_writer", || {
        drop(t_index);
        Ok(())
    })?;
    // Refuse an unreadable/hollow candidate before it can replace a usable
    // live generation. Reopen and validate the actual live path again after swap.
    if let Err(refusal) = publication.run("validate_candidate", || {
        crate::search::tantivy::validate_searchable_index_contract(&build_path)?;
        verify_published_lexical_doc_count(&build_path, indexed_docs, "candidate")
    }) {
        // GH #494: only a resumed staged candidate whose count disagrees is
        // rebuilt. A from-zero build that miscounts is a defect to report, and
        // a candidate building in the live tree is never moved aside.
        if resumed_from_checkpoint
            && staged_build_path.is_some()
            && refusal.downcast_ref::<LexicalDocCountMismatch>().is_some()
        {
            return Err(retain_uncertifiable_resumed_candidate(
                &index_path,
                &build_path,
                &rebuild_state,
                &db_state,
                refusal,
            ));
        }
        return Err(refusal);
    }

    // GH #440: indexed_docs omits hard-noise messages from the committed
    // prefix, so prefix docs + newly streamed rows is not an exact canonical
    // count after resume. Count once at completion while the readonly handle
    // is still open; fresh rebuilds already observed every canonical packet.
    let final_observed_messages = if resumed_from_checkpoint {
        publication.run("count_canonical_messages", || {
            count_total_messages_exact(&marks_storage)
        })?
    } else {
        // `indexed_docs` includes the Task-5 region documents converged
        // above — the canonical message count must not inherit them.
        observed_messages.max(indexed_docs.saturating_sub(region_docs))
    };
    let final_total_conversations = if options.defer_initial_content_fingerprint {
        processed_conversations
    } else {
        total_conversations
    };
    if options.defer_initial_content_fingerprint {
        rebuild_state.db.storage_fingerprint = lexical_rebuild_content_fingerprint_value(
            final_total_conversations,
            max_conversation_id,
            max_message_id,
        );
    } else {
        // A compatible interrupted checkpoint may carry content-pending-v1.
        // This run obtained an exact startup fingerprint: publish that value,
        // rather than certifying the placeholder loaded from the checkpoint.
        rebuild_state.db.storage_fingerprint = db_state.storage_fingerprint;
    }
    rebuild_state.db.total_conversations = final_total_conversations;
    rebuild_state.db.total_messages = final_observed_messages;
    rebuild_state.committed_offset = i64::try_from(final_total_conversations).unwrap_or(i64::MAX);
    rebuild_state.committed_conversation_id = last_processed_conversation_id;
    rebuild_state.processed_conversations = processed_conversations;
    rebuild_state.indexed_docs = indexed_docs;
    rebuild_state.indexed_region_docs = region_docs;
    // GH #494: prepare the complete generation in its CONTENT directory before
    // swapping. Keeping the completed checkpoint only on the old live path
    // loses it during the swap; a crash then exposes new documents without
    // their publication receipt and every search can start rebuilding again.
    // GH #457 still requires the manifest BEFORE the completed checkpoint.
    // Before publication, the old live checkpoint remains incomplete and can
    // resume the candidate if any certification or rename operation fails.
    let lexical_rebuild_duration = lexical_rebuild_started.elapsed();
    let publish_started = Instant::now();
    let equivalence_evidence = equivalence_accumulator.finalize();
    let generation_manifest = publication.run("persist_generation_manifest", || {
        persist_lexical_rebuild_generation_artifacts(
            &build_path,
            &rebuild_state.db.storage_fingerprint,
            rebuild_state.processed_conversations,
            final_total_conversations,
            final_observed_messages,
            indexed_docs,
            &equivalence_evidence,
        )
    })?;
    let completed_fingerprint = publication.run("completed_generation_fingerprint", || {
        // Bind the receipt to the immutable generation after writer release,
        // not a cached fingerprint taken before the writer was dropped.
        index_meta_fingerprint(&build_path)
    })?;
    rebuild_state.mark_completed(completed_fingerprint);
    publication.run("persist_completed_checkpoint", || {
        persist_lexical_rebuild_state(&build_path, &rebuild_state)
    })?;
    publication.run("persist_publication_receipt", || {
        persist_lexical_publication_receipt(&build_path, &rebuild_state)
    })?;
    publication.run("sync_certified_candidate", || {
        sync_parent_directory(&lexical_rebuild_state_path(&build_path))
    })?;

    // The atomic swap now publishes documents, generation manifest, and the
    // completed checkpoint together. A post-swap failure is still reported,
    // but a subsequent invocation can validate/reuse the generation instead
    // of discarding its ingestion progress or adopting the prior-live scratch.
    if let Some(staged) = staged_build_path.as_ref() {
        publication.run("publish_staged_generation", || {
            publish_staged_lexical_index(staged, &index_path)
        })?;
    }
    publication.run("validate_published_generation", || {
        crate::search::tantivy::validate_searchable_index_contract(&index_path)?;
        verify_published_lexical_doc_count(&index_path, indexed_docs, "direct")
    })?;
    // Task 8: certify ONLY now — the candidate passed validation, any
    // staged swap published it, and the post-swap verify proved the
    // manifest readers actually open. A refusal above never reached this
    // point, so no durable certification can outlive a manifest that
    // never went live. The floor captured at build start bounds the
    // receipt's coverage claim.
    TantivyIndex::certify_published_manifest(marks_storage.raw(), &index_path, certification_floor);
    marks_storage.close().with_context(|| {
        format!(
            "closing region-marks writer after Tantivy rebuild: {}",
            db_path.display()
        )
    })?;
    log_lexical_generation_manifest_published(&generation_manifest, &equivalence_evidence);

    if let Some(p) = &progress {
        p.current
            .store(final_total_conversations, Ordering::Relaxed);
        p.total.store(final_total_conversations, Ordering::Relaxed);
        p.total_is_final.store(true, Ordering::Relaxed);
        p.phase.store(0, Ordering::Relaxed);
        p.is_rebuilding.store(false, Ordering::Relaxed);
    }

    if let Some(profile) = perf_profile.as_mut() {
        if let Some(started) = rebuild_profile_started {
            profile.total_duration = started.elapsed();
        }
        profile.log_summary();
    }

    let refresh_ledger =
        build_authoritative_lexical_refresh_ledger(AuthoritativeLexicalRefreshLedgerInput {
            publish_mode: "direct_live_commit",
            lexical_duration: lexical_rebuild_duration,
            publish_duration: publish_started.elapsed(),
            processed_conversations: rebuild_state.processed_conversations,
            total_conversations: final_total_conversations,
            final_observed_messages,
            indexed_docs,
            equivalence_evidence: &equivalence_evidence,
        });
    publication.run("persist_refresh_ledger", || {
        persist_lexical_refresh_ledger(&index_path, &refresh_ledger)
    })?;
    log_lexical_refresh_ledger_published(&refresh_ledger);

    Ok(LexicalRebuildOutcome {
        observed_conversations: final_total_conversations,
        indexed_docs,
        indexed_region_docs: region_docs,
        observed_messages: Some(final_observed_messages),
        exact_checkpoint_persisted: true,
        equivalence: Some(equivalence_evidence),
    })
}
