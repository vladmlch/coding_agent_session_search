use super::*;

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn rebuild_tantivy_from_db_via_staged_shards(
    db_path: &Path,
    index_path: &Path,
    total_conversations: usize,
    progress: Option<Arc<IndexingProgress>>,
    options: LexicalRebuildStartupOptions,
    prep_profile_started: Option<Instant>,
    storage: FrankenStorage,
    build_start_floor: Option<crate::viewer_extensions::changes::JournalRevisions>,
    mut rebuild_state: LexicalRebuildState,
    shard_plan: LexicalShardPlan,
    pipeline_settings: LexicalRebuildPipelineSettingsSnapshot,
    progress_heartbeat_interval_conversations: usize,
    progress_heartbeat_interval: Duration,
    mut latest_pipeline_runtime: LexicalRebuildPipelineRuntimeSnapshot,
    mut responsiveness_controller: LexicalRebuildResponsivenessController,
    pipeline_budget_controller: Arc<LexicalRebuildPipelineBudgetController>,
    mut current_batch_conversation_limit: usize,
    lexical_rebuild_flow_limiter: Arc<StreamingByteLimiter>,
    producer_telemetry: Arc<LexicalRebuildProducerTelemetry>,
    lexical_rebuild_worker_pool: Option<Arc<ThreadPool>>,
    pipeline_rx: Receiver<LexicalRebuildPipelineMessage>,
    producer_handle: JoinHandle<()>,
    mut perf_profile: Option<LexicalRebuildPerfProfile>,
    rebuild_profile_started: Option<Instant>,
    progress_bump: Option<Arc<AtomicI64>>,
) -> Result<LexicalRebuildOutcome> {
    let persist_initial_checkpoint_started = Instant::now();
    persist_lexical_rebuild_state_for_active_run_start(index_path, &rebuild_state)?;
    bump_index_run_lock_progress_if_present(progress_bump.as_ref());
    log_lexical_rebuild_prep_profile_step(
        prep_profile_started,
        persist_initial_checkpoint_started,
        "persist_initial_checkpoint",
    );

    if let Some(p) = &progress {
        p.phase.store(2, Ordering::Relaxed);
        p.is_rebuilding.store(true, Ordering::Relaxed);
        p.total.store(total_conversations, Ordering::Relaxed);
        p.current
            .store(rebuild_state.processed_conversations, Ordering::Relaxed);
        // Count-free restart receives only the interrupted checkpoint's hint.
        // The exact denominator is known after authoritative enumeration.
        p.total_is_final.store(
            !options.defer_initial_content_fingerprint,
            Ordering::Relaxed,
        );
        p.discovered_agents.store(0, Ordering::Relaxed);
    }
    let lexical_rebuild_started = Instant::now();

    let stage_parent = index_path.parent().unwrap_or(index_path);
    let shard_stage_root = TempDirBuilder::new()
        .prefix("cass-lexical-shards.")
        .tempdir_in(stage_parent)
        .with_context(|| {
            format!(
                "creating staged lexical shard-build root under {}",
                stage_parent.display()
            )
        })?;
    let merge_stage_root = TempDirBuilder::new()
        .prefix("cass-lexical-merge.")
        .tempdir_in(stage_parent)
        .with_context(|| {
            format!(
                "creating staged lexical merge root under {}",
                stage_parent.display()
            )
        })?;
    let eager_merge_stage_root = merge_stage_root.path().join("eager");
    let final_merge_stage_root = merge_stage_root.path().join("final");
    let staged_merged_index_path = merge_stage_root.path().join("index");
    let staged_publish_base_meta_fingerprint = index_meta_fingerprint(index_path)?;
    let shard_builder_settings =
        lexical_rebuild_staged_shard_builder_settings(&pipeline_settings, shard_plan.shards.len());
    let shard_merge_settings =
        lexical_rebuild_staged_shard_merge_settings(&pipeline_settings, shard_plan.shards.len());
    tracing::info!(
        db_path = %db_path.display(),
        planned_shards = shard_plan.shards.len(),
        plan_id = %shard_plan.plan_id,
        shard_builders_max = shard_builder_settings.max_builders,
        shard_builder_writer_parallelism_budget =
            shard_builder_settings.writer_parallelism_budget,
        eager_merge_workers = shard_merge_settings.workers,
        "running fresh authoritative lexical rebuild via staged shard-build path"
    );

    let (shard_work_tx, shard_work_rx) =
        bounded::<LexicalRebuildShardBuildWork>(shard_builder_settings.max_builders.max(1));
    let (shard_result_tx, shard_result_rx) = bounded::<LexicalRebuildShardBuildMessage>(
        shard_plan
            .shards
            .len()
            .max(shard_builder_settings.max_builders)
            .max(1),
    );
    let shard_builder_handles = spawn_lexical_rebuild_shard_builder_workers(
        shard_builder_settings.max_builders,
        shard_work_rx,
        shard_result_tx,
        Arc::clone(&lexical_rebuild_flow_limiter),
        lexical_rebuild_worker_pool.clone(),
        lexical_index_heartbeat(progress.as_ref()),
    );
    let shard_work_dispatch_tx = shard_work_tx.clone();
    let (merge_work_tx, merge_work_rx) =
        bounded::<LexicalRebuildShardMergeJob>(shard_merge_settings.workers.max(1));
    let (merge_result_tx, merge_result_rx) =
        bounded::<LexicalRebuildShardMergeMessage>(shard_plan.shards.len().max(1));
    let merge_worker_handles = spawn_lexical_rebuild_shard_merge_workers(
        shard_merge_settings.workers,
        merge_work_rx,
        merge_result_tx,
    );

    let mut indexed_docs = rebuild_state.indexed_docs;
    let mut observed_messages = rebuild_state.indexed_docs;
    let mut processed_conversations = rebuild_state.processed_conversations;
    let mut last_processed_conversation_id = rebuild_state.committed_conversation_id;
    let mut conversations_since_progress_persist = 0usize;
    let mut last_progress_persist = Instant::now();
    let mut current_shard_packets = Vec::new();
    let mut current_shard_index = None;
    let mut current_shard_message_bytes = 0usize;
    let mut enqueued_shards = 0usize;
    let mut received_shard_results = 0usize;
    let mut active_shard_build_jobs = 0usize;
    let mut next_shard_to_commit = 0usize;
    let mut pending_completed_shards = BTreeMap::new();
    let mut completed_shard_artifacts = Vec::with_capacity(shard_plan.shards.len());
    let mut scheduled_completed_shard_artifacts = 0usize;
    let mut pending_shard_build_jobs: VecDeque<LexicalRebuildShardBuildWork> = VecDeque::new();
    let mut pending_shard_build_message_bytes = 0usize;
    let pending_shard_build_max_message_bytes =
        lexical_rebuild_pending_shard_build_max_message_bytes(&pipeline_settings);
    let pending_shard_build_max_jobs =
        lexical_rebuild_pending_shard_build_max_jobs(&pipeline_settings);
    let mut merge_coordinator = LexicalRebuildShardMergeCoordinator::new(eager_merge_stage_root);
    let staged_merge_controller = LexicalRebuildStagedMergeController::new(
        shard_merge_settings.workers,
        pipeline_settings.controller_loadavg_high_watermark_1m_milli,
    );
    let staged_shard_build_controller = LexicalRebuildStagedShardBuildController::new(
        shard_builder_settings.max_builders,
        pipeline_settings.controller_loadavg_high_watermark_1m_milli,
    );
    let shard_build_telemetry = LexicalRebuildShardBuildTelemetry::default();
    let mut max_conversation_id = 0i64;
    let mut max_message_id = 0i64;
    let mut final_merge_input_artifacts: Option<Vec<LexicalRebuildShardMergeArtifact>> = None;
    let mut equivalence_accumulator = LexicalRebuildEquivalenceAccumulator::new();

    let refresh_runtime =
        |latest_pipeline_runtime: &mut LexicalRebuildPipelineRuntimeSnapshot,
         responsiveness_controller: &mut LexicalRebuildResponsivenessController,
         current_batch_conversation_limit: &mut usize,
         pending_batch_conversations: usize,
         pending_batch_message_bytes: usize| {
            refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
                latest_pipeline_runtime,
                progress.as_ref(),
                lexical_rebuild_flow_limiter.as_ref(),
                Some(producer_telemetry.as_ref()),
                responsiveness_controller,
                pipeline_budget_controller.as_ref(),
                current_batch_conversation_limit,
                None,
                LexicalRebuildPipelineSinkRuntimeSnapshot::new(
                    pipeline_rx.len(),
                    pending_batch_conversations,
                    pending_batch_message_bytes,
                ),
            );
        };

    let refresh_staged_parallelism_and_dispatch =
        |latest_pipeline_runtime: &mut LexicalRebuildPipelineRuntimeSnapshot,
         merge_coordinator: &mut LexicalRebuildShardMergeCoordinator,
         pending_shard_build_jobs: &mut VecDeque<LexicalRebuildShardBuildWork>,
         pending_shard_build_message_bytes: &mut usize,
         active_shard_build_jobs: &mut usize,
         enqueued_shards: &mut usize,
         producer_finished: bool|
         -> Result<()> {
            let staged_merge_runtime = staged_merge_controller.decide(
                producer_finished,
                latest_pipeline_runtime,
                merge_coordinator,
            );
            merge_coordinator.set_allowed_pending_merge_jobs(
                staged_merge_runtime.allowed_jobs,
                &merge_work_tx,
            )?;
            let applied_runtime = staged_merge_controller.decide(
                producer_finished,
                latest_pipeline_runtime,
                merge_coordinator,
            );
            apply_staged_merge_runtime_snapshot(
                latest_pipeline_runtime,
                progress.as_ref(),
                &applied_runtime,
            );
            apply_staged_shard_build_telemetry_snapshot(
                latest_pipeline_runtime,
                progress.as_ref(),
                staged_shard_build_controller.memory_reserve_bytes,
                staged_shard_build_controller.emergency_memory_reserve_bytes,
                &shard_build_telemetry,
            );
            let staged_shard_build_runtime = staged_shard_build_controller.decide(
                latest_pipeline_runtime,
                &applied_runtime,
                *active_shard_build_jobs,
                pending_shard_build_jobs.len(),
                pending_shard_build_jobs
                    .front()
                    .map(|work| work.shard.message_bytes),
            );
            while *active_shard_build_jobs < staged_shard_build_runtime.allowed_jobs {
                let Some(mut work) = pending_shard_build_jobs.pop_front() else {
                    break;
                };
                *pending_shard_build_message_bytes =
                    pending_shard_build_message_bytes.saturating_sub(work.message_bytes);
                let dispatch_slot_index = *active_shard_build_jobs;
                work.writer_parallelism =
                    lexical_rebuild_staged_shard_builder_writer_parallelism_for_dispatch(
                        shard_builder_settings.writer_parallelism_budget,
                        staged_shard_build_runtime.allowed_jobs,
                        dispatch_slot_index,
                    );
                shard_work_dispatch_tx.send(work).map_err(|_| {
                    anyhow::anyhow!(
                        "staged lexical rebuild shard builder queue disconnected before shard enqueue"
                    )
                })?;
                *active_shard_build_jobs = active_shard_build_jobs.saturating_add(1);
                *enqueued_shards = enqueued_shards.saturating_add(1);
            }
            let applied_shard_build_runtime = staged_shard_build_controller.decide(
                latest_pipeline_runtime,
                &applied_runtime,
                *active_shard_build_jobs,
                pending_shard_build_jobs.len(),
                pending_shard_build_jobs
                    .front()
                    .map(|work| work.shard.message_bytes),
            );
            apply_staged_shard_build_runtime_snapshot(
                latest_pipeline_runtime,
                progress.as_ref(),
                &applied_shard_build_runtime,
            );
            apply_staged_shard_build_telemetry_snapshot(
                latest_pipeline_runtime,
                progress.as_ref(),
                staged_shard_build_controller.memory_reserve_bytes,
                staged_shard_build_controller.emergency_memory_reserve_bytes,
                &shard_build_telemetry,
            );
            Ok(())
        };

    let initial_staged_merge_runtime =
        staged_merge_controller.decide(false, &latest_pipeline_runtime, &merge_coordinator);
    apply_staged_merge_runtime_snapshot(
        &mut latest_pipeline_runtime,
        progress.as_ref(),
        &initial_staged_merge_runtime,
    );
    let initial_staged_shard_build_runtime = staged_shard_build_controller.decide(
        &latest_pipeline_runtime,
        &initial_staged_merge_runtime,
        active_shard_build_jobs,
        pending_shard_build_jobs.len(),
        pending_shard_build_jobs
            .front()
            .map(|work| work.shard.message_bytes),
    );
    apply_staged_shard_build_runtime_snapshot(
        &mut latest_pipeline_runtime,
        progress.as_ref(),
        &initial_staged_shard_build_runtime,
    );
    apply_staged_shard_build_telemetry_snapshot(
        &mut latest_pipeline_runtime,
        progress.as_ref(),
        staged_shard_build_controller.memory_reserve_bytes,
        staged_shard_build_controller.emergency_memory_reserve_bytes,
        &shard_build_telemetry,
    );

    let advance_completed_shards =
        |pending_completed_shards: &mut BTreeMap<usize, LexicalRebuildShardBuildResult>,
         next_shard_to_commit: &mut usize,
         completed_shard_artifacts: &mut Vec<LexicalRebuildShardMergeArtifact>,
         processed_conversations: &mut usize,
         indexed_docs: &mut usize,
         observed_messages: &mut usize,
         last_processed_conversation_id: &mut Option<i64>,
         conversations_since_progress_persist: &mut usize,
         responsiveness_controller: &mut LexicalRebuildResponsivenessController,
         current_batch_conversation_limit: &mut usize|
         -> Result<bool> {
            let mut force_progress_persist = false;
            while let Some(result) = pending_completed_shards.remove(next_shard_to_commit) {
                let validated_artifact = validate_lexical_rebuild_shard_build_result(&result)?;
                *processed_conversations =
                    processed_conversations.saturating_add(result.shard.conversation_count);
                *indexed_docs = indexed_docs.saturating_add(result.indexed_docs);
                let shard_observed_messages =
                    if lexical_shard_message_count_is_known(result.shard.message_count) {
                        result.shard.message_count
                    } else {
                        result.indexed_docs
                    };
                *observed_messages = observed_messages.saturating_add(shard_observed_messages);
                *last_processed_conversation_id = Some(result.shard.last_conversation_id);
                completed_shard_artifacts.push(validated_artifact);
                *conversations_since_progress_persist = conversations_since_progress_persist
                    .saturating_add(result.shard.conversation_count);
                if let Some(p) = &progress {
                    p.current.store(*processed_conversations, Ordering::Relaxed);
                    if options.defer_initial_content_fingerprint {
                        p.total
                            .fetch_max(*processed_conversations, Ordering::Relaxed);
                    }
                }
                bump_index_run_lock_progress_if_present(progress_bump.as_ref());
                if let Some(transition) = responsiveness_controller.record_first_durable_commit() {
                    apply_lexical_rebuild_budget_transition(
                        transition,
                        lexical_rebuild_flow_limiter.as_ref(),
                        pipeline_budget_controller.as_ref(),
                        current_batch_conversation_limit,
                        None,
                    );
                    force_progress_persist = true;
                }
                *next_shard_to_commit = next_shard_to_commit.saturating_add(1);
            }
            Ok(force_progress_persist)
        };

    let queue_newly_completed_shard_artifacts =
        |merge_coordinator: &mut LexicalRebuildShardMergeCoordinator,
         completed_shard_artifacts: &[LexicalRebuildShardMergeArtifact],
         scheduled_completed_shard_artifacts: &mut usize|
         -> Result<()> {
            while *scheduled_completed_shard_artifacts < completed_shard_artifacts.len() {
                merge_coordinator.queue_base_artifact(
                    completed_shard_artifacts[*scheduled_completed_shard_artifacts].clone(),
                    &merge_work_tx,
                )?;
                *scheduled_completed_shard_artifacts =
                    (*scheduled_completed_shard_artifacts).saturating_add(1);
            }
            Ok(())
        };

    let main_result: Result<()> = (|| {
        let never_shard_result_rx = never::<LexicalRebuildShardBuildMessage>();
        let never_merge_result_rx = never::<LexicalRebuildShardMergeMessage>();
        let never_pipeline_rx = never::<LexicalRebuildPipelineMessage>();
        let mut shard_result_channel_open = true;
        let mut merge_result_channel_open = true;
        let mut producer_finished = false;
        let mut pipeline_backlog_pause_logged = false;
        while !producer_finished {
            let active_shard_result_rx = if shard_result_channel_open {
                &shard_result_rx
            } else {
                &never_shard_result_rx
            };
            let active_merge_result_rx = if merge_result_channel_open {
                &merge_result_rx
            } else {
                &never_merge_result_rx
            };
            let pipeline_backlog_paused = pending_shard_build_message_bytes
                >= pending_shard_build_max_message_bytes
                || pending_shard_build_jobs.len() >= pending_shard_build_max_jobs;
            let pending_merge_jobs = merge_coordinator.pending_merge_jobs();
            let pause_pipeline_handoff = should_pause_staged_lexical_pipeline_handoff(
                pipeline_backlog_paused,
                active_shard_build_jobs,
                pending_merge_jobs,
            );
            if pause_pipeline_handoff && !pipeline_backlog_pause_logged {
                tracing::info!(
                    pending_shard_build_jobs = pending_shard_build_jobs.len(),
                    pending_shard_build_max_jobs,
                    pending_shard_build_message_bytes,
                    pending_shard_build_max_message_bytes,
                    active_shard_build_jobs,
                    pending_merge_jobs,
                    "pausing lexical rebuild producer handoff while staged shard-build backlog drains"
                );
                pipeline_backlog_pause_logged = true;
            } else if !pause_pipeline_handoff && pipeline_backlog_pause_logged {
                tracing::info!(
                    pending_shard_build_jobs = pending_shard_build_jobs.len(),
                    pending_shard_build_max_jobs,
                    pending_shard_build_message_bytes,
                    pending_shard_build_max_message_bytes,
                    active_shard_build_jobs,
                    pending_merge_jobs,
                    "resuming lexical rebuild producer handoff after staged backlog drained or lost its async wake source"
                );
                pipeline_backlog_pause_logged = false;
            }
            if pipeline_backlog_paused && !pause_pipeline_handoff {
                tracing::debug!(
                    pending_shard_build_jobs = pending_shard_build_jobs.len(),
                    pending_shard_build_max_jobs,
                    pending_shard_build_message_bytes,
                    pending_shard_build_max_message_bytes,
                    active_shard_build_jobs,
                    pending_merge_jobs,
                    "continuing to drain lexical rebuild producer handoff because no staged shard or merge job can wake the sink"
                );
            }
            let active_pipeline_rx = if pause_pipeline_handoff {
                &never_pipeline_rx
            } else {
                &pipeline_rx
            };
            select! {
                recv(active_shard_result_rx) -> message => {
                    match message {
                        Ok(LexicalRebuildShardBuildMessage::Built(result)) => {
                            active_shard_build_jobs = active_shard_build_jobs.saturating_sub(1);
                            received_shard_results = received_shard_results.saturating_add(1);
                            shard_build_telemetry.record(&result);
                            pending_completed_shards.insert(result.shard.shard_index, result);
                        }
                        Ok(LexicalRebuildShardBuildMessage::Error { shard_index, error }) => {
                            active_shard_build_jobs = active_shard_build_jobs.saturating_sub(1);
                            return Err(anyhow::anyhow!(
                                "building lexical rebuild shard {shard_index} failed: {error}"
                            ));
                        }
                        Err(_) => {
                            if received_shard_results < enqueued_shards {
                                return Err(anyhow::anyhow!(
                                    "staged lexical rebuild shard builder channel closed before all enqueued shards completed"
                                ));
                            }
                            shard_result_channel_open = false;
                            continue;
                        }
                    }

                    let force_progress_persist = advance_completed_shards(
                        &mut pending_completed_shards,
                        &mut next_shard_to_commit,
                        &mut completed_shard_artifacts,
                        &mut processed_conversations,
                        &mut indexed_docs,
                        &mut observed_messages,
                        &mut last_processed_conversation_id,
                        &mut conversations_since_progress_persist,
                        &mut responsiveness_controller,
                        &mut current_batch_conversation_limit,
                    )?;
                    queue_newly_completed_shard_artifacts(
                        &mut merge_coordinator,
                        &completed_shard_artifacts,
                        &mut scheduled_completed_shard_artifacts,
                    )?;

                    refresh_runtime(
                        &mut latest_pipeline_runtime,
                        &mut responsiveness_controller,
                        &mut current_batch_conversation_limit,
                        current_shard_packets.len(),
                        current_shard_message_bytes,
                    );
                    refresh_staged_parallelism_and_dispatch(
                        &mut latest_pipeline_runtime,
                        &mut merge_coordinator,
                        &mut pending_shard_build_jobs,
                        &mut pending_shard_build_message_bytes,
                        &mut active_shard_build_jobs,
                        &mut enqueued_shards,
                        producer_finished,
                    )?;
                    maybe_persist_staged_lexical_rebuild_progress(
                        index_path,
                        &mut rebuild_state,
                        last_processed_conversation_id,
                        processed_conversations,
                        indexed_docs,
                        &latest_pipeline_runtime,
                        staged_publish_base_meta_fingerprint.as_deref(),
                        force_progress_persist,
                        &mut conversations_since_progress_persist,
                        progress_heartbeat_interval_conversations,
                        &mut last_progress_persist,
                        progress_heartbeat_interval,
                        progress_bump.as_ref(),
                        perf_profile.as_mut(),
                    )?;
                }
                recv(active_merge_result_rx) -> message => {
                    match message {
                        Ok(LexicalRebuildShardMergeMessage::Built(result)) => {
                            merge_coordinator.complete_merge(result, &merge_work_tx)?;
                            bump_index_run_lock_progress_if_present(progress_bump.as_ref());
                        }
                        Ok(LexicalRebuildShardMergeMessage::Error {
                            output_level,
                            first_shard_index,
                            last_shard_index,
                            error,
                        }) => {
                            return Err(anyhow::anyhow!(
                                "eager staged lexical merge at level {output_level} for shard range {first_shard_index}..={last_shard_index} failed: {error}"
                            ));
                        }
                        Err(_) => {
                            if merge_coordinator.pending_merge_jobs() > 0 {
                                return Err(anyhow::anyhow!(
                                    "staged lexical rebuild eager merge channel closed before all queued merges completed"
                                ));
                            }
                            merge_result_channel_open = false;
                            continue;
                        }
                    }
                    refresh_runtime(
                        &mut latest_pipeline_runtime,
                        &mut responsiveness_controller,
                        &mut current_batch_conversation_limit,
                        current_shard_packets.len(),
                        current_shard_message_bytes,
                    );
                    refresh_staged_parallelism_and_dispatch(
                        &mut latest_pipeline_runtime,
                        &mut merge_coordinator,
                        &mut pending_shard_build_jobs,
                        &mut pending_shard_build_message_bytes,
                        &mut active_shard_build_jobs,
                        &mut enqueued_shards,
                        producer_finished,
                    )?;
                    maybe_persist_staged_lexical_rebuild_progress(
                        index_path,
                        &mut rebuild_state,
                        last_processed_conversation_id,
                        processed_conversations,
                        indexed_docs,
                        &latest_pipeline_runtime,
                        staged_publish_base_meta_fingerprint.as_deref(),
                        false,
                        &mut conversations_since_progress_persist,
                        progress_heartbeat_interval_conversations,
                        &mut last_progress_persist,
                        progress_heartbeat_interval,
                        progress_bump.as_ref(),
                        perf_profile.as_mut(),
                    )?;
                }
                recv(active_pipeline_rx) -> message => {
                    match message {
                        Ok(LexicalRebuildPipelineMessage::Batch(prepared_page)) => {
                            let mut page_reservation = StreamingByteReservation::new(
                                lexical_rebuild_flow_limiter.as_ref(),
                                lexical_rebuild_prepared_page_reserved_bytes(&prepared_page),
                            );
                            if let Some(profile) = perf_profile.as_mut() {
                                profile.conversation_list_duration +=
                                    prepared_page.conversation_list_duration;
                                profile.message_stream_duration += prepared_page.message_fetch_duration;
                                profile.finish_conversation_duration +=
                                    prepared_page.packet_prepare_duration;
                            }

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

                            let planned_shard_index = planned_shard_index.ok_or_else(|| {
                                anyhow::anyhow!(
                                    "staged lexical rebuild received a prepared page without planned shard metadata"
                                )
                            })?;
                            if current_shard_index.is_none() {
                                current_shard_index = Some(planned_shard_index);
                            }
                            if current_shard_index != Some(planned_shard_index) {
                                return Err(anyhow::anyhow!(
                                    "staged lexical rebuild crossed from shard {} into shard {} before closing the prior shard",
                                    current_shard_index.unwrap_or(usize::MAX),
                                    planned_shard_index
                                ));
                            }

                            for mut packet in packets {
                                if options.defer_initial_content_fingerprint
                                    && let Some(last_message_id) = packet.last_message_id
                                {
                                    max_message_id = max_message_id.max(last_message_id);
                                }
                                equivalence_accumulator.absorb_packet(&packet);
                                current_shard_message_bytes =
                                    current_shard_message_bytes.saturating_add(packet.message_bytes);
                                packet.flow_reservation_bytes = 0;
                                current_shard_packets.push(packet);
                            }
                            bump_index_run_lock_progress_if_present(progress_bump.as_ref());
                            // The byte limiter covers producer-prepared pages
                            // waiting for this consumer. After packets enter
                            // the staged shard buffers, the pending-shard
                            // backlog cap accounts for their heap footprint.
                            page_reservation.release_now();

                            refresh_runtime(
                                &mut latest_pipeline_runtime,
                                &mut responsiveness_controller,
                                &mut current_batch_conversation_limit,
                                current_shard_packets.len(),
                                current_shard_message_bytes,
                            );
                            refresh_staged_parallelism_and_dispatch(
                                &mut latest_pipeline_runtime,
                                &mut merge_coordinator,
                                &mut pending_shard_build_jobs,
                                &mut pending_shard_build_message_bytes,
                                &mut active_shard_build_jobs,
                                &mut enqueued_shards,
                                producer_finished,
                            )?;

                            if finishes_planned_shard {
                                let shard = shard_plan
                                    .shards
                                    .get(planned_shard_index)
                                    .cloned()
                                    .ok_or_else(|| {
                                        anyhow::anyhow!(
                                            "missing staged lexical rebuild shard metadata for shard {}",
                                            planned_shard_index
                                        )
                                    })?;
                                let shard_message_bytes = current_shard_message_bytes;
                                let shard_packets = std::mem::take(&mut current_shard_packets);
                                current_shard_message_bytes = 0;
                                current_shard_index = None;
                                pending_shard_build_message_bytes =
                                    pending_shard_build_message_bytes
                                        .saturating_add(shard_message_bytes);
                                pending_shard_build_jobs.push_back(LexicalRebuildShardBuildWork {
                                    shard,
                                    packets: shard_packets,
                                    message_bytes: shard_message_bytes,
                                    shard_index_path: shard_stage_root
                                        .path()
                                        .join(format!("shard-{planned_shard_index:05}")),
                                    writer_parallelism: 1,
                                });
                                refresh_staged_parallelism_and_dispatch(
                                    &mut latest_pipeline_runtime,
                                    &mut merge_coordinator,
                                    &mut pending_shard_build_jobs,
                                    &mut pending_shard_build_message_bytes,
                                    &mut active_shard_build_jobs,
                                    &mut enqueued_shards,
                                    producer_finished,
                                )?;
                            }

                            maybe_persist_staged_lexical_rebuild_progress(
                                index_path,
                                &mut rebuild_state,
                                last_processed_conversation_id,
                                processed_conversations,
                                indexed_docs,
                                &latest_pipeline_runtime,
                                staged_publish_base_meta_fingerprint.as_deref(),
                                false,
                                &mut conversations_since_progress_persist,
                                progress_heartbeat_interval_conversations,
                                &mut last_progress_persist,
                                progress_heartbeat_interval,
                                progress_bump.as_ref(),
                                perf_profile.as_mut(),
                            )?;
                        }
                        Ok(LexicalRebuildPipelineMessage::Error(error)) => {
                            return Err(error);
                        }
                        Ok(LexicalRebuildPipelineMessage::Done) => {
                            producer_finished = true;
                            refresh_runtime(
                                &mut latest_pipeline_runtime,
                                &mut responsiveness_controller,
                                &mut current_batch_conversation_limit,
                                current_shard_packets.len(),
                                current_shard_message_bytes,
                            );
                            refresh_staged_parallelism_and_dispatch(
                                &mut latest_pipeline_runtime,
                                &mut merge_coordinator,
                                &mut pending_shard_build_jobs,
                                &mut pending_shard_build_message_bytes,
                                &mut active_shard_build_jobs,
                                &mut enqueued_shards,
                                producer_finished,
                            )?;
                            maybe_persist_staged_lexical_rebuild_progress(
                                index_path,
                                &mut rebuild_state,
                                last_processed_conversation_id,
                                processed_conversations,
                                indexed_docs,
                                &latest_pipeline_runtime,
                                staged_publish_base_meta_fingerprint.as_deref(),
                                false,
                                &mut conversations_since_progress_persist,
                                progress_heartbeat_interval_conversations,
                                &mut last_progress_persist,
                                progress_heartbeat_interval,
                                progress_bump.as_ref(),
                                perf_profile.as_mut(),
                            )?;
                        }
                        Err(_) => {
                            return Err(anyhow::anyhow!(
                                "lexical rebuild pipeline channel closed before producer completion"
                            ));
                        }
                    }
                }
                default(Duration::from_millis(250)) => {
                    refresh_runtime(
                        &mut latest_pipeline_runtime,
                        &mut responsiveness_controller,
                        &mut current_batch_conversation_limit,
                        current_shard_packets.len(),
                        current_shard_message_bytes,
                    );
                    refresh_staged_parallelism_and_dispatch(
                        &mut latest_pipeline_runtime,
                        &mut merge_coordinator,
                        &mut pending_shard_build_jobs,
                        &mut pending_shard_build_message_bytes,
                        &mut active_shard_build_jobs,
                        &mut enqueued_shards,
                        producer_finished,
                    )?;
                }
            }
        }

        if !current_shard_packets.is_empty() || current_shard_index.is_some() {
            return Err(anyhow::anyhow!(
                "staged lexical rebuild finished with an unterminated shard packet buffer"
            ));
        }

        while !pending_shard_build_jobs.is_empty()
            || received_shard_results < enqueued_shards
            || merge_coordinator.pending_merge_jobs() > 0
        {
            let active_shard_result_rx = if shard_result_channel_open {
                &shard_result_rx
            } else {
                &never_shard_result_rx
            };
            let active_merge_result_rx = if merge_result_channel_open {
                &merge_result_rx
            } else {
                &never_merge_result_rx
            };
            select! {
                recv(active_shard_result_rx) -> message => {
                    match message {
                        Ok(LexicalRebuildShardBuildMessage::Built(result)) => {
                            active_shard_build_jobs = active_shard_build_jobs.saturating_sub(1);
                            received_shard_results = received_shard_results.saturating_add(1);
                            shard_build_telemetry.record(&result);
                            pending_completed_shards.insert(result.shard.shard_index, result);
                        }
                        Ok(LexicalRebuildShardBuildMessage::Error { shard_index, error }) => {
                            active_shard_build_jobs = active_shard_build_jobs.saturating_sub(1);
                            return Err(anyhow::anyhow!(
                                "building lexical rebuild shard {shard_index} failed: {error}"
                            ));
                        }
                        Err(_) => {
                            if received_shard_results < enqueued_shards {
                                return Err(anyhow::anyhow!(
                                    "staged lexical rebuild shard builder channel closed before all shards completed"
                                ));
                            }
                            shard_result_channel_open = false;
                            continue;
                        }
                    }
                    let force_progress_persist = advance_completed_shards(
                        &mut pending_completed_shards,
                        &mut next_shard_to_commit,
                        &mut completed_shard_artifacts,
                        &mut processed_conversations,
                        &mut indexed_docs,
                        &mut observed_messages,
                        &mut last_processed_conversation_id,
                        &mut conversations_since_progress_persist,
                        &mut responsiveness_controller,
                        &mut current_batch_conversation_limit,
                    )?;
                    queue_newly_completed_shard_artifacts(
                        &mut merge_coordinator,
                        &completed_shard_artifacts,
                        &mut scheduled_completed_shard_artifacts,
                    )?;
                    refresh_runtime(
                        &mut latest_pipeline_runtime,
                        &mut responsiveness_controller,
                        &mut current_batch_conversation_limit,
                        current_shard_packets.len(),
                        current_shard_message_bytes,
                    );
                    refresh_staged_parallelism_and_dispatch(
                        &mut latest_pipeline_runtime,
                        &mut merge_coordinator,
                        &mut pending_shard_build_jobs,
                        &mut pending_shard_build_message_bytes,
                        &mut active_shard_build_jobs,
                        &mut enqueued_shards,
                        true,
                    )?;
                    maybe_persist_staged_lexical_rebuild_progress(
                        index_path,
                        &mut rebuild_state,
                        last_processed_conversation_id,
                        processed_conversations,
                        indexed_docs,
                        &latest_pipeline_runtime,
                        staged_publish_base_meta_fingerprint.as_deref(),
                        force_progress_persist,
                        &mut conversations_since_progress_persist,
                        progress_heartbeat_interval_conversations,
                        &mut last_progress_persist,
                        progress_heartbeat_interval,
                        progress_bump.as_ref(),
                        perf_profile.as_mut(),
                    )?;
                }
                recv(active_merge_result_rx) -> message => {
                    match message {
                        Ok(LexicalRebuildShardMergeMessage::Built(result)) => {
                            merge_coordinator.complete_merge(result, &merge_work_tx)?;
                            bump_index_run_lock_progress_if_present(progress_bump.as_ref());
                        }
                        Ok(LexicalRebuildShardMergeMessage::Error {
                            output_level,
                            first_shard_index,
                            last_shard_index,
                            error,
                        }) => {
                            return Err(anyhow::anyhow!(
                                "eager staged lexical merge at level {output_level} for shard range {first_shard_index}..={last_shard_index} failed: {error}"
                            ));
                        }
                        Err(_) => {
                            if merge_coordinator.pending_merge_jobs() > 0 {
                                return Err(anyhow::anyhow!(
                                    "staged lexical rebuild eager merge channel closed before all queued merges completed"
                                ));
                            }
                            merge_result_channel_open = false;
                            continue;
                        }
                    }
                    refresh_runtime(
                        &mut latest_pipeline_runtime,
                        &mut responsiveness_controller,
                        &mut current_batch_conversation_limit,
                        current_shard_packets.len(),
                        current_shard_message_bytes,
                    );
                    refresh_staged_parallelism_and_dispatch(
                        &mut latest_pipeline_runtime,
                        &mut merge_coordinator,
                        &mut pending_shard_build_jobs,
                        &mut pending_shard_build_message_bytes,
                        &mut active_shard_build_jobs,
                        &mut enqueued_shards,
                        true,
                    )?;
                    maybe_persist_staged_lexical_rebuild_progress(
                        index_path,
                        &mut rebuild_state,
                        last_processed_conversation_id,
                        processed_conversations,
                        indexed_docs,
                        &latest_pipeline_runtime,
                        staged_publish_base_meta_fingerprint.as_deref(),
                        false,
                        &mut conversations_since_progress_persist,
                        progress_heartbeat_interval_conversations,
                        &mut last_progress_persist,
                        progress_heartbeat_interval,
                        progress_bump.as_ref(),
                        perf_profile.as_mut(),
                    )?;
                }
                default(Duration::from_millis(250)) => {
                    refresh_runtime(
                        &mut latest_pipeline_runtime,
                        &mut responsiveness_controller,
                        &mut current_batch_conversation_limit,
                        current_shard_packets.len(),
                        current_shard_message_bytes,
                    );
                    refresh_staged_parallelism_and_dispatch(
                        &mut latest_pipeline_runtime,
                        &mut merge_coordinator,
                        &mut pending_shard_build_jobs,
                        &mut pending_shard_build_message_bytes,
                        &mut active_shard_build_jobs,
                        &mut enqueued_shards,
                        true,
                    )?;
                    // #282: the staging→publish handoff wedged the stall
                    // watchdog at 100% because a long final-merge / frontier
                    // reduction crosses NO shard-or-merge result boundary for
                    // minutes, so `last_progress_at_ms` froze and the abort
                    // fired even though the merge workers were busy. Bump the
                    // progress atomic on this busy-poll tick ONLY when work is
                    // genuinely in flight (active shard builds or pending
                    // merges) — never on a true zero-work deadlock, so the
                    // watchdog still catches real wedges.
                    if active_shard_build_jobs > 0 || merge_coordinator.pending_merge_jobs() > 0 {
                        bump_index_run_lock_progress_if_present(progress_bump.as_ref());
                    }
                }
            }
        }
        if next_shard_to_commit != shard_plan.shards.len() {
            return Err(anyhow::anyhow!(
                "staged lexical rebuild only committed {} of {} planned shards",
                next_shard_to_commit,
                shard_plan.shards.len()
            ));
        }
        validate_complete_lexical_rebuild_shard_artifacts(&shard_plan, &completed_shard_artifacts)?;
        let mut reduced_final_merge_artifacts = merge_coordinator.final_merge_input_artifacts();
        if should_reduce_staged_lexical_final_frontier(reduced_final_merge_artifacts.len()) {
            reduced_final_merge_artifacts = reduce_staged_lexical_final_merge_frontier_via_workers(
                reduced_final_merge_artifacts,
                final_merge_stage_root.as_path(),
                shard_merge_settings.workers,
                &merge_work_tx,
                &merge_result_rx,
                progress_bump.as_ref(),
            )?;
        }
        final_merge_input_artifacts = Some(reduced_final_merge_artifacts);
        Ok(())
    })();

    if main_result.is_err() {
        lexical_rebuild_flow_limiter.close();
    }
    drop(pipeline_rx);
    // Disconnect both shard-work senders on every exit before joining the
    // builders.  Previously the original sender was dropped only on the happy
    // path inside `main_result`; an early error left it alive, so idle builders
    // remained blocked in `recv()` and this join never returned.
    drop(shard_work_dispatch_tx);
    drop(shard_work_tx);
    drop(merge_work_tx);
    match producer_handle.join() {
        Ok(()) => {}
        Err(payload) => {
            let panic_message = panic_payload_message(payload);
            if main_result.is_ok() {
                return Err(anyhow::anyhow!(
                    "lexical rebuild packet producer panicked: {}",
                    panic_message
                ));
            }
            tracing::warn!(
                error = %panic_message,
                "lexical rebuild packet producer panicked while the staged shard consumer was already failing"
            );
        }
    }

    for handle in shard_builder_handles {
        if let Err(payload) = handle.join() {
            let panic_message = panic_payload_message(payload);
            if main_result.is_ok() {
                return Err(anyhow::anyhow!(
                    "staged lexical rebuild shard builder panicked: {}",
                    panic_message
                ));
            }
            tracing::warn!(
                error = %panic_message,
                "staged lexical rebuild shard builder panicked while the consumer was already failing"
            );
        }
    }
    for handle in merge_worker_handles {
        if let Err(payload) = handle.join() {
            let panic_message = panic_payload_message(payload);
            if main_result.is_ok() {
                return Err(anyhow::anyhow!(
                    "staged lexical rebuild eager merge worker panicked: {}",
                    panic_message
                ));
            }
            tracing::warn!(
                error = %panic_message,
                "staged lexical rebuild eager merge worker panicked while the consumer was already failing"
            );
        }
    }
    main_result?;

    refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
        &mut latest_pipeline_runtime,
        progress.as_ref(),
        lexical_rebuild_flow_limiter.as_ref(),
        Some(producer_telemetry.as_ref()),
        &mut responsiveness_controller,
        pipeline_budget_controller.as_ref(),
        &mut current_batch_conversation_limit,
        None,
        LexicalRebuildPipelineSinkRuntimeSnapshot::new(0, 0, 0),
    );
    maybe_persist_staged_lexical_rebuild_progress(
        index_path,
        &mut rebuild_state,
        last_processed_conversation_id,
        processed_conversations,
        indexed_docs,
        &latest_pipeline_runtime,
        staged_publish_base_meta_fingerprint.as_deref(),
        true,
        &mut conversations_since_progress_persist,
        progress_heartbeat_interval_conversations,
        &mut last_progress_persist,
        progress_heartbeat_interval,
        progress_bump.as_ref(),
        perf_profile.as_mut(),
    )?;

    let final_merge_inputs = final_merge_input_artifacts
        .unwrap_or_else(|| merge_coordinator.final_merge_input_artifacts());
    let final_merge_artifact = finalize_staged_lexical_rebuild_publish_artifact_from_artifacts(
        &staged_merged_index_path,
        &final_merge_inputs,
        &final_merge_stage_root,
        shard_merge_settings.workers,
    )?;
    let merged_docs = final_merge_artifact.docs;
    if merged_docs != indexed_docs {
        return Err(anyhow::anyhow!(
            "staged lexical rebuild merged {} docs but durable shard builds produced {} docs",
            merged_docs,
            indexed_docs
        ));
    }
    // Task 5: the merged candidate owns no region documents — converge
    // EVERY marked session into the publish path BEFORE the manifest and
    // fingerprint are recorded, so the staged swap ships the persisted
    // inventory whole and the post-publish doc-count verify stays exact.
    storage.close_without_checkpoint().with_context(|| {
        format!(
            "closing readonly database after staged Tantivy rebuild without checkpoint: {}",
            db_path.display()
        )
    })?;
    let (region_docs, certification_floor, marks_storage) = {
        let mut region_index = TantivyIndex::open_or_create(&final_merge_artifact.publish_path)
            .with_context(|| {
                format!(
                    "opening merged lexical candidate {} for search-region convergence",
                    final_merge_artifact.publish_path.display()
                )
            })?;
        // Task 8: arm the floor captured at build start — a lazy capture
        // here would cover canonical commits the merged candidate does
        // not contain.
        region_index.arm_publication_floor(build_start_floor);
        let marks_storage = FrankenStorage::open_writer(db_path).with_context(|| {
            format!(
                "opening writable database to stamp search-region docs published: {}",
                db_path.display()
            )
        })?;
        let (_sessions, docs) =
            region_index.converge_all_search_region_docs(marks_storage.raw(), i64::MAX)?;
        // Two-phase: the commit seals the staged region documents, then
        // the queued marks stamps land `published` — never before.
        // Task 8: certification is DEFERRED past the staged swap and the
        // post-publish doc-count verify — `commit_with_region_doc_stamps`
        // would certify this candidate manifest before it goes live, and
        // a refusal below would leave the receipt forever claiming a
        // generation that was never published.
        region_index.commit()?;
        region_index.replay_region_doc_stamps(marks_storage.raw())?;
        indexed_docs += docs;
        (docs, region_index.take_publication_floor(), marks_storage)
    };
    let final_total_conversations = if options.defer_initial_content_fingerprint {
        processed_conversations
    } else {
        total_conversations
    };
    let final_storage_fingerprint = if options.defer_initial_content_fingerprint {
        lexical_rebuild_content_fingerprint_value(
            final_total_conversations,
            max_conversation_id,
            max_message_id,
        )
    } else {
        rebuild_state.db.storage_fingerprint.clone()
    };
    // `indexed_docs` now includes the converged region documents — the
    // canonical message count must not inherit them.
    let final_observed_messages = observed_messages.max(indexed_docs.saturating_sub(region_docs));
    let lexical_rebuild_duration = lexical_rebuild_started.elapsed();
    let publish_started = Instant::now();
    // #282: entering the finalize→publish stretch (manifest persistence,
    // fingerprinting, atomic swap) is a real forward-progress boundary after
    // the merge drain loop exited. Bump the progress atomic so a slow publish
    // on a large index does not freeze `last_progress_at_ms` and trip the
    // stall-abort right at the staging→publish handoff.
    bump_index_run_lock_progress_if_present(progress_bump.as_ref());
    let equivalence_evidence = equivalence_accumulator.finalize();
    let generation_manifest = persist_lexical_rebuild_generation_artifacts(
        &final_merge_artifact.publish_path,
        &final_storage_fingerprint,
        processed_conversations,
        final_total_conversations,
        final_observed_messages,
        indexed_docs,
        &equivalence_evidence,
    )?;
    let staged_published_meta_fingerprint =
        index_meta_fingerprint(&final_merge_artifact.publish_path)?;
    publish_staged_lexical_index(&final_merge_artifact.publish_path, index_path)?;
    log_lexical_generation_manifest_published(&generation_manifest, &equivalence_evidence);

    crate::search::tantivy::validate_searchable_index_contract(index_path).with_context(|| {
        format!(
            "validating staged lexical rebuild after publish: {}",
            index_path.display()
        )
    })?;
    verify_published_lexical_doc_count(index_path, indexed_docs, "staged")?;
    // Task 8: certify ONLY now — the staged swap published this
    // generation and the post-swap verify proved the manifest readers
    // actually open. A refusal above never reached this point, so no
    // durable certification can outlive a manifest that never went live.
    TantivyIndex::certify_published_manifest(marks_storage.raw(), index_path, certification_floor);
    let refresh_ledger =
        build_authoritative_lexical_refresh_ledger(AuthoritativeLexicalRefreshLedgerInput {
            publish_mode: "atomic_staged_swap",
            lexical_duration: lexical_rebuild_duration,
            publish_duration: publish_started.elapsed(),
            processed_conversations,
            total_conversations: final_total_conversations,
            final_observed_messages,
            indexed_docs,
            equivalence_evidence: &equivalence_evidence,
        });
    persist_lexical_refresh_ledger(index_path, &refresh_ledger)?;
    log_lexical_refresh_ledger_published(&refresh_ledger);

    marks_storage.close().with_context(|| {
        format!(
            "closing region-marks writer after staged Tantivy rebuild: {}",
            db_path.display()
        )
    })?;
    rebuild_state.db.storage_fingerprint = final_storage_fingerprint;
    rebuild_state.db.total_conversations = final_total_conversations;
    rebuild_state.db.total_messages = final_observed_messages;
    rebuild_state.committed_offset = i64::try_from(final_total_conversations).unwrap_or(i64::MAX);
    rebuild_state.committed_conversation_id = last_processed_conversation_id;
    rebuild_state.processed_conversations = processed_conversations;
    rebuild_state.indexed_docs = indexed_docs;
    rebuild_state.indexed_region_docs = region_docs;
    rebuild_state.mark_completed(staged_published_meta_fingerprint);
    persist_lexical_rebuild_state(index_path, &rebuild_state)?;

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

    Ok(LexicalRebuildOutcome {
        observed_conversations: final_total_conversations,
        indexed_docs,
        indexed_region_docs: region_docs,
        observed_messages: Some(final_observed_messages),
        exact_checkpoint_persisted: true,
        equivalence: Some(equivalence_evidence),
    })
}
