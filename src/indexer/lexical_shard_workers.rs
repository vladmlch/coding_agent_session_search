use super::*;

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildShardBuildWork {
    pub(in crate::indexer) shard: LexicalShardPlanShard,
    pub(in crate::indexer) packets: Vec<LexicalRebuildConversationPacket>,
    pub(in crate::indexer) message_bytes: usize,
    pub(in crate::indexer) shard_index_path: PathBuf,
    pub(in crate::indexer) writer_parallelism: usize,
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildShardBuildResult {
    pub(in crate::indexer) shard: LexicalShardPlanShard,
    pub(in crate::indexer) indexed_docs: usize,
    pub(in crate::indexer) segments: usize,
    pub(in crate::indexer) shard_index_path: PathBuf,
    pub(in crate::indexer) message_bytes: usize,
    pub(in crate::indexer) index_size_bytes: u64,
    pub(in crate::indexer) build_duration_ms: u64,
    pub(in crate::indexer) amplification_milli: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildShardBuildTelemetrySnapshot {
    pub(in crate::indexer) completed_jobs: usize,
    pub(in crate::indexer) last_shard_index: Option<usize>,
    pub(in crate::indexer) last_message_bytes: usize,
    pub(in crate::indexer) last_index_size_bytes: u64,
    pub(in crate::indexer) last_duration_ms: u64,
    pub(in crate::indexer) last_amplification_milli: Option<u64>,
    pub(in crate::indexer) observed_amplification_milli: Option<u64>,
}

#[derive(Debug, Default)]
pub(in crate::indexer) struct LexicalRebuildShardBuildTelemetry {
    pub(in crate::indexer) completed_jobs: AtomicUsize,
    pub(in crate::indexer) last_shard_index: AtomicUsize,
    pub(in crate::indexer) last_message_bytes: AtomicUsize,
    pub(in crate::indexer) last_index_size_bytes: AtomicU64,
    pub(in crate::indexer) last_duration_ms: AtomicU64,
    pub(in crate::indexer) last_amplification_milli: AtomicU64,
    pub(in crate::indexer) observed_amplification_milli: AtomicU64,
}

impl LexicalRebuildShardBuildTelemetry {
    pub(in crate::indexer) fn record(&self, result: &LexicalRebuildShardBuildResult) {
        self.completed_jobs.fetch_add(1, Ordering::Relaxed);
        self.last_shard_index
            .store(result.shard.shard_index, Ordering::Relaxed);
        self.last_message_bytes
            .store(result.message_bytes, Ordering::Relaxed);
        self.last_index_size_bytes
            .store(result.index_size_bytes, Ordering::Relaxed);
        self.last_duration_ms
            .store(result.build_duration_ms, Ordering::Relaxed);
        self.last_amplification_milli
            .store(result.amplification_milli.unwrap_or(0), Ordering::Relaxed);
        if let Some(amplification_milli) = result.amplification_milli {
            let conservative_amplification = amplification_milli
                .max(LEXICAL_REBUILD_STAGED_SHARD_BUILD_AMPLIFICATION_FLOOR_MILLI);
            let _ = self.observed_amplification_milli.try_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| Some(current.max(conservative_amplification)),
            );
        }
    }

    pub(in crate::indexer) fn snapshot(&self) -> LexicalRebuildShardBuildTelemetrySnapshot {
        let completed_jobs = self.completed_jobs.load(Ordering::Relaxed);
        let last_shard_index = self.last_shard_index.load(Ordering::Relaxed);
        let last_amplification_milli = self.last_amplification_milli.load(Ordering::Relaxed);
        let observed_amplification_milli =
            self.observed_amplification_milli.load(Ordering::Relaxed);
        LexicalRebuildShardBuildTelemetrySnapshot {
            completed_jobs,
            last_shard_index: (completed_jobs > 0).then_some(last_shard_index),
            last_message_bytes: self.last_message_bytes.load(Ordering::Relaxed),
            last_index_size_bytes: self.last_index_size_bytes.load(Ordering::Relaxed),
            last_duration_ms: self.last_duration_ms.load(Ordering::Relaxed),
            last_amplification_milli: (last_amplification_milli > 0)
                .then_some(last_amplification_milli),
            observed_amplification_milli: (observed_amplification_milli > 0)
                .then_some(observed_amplification_milli),
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub(in crate::indexer) struct LexicalRebuildShardMergeWork {
    pub(in crate::indexer) output_path: PathBuf,
    pub(in crate::indexer) input_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildShardMergeArtifact {
    pub(in crate::indexer) first_shard_index: usize,
    pub(in crate::indexer) last_shard_index: usize,
    pub(in crate::indexer) index_path: PathBuf,
    pub(in crate::indexer) docs: usize,
    pub(in crate::indexer) segments: usize,
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildShardMergeResult {
    pub(in crate::indexer) output_level: usize,
    pub(in crate::indexer) artifact: LexicalRebuildShardMergeArtifact,
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildShardMergeJob {
    pub(in crate::indexer) output_level: usize,
    pub(in crate::indexer) output_path: PathBuf,
    pub(in crate::indexer) input_artifacts: Vec<LexicalRebuildShardMergeArtifact>,
}

#[derive(Debug)]
pub(in crate::indexer) enum LexicalRebuildShardMergeMessage {
    Built(LexicalRebuildShardMergeResult),
    Error {
        output_level: usize,
        first_shard_index: usize,
        last_shard_index: usize,
        error: String,
    },
}

#[derive(Debug)]
pub(in crate::indexer) enum LexicalRebuildShardBuildMessage {
    Built(LexicalRebuildShardBuildResult),
    Error { shard_index: usize, error: String },
}

pub(in crate::indexer) fn lexical_rebuild_amplification_milli(
    index_size_bytes: u64,
    message_bytes: usize,
) -> Option<u64> {
    if message_bytes == 0 {
        return None;
    }
    let scaled = u128::from(index_size_bytes).saturating_mul(1_000);
    u64::try_from(scaled / (message_bytes as u128)).ok()
}

pub(in crate::indexer) fn spawn_lexical_rebuild_shard_builder_workers(
    worker_count: usize,
    rx: Receiver<LexicalRebuildShardBuildWork>,
    tx: Sender<LexicalRebuildShardBuildMessage>,
    flow_limiter: Arc<StreamingByteLimiter>,
    lexical_rebuild_worker_pool: Option<Arc<ThreadPool>>,
    heartbeat: Option<crate::search::quill_bridge::EngineHeartbeat>,
) -> Vec<JoinHandle<()>> {
    let tracing_dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());
    (0..worker_count.max(1))
        .map(|worker_idx| {
            let rx = rx.clone();
            let tx = tx.clone();
            let flow_limiter = Arc::clone(&flow_limiter);
            let lexical_rebuild_worker_pool = lexical_rebuild_worker_pool.clone();
            let heartbeat = heartbeat.clone();
            let tracing_dispatch = tracing_dispatch.clone();
            thread::spawn(move || {
                tracing::dispatcher::with_default(&tracing_dispatch, || {
                    while let Ok(work) = rx.recv() {
                        let flow_reservation_bytes = work
                            .packets
                            .iter()
                            .map(|packet| packet.flow_reservation_bytes)
                            .sum::<usize>();
                        let shard_message_bytes = work.message_bytes;
                        let build_started = Instant::now();
                        // #282/#288: a panicking shard-build worker (a Tantivy
                        // assert, an allocator abort, etc.) used to unwind
                        // silently — sibling workers' `tx` clones kept the
                        // result channel open, so the staging→publish consumer
                        // parked forever (all threads on a condvar at the
                        // handoff: the terminal-wedge signature). Mirror the
                        // page-prep containment (see ~16456) and convert the
                        // panic into the same `Error` teardown the consumer
                        // already handles, so it aborts loudly instead.
                        let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                            || {
                                #[cfg(test)]
                                lexical_rebuild_shard_build_injected_panic_hook(
                                    &work.shard_index_path,
                                );
                                build_lexical_rebuild_shard_index_summary_with_writer_parallelism(
                                    &work.shard_index_path,
                                    &work.packets,
                                    lexical_rebuild_worker_pool.as_deref(),
                                    Some(work.writer_parallelism),
                                    heartbeat.as_ref().map(Arc::clone),
                                )
                            },
                        )) {
                            Ok(result) => result,
                            Err(payload) => Err(anyhow::anyhow!(
                                "lexical rebuild shard-build worker {worker_idx} panicked while building shard {}: {}",
                                work.shard.shard_index,
                                panic_payload_message(payload)
                            )),
                        };
                        let build_duration_ms =
                            u64::try_from(build_started.elapsed().as_millis()).unwrap_or(u64::MAX);
                        let index_size_bytes =
                            directory_size_bytes_best_effort(&work.shard_index_path);
                        let amplification_milli = lexical_rebuild_amplification_milli(
                            index_size_bytes,
                            shard_message_bytes,
                        );
                        // The reservation covers the packet heap, not merely
                        // the Tantivy build call. Free that heap before waking
                        // producers; otherwise a newly admitted giant page can
                        // overlap the completed shard packets in RSS while the
                        // limiter already reports their bytes as available.
                        drop(work.packets);
                        flow_limiter.release(flow_reservation_bytes);
                        match result {
                            Ok(summary) => {
                                tracing::info!(
                                    worker_idx,
                                    shard_index = work.shard.shard_index,
                                    writer_parallelism = work.writer_parallelism,
                                    indexed_docs = summary.docs,
                                    shard_conversations = work.shard.conversation_count,
                                    shard_message_bytes,
                                    index_size_bytes,
                                    build_duration_ms,
                                    amplification_milli,
                                    "built lexical rebuild shard index"
                                );
                                if tx
                                    .send(LexicalRebuildShardBuildMessage::Built(
                                        LexicalRebuildShardBuildResult {
                                            shard: work.shard,
                                            indexed_docs: summary.docs,
                                            segments: summary.segments,
                                            shard_index_path: work.shard_index_path,
                                            message_bytes: shard_message_bytes,
                                            index_size_bytes,
                                            build_duration_ms,
                                            amplification_milli,
                                        },
                                    ))
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Err(err) => {
                                let _ = tx.send(LexicalRebuildShardBuildMessage::Error {
                                    shard_index: work.shard.shard_index,
                                    error: format!("{err:#}"),
                                });
                                return;
                            }
                        }
                    }
                });
            })
        })
        .collect()
}

pub(in crate::indexer) fn spawn_lexical_rebuild_shard_merge_workers(
    worker_count: usize,
    rx: Receiver<LexicalRebuildShardMergeJob>,
    tx: Sender<LexicalRebuildShardMergeMessage>,
) -> Vec<JoinHandle<()>> {
    let tracing_dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());
    (0..worker_count.max(1))
        .map(|worker_idx| {
            let rx = rx.clone();
            let tx = tx.clone();
            let tracing_dispatch = tracing_dispatch.clone();
            thread::spawn(move || {
                tracing::dispatcher::with_default(&tracing_dispatch, || {
                    while let Ok(work) = rx.recv() {
                        let first_shard_index = work
                            .input_artifacts
                            .first()
                            .map(|artifact| artifact.first_shard_index)
                            .unwrap_or(usize::MAX);
                        let last_shard_index = work
                            .input_artifacts
                            .last()
                            .map(|artifact| artifact.last_shard_index)
                            .unwrap_or(usize::MAX);
                        let input_paths = work
                            .input_artifacts
                            .iter()
                            .map(|artifact| artifact.index_path.clone())
                            .collect::<Vec<_>>();
                        // #282: a panicking shard-merge worker used to unwind
                        // silently, leaving the result channel open and parking
                        // the final-frontier reducer forever on
                        // `merge_result_rx.recv()`. Contain the panic and emit
                        // the same `Error` teardown the reducer already handles.
                        let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                            || {
                                #[cfg(test)]
                                lexical_rebuild_shard_merge_injected_panic_hook(&work.output_path);
                                crate::search::tantivy::TantivyIndex::merge_compatible_index_directories(
                                    &work.output_path,
                                    &input_paths,
                                )
                            },
                        )) {
                            Ok(result) => result,
                            Err(payload) => Err(anyhow::anyhow!(
                                "lexical rebuild shard-merge worker {worker_idx} panicked while merging shards {first_shard_index}..={last_shard_index}: {}",
                                panic_payload_message(payload)
                            )),
                        };
                        match result {
                            Ok(merged_index) => {
                                let docs = work
                                    .input_artifacts
                                    .iter()
                                    .map(|artifact| artifact.docs)
                                    .sum();
                                let segments = merged_index.segment_count();
                                drop(merged_index);
                                tracing::info!(
                                    worker_idx,
                                    output_level = work.output_level,
                                    first_shard_index,
                                    last_shard_index,
                                    input_count = input_paths.len(),
                                    "merged staged lexical rebuild shard group"
                                );
                                if tx
                                    .send(LexicalRebuildShardMergeMessage::Built(
                                        LexicalRebuildShardMergeResult {
                                            output_level: work.output_level,
                                            artifact: LexicalRebuildShardMergeArtifact {
                                                first_shard_index,
                                                last_shard_index,
                                                index_path: work.output_path,
                                                docs,
                                                segments,
                                            },
                                        },
                                    ))
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Err(err) => {
                                let _ = tx.send(LexicalRebuildShardMergeMessage::Error {
                                    output_level: work.output_level,
                                    first_shard_index,
                                    last_shard_index,
                                    error: format!("{err:#}"),
                                });
                                return;
                            }
                        }
                    }
                });
            })
        })
        .collect()
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildShardMergeCoordinator {
    pub(in crate::indexer) stage_root: PathBuf,
    pub(in crate::indexer) ready_levels: Vec<VecDeque<LexicalRebuildShardMergeArtifact>>,
    pub(in crate::indexer) next_output_seq_by_level: Vec<usize>,
    pub(in crate::indexer) pending_merge_jobs: usize,
    pub(in crate::indexer) allowed_pending_merge_jobs: usize,
}

impl LexicalRebuildShardMergeCoordinator {
    pub(in crate::indexer) const EAGER_MERGE_FAN_IN: usize = 8;

    pub(in crate::indexer) fn new(stage_root: PathBuf) -> Self {
        Self {
            stage_root,
            ready_levels: Vec::new(),
            next_output_seq_by_level: Vec::new(),
            pending_merge_jobs: 0,
            allowed_pending_merge_jobs: 0,
        }
    }

    pub(in crate::indexer) fn pending_merge_jobs(&self) -> usize {
        self.pending_merge_jobs
    }

    pub(in crate::indexer) fn ready_artifact_count(&self) -> usize {
        self.ready_levels.iter().map(VecDeque::len).sum()
    }

    pub(in crate::indexer) fn ready_merge_groups(&self) -> usize {
        self.ready_levels
            .iter()
            .map(|level| level.len() / Self::EAGER_MERGE_FAN_IN)
            .sum()
    }

    pub(in crate::indexer) fn set_allowed_pending_merge_jobs(
        &mut self,
        allowed_pending_merge_jobs: usize,
        merge_work_tx: &Sender<LexicalRebuildShardMergeJob>,
    ) -> Result<()> {
        self.allowed_pending_merge_jobs = allowed_pending_merge_jobs;
        self.schedule_ready_merges(merge_work_tx)
    }

    pub(in crate::indexer) fn queue_base_artifact(
        &mut self,
        artifact: LexicalRebuildShardMergeArtifact,
        merge_work_tx: &Sender<LexicalRebuildShardMergeJob>,
    ) -> Result<()> {
        self.queue_artifact_at_level(0, artifact, merge_work_tx)
    }

    pub(in crate::indexer) fn complete_merge(
        &mut self,
        result: LexicalRebuildShardMergeResult,
        merge_work_tx: &Sender<LexicalRebuildShardMergeJob>,
    ) -> Result<()> {
        self.pending_merge_jobs = self.pending_merge_jobs.saturating_sub(1);
        self.queue_artifact_at_level(result.output_level, result.artifact, merge_work_tx)
    }

    pub(in crate::indexer) fn final_merge_input_artifacts(
        &self,
    ) -> Vec<LexicalRebuildShardMergeArtifact> {
        let mut artifacts = self
            .ready_levels
            .iter()
            .flat_map(|level| level.iter().cloned())
            .collect::<Vec<_>>();
        artifacts.sort_by_key(|artifact| (artifact.first_shard_index, artifact.last_shard_index));
        artifacts
    }

    pub(in crate::indexer) fn queue_artifact_at_level(
        &mut self,
        level: usize,
        artifact: LexicalRebuildShardMergeArtifact,
        merge_work_tx: &Sender<LexicalRebuildShardMergeJob>,
    ) -> Result<()> {
        self.ensure_level(level);
        self.ready_levels[level].push_back(artifact);
        self.schedule_ready_merges(merge_work_tx)
    }

    pub(in crate::indexer) fn schedule_ready_merges(
        &mut self,
        merge_work_tx: &Sender<LexicalRebuildShardMergeJob>,
    ) -> Result<()> {
        let mut level = 0usize;
        while level < self.ready_levels.len() {
            while self.pending_merge_jobs < self.allowed_pending_merge_jobs
                && self.ready_levels[level].len() >= Self::EAGER_MERGE_FAN_IN
            {
                let output_level = level.saturating_add(1);
                self.ensure_level(output_level);
                let inputs = (0..Self::EAGER_MERGE_FAN_IN)
                    .map(|_| {
                        self.ready_levels[level]
                            .pop_front()
                            .expect("merge coordinator should have enough artifacts ready")
                    })
                    .collect::<Vec<_>>();
                let seq = self.next_output_seq_by_level[output_level];
                self.next_output_seq_by_level[output_level] =
                    self.next_output_seq_by_level[output_level].saturating_add(1);
                let round_dir = self.stage_root.join(format!("round-{output_level:05}"));
                fs::create_dir_all(&round_dir).with_context(|| {
                    format!(
                        "creating eager staged lexical merge round directory {}",
                        round_dir.display()
                    )
                })?;
                let first_shard_index = inputs
                    .first()
                    .map(|artifact| artifact.first_shard_index)
                    .unwrap_or(usize::MAX);
                let last_shard_index = inputs
                    .last()
                    .map(|artifact| artifact.last_shard_index)
                    .unwrap_or(usize::MAX);
                let output_path = round_dir.join(format!("merge-{seq:05}"));
                tracing::info!(
                    merge_level = output_level,
                    first_shard_index,
                    last_shard_index,
                    input_count = inputs.len(),
                    "queueing eager staged lexical rebuild merge job"
                );
                merge_work_tx
                    .send(LexicalRebuildShardMergeJob {
                        output_level,
                        output_path,
                        input_artifacts: inputs,
                    })
                    .map_err(|_| {
                        anyhow::anyhow!(
                            "staged lexical rebuild eager merge worker queue disconnected"
                        )
                    })?;
                self.pending_merge_jobs = self.pending_merge_jobs.saturating_add(1);
            }
            level = level.saturating_add(1);
        }
        Ok(())
    }

    pub(in crate::indexer) fn ensure_level(&mut self, level: usize) {
        while self.ready_levels.len() <= level {
            self.ready_levels.push(VecDeque::new());
        }
        while self.next_output_seq_by_level.len() <= level {
            self.next_output_seq_by_level.push(0);
        }
    }
}

// The final publish path can expose a federated shard bundle directly, so the
// rebuild critical path should not compact every small residual frontier down
// to one artifact. This is the LSM-style tiered publish threshold: keep query
// fan-out bounded, but avoid foreground compaction for the common 5-32 shard
// tail left after eager merging.
pub(in crate::indexer) const LEXICAL_REBUILD_FINAL_FRONTIER_FEDERATED_SHARD_LIMIT: usize = 32;

pub(in crate::indexer) fn should_reduce_staged_lexical_final_frontier(
    frontier_artifacts: usize,
) -> bool {
    frontier_artifacts > LEXICAL_REBUILD_FINAL_FRONTIER_FEDERATED_SHARD_LIMIT
}

pub(in crate::indexer) fn reduce_staged_lexical_final_merge_frontier_via_workers(
    mut frontier: Vec<LexicalRebuildShardMergeArtifact>,
    stage_root: &Path,
    max_parallel_jobs: usize,
    merge_work_tx: &Sender<LexicalRebuildShardMergeJob>,
    merge_result_rx: &Receiver<LexicalRebuildShardMergeMessage>,
    // #282: a large final frontier can spend minutes here blocked on
    // `merge_result_rx.recv()` with no shard/merge boundary in the outer drain
    // loop, freezing `last_progress_at_ms` and tripping the stall-abort at
    // 100%. Each completed reduction merge is real forward progress, so bump
    // the lock-file progress atomic on every `Built` result.
    progress_bump: Option<&Arc<AtomicI64>>,
) -> Result<Vec<LexicalRebuildShardMergeArtifact>> {
    if frontier.len() <= 1 {
        return Ok(frontier);
    }

    let worker_limit = max_parallel_jobs.max(1);
    let reduction_stage_root = stage_root.join("worker-final-frontier");
    fs::create_dir_all(&reduction_stage_root).with_context(|| {
        format!(
            "creating staged lexical final-frontier reduction directory {}",
            reduction_stage_root.display()
        )
    })?;
    frontier.sort_by_key(|artifact| (artifact.first_shard_index, artifact.last_shard_index));
    tracing::info!(
        ready_artifacts = frontier.len(),
        worker_limit,
        "draining staged lexical rebuild final merge frontier via merge workers"
    );

    let mut pending_jobs = 0usize;
    let mut next_output_seq = 0usize;
    while frontier.len().saturating_add(pending_jobs) > 1 {
        while pending_jobs < worker_limit && frontier.len() > 1 {
            let input_count = frontier
                .len()
                .min(LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN);
            let inputs = frontier.drain(0..input_count).collect::<Vec<_>>();
            let first_shard_index = inputs
                .first()
                .map(|artifact| artifact.first_shard_index)
                .unwrap_or(usize::MAX);
            let last_shard_index = inputs
                .last()
                .map(|artifact| artifact.last_shard_index)
                .unwrap_or(usize::MAX);
            let output_path = reduction_stage_root.join(format!("merge-{next_output_seq:05}"));
            next_output_seq = next_output_seq.saturating_add(1);
            tracing::info!(
                first_shard_index,
                last_shard_index,
                input_count = inputs.len(),
                "queueing staged lexical rebuild final-frontier merge job"
            );
            merge_work_tx
                .send(LexicalRebuildShardMergeJob {
                    output_level: 0,
                    output_path,
                    input_artifacts: inputs,
                })
                .map_err(|_| {
                    anyhow::anyhow!(
                        "staged lexical rebuild eager merge worker queue disconnected during final-frontier reduction"
                    )
                })?;
            pending_jobs = pending_jobs.saturating_add(1);
        }

        let message = merge_result_rx.recv().map_err(|_| {
            anyhow::anyhow!(
                "staged lexical rebuild eager merge channel closed during final-frontier reduction"
            )
        })?;
        match message {
            LexicalRebuildShardMergeMessage::Built(result) => {
                pending_jobs = pending_jobs.saturating_sub(1);
                bump_index_run_lock_progress_if_present(progress_bump);
                frontier.push(result.artifact);
                frontier.sort_by_key(|artifact| {
                    (artifact.first_shard_index, artifact.last_shard_index)
                });
            }
            LexicalRebuildShardMergeMessage::Error {
                output_level,
                first_shard_index,
                last_shard_index,
                error,
            } => {
                return Err(anyhow::anyhow!(
                    "staged lexical final-frontier merge at level {output_level} for shard range {first_shard_index}..={last_shard_index} failed: {error}"
                ));
            }
        }
    }

    Ok(frontier)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildStagedMergeRuntimeSnapshot {
    pub(in crate::indexer) workers_max: usize,
    pub(in crate::indexer) allowed_jobs: usize,
    pub(in crate::indexer) active_jobs: usize,
    pub(in crate::indexer) ready_artifacts: usize,
    pub(in crate::indexer) ready_groups: usize,
    pub(in crate::indexer) controller_reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildStagedShardBuildRuntimeSnapshot {
    pub(in crate::indexer) workers_max: usize,
    pub(in crate::indexer) allowed_jobs: usize,
    pub(in crate::indexer) active_jobs: usize,
    pub(in crate::indexer) pending_jobs: usize,
    pub(in crate::indexer) controller_reason: String,
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildStagedMergeController {
    pub(in crate::indexer) max_workers: usize,
    pub(in crate::indexer) loadavg_high_watermark_1m_milli: Option<u32>,
}

impl LexicalRebuildStagedMergeController {
    pub(in crate::indexer) fn new(
        max_workers: usize,
        loadavg_high_watermark_1m_milli: Option<u32>,
    ) -> Self {
        Self {
            max_workers: max_workers.max(1),
            loadavg_high_watermark_1m_milli,
        }
    }

    pub(in crate::indexer) fn decide(
        &self,
        producer_finished: bool,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
        merge_coordinator: &LexicalRebuildShardMergeCoordinator,
    ) -> LexicalRebuildStagedMergeRuntimeSnapshot {
        let active_jobs = merge_coordinator.pending_merge_jobs();
        let ready_artifacts = merge_coordinator.ready_artifact_count();
        let ready_groups = merge_coordinator.ready_merge_groups();
        let backlog_groups = active_jobs.saturating_add(ready_groups);
        if backlog_groups == 0 {
            return LexicalRebuildStagedMergeRuntimeSnapshot {
                workers_max: self.max_workers,
                controller_reason: "no_staged_merge_backlog".to_string(),
                ..LexicalRebuildStagedMergeRuntimeSnapshot::default()
            };
        }

        let (allowed_jobs, controller_reason) = if !producer_finished {
            if let (Some(loadavg_1m_milli), Some(high_watermark_1m_milli)) = (
                runtime.host_loadavg_1m_milli,
                self.loadavg_high_watermark_1m_milli,
            ) && loadavg_1m_milli >= high_watermark_1m_milli
            {
                (
                    0,
                    format!(
                        "host_loadavg_1m_{}_reached_high_watermark_{}",
                        format_lexical_rebuild_loadavg_1m_milli(loadavg_1m_milli),
                        format_lexical_rebuild_loadavg_1m_milli(high_watermark_1m_milli)
                    ),
                )
            } else if runtime.ordered_buffered_pages > 0 || runtime.queue_depth > 0 {
                let debt_budget = ready_groups.min(self.max_workers).max(1);
                let trickle_budget = active_jobs.max(debt_budget).min(self.max_workers);
                (
                    trickle_budget,
                    format!(
                        "builder_handoff_pressure_scaling_staged_merge_budget_{}_active_jobs_{}_ready_groups_{}_debt_budget_{}_buffered_pages_{}_queue_depth_{}",
                        trickle_budget,
                        active_jobs,
                        ready_groups,
                        debt_budget,
                        runtime.ordered_buffered_pages,
                        runtime.queue_depth
                    ),
                )
            } else if runtime.page_prep_workers > 0
                && runtime.active_page_prep_jobs >= runtime.page_prep_workers
            {
                let saturated_budget = ready_groups
                    .min(self.max_workers.div_ceil(2))
                    .max(1)
                    .max(active_jobs.min(self.max_workers));
                (
                    saturated_budget,
                    if saturated_budget == 1 {
                        format!(
                            "page_prep_workers_saturated_{}_of_{}",
                            runtime.active_page_prep_jobs, runtime.page_prep_workers
                        )
                    } else {
                        format!(
                            "page_prep_workers_saturated_{}_of_{}_merge_budget_{}_active_jobs_{}_ready_groups_{}",
                            runtime.active_page_prep_jobs,
                            runtime.page_prep_workers,
                            saturated_budget,
                            active_jobs,
                            ready_groups
                        )
                    },
                )
            } else if runtime.active_page_prep_jobs > 0
                || runtime.pending_batch_conversations > 0
                || runtime.pending_batch_message_bytes > 0
            {
                (
                    self.max_workers.div_ceil(2).max(1),
                    format!(
                        "pipeline_active_page_prep_jobs_{}_pending_batch_conversations_{}_pending_batch_message_bytes_{}",
                        runtime.active_page_prep_jobs,
                        runtime.pending_batch_conversations,
                        runtime.pending_batch_message_bytes
                    ),
                )
            } else {
                (
                    self.max_workers,
                    "producer_idle_allowing_max_staged_merge_parallelism".to_string(),
                )
            }
        } else if ready_artifacts.saturating_add(active_jobs)
            <= LEXICAL_REBUILD_FINAL_FRONTIER_FEDERATED_SHARD_LIMIT
        {
            (
                active_jobs.min(self.max_workers),
                format!(
                    "producer_finished_final_frontier_within_federated_cap_{}_active_jobs_{}_ready_artifacts_{}",
                    LEXICAL_REBUILD_FINAL_FRONTIER_FEDERATED_SHARD_LIMIT,
                    active_jobs,
                    ready_artifacts
                ),
            )
        } else if let (Some(loadavg_1m_milli), Some(high_watermark_1m_milli)) = (
            runtime.host_loadavg_1m_milli,
            self.loadavg_high_watermark_1m_milli,
        ) && loadavg_1m_milli >= high_watermark_1m_milli
        {
            (
                1.min(self.max_workers),
                format!(
                    "producer_finished_but_host_loadavg_1m_{}_remains_high",
                    format_lexical_rebuild_loadavg_1m_milli(loadavg_1m_milli)
                ),
            )
        } else {
            (
                self.max_workers,
                "producer_finished_allowing_max_staged_merge_parallelism".to_string(),
            )
        };

        LexicalRebuildStagedMergeRuntimeSnapshot {
            workers_max: self.max_workers,
            allowed_jobs: allowed_jobs.min(backlog_groups),
            active_jobs,
            ready_artifacts,
            ready_groups,
            controller_reason,
        }
    }
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildStagedShardBuildController {
    pub(in crate::indexer) max_workers: usize,
    pub(in crate::indexer) loadavg_high_watermark_1m_milli: Option<u32>,
    pub(in crate::indexer) memory_reserve_bytes: usize,
    pub(in crate::indexer) emergency_memory_reserve_bytes: usize,
}

impl LexicalRebuildStagedShardBuildController {
    pub(in crate::indexer) fn new(
        max_workers: usize,
        loadavg_high_watermark_1m_milli: Option<u32>,
    ) -> Self {
        Self::new_with_memory_reserves(
            max_workers,
            loadavg_high_watermark_1m_milli,
            lexical_rebuild_staged_shard_build_memory_reserve_bytes(),
            lexical_rebuild_staged_shard_build_emergency_memory_reserve_bytes(),
        )
    }

    pub(in crate::indexer) fn new_with_memory_reserves(
        max_workers: usize,
        loadavg_high_watermark_1m_milli: Option<u32>,
        memory_reserve_bytes: usize,
        emergency_memory_reserve_bytes: usize,
    ) -> Self {
        let memory_reserve_bytes = memory_reserve_bytes.max(1);
        Self {
            max_workers: max_workers.max(1),
            loadavg_high_watermark_1m_milli,
            memory_reserve_bytes,
            emergency_memory_reserve_bytes: emergency_memory_reserve_bytes
                .max(1)
                .min(memory_reserve_bytes),
        }
    }

    pub(in crate::indexer) fn decide(
        &self,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
        staged_merge_runtime: &LexicalRebuildStagedMergeRuntimeSnapshot,
        active_jobs: usize,
        pending_jobs: usize,
        next_pending_job_message_bytes: Option<usize>,
    ) -> LexicalRebuildStagedShardBuildRuntimeSnapshot {
        let backlog_jobs = active_jobs.saturating_add(pending_jobs);
        if backlog_jobs == 0 {
            return LexicalRebuildStagedShardBuildRuntimeSnapshot {
                workers_max: self.max_workers,
                allowed_jobs: 0,
                active_jobs,
                pending_jobs,
                controller_reason: "no_staged_shard_build_backlog".to_string(),
            };
        }

        let (allowed_jobs, controller_reason) =
            if let (Some(loadavg_1m_milli), Some(high_watermark_1m_milli)) = (
                runtime.host_loadavg_1m_milli,
                self.loadavg_high_watermark_1m_milli,
            ) && loadavg_1m_milli >= high_watermark_1m_milli
            {
                (
                    1.min(self.max_workers),
                    format!(
                        "host_loadavg_1m_{}_reached_high_watermark_{}",
                        format_lexical_rebuild_loadavg_1m_milli(loadavg_1m_milli),
                        format_lexical_rebuild_loadavg_1m_milli(high_watermark_1m_milli)
                    ),
                )
            } else {
                let merge_reserved_slots = staged_merge_runtime.allowed_jobs.min(self.max_workers);
                let shard_budget = self.max_workers.saturating_sub(merge_reserved_slots).max(1);
                if merge_reserved_slots > 0 {
                    (
                        shard_budget,
                        format!(
                            "reserving_{}_slots_for_staged_merge_active_jobs_{}_ready_groups_{}",
                            merge_reserved_slots,
                            staged_merge_runtime.active_jobs,
                            staged_merge_runtime.ready_groups
                        ),
                    )
                } else {
                    (
                        self.max_workers,
                        "staged_merge_idle_lending_full_budget_to_shard_builders".to_string(),
                    )
                }
            };

        let (allowed_jobs, controller_reason) = self.apply_memory_admission(
            runtime,
            active_jobs,
            backlog_jobs,
            next_pending_job_message_bytes,
            allowed_jobs,
            controller_reason,
        );

        LexicalRebuildStagedShardBuildRuntimeSnapshot {
            workers_max: self.max_workers,
            allowed_jobs: allowed_jobs.min(backlog_jobs),
            active_jobs,
            pending_jobs,
            controller_reason,
        }
    }

    pub(in crate::indexer) fn apply_memory_admission(
        &self,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
        active_jobs: usize,
        backlog_jobs: usize,
        next_pending_job_message_bytes: Option<usize>,
        allowed_jobs: usize,
        controller_reason: String,
    ) -> (usize, String) {
        let Some(available_memory_bytes) = runtime.host_available_memory_bytes else {
            return (allowed_jobs, controller_reason);
        };
        let available_memory_bytes = usize_from_u64_saturating(available_memory_bytes);
        let estimated_builder_bytes =
            self.estimated_builder_memory_bytes(runtime, next_pending_job_message_bytes);
        if available_memory_bytes <= self.emergency_memory_reserve_bytes {
            if active_jobs == 0 && backlog_jobs > 0 {
                let probe_ceiling =
                    available_memory_bytes.saturating_sub(available_memory_bytes / 4);
                if estimated_builder_bytes <= probe_ceiling {
                    return (
                        1.min(allowed_jobs).min(backlog_jobs),
                        format!(
                            "host_available_memory_bytes_{}_below_emergency_reserve_{}_admitting_single_small_staged_shard_build_estimated_builder_bytes_{}",
                            available_memory_bytes,
                            self.emergency_memory_reserve_bytes,
                            estimated_builder_bytes
                        ),
                    );
                }
            }
            return (
                active_jobs.min(backlog_jobs),
                format!(
                    "host_available_memory_bytes_{}_below_emergency_reserve_{}_pausing_new_staged_shard_builds",
                    available_memory_bytes, self.emergency_memory_reserve_bytes
                ),
            );
        }
        if available_memory_bytes <= self.memory_reserve_bytes {
            let allowed_under_reserve = if active_jobs == 0 && backlog_jobs > 0 {
                1
            } else {
                active_jobs
            }
            .min(allowed_jobs)
            .min(backlog_jobs);
            return (
                allowed_under_reserve,
                format!(
                    "host_available_memory_bytes_{}_below_reserve_{}_limiting_staged_shard_builds_to_{}",
                    available_memory_bytes, self.memory_reserve_bytes, allowed_under_reserve
                ),
            );
        }

        let memory_headroom = available_memory_bytes.saturating_sub(self.memory_reserve_bytes);
        let active_estimated_bytes = estimated_builder_bytes.saturating_mul(active_jobs);
        let additional_jobs =
            memory_headroom.saturating_sub(active_estimated_bytes) / estimated_builder_bytes.max(1);
        let memory_allowed_jobs = active_jobs
            .saturating_add(additional_jobs)
            .min(self.max_workers)
            .min(backlog_jobs)
            .max(active_jobs.min(backlog_jobs));
        if memory_allowed_jobs < allowed_jobs {
            (
                memory_allowed_jobs,
                format!(
                    "host_available_memory_bytes_{}_reserve_{}_estimated_builder_bytes_{}_limiting_staged_shard_builds_to_{}",
                    available_memory_bytes,
                    self.memory_reserve_bytes,
                    estimated_builder_bytes,
                    memory_allowed_jobs
                ),
            )
        } else {
            (allowed_jobs, controller_reason)
        }
    }

    pub(in crate::indexer) fn estimated_builder_memory_bytes(
        &self,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
        next_pending_job_message_bytes: Option<usize>,
    ) -> usize {
        let amplification_milli = runtime
            .staged_shard_build_observed_amplification_milli
            .unwrap_or(LEXICAL_REBUILD_STAGED_SHARD_BUILD_AMPLIFICATION_FLOOR_MILLI)
            .max(LEXICAL_REBUILD_STAGED_SHARD_BUILD_AMPLIFICATION_FLOOR_MILLI)
            .saturating_mul(LEXICAL_REBUILD_STAGED_SHARD_BUILD_AMPLIFICATION_HEADROOM_MILLI)
            / 1_000;
        let amplified_message_bytes = next_pending_job_message_bytes
            .map(|message_bytes| {
                let scaled =
                    (message_bytes as u128).saturating_mul(u128::from(amplification_milli)) / 1_000;
                usize::try_from(scaled).unwrap_or(usize::MAX)
            })
            .unwrap_or(0);
        amplified_message_bytes
            .max(usize_from_u64_saturating(
                LEXICAL_REBUILD_STAGED_SHARD_BUILD_MIN_ESTIMATED_BYTES,
            ))
            .max(1)
    }
}

pub(in crate::indexer) fn should_pause_staged_lexical_pipeline_handoff(
    pipeline_backlog_paused: bool,
    active_shard_build_jobs: usize,
    pending_merge_jobs: usize,
) -> bool {
    pipeline_backlog_paused && (active_shard_build_jobs > 0 || pending_merge_jobs > 0)
}

pub(in crate::indexer) fn apply_staged_merge_runtime_snapshot(
    latest_runtime: &mut LexicalRebuildPipelineRuntimeSnapshot,
    progress: Option<&Arc<IndexingProgress>>,
    staged_merge_runtime: &LexicalRebuildStagedMergeRuntimeSnapshot,
) {
    latest_runtime.staged_merge_workers_max = staged_merge_runtime.workers_max;
    latest_runtime.staged_merge_allowed_jobs = staged_merge_runtime.allowed_jobs;
    latest_runtime.staged_merge_active_jobs = staged_merge_runtime.active_jobs;
    latest_runtime.staged_merge_ready_artifacts = staged_merge_runtime.ready_artifacts;
    latest_runtime.staged_merge_ready_groups = staged_merge_runtime.ready_groups;
    latest_runtime.staged_merge_controller_reason = staged_merge_runtime.controller_reason.clone();
    let Some(progress) = progress else {
        return;
    };
    progress
        .rebuild_pipeline_staged_merge_workers_max
        .store(staged_merge_runtime.workers_max, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_allowed_jobs
        .store(staged_merge_runtime.allowed_jobs, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_active_jobs
        .store(staged_merge_runtime.active_jobs, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_ready_artifacts
        .store(staged_merge_runtime.ready_artifacts, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_ready_groups
        .store(staged_merge_runtime.ready_groups, Ordering::Relaxed);
    if let Ok(mut staged_merge_reason) = progress
        .rebuild_pipeline_staged_merge_controller_reason
        .lock()
    {
        *staged_merge_reason = staged_merge_runtime.controller_reason.clone();
    }
}

pub(in crate::indexer) fn apply_staged_shard_build_runtime_snapshot(
    latest_runtime: &mut LexicalRebuildPipelineRuntimeSnapshot,
    progress: Option<&Arc<IndexingProgress>>,
    staged_shard_build_runtime: &LexicalRebuildStagedShardBuildRuntimeSnapshot,
) {
    latest_runtime.staged_shard_build_workers_max = staged_shard_build_runtime.workers_max;
    latest_runtime.staged_shard_build_allowed_jobs = staged_shard_build_runtime.allowed_jobs;
    latest_runtime.staged_shard_build_active_jobs = staged_shard_build_runtime.active_jobs;
    latest_runtime.staged_shard_build_pending_jobs = staged_shard_build_runtime.pending_jobs;
    latest_runtime.staged_shard_build_controller_reason =
        staged_shard_build_runtime.controller_reason.clone();
    let Some(progress) = progress else {
        return;
    };
    progress
        .rebuild_pipeline_staged_shard_build_workers_max
        .store(staged_shard_build_runtime.workers_max, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_allowed_jobs
        .store(staged_shard_build_runtime.allowed_jobs, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_active_jobs
        .store(staged_shard_build_runtime.active_jobs, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_pending_jobs
        .store(staged_shard_build_runtime.pending_jobs, Ordering::Relaxed);
    if let Ok(mut staged_shard_build_reason) = progress
        .rebuild_pipeline_staged_shard_build_controller_reason
        .lock()
    {
        *staged_shard_build_reason = staged_shard_build_runtime.controller_reason.clone();
    }
}

pub(in crate::indexer) fn apply_staged_shard_build_telemetry_snapshot(
    latest_runtime: &mut LexicalRebuildPipelineRuntimeSnapshot,
    progress: Option<&Arc<IndexingProgress>>,
    memory_reserve_bytes: usize,
    emergency_memory_reserve_bytes: usize,
    telemetry: &LexicalRebuildShardBuildTelemetry,
) {
    let snapshot = telemetry.snapshot();
    latest_runtime.staged_shard_build_memory_reserve_bytes = memory_reserve_bytes;
    latest_runtime.staged_shard_build_emergency_memory_reserve_bytes =
        emergency_memory_reserve_bytes;
    latest_runtime.staged_shard_build_completed_jobs = snapshot.completed_jobs;
    latest_runtime.staged_shard_build_last_shard_index = snapshot.last_shard_index;
    latest_runtime.staged_shard_build_last_message_bytes = snapshot.last_message_bytes;
    latest_runtime.staged_shard_build_last_index_size_bytes = snapshot.last_index_size_bytes;
    latest_runtime.staged_shard_build_last_duration_ms = snapshot.last_duration_ms;
    latest_runtime.staged_shard_build_last_amplification_milli = snapshot.last_amplification_milli;
    latest_runtime.staged_shard_build_observed_amplification_milli =
        snapshot.observed_amplification_milli;
    let Some(progress) = progress else {
        return;
    };
    progress
        .rebuild_pipeline_staged_shard_build_memory_reserve_bytes
        .store(memory_reserve_bytes, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_emergency_memory_reserve_bytes
        .store(emergency_memory_reserve_bytes, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_completed_jobs
        .store(snapshot.completed_jobs, Ordering::Relaxed);
    if let Ok(mut last_shard_index) = progress
        .rebuild_pipeline_staged_shard_build_last_shard_index
        .lock()
    {
        *last_shard_index = snapshot.last_shard_index;
    }
    progress
        .rebuild_pipeline_staged_shard_build_last_message_bytes
        .store(snapshot.last_message_bytes, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_last_index_size_bytes
        .store(snapshot.last_index_size_bytes, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_last_duration_ms
        .store(snapshot.last_duration_ms, Ordering::Relaxed);
    if let Ok(mut last_amplification) = progress
        .rebuild_pipeline_staged_shard_build_last_amplification_milli
        .lock()
    {
        *last_amplification = snapshot.last_amplification_milli;
    }
    if let Ok(mut observed_amplification) = progress
        .rebuild_pipeline_staged_shard_build_observed_amplification_milli
        .lock()
    {
        *observed_amplification = snapshot.observed_amplification_milli;
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn flush_streamed_lexical_rebuild_batch_for_planned_shard_boundary(
    planned_shard_index: Option<usize>,
    finishes_planned_shard: bool,
    pending_batch: &mut Vec<LexicalRebuildConversationPacket>,
    pending_batch_message_count: &mut usize,
    pending_batch_message_bytes: &mut usize,
    lexical_rebuild_flow_limiter: Option<&StreamingByteLimiter>,
    lexical_rebuild_worker_pool: Option<&ThreadPool>,
    t_index: &mut TantivyIndex,
    indexed_docs: &mut usize,
    messages_since_commit: &mut usize,
    message_bytes_since_commit: &mut usize,
    current_batch_conversation_limit: usize,
    page_size: i64,
    perf_profile: Option<&mut LexicalRebuildPerfProfile>,
) -> Result<bool> {
    if !finishes_planned_shard || pending_batch.is_empty() {
        return Ok(false);
    }

    tracing::info!(
        planned_shard_index,
        shard_conversations = pending_batch.len(),
        shard_message_bytes = *pending_batch_message_bytes,
        "lexical rebuild flushing pending batch at planned shard boundary"
    );
    flush_streamed_lexical_rebuild_batch(
        pending_batch,
        pending_batch_message_count,
        pending_batch_message_bytes,
        lexical_rebuild_flow_limiter,
        lexical_rebuild_worker_pool,
        t_index,
        indexed_docs,
        messages_since_commit,
        message_bytes_since_commit,
        current_batch_conversation_limit,
        page_size,
        perf_profile,
    )?;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
/// Commit a batch and checkpoint the rebuild.
///
/// `state_path` and `content_path` are separate because a staged full rebuild
/// builds index CONTENT into a scratch sibling while its checkpoint stays next
/// to the live index. They are the same directory for an ordinary in-place
/// incremental run.
///
/// Mixing them up is silent: the checkpoint would record the fingerprint of the
/// LIVE index (untouched during a staged build) rather than of the index just
/// committed, so the fingerprint would be watching the wrong directory for
/// change. A concurrent publish to the live path would then invalidate a
/// checkpoint whose staged content is perfectly fine, forcing a full restart.
pub(in crate::indexer) fn commit_lexical_rebuild_progress(
    state_path: &Path,
    content_path: &Path,
    rebuild_state: &mut LexicalRebuildState,
    next_conversation_id: Option<i64>,
    processed_conversations: usize,
    indexed_docs: usize,
    runtime: &LexicalRebuildPipelineRuntimeSnapshot,
    t_index: &mut TantivyIndex,
    persist_finalized_checkpoint: bool,
    mut perf_profile: Option<&mut LexicalRebuildPerfProfile>,
    region_docs: usize,
) -> Result<()> {
    let pending_progress_started = perf_profile.as_ref().map(|_| Instant::now());
    persist_pending_lexical_rebuild_progress(
        state_path,
        content_path,
        rebuild_state,
        next_conversation_id,
        processed_conversations,
        indexed_docs,
        runtime,
        region_docs,
    )?;
    if let (Some(profile), Some(started)) = (perf_profile.as_mut(), pending_progress_started) {
        profile.pending_progress_duration += started.elapsed();
    }
    let commit_started = perf_profile.as_ref().map(|_| Instant::now());
    t_index.commit()?;
    if let (Some(profile), Some(started)) = (perf_profile.as_mut(), commit_started) {
        profile.commit_count = profile.commit_count.saturating_add(1);
        profile.commit_duration += started.elapsed();
    }
    // GH #440 test hook: the engine commit above is the moment the published
    // authority moves ahead of the durable checkpoint (written below). A
    // process killed here leaves exactly the state #440 reported — a staging
    // MANIFEST several conversations ahead of `.lexical-rebuild-state.json` —
    // and the next plain `cass index` must resume through it, not exit 9.
    maybe_pause_lexical_rebuild_after_commit_for_kill(
        state_path,
        rebuild_state.indexed_docs,
        indexed_docs,
    )?;
    let meta_fingerprint_started = perf_profile.as_ref().map(|_| Instant::now());
    let meta_fingerprint = index_meta_fingerprint(content_path)?;
    if let (Some(profile), Some(started)) = (perf_profile.as_mut(), meta_fingerprint_started) {
        profile.meta_fingerprint_duration += started.elapsed();
    }
    let checkpoint_persist_started = perf_profile.as_ref().map(|_| Instant::now());
    rebuild_state.finalize_commit(meta_fingerprint);
    if persist_finalized_checkpoint {
        persist_lexical_rebuild_state(state_path, rebuild_state)?;
        if let (Some(profile), Some(started)) = (perf_profile.as_mut(), checkpoint_persist_started)
        {
            profile.checkpoint_persist_duration += started.elapsed();
        }
    }
    Ok(())
}

pub(in crate::indexer) fn maintenance_job_kind_for_mode(
    _mode: SearchMaintenanceMode,
) -> SearchMaintenanceJobKind {
    SearchMaintenanceJobKind::LexicalRefresh
}

#[derive(Debug, Default)]
pub(in crate::indexer) struct LexicalRebuildPerfProfile {
    pub(in crate::indexer) total_duration: Duration,
    pub(in crate::indexer) batch_flushes: usize,
    pub(in crate::indexer) commit_count: usize,
    pub(in crate::indexer) heartbeat_persist_count: usize,
    pub(in crate::indexer) batch_conversations: usize,
    pub(in crate::indexer) batch_messages: usize,
    pub(in crate::indexer) batch_message_bytes: usize,
    pub(in crate::indexer) conversation_list_duration: Duration,
    pub(in crate::indexer) message_stream_duration: Duration,
    pub(in crate::indexer) finish_conversation_duration: Duration,
    pub(in crate::indexer) prepare_duration: Duration,
    pub(in crate::indexer) add_duration: Duration,
    pub(in crate::indexer) commit_duration: Duration,
    pub(in crate::indexer) pending_progress_duration: Duration,
    pub(in crate::indexer) heartbeat_progress_duration: Duration,
    pub(in crate::indexer) checkpoint_persist_duration: Duration,
    pub(in crate::indexer) meta_fingerprint_duration: Duration,
}

impl LexicalRebuildPerfProfile {
    pub(in crate::indexer) fn from_env() -> Option<Self> {
        std::env::var_os("CASS_TANTIVY_REBUILD_PROFILE").map(|_| Self::default())
    }

    pub(in crate::indexer) fn millis(duration: Duration) -> f64 {
        duration.as_secs_f64() * 1000.0
    }

    pub(in crate::indexer) fn log_summary(&self) {
        let flushes = self.batch_flushes.max(1) as f64;
        let commits = self.commit_count.max(1) as f64;
        let heartbeat_persists = self.heartbeat_persist_count.max(1) as f64;
        let accounted_duration = self.conversation_list_duration
            + self.message_stream_duration
            + self.prepare_duration
            + self.add_duration
            + self.commit_duration
            + self.pending_progress_duration
            + self.heartbeat_progress_duration
            + self.checkpoint_persist_duration
            + self.meta_fingerprint_duration;
        let residual_duration = self.total_duration.saturating_sub(accounted_duration);
        eprintln!(
            concat!(
                "CASS_REBUILD_PROFILE ",
                "flushes={} commits={} heartbeat_persists={} ",
                "batch_conversations={} batch_messages={} batch_message_bytes={} ",
                "total_ms={:.3} conversation_list_ms={:.3} message_stream_ms={:.3} ",
                "finish_conversation_ms={:.3} residual_ms={:.3} ",
                "prepare_ms={:.3} add_ms={:.3} commit_ms={:.3} ",
                "pending_progress_ms={:.3} heartbeat_progress_ms={:.3} ",
                "checkpoint_persist_ms={:.3} meta_fingerprint_ms={:.3} ",
                "avg_prepare_ms_per_flush={:.3} avg_add_ms_per_flush={:.3} ",
                "avg_commit_ms_per_commit={:.3} avg_pending_progress_ms_per_commit={:.3} ",
                "avg_heartbeat_progress_ms={:.3}"
            ),
            self.batch_flushes,
            self.commit_count,
            self.heartbeat_persist_count,
            self.batch_conversations,
            self.batch_messages,
            self.batch_message_bytes,
            Self::millis(self.total_duration),
            Self::millis(self.conversation_list_duration),
            Self::millis(self.message_stream_duration),
            Self::millis(self.finish_conversation_duration),
            Self::millis(residual_duration),
            Self::millis(self.prepare_duration),
            Self::millis(self.add_duration),
            Self::millis(self.commit_duration),
            Self::millis(self.pending_progress_duration),
            Self::millis(self.heartbeat_progress_duration),
            Self::millis(self.checkpoint_persist_duration),
            Self::millis(self.meta_fingerprint_duration),
            Self::millis(self.prepare_duration) / flushes,
            Self::millis(self.add_duration) / flushes,
            Self::millis(self.commit_duration) / commits,
            Self::millis(self.pending_progress_duration) / commits,
            Self::millis(self.heartbeat_progress_duration) / heartbeat_persists,
        );
    }
}

pub(in crate::indexer) fn log_lexical_rebuild_prep_profile_step(
    rebuild_started: Option<Instant>,
    step_started: Instant,
    step: &str,
) {
    if let Some(rebuild_started) = rebuild_started {
        let step_ms = step_started.elapsed().as_millis() as u64;
        let total_ms = rebuild_started.elapsed().as_millis() as u64;
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
}
