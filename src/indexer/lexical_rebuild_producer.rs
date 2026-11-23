use super::*;

pub(crate) fn rebuild_tantivy_from_db(
    db_path: &Path,
    data_dir: &Path,
    total_conversations: usize,
    progress: Option<Arc<IndexingProgress>>,
) -> Result<LexicalRebuildOutcome> {
    rebuild_tantivy_from_db_with_options(
        db_path,
        data_dir,
        total_conversations,
        progress,
        LexicalRebuildStartupOptions::default(),
        None,
    )
}

pub(in crate::indexer) fn rebuild_tantivy_from_db_with_progress_bump(
    db_path: &Path,
    data_dir: &Path,
    total_conversations: usize,
    progress: Option<Arc<IndexingProgress>>,
    progress_bump: Arc<AtomicI64>,
) -> Result<LexicalRebuildOutcome> {
    rebuild_tantivy_from_db_with_options(
        db_path,
        data_dir,
        total_conversations,
        progress,
        LexicalRebuildStartupOptions::default(),
        Some(progress_bump),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SearchLexicalRepairOutcome {
    pub indexed_docs: usize,
}

pub(crate) fn refresh_completed_lexical_rebuild_checkpoint_from_live_index(
    db_path: &Path,
    data_dir: &Path,
) -> Result<()> {
    let storage = FrankenStorage::open_readonly(db_path).with_context(|| {
        format!(
            "opening database to refresh lexical checkpoint: {}",
            db_path.display()
        )
    })?;
    refresh_completed_lexical_rebuild_checkpoint(&storage, db_path, data_dir)?;
    storage.close_without_checkpoint().with_context(|| {
        format!(
            "closing readonly database after lexical checkpoint refresh: {}",
            db_path.display()
        )
    })
}

pub(crate) fn repair_lexical_index_from_canonical_db_for_search(
    db_path: &Path,
    data_dir: &Path,
    progress: Option<Arc<IndexingProgress>>,
) -> Result<SearchLexicalRepairOutcome> {
    // GH #436: probe canonical DB openability BEFORE acquiring the index-run
    // lock. When the DB is unopenable (e.g. a malformed sqlite image), the
    // old order created `index-run.lock` (+ `.meta` sidecar) on this
    // read-triggered path and then bailed — `IndexRunLockGuard`'s Drop
    // truncates but never unlinks, so a plain `cass search` left a dead
    // zero-byte lock behind for existence-gated tooling to trip on.
    //
    // Deliberately NOT fixed by unlinking in the guard's Drop: every
    // acquisition site opens the path first and then `try_lock_exclusive`s
    // the fd, so an unlink opens the classic lockfile race — a process
    // holding an fd to the old (now unlinked) inode can acquire its lock
    // while another process locks a freshly created file at the same path,
    // yielding two concurrent exclusive "holders". Keeping the file
    // immortal once created is what makes the flock sound; instead we avoid
    // creating it when the repair cannot possibly proceed. The initial probe
    // is strictly read-only; a recognized recovery-required state must acquire
    // and retain both writer leases before using a recovery-capable reader.
    let mut recovery_guard = None;
    let probed_storage = open_readonly_rebuild_storage(db_path, || {
        recovery_guard = Some(acquire_search_maintenance_mutation_lock(
            data_dir,
            db_path,
            SearchMaintenanceJobKind::LexicalRefresh,
        )?);
        Ok(())
    })
    .with_context(|| {
        format!(
            "opening database to repair lexical index for search: {}",
            db_path.display()
        )
    })?;

    let index_run_lock = match recovery_guard {
        Some(guard) => guard,
        None => acquire_search_asset_mutation_lock(
            data_dir,
            db_path,
            SearchMaintenanceJobKind::LexicalRefresh,
        )?,
    };
    // Declare the reader after the combined guard so every error path closes
    // storage before joining the heartbeat and releasing either lease.
    let storage = probed_storage;
    // GH#324: this repair path also stages under the index root; sweep
    // debris from previous crashed runs while we hold the exclusive lock.
    staging_reclaim::reclaim_orphaned_staging_dirs_for_data_dir(data_dir, SystemTime::now()).log();

    let total_conversations = count_total_conversations_exact(&storage)?;
    if total_conversations == 0 {
        let index_path = index_dir(data_dir)?;
        let stage_parent = index_path.parent().unwrap_or(data_dir);
        let stage_root = TempDirBuilder::new()
            .prefix("cass-empty-lexical-repair-")
            .tempdir_in(stage_parent)
            .with_context(|| {
                format!(
                    "creating staging directory for empty lexical repair beside {}",
                    index_path.display()
                )
            })?;
        let staged_index_path = stage_root.path().join("index");
        let empty_index = TantivyIndex::open_or_create(&staged_index_path).with_context(|| {
            format!(
                "creating empty lexical index for empty canonical database: {}",
                staged_index_path.display()
            )
        })?;
        drop(empty_index);
        publish_staged_lexical_index(&staged_index_path, &index_path).with_context(|| {
            format!(
                "publishing empty lexical index repair {} -> {}",
                staged_index_path.display(),
                index_path.display()
            )
        })?;
        refresh_completed_lexical_rebuild_checkpoint(&storage, db_path, data_dir)?;
        storage.close_without_checkpoint().with_context(|| {
            format!(
                "closing readonly database after empty search-triggered lexical repair: {}",
                db_path.display()
            )
        })?;
        return Ok(SearchLexicalRepairOutcome { indexed_docs: 0 });
    }
    storage.close_without_checkpoint().with_context(|| {
        format!(
            "closing readonly database before search-triggered lexical repair: {}",
            db_path.display()
        )
    })?;

    let rebuild = rebuild_tantivy_from_db_deferred_startup_with_progress_bump(
        db_path,
        data_dir,
        total_conversations,
        progress,
        index_run_lock.progress_atomic(),
    )?;
    Ok(SearchLexicalRepairOutcome {
        indexed_docs: rebuild.indexed_docs,
    })
}

#[cfg(test)]
pub(in crate::indexer) fn rebuild_tantivy_from_db_deferred_startup(
    db_path: &Path,
    data_dir: &Path,
    total_conversations: usize,
    progress: Option<Arc<IndexingProgress>>,
) -> Result<LexicalRebuildOutcome> {
    rebuild_tantivy_from_db_deferred_startup_with_options(
        db_path,
        data_dir,
        total_conversations,
        progress,
        None,
    )
}

pub(in crate::indexer) fn rebuild_tantivy_from_db_deferred_startup_with_progress_bump(
    db_path: &Path,
    data_dir: &Path,
    total_conversations: usize,
    progress: Option<Arc<IndexingProgress>>,
    progress_bump: Arc<AtomicI64>,
) -> Result<LexicalRebuildOutcome> {
    rebuild_tantivy_from_db_deferred_startup_with_options(
        db_path,
        data_dir,
        total_conversations,
        progress,
        Some(progress_bump),
    )
}

pub(in crate::indexer) fn rebuild_tantivy_from_db_deferred_startup_with_options(
    db_path: &Path,
    data_dir: &Path,
    total_conversations: usize,
    progress: Option<Arc<IndexingProgress>>,
    progress_bump: Option<Arc<AtomicI64>>,
) -> Result<LexicalRebuildOutcome> {
    rebuild_tantivy_from_db_with_options(
        db_path,
        data_dir,
        total_conversations,
        progress,
        LexicalRebuildStartupOptions {
            defer_initial_content_fingerprint: true,
        },
        progress_bump,
    )
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildPreparedPage {
    pub(in crate::indexer) packets: Vec<LexicalRebuildConversationPacket>,
    pub(in crate::indexer) page_last_conversation_id: i64,
    pub(in crate::indexer) planned_shard_index: Option<usize>,
    pub(in crate::indexer) finishes_planned_shard: bool,
    pub(in crate::indexer) conversation_list_duration: Duration,
    pub(in crate::indexer) message_fetch_duration: Duration,
    pub(in crate::indexer) packet_prepare_duration: Duration,
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildPagePrepWork {
    pub(in crate::indexer) sequence: u64,
    pub(in crate::indexer) conversation_page:
        Vec<crate::storage::sqlite::LexicalRebuildConversationRow>,
    pub(in crate::indexer) page_last_conversation_id: i64,
    pub(in crate::indexer) configured_page_size: i64,
    pub(in crate::indexer) planned_shard_index: Option<usize>,
    pub(in crate::indexer) finishes_planned_shard: bool,
    pub(in crate::indexer) conversation_list_duration: Duration,
    pub(in crate::indexer) pipeline_budget: LexicalRebuildPipelineBudgetSnapshot,
    pub(in crate::indexer) budget_generation: usize,
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildSequencedPreparedPage {
    pub(in crate::indexer) sequence: u64,
    pub(in crate::indexer) page: LexicalRebuildPreparedPage,
}

#[derive(Debug)]
pub(in crate::indexer) enum LexicalRebuildPagePrepResult {
    Prepared(LexicalRebuildSequencedPreparedPage),
    /// An early part of a page still being prepared: one chunk of a long
    /// conversation (bgn6s). Parts of a sequence are forwarded in order, before
    /// its `Prepared` page, and hold no flow reservation.
    Part(LexicalRebuildSequencedPreparedPage),
    Error {
        sequence: u64,
        error: anyhow::Error,
    },
}

#[derive(Debug)]
pub(in crate::indexer) enum LexicalRebuildPipelineMessage {
    Batch(LexicalRebuildPreparedPage),
    Error(anyhow::Error),
    Done,
}

pub(in crate::indexer) fn lexical_rebuild_page_prep_error(
    sequence: u64,
    error: anyhow::Error,
) -> anyhow::Error {
    error.context(format!(
        "lexical rebuild page-prep worker failed at sequence {sequence}"
    ))
}

/// Error chains are Send + Sync; carrying them through the existing channel
/// preserves database/I/O classification without printing private cause text.
pub(in crate::indexer) fn send_lexical_rebuild_pipeline_error(
    sender: &Sender<LexicalRebuildPipelineMessage>,
    error: anyhow::Error,
) {
    let _ = sender.send(LexicalRebuildPipelineMessage::Error(
        error.context("lexical rebuild packet producer failed"),
    ));
}

pub(in crate::indexer) fn lexical_rebuild_prepared_page_reserved_bytes(
    page: &LexicalRebuildPreparedPage,
) -> usize {
    page.packets
        .iter()
        .map(|packet| packet.flow_reservation_bytes)
        .sum::<usize>()
}

pub(in crate::indexer) fn release_lexical_rebuild_prepared_page_reservation(
    page: &LexicalRebuildPreparedPage,
    flow_limiter: &StreamingByteLimiter,
) {
    flow_limiter.release(lexical_rebuild_prepared_page_reserved_bytes(page));
}

pub(in crate::indexer) fn release_completed_lexical_rebuild_pages(
    completed_pages: &mut BTreeMap<u64, LexicalRebuildPreparedPage>,
    flow_limiter: &StreamingByteLimiter,
) {
    for (_, page) in std::mem::take(completed_pages) {
        release_lexical_rebuild_prepared_page_reservation(&page, flow_limiter);
    }
}

pub(in crate::indexer) struct StreamingByteReservation<'a> {
    pub(in crate::indexer) flow_limiter: &'a StreamingByteLimiter,
    pub(in crate::indexer) reserved_bytes: usize,
}

impl<'a> StreamingByteReservation<'a> {
    pub(in crate::indexer) fn new(
        flow_limiter: &'a StreamingByteLimiter,
        reserved_bytes: usize,
    ) -> Self {
        Self {
            flow_limiter,
            reserved_bytes,
        }
    }

    pub(in crate::indexer) fn disarm(&mut self) {
        self.reserved_bytes = 0;
    }

    pub(in crate::indexer) fn release_now(&mut self) {
        if self.reserved_bytes > 0 {
            self.flow_limiter.release(self.reserved_bytes);
            self.disarm();
        }
    }
}

impl Drop for StreamingByteReservation<'_> {
    fn drop(&mut self) {
        self.release_now();
    }
}

/// Test-only panic injection for the worker panic-containment regression
/// tests (#282/#288). A test arms an injection for its own temp directory;
/// only a worker whose database or output path lies inside that directory
/// consumes it. A process-global flag let a rebuild running in any parallel
/// test consume the injection, so the arming test saw no panic and the other
/// test died of it (2l1b0.70).
#[cfg(test)]
pub(in crate::indexer) struct ScopedPanicInjection(std::sync::Mutex<Option<PathBuf>>);

#[cfg(test)]
impl ScopedPanicInjection {
    pub(in crate::indexer) const fn new() -> Self {
        Self(std::sync::Mutex::new(None))
    }

    pub(in crate::indexer) fn arm(&self, scope: &Path) {
        let mut armed = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *armed = Some(scope.to_path_buf());
    }

    pub(in crate::indexer) fn fire_if_armed_for(&self, path: &Path, message: &str) {
        let mut armed = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if armed
            .as_deref()
            .is_some_and(|scope| path.starts_with(scope))
        {
            *armed = None;
            drop(armed);
            panic!("{message}");
        }
    }
}

#[cfg(test)]
pub(in crate::indexer) static LEXICAL_REBUILD_PAGE_PREP_INJECTED_PANIC: ScopedPanicInjection =
    ScopedPanicInjection::new();

#[cfg(test)]
pub(in crate::indexer) fn lexical_rebuild_page_prep_injected_panic_hook(db_path: &Path) {
    LEXICAL_REBUILD_PAGE_PREP_INJECTED_PANIC.fire_if_armed_for(
        db_path,
        "injected lexical rebuild page-prep panic for the #288 regression test",
    );
}

#[cfg(test)]
pub(in crate::indexer) static LEXICAL_REBUILD_SHARD_BUILD_INJECTED_PANIC: ScopedPanicInjection =
    ScopedPanicInjection::new();

#[cfg(test)]
pub(in crate::indexer) fn lexical_rebuild_shard_build_injected_panic_hook(shard_index_path: &Path) {
    LEXICAL_REBUILD_SHARD_BUILD_INJECTED_PANIC.fire_if_armed_for(
        shard_index_path,
        "injected lexical rebuild shard-build panic for the #282 regression test",
    );
}

#[cfg(test)]
pub(in crate::indexer) static LEXICAL_REBUILD_SHARD_MERGE_INJECTED_PANIC: ScopedPanicInjection =
    ScopedPanicInjection::new();

#[cfg(test)]
pub(in crate::indexer) fn lexical_rebuild_shard_merge_injected_panic_hook(output_path: &Path) {
    LEXICAL_REBUILD_SHARD_MERGE_INJECTED_PANIC.fire_if_armed_for(
        output_path,
        "injected lexical rebuild shard-merge panic for the #282 regression test",
    );
}

/// Split a conversation's `(idx, indexed bytes)` footprints (in idx order) into
/// inclusive idx ranges of at most `chunk_bytes` of text each; a single larger
/// message is a range of its own. One range means the conversation fits.
pub(in crate::indexer) fn lexical_rebuild_conversation_chunks(
    footprints: &[(i64, usize)],
    chunk_bytes: usize,
) -> Vec<(i64, i64)> {
    let mut chunks = Vec::new();
    let mut current: Option<(i64, i64, usize)> = None;
    for &(idx, bytes) in footprints {
        match current.as_mut() {
            Some((_, last, used)) if used.saturating_add(bytes) <= chunk_bytes => {
                *last = idx;
                *used = used.saturating_add(bytes);
            }
            _ => {
                if let Some((first, last, _)) = current.take() {
                    chunks.push((first, last));
                }
                current = Some((idx, idx, bytes));
            }
        }
    }
    if let Some((first, last, _)) = current {
        chunks.push((first, last));
    }
    chunks
}

/// An early part of a page: packets handed to the consumer ahead of the page's
/// completion (bgn6s). It holds no flow reservation and never finishes a
/// planned shard.
pub(in crate::indexer) fn lexical_rebuild_page_part(
    packets: Vec<LexicalRebuildConversationPacket>,
    page_last_conversation_id: i64,
    planned_shard_index: Option<usize>,
) -> LexicalRebuildPreparedPage {
    LexicalRebuildPreparedPage {
        packets,
        page_last_conversation_id,
        planned_shard_index,
        finishes_planned_shard: false,
        conversation_list_duration: Duration::ZERO,
        message_fetch_duration: Duration::ZERO,
        packet_prepare_duration: Duration::ZERO,
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn prepare_lexical_rebuild_page_work(
    storage: &mut FrankenStorage,
    source_map: &HashMap<String, (SourceKind, Option<String>)>,
    flow_limiter: &StreamingByteLimiter,
    reservation_order: &LexicalRebuildReservationOrder,
    producer_telemetry: &LexicalRebuildProducerTelemetry,
    lexical_rebuild_worker_pool: Option<&ThreadPool>,
    work: LexicalRebuildPagePrepWork,
    emit_part: &mut dyn FnMut(LexicalRebuildPreparedPage) -> Result<()>,
) -> Result<LexicalRebuildSequencedPreparedPage> {
    let sequence = work.sequence;
    let conversation_ids = work
        .conversation_page
        .iter()
        .filter_map(|conv| conv.id)
        .collect::<Vec<_>>();

    let requested_working_set_bytes = lexical_rebuild_page_working_set_reservation_bytes(
        work.pipeline_budget.batch_fetch_message_bytes_limit,
        work.pipeline_budget.batch_fetch_message_limit,
        conversation_ids.len(),
    );
    let (reserved_bytes, budget_wait_duration, waited_for_budget) =
        acquire_ordered_lexical_rebuild_page_budget(
            reservation_order,
            flow_limiter,
            producer_telemetry,
            sequence,
            requested_working_set_bytes,
        )
        .with_context(|| {
            format!(
                "acquiring lexical rebuild pipeline byte budget for ordered page sequence {}",
                sequence
            )
        })?;
    if waited_for_budget {
        producer_telemetry.record_budget_wait(budget_wait_duration);
    }
    let mut reservation = StreamingByteReservation::new(flow_limiter, reserved_bytes);

    let message_fetch_started = Instant::now();
    let grouped_messages = storage.fetch_messages_for_lexical_rebuild_batch(
        &conversation_ids,
        Some(work.pipeline_budget.batch_fetch_message_limit),
        Some(work.pipeline_budget.batch_fetch_message_bytes_limit),
    );
    let mut message_fetch_duration = message_fetch_started.elapsed();
    let mut packet_prepare_duration = Duration::ZERO;
    let mut prepared_packets = match grouped_messages {
        Ok(grouped_messages) => {
            let prepare_started = Instant::now();
            let prepared = prepare_lexical_rebuild_packet_batch(
                work.conversation_page,
                grouped_messages,
                source_map,
                lexical_rebuild_worker_pool,
            )
            .with_context(|| {
                format!(
                    "preparing lexical rebuild packets for ordered page sequence {}",
                    sequence
                )
            })?;
            packet_prepare_duration =
                packet_prepare_duration.saturating_add(prepare_started.elapsed());
            prepared
        }
        Err(err) if format!("{err:#}").contains("guardrail") => {
            tracing::warn!(
                sequence,
                conversations = conversation_ids.len(),
                max_messages = work.pipeline_budget.batch_fetch_message_limit,
                max_content_bytes = work.pipeline_budget.batch_fetch_message_bytes_limit,
                error = %err,
                "lexical rebuild page exceeded batch-fetch guardrail inside page-prep worker; preparing one conversation at a time"
            );
            let mut prepared_packets = Vec::with_capacity(work.conversation_page.len());
            // A conversation larger than one page's content budget is streamed
            // in message-range chunks instead of held whole (bgn6s).
            let chunk_bytes = work.pipeline_budget.batch_fetch_message_bytes_limit.max(1);
            for conversation in work.conversation_page {
                let conversation_id = conversation.id.ok_or_else(|| {
                    anyhow::anyhow!(
                        "lexical rebuild page sequence {sequence} contains a conversation without an id"
                    )
                })?;
                let fallback_fetch_started = Instant::now();
                let footprints = storage
                    .lexical_rebuild_message_footprints(conversation_id)
                    .with_context(|| {
                        format!(
                            "reading lexical rebuild footprints for conversation {conversation_id}"
                        )
                    })?;
                let chunks = lexical_rebuild_conversation_chunks(&footprints, chunk_bytes);
                if chunks.len() > 1 {
                    message_fetch_duration =
                        message_fetch_duration.saturating_add(fallback_fetch_started.elapsed());
                    // Everything prepared so far precedes this conversation.
                    if !prepared_packets.is_empty() {
                        emit_part(lexical_rebuild_page_part(
                            std::mem::take(&mut prepared_packets),
                            work.page_last_conversation_id,
                            work.planned_shard_index,
                        ))?;
                    }
                    tracing::info!(
                        sequence,
                        conversation_id,
                        messages = footprints.len(),
                        indexed_bytes = footprints.iter().map(|(_, bytes)| *bytes).sum::<usize>(),
                        chunks = chunks.len(),
                        chunk_bytes,
                        "lexical rebuild streaming a long conversation in message-range chunks"
                    );
                    let gate = Arc::new(LexicalRebuildChunkGate::default());
                    for (chunk_index, (first_idx, last_idx)) in chunks.iter().enumerate() {
                        let chunk_fetch_started = Instant::now();
                        let messages = storage
                            .fetch_messages_for_lexical_rebuild_idx_range(
                                conversation_id,
                                *first_idx,
                                *last_idx,
                            )
                            .with_context(|| {
                                format!(
                                    "fetching lexical rebuild messages {first_idx}..={last_idx} for conversation {conversation_id}"
                                )
                            })?;
                        message_fetch_duration =
                            message_fetch_duration.saturating_add(chunk_fetch_started.elapsed());
                        let mut grouped_messages = HashMap::with_capacity(1);
                        if !messages.is_empty() {
                            grouped_messages.insert(conversation_id, messages);
                        }
                        let chunk_prepare_started = Instant::now();
                        let mut chunk_packets = prepare_lexical_rebuild_packet_batch(
                            vec![conversation.clone()],
                            grouped_messages,
                            source_map,
                            lexical_rebuild_worker_pool,
                        )
                        .with_context(|| {
                            format!(
                                "preparing lexical rebuild chunk {chunk_index} of conversation {conversation_id}"
                            )
                        })?;
                        packet_prepare_duration =
                            packet_prepare_duration.saturating_add(chunk_prepare_started.elapsed());
                        let is_final = chunk_index + 1 == chunks.len();
                        for packet in &mut chunk_packets {
                            packet.chunk = Some(LexicalRebuildChunk {
                                is_final,
                                gate: Arc::clone(&gate),
                            });
                        }
                        if is_final {
                            // The last chunk rides the completed page and
                            // finishes the conversation there.
                            prepared_packets.extend(chunk_packets);
                        } else {
                            // One chunk in flight: the previous must be flushed
                            // into the index before this one is handed over.
                            gate.wait_until_flushed(reservation_order, flow_limiter)?;
                            gate.sent();
                            emit_part(lexical_rebuild_page_part(
                                chunk_packets,
                                work.page_last_conversation_id,
                                work.planned_shard_index,
                            ))?;
                        }
                    }
                    continue;
                }
                let messages = storage
                    .fetch_messages_for_lexical_rebuild(conversation_id)
                    .with_context(|| {
                        format!(
                            "fetching lexical rebuild messages for conversation {}",
                            conversation_id
                        )
                    })?;
                message_fetch_duration =
                    message_fetch_duration.saturating_add(fallback_fetch_started.elapsed());
                let mut grouped_messages = HashMap::with_capacity(1);
                if !messages.is_empty() {
                    grouped_messages.insert(conversation_id, messages);
                }
                let fallback_prepare_started = Instant::now();
                let conversation_packets = prepare_lexical_rebuild_packet_batch(
                    vec![conversation],
                    grouped_messages,
                    source_map,
                    lexical_rebuild_worker_pool,
                )
                .with_context(|| {
                    format!(
                        "preparing lexical rebuild packet for conversation {conversation_id} after page guardrail overflow"
                    )
                })?;
                packet_prepare_duration =
                    packet_prepare_duration.saturating_add(fallback_prepare_started.elapsed());
                prepared_packets.extend(conversation_packets);
            }
            prepared_packets
        }
        Err(err) => {
            return Err(err).context(format!(
                "fetching lexical rebuild messages for {} conversations",
                conversation_ids.len()
            ));
        }
    };

    let page_message_bytes = prepared_packets
        .iter()
        .map(|packet| packet.message_bytes)
        .sum::<usize>();
    let page_message_count = prepared_packets
        .iter()
        .map(|packet| packet.message_count)
        .sum::<usize>();
    if prepared_packets.is_empty() {
        reservation.release_now();
    } else {
        assign_lexical_rebuild_flow_reservation_bytes(&mut prepared_packets, reserved_bytes);
        reservation.disarm();
    }
    let configured_page_size = usize::try_from(work.configured_page_size.max(1))
        .unwrap_or(usize::MAX)
        .max(1);
    let content_bounded_page_conversation_limit =
        lexical_rebuild_content_bounded_page_conversation_limit(
            work.pipeline_budget.page_conversation_limit,
            work.pipeline_budget.batch_fetch_message_bytes_limit,
        );
    let budget_shrink_decision =
        if content_bounded_page_conversation_limit < work.pipeline_budget.page_conversation_limit {
            "bounded_by_content_reservation"
        } else if work.pipeline_budget.page_conversation_limit < configured_page_size {
            "bounded_below_configured_page"
        } else {
            "using_configured_page"
        };
    tracing::info!(
        sequence,
        configured_page_size = work.configured_page_size,
        page_conversation_limit = work.pipeline_budget.page_conversation_limit,
        content_bounded_page_conversation_limit,
        budget_generation = work.budget_generation,
        budget_shrink_decision,
        page_conversations = prepared_packets.len(),
        page_last_conversation_id = work.page_last_conversation_id,
        page_messages = page_message_count,
        page_message_bytes,
        reserved_bytes,
        requested_working_set_bytes,
        budget_wait_ms = budget_wait_duration.as_millis() as u64,
        waited_for_budget,
        batch_fetch_message_limit = work.pipeline_budget.batch_fetch_message_limit,
        batch_fetch_message_bytes_limit = work.pipeline_budget.batch_fetch_message_bytes_limit,
        max_message_bytes_in_flight = work.pipeline_budget.max_message_bytes_in_flight,
        conversation_list_ms = work.conversation_list_duration.as_millis() as u64,
        message_fetch_ms = message_fetch_duration.as_millis() as u64,
        packet_prepare_ms = packet_prepare_duration.as_millis() as u64,
        "lexical rebuild prepared bounded page"
    );

    Ok(LexicalRebuildSequencedPreparedPage {
        sequence,
        page: LexicalRebuildPreparedPage {
            packets: prepared_packets,
            page_last_conversation_id: work.page_last_conversation_id,
            planned_shard_index: work.planned_shard_index,
            finishes_planned_shard: work.finishes_planned_shard,
            conversation_list_duration: work.conversation_list_duration,
            message_fetch_duration,
            packet_prepare_duration,
        },
    })
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn spawn_lexical_rebuild_page_prep_workers(
    worker_count: usize,
    db_path: PathBuf,
    source_map: Arc<HashMap<String, (SourceKind, Option<String>)>>,
    work_rx: Receiver<LexicalRebuildPagePrepWork>,
    result_tx: Sender<LexicalRebuildPagePrepResult>,
    flow_limiter: Arc<StreamingByteLimiter>,
    reservation_order: Arc<LexicalRebuildReservationOrder>,
    producer_telemetry: Arc<LexicalRebuildProducerTelemetry>,
    lexical_rebuild_worker_pool: Option<Arc<ThreadPool>>,
) -> Result<Vec<JoinHandle<()>>> {
    let tracing_dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());
    (0..worker_count.max(1))
        .map(|worker_idx| {
            let worker_db_path = db_path.clone();
            let worker_source_map = Arc::clone(&source_map);
            let worker_rx = work_rx.clone();
            let worker_result_tx = result_tx.clone();
            let worker_flow_limiter = Arc::clone(&flow_limiter);
            let worker_reservation_order = Arc::clone(&reservation_order);
            let worker_producer_telemetry = Arc::clone(&producer_telemetry);
            let worker_pool = lexical_rebuild_worker_pool.clone();
            let worker_dispatch = tracing_dispatch.clone();
            thread::Builder::new()
                .name(format!("cass-lexical-page-prep-{worker_idx}"))
                .spawn(move || {
                    tracing::dispatcher::with_default(&worker_dispatch, || {
                        // Page-prep workers are pure readers.  The ordinary
                        // `open_readonly` lane may perform two sanctioned
                        // recovery writes (duplicate-FTS repair and a dirty-WAL
                        // checkpoint), which is unsafe to multiply across this
                        // worker pool while the rebuild's canonical writer is
                        // active.  Require an already-readable archive here and
                        // leave any repair to the single mutating owner.
                        let mut storage =
                            match FrankenStorage::open_strict_readonly(&worker_db_path) {
                            Ok(storage) => storage,
                            Err(err) => {
                                let _ = worker_result_tx.send(LexicalRebuildPagePrepResult::Error {
                                    sequence: 0,
                                    error: err.context(format!(
                                        "opening readonly storage for lexical rebuild page-prep worker {worker_idx}"
                                    )),
                                });
                                return;
                            }
                        };

                        while let Ok(work) = worker_rx.recv() {
                            let sequence = work.sequence;
                            // #288: a panicking page-prep worker used to unwind
                            // silently — the surviving workers' `result_tx`
                            // clones kept the channel open, the producer's
                            // `active_work` count never drained, and the
                            // producer parked forever at `result_rx.recv()`
                            // (the reporter's all-parked dump: queue_depth=0,
                            // active_page_prep_jobs=0, inflight=0). Convert the
                            // panic into the same teardown as the `Err` arm so
                            // the producer observes a result and aborts loudly.
                            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                                || {
                                    #[cfg(test)]
                                    lexical_rebuild_page_prep_injected_panic_hook(&worker_db_path);
                                    prepare_lexical_rebuild_page_work(
                                        &mut storage,
                                        worker_source_map.as_ref(),
                                        worker_flow_limiter.as_ref(),
                                        worker_reservation_order.as_ref(),
                                        worker_producer_telemetry.as_ref(),
                                        worker_pool.as_deref(),
                                        work,
                                        &mut |page| {
                                            worker_result_tx
                                                .send(LexicalRebuildPagePrepResult::Part(
                                                    LexicalRebuildSequencedPreparedPage {
                                                        sequence,
                                                        page,
                                                    },
                                                ))
                                                .map_err(|_| {
                                                    anyhow::anyhow!(
                                                        "lexical rebuild producer disconnected while a long conversation was streaming"
                                                    )
                                                })
                                        },
                                    )
                                },
                            ));
                            match outcome {
                                Ok(Ok(prepared)) => {
                                    let reserved_bytes =
                                        lexical_rebuild_prepared_page_reserved_bytes(&prepared.page);
                                    if worker_result_tx
                                        .send(LexicalRebuildPagePrepResult::Prepared(prepared))
                                        .is_err()
                                    {
                                        worker_reservation_order.close();
                                        worker_flow_limiter.release(reserved_bytes);
                                        break;
                                    }
                                }
                                Ok(Err(err)) => {
                                    worker_reservation_order.close();
                                    let _ = worker_result_tx.send(LexicalRebuildPagePrepResult::Error {
                                        sequence,
                                        error: err.context(format!(
                                            "lexical rebuild page-prep worker {worker_idx} could not prepare page sequence {sequence}"
                                        )),
                                    });
                                    break;
                                }
                                Err(payload) => {
                                    worker_reservation_order.close();
                                    let _ = worker_result_tx.send(LexicalRebuildPagePrepResult::Error {
                                        sequence,
                                        error: anyhow::anyhow!(panic_payload_message(payload))
                                            .context(format!(
                                                "lexical rebuild page-prep worker {worker_idx} panicked while preparing page sequence {sequence}"
                                            )),
                                    });
                                    break;
                                }
                            }
                        }

                        if let Err(err) = storage.close_without_checkpoint() {
                            tracing::warn!(
                                worker_idx,
                                db = %worker_db_path.display(),
                                error = %err,
                                "lexical rebuild page-prep worker failed to close strict readonly storage cleanly"
                            );
                        }
                    });
                })
                .with_context(|| {
                    format!("spawning lexical rebuild page-prep worker {worker_idx}")
                })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn spawn_lexical_rebuild_packet_producer(
    db_path: PathBuf,
    resume_after_conversation_id: Option<i64>,
    planned_shard_plan: Option<LexicalShardPlan>,
    page_size: i64,
    pipeline_channel_size: usize,
    first_budget_promotion_wait: Option<LexicalRebuildFirstBudgetPromotionWait>,
    pipeline_budget_controller: Arc<LexicalRebuildPipelineBudgetController>,
    tx: Sender<LexicalRebuildPipelineMessage>,
    flow_limiter: Arc<StreamingByteLimiter>,
    lexical_rebuild_worker_pool: Option<Arc<ThreadPool>>,
    producer_telemetry: Arc<LexicalRebuildProducerTelemetry>,
) -> JoinHandle<()> {
    let tracing_dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());
    thread::spawn(move || {
        tracing::dispatcher::with_default(&tracing_dispatch, || {
            let prep_profile = std::env::var_os("CASS_PREP_PROFILE").is_some();
            let prep_started = Instant::now();
            let mut prep_step_started = Instant::now();
            let log_prep_step = |step: &str, step_started: &mut Instant| {
                let step_ms = step_started.elapsed().as_millis() as u64;
                let total_ms = prep_started.elapsed().as_millis() as u64;
                if prep_profile {
                    eprintln!(
                        "CASS_PREP_PROFILE component=producer step={step} step_ms={} total_ms={}",
                        step_ms, total_ms
                    );
                    tracing::info!(
                        component = "producer",
                        step,
                        step_ms,
                        total_ms,
                        "lexical rebuild prep profile"
                    );
                }
                *step_started = Instant::now();
            };
            let send_error = |error: anyhow::Error| {
                send_lexical_rebuild_pipeline_error(&tx, error);
            };

            // Preparation owns any recovery writes. Worker readers cannot
            // acquire an independent recovery writer during a readonly build.
            let mut storage = match FrankenStorage::open_strict_readonly(&db_path) {
                Ok(storage) => storage,
                Err(err) => {
                    send_error(err.context(format!(
                        "opening readonly storage for lexical rebuild packet producer: {}",
                        db_path.display()
                    )));
                    return;
                }
            };
            log_prep_step("open_readonly", &mut prep_step_started);

            let source_map = storage
                .list_sources()
                .unwrap_or_default()
                .into_iter()
                .map(|source| (source.id, (source.kind, source.host_label)))
                .collect::<HashMap<_, _>>();
            log_prep_step("load_sources", &mut prep_step_started);

            // Pre-fetch agent/workspace lookup maps to avoid 3-table JOINs in the
            // rebuild query path — frankensqlite materialises the full Cartesian
            // product for multi-table JOINs, causing 200x+ regressions.
            let (agent_slugs, workspace_paths) = match storage.build_lexical_rebuild_lookups() {
                Ok(lookups) => lookups,
                Err(err) => {
                    send_error(err.context(format!(
                        "building lexical rebuild lookup tables inside packet producer: {}",
                        db_path.display()
                    )));
                    return;
                }
            };
            log_prep_step("build_lookups", &mut prep_step_started);

            let mut last_conversation_id = resume_after_conversation_id.unwrap_or(0);
            let planned_shards = if let Some(shard_plan) = planned_shard_plan {
                let largest_shard_conversations = shard_plan
                    .shards
                    .iter()
                    .map(|shard| shard.conversation_count)
                    .max()
                    .unwrap_or(0);
                let largest_shard_messages = shard_plan
                    .shards
                    .iter()
                    .map(|shard| shard.message_count)
                    .max()
                    .unwrap_or(0);
                let largest_shard_message_bytes = shard_plan
                    .shards
                    .iter()
                    .map(|shard| shard.message_bytes)
                    .max()
                    .unwrap_or(0);
                tracing::info!(
                    plan_id = %shard_plan.plan_id,
                    shard_count = shard_plan.shards.len(),
                    oversized_conversations = shard_plan.oversized_conversation_ids.len(),
                    max_conversations_per_shard = shard_plan.budgets.max_conversations_per_shard,
                    max_messages_per_shard = shard_plan.budgets.max_messages_per_shard,
                    max_message_bytes_per_shard = shard_plan.budgets.max_message_bytes_per_shard,
                    largest_shard_conversations,
                    largest_shard_messages,
                    largest_shard_message_bytes,
                    total_conversations = shard_plan.total_conversations,
                    total_messages = shard_plan.total_messages,
                    total_message_bytes = shard_plan.total_message_bytes,
                    "reused deterministic lexical rebuild shard plan inside packet producer"
                );
                Some(shard_plan.shards)
            } else {
                None
            };
            let mut planned_shard_cursor = planned_shards.and_then(|planned_shards| {
                LexicalRebuildPlannedShardCursor::for_resume(
                    planned_shards,
                    resume_after_conversation_id,
                )
            });
            log_prep_step("load_resume_cursor", &mut prep_step_started);

            let page_prep_worker_count = lexical_rebuild_page_prep_worker_parallelism();
            let reservation_order = Arc::new(LexicalRebuildReservationOrder::new());
            let (work_tx, work_rx) = bounded::<LexicalRebuildPagePrepWork>(page_prep_worker_count);
            let (result_tx, result_rx) =
                bounded::<LexicalRebuildPagePrepResult>(page_prep_worker_count);
            let source_map = Arc::new(source_map);
            let worker_handles = match spawn_lexical_rebuild_page_prep_workers(
                page_prep_worker_count,
                db_path.clone(),
                Arc::clone(&source_map),
                work_rx,
                result_tx,
                Arc::clone(&flow_limiter),
                Arc::clone(&reservation_order),
                Arc::clone(&producer_telemetry),
                lexical_rebuild_worker_pool.clone(),
            ) {
                Ok(handles) => handles,
                Err(err) => {
                    send_error(err);
                    producer_telemetry.record(page_prep_worker_count, 0, 0);
                    storage.close_best_effort_in_place();
                    return;
                }
            };
            producer_telemetry.record(page_prep_worker_count, 0, 0);
            tracing::info!(
                page_prep_workers = page_prep_worker_count,
                work_queue_capacity = page_prep_worker_count,
                result_queue_capacity = page_prep_worker_count,
                pipeline_channel_size,
                "lexical rebuild producer started ordered page-prep workers"
            );
            log_prep_step("start_page_prep_workers", &mut prep_step_started);

            let producer_result: Result<()> = (|| {
                let mut logged_first_batch_handoff = false;
                let mut logged_first_page_enqueued = false;
                let mut last_logged_budget = None;
                let mut next_sequence_to_enqueue = 0u64;
                let mut next_sequence_to_emit = 0u64;
                let mut active_work = 0usize;
                let mut enumeration_done = false;
                let mut first_budget_promotion_observed = first_budget_promotion_wait.is_none();
                let mut handoff_conversations_since_budget = 0usize;
                let mut handoff_messages_since_budget = 0usize;
                let mut handoff_message_bytes_since_budget = 0usize;
                let mut completed_pages = BTreeMap::<u64, LexicalRebuildPreparedPage>::new();
                // Early parts (long-conversation chunks) per sequence, forwarded
                // in order ahead of that sequence's completed page (bgn6s).
                let mut pending_parts =
                    BTreeMap::<u64, VecDeque<LexicalRebuildPreparedPage>>::new();
                let mut logged_current_shard_index = None;

                loop {
                    while !enumeration_done && active_work < page_prep_worker_count {
                        if let Some(shard_cursor) = planned_shard_cursor.as_mut() {
                            shard_cursor.skip_completed(last_conversation_id);
                        }
                        if let Some(shard) = planned_shard_cursor
                            .as_ref()
                            .and_then(LexicalRebuildPlannedShardCursor::current)
                        {
                            if logged_current_shard_index != Some(shard.shard_index) {
                                tracing::info!(
                                    shard_index = shard.shard_index,
                                    first_conversation_id = shard.first_conversation_id,
                                    last_conversation_id = shard.last_conversation_id,
                                    conversation_count = shard.conversation_count,
                                    message_count = shard.message_count,
                                    message_bytes = shard.message_bytes,
                                    conversation_id_fingerprint =
                                        %shard.conversation_id_fingerprint,
                                    oversized_single_conversation =
                                        shard.oversized_single_conversation,
                                    "lexical rebuild producer opened planned shard boundary"
                                );
                                logged_current_shard_index = Some(shard.shard_index);
                            }
                        } else if planned_shard_cursor.is_some() {
                            enumeration_done = true;
                            break;
                        }

                        let pipeline_budget = pipeline_budget_controller.snapshot();
                        let max_active_page_work = page_prep_worker_count
                            .min(pipeline_budget.page_conversation_limit.max(1));
                        if active_work >= max_active_page_work {
                            break;
                        }
                        if last_logged_budget != Some(pipeline_budget) {
                            tracing::info!(
                                page_conversation_limit = pipeline_budget.page_conversation_limit,
                                batch_fetch_message_limit =
                                    pipeline_budget.batch_fetch_message_limit,
                                batch_fetch_message_bytes_limit =
                                    pipeline_budget.batch_fetch_message_bytes_limit,
                                max_message_bytes_in_flight =
                                    pipeline_budget.max_message_bytes_in_flight,
                                commit_interval_conversations =
                                    pipeline_budget.commit_interval_conversations,
                                commit_interval_messages = pipeline_budget.commit_interval_messages,
                                commit_interval_message_bytes =
                                    pipeline_budget.commit_interval_message_bytes,
                                "lexical rebuild producer adopted pipeline budgets"
                            );
                            last_logged_budget = Some(pipeline_budget);
                        }
                        let content_bounded_page_conversation_limit =
                            lexical_rebuild_content_bounded_page_conversation_limit(
                                pipeline_budget.page_conversation_limit,
                                pipeline_budget.batch_fetch_message_bytes_limit,
                            );
                        let conversation_page_limit =
                            i64::try_from(content_bounded_page_conversation_limit)
                                .unwrap_or(i64::MAX)
                                .min(page_size.max(1));
                        let current_planned_shard = planned_shard_cursor
                            .as_ref()
                            .and_then(LexicalRebuildPlannedShardCursor::current)
                            .cloned();
                        let conversation_list_started = Instant::now();
                        producer_telemetry
                            .set_producer_state(LexicalRebuildProducerParkSite::ListingDb);
                        let conversation_page = if let Some(shard) = &current_planned_shard {
                            storage
                                .list_conversations_for_lexical_rebuild_after_id_through_id(
                                    conversation_page_limit,
                                    last_conversation_id,
                                    shard.last_conversation_id,
                                    &agent_slugs,
                                    &workspace_paths,
                                )
                                .with_context(|| {
                                    format!(
                                        "listing lexical rebuild conversations after id {} through planned shard {} upper bound {}",
                                        last_conversation_id,
                                        shard.shard_index,
                                        shard.last_conversation_id
                                    )
                                })?
                        } else {
                            storage
                                .list_conversations_for_lexical_rebuild_after_id(
                                    conversation_page_limit,
                                    last_conversation_id,
                                    &agent_slugs,
                                    &workspace_paths,
                                )
                                .with_context(|| {
                                    format!(
                                        "listing lexical rebuild conversations after id {}",
                                        last_conversation_id
                                    )
                                })?
                        };
                        producer_telemetry.set_producer_state(LexicalRebuildProducerParkSite::Idle);
                        let conversation_list_duration = conversation_list_started.elapsed();

                        if conversation_page.is_empty() {
                            if let Some(shard_cursor) = planned_shard_cursor.as_mut() {
                                shard_cursor.advance();
                                continue;
                            }
                            enumeration_done = true;
                            break;
                        }

                        let page_last_conversation_id = conversation_page
                            .last()
                            .and_then(|conv| conv.id)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "lexical rebuild page missing terminal conversation id after {}",
                                    last_conversation_id
                                )
                            })?;
                        let page_conversation_count =
                            conversation_page.iter().filter_map(|conv| conv.id).count();
                        let finishes_planned_shard =
                            current_planned_shard.as_ref().is_some_and(|shard| {
                                page_last_conversation_id >= shard.last_conversation_id
                            });

                        let work = LexicalRebuildPagePrepWork {
                            sequence: next_sequence_to_enqueue,
                            conversation_page,
                            page_last_conversation_id,
                            configured_page_size: page_size,
                            planned_shard_index: current_planned_shard
                                .as_ref()
                                .map(|shard| shard.shard_index),
                            finishes_planned_shard,
                            conversation_list_duration,
                            pipeline_budget,
                            budget_generation: pipeline_budget_controller.generation(),
                        };
                        work_tx.send(work).map_err(|_| {
                            anyhow::anyhow!(
                                "lexical rebuild page-prep work queue closed before producer finished"
                            )
                        })?;
                        if !logged_first_page_enqueued {
                            log_prep_step("first_page_enqueued", &mut prep_step_started);
                            logged_first_page_enqueued = true;
                        }
                        tracing::debug!(
                            sequence = next_sequence_to_enqueue,
                            active_work = active_work + 1,
                            shard_index = current_planned_shard
                                .as_ref()
                                .map(|shard| shard.shard_index),
                            page_last_conversation_id,
                            page_conversations = page_conversation_count,
                            "lexical rebuild producer enqueued page-prep work"
                        );

                        next_sequence_to_enqueue = next_sequence_to_enqueue.saturating_add(1);
                        active_work = active_work.saturating_add(1);
                        producer_telemetry.record(
                            page_prep_worker_count,
                            active_work,
                            completed_pages.len(),
                        );
                        last_conversation_id = page_last_conversation_id;
                        if finishes_planned_shard {
                            tracing::info!(
                                shard_index = current_planned_shard
                                    .as_ref()
                                    .map(|shard| shard.shard_index),
                                shard_last_conversation_id = current_planned_shard
                                    .as_ref()
                                    .map(|shard| shard.last_conversation_id),
                                "lexical rebuild producer completed planned shard boundary"
                            );
                            if let Some(shard_cursor) = planned_shard_cursor.as_mut() {
                                shard_cursor.advance();
                            }
                        }
                    }

                    loop {
                        // Parts of the head sequence (chunks of a long
                        // conversation) go out as soon as they arrive: the
                        // worker streaming it waits for each to be flushed.
                        if let Some(parts) = pending_parts.get_mut(&next_sequence_to_emit) {
                            while let Some(part) = parts.pop_front() {
                                if tx.send(LexicalRebuildPipelineMessage::Batch(part)).is_err() {
                                    reservation_order.close();
                                    release_completed_lexical_rebuild_pages(
                                        &mut completed_pages,
                                        flow_limiter.as_ref(),
                                    );
                                    return Err(anyhow::anyhow!(
                                        "lexical rebuild consumer disconnected before an ordered long-conversation chunk handoff"
                                    ));
                                }
                            }
                        }
                        let Some(prepared_page) = completed_pages.remove(&next_sequence_to_emit)
                        else {
                            break;
                        };
                        pending_parts.remove(&next_sequence_to_emit);
                        producer_telemetry.record(
                            page_prep_worker_count,
                            active_work,
                            completed_pages.len(),
                        );
                        let reserved_bytes =
                            lexical_rebuild_prepared_page_reserved_bytes(&prepared_page);
                        let page_handoff_conversations = prepared_page.packets.len();
                        let page_handoff_messages = prepared_page
                            .packets
                            .iter()
                            .map(|packet| packet.message_count)
                            .sum::<usize>();
                        let page_handoff_message_bytes = prepared_page
                            .packets
                            .iter()
                            .map(|packet| packet.message_bytes)
                            .sum::<usize>();
                        let page_planned_shard_index = prepared_page.planned_shard_index;
                        let page_finishes_planned_shard = prepared_page.finishes_planned_shard;
                        match tx.try_send(LexicalRebuildPipelineMessage::Batch(prepared_page)) {
                            Ok(()) => {}
                            Err(TrySendError::Full(message)) => {
                                let handoff_wait_started = Instant::now();
                                producer_telemetry.set_producer_state(
                                    LexicalRebuildProducerParkSite::HandoffSend,
                                );
                                let handoff_send = tx.send(message);
                                producer_telemetry
                                    .set_producer_state(LexicalRebuildProducerParkSite::Idle);
                                if handoff_send.is_err() {
                                    reservation_order.close();
                                    flow_limiter.release(reserved_bytes);
                                    release_completed_lexical_rebuild_pages(
                                        &mut completed_pages,
                                        flow_limiter.as_ref(),
                                    );
                                    return Err(anyhow::anyhow!(
                                        "lexical rebuild consumer disconnected before ordered page handoff"
                                    ));
                                }
                                producer_telemetry
                                    .record_handoff_wait(handoff_wait_started.elapsed());
                            }
                            Err(TrySendError::Disconnected(_message)) => {
                                reservation_order.close();
                                flow_limiter.release(reserved_bytes);
                                release_completed_lexical_rebuild_pages(
                                    &mut completed_pages,
                                    flow_limiter.as_ref(),
                                );
                                return Err(anyhow::anyhow!(
                                    "lexical rebuild consumer disconnected before ordered page handoff"
                                ));
                            }
                        }
                        handoff_conversations_since_budget = handoff_conversations_since_budget
                            .saturating_add(page_handoff_conversations);
                        handoff_messages_since_budget =
                            handoff_messages_since_budget.saturating_add(page_handoff_messages);
                        handoff_message_bytes_since_budget = handoff_message_bytes_since_budget
                            .saturating_add(page_handoff_message_bytes);
                        if !logged_first_batch_handoff {
                            log_prep_step("first_batch_handoff", &mut prep_step_started);
                            let total_startup_ms = prep_started.elapsed().as_millis() as u64;
                            tracing::info!(
                                component = "producer",
                                total_startup_ms,
                                conversations_in_first_batch = page_handoff_conversations,
                                "lexical rebuild producer delivered first batch — serial startup complete"
                            );
                            logged_first_batch_handoff = true;
                        }
                        tracing::debug!(
                            sequence = next_sequence_to_emit,
                            active_work,
                            ordered_buffered_pages = completed_pages.len(),
                            planned_shard_index = page_planned_shard_index,
                            finishes_planned_shard = page_finishes_planned_shard,
                            "lexical rebuild producer handed off ordered prepared page"
                        );
                        next_sequence_to_emit = next_sequence_to_emit.saturating_add(1);

                        if !first_budget_promotion_observed
                            && let Some(first_budget_wait) = first_budget_promotion_wait
                        {
                            let observed_generation = pipeline_budget_controller.generation();
                            if observed_generation > first_budget_wait.generation {
                                first_budget_promotion_observed = true;
                                handoff_conversations_since_budget = 0;
                                handoff_messages_since_budget = 0;
                                handoff_message_bytes_since_budget = 0;
                                tracing::info!(
                                    initial_generation = first_budget_wait.generation,
                                    observed_generation,
                                    "lexical rebuild producer observed first budget promotion before waiting"
                                );
                            } else if should_commit_lexical_rebuild(
                                handoff_conversations_since_budget,
                                handoff_messages_since_budget,
                                handoff_message_bytes_since_budget,
                                first_budget_wait.commit_interval_conversations,
                                first_budget_wait.commit_interval_messages,
                                first_budget_wait.commit_interval_message_bytes,
                            ) {
                                tracing::info!(
                                    observed_generation,
                                    handoff_conversations_since_budget,
                                    handoff_messages_since_budget,
                                    handoff_message_bytes_since_budget,
                                    "lexical rebuild producer waiting for first durable budget promotion"
                                );
                                if pipeline_budget_controller.wait_for_update_after(
                                    observed_generation,
                                    lexical_rebuild_first_budget_promotion_wait(),
                                ) {
                                    first_budget_promotion_observed = true;
                                    handoff_conversations_since_budget = 0;
                                    handoff_messages_since_budget = 0;
                                    handoff_message_bytes_since_budget = 0;
                                    tracing::info!(
                                        new_generation = pipeline_budget_controller.generation(),
                                        "lexical rebuild producer observed first durable budget promotion"
                                    );
                                } else {
                                    first_budget_promotion_observed = true;
                                    tracing::warn!(
                                        observed_generation,
                                        wait_ms = lexical_rebuild_first_budget_promotion_wait()
                                            .as_millis()
                                            as u64,
                                        "lexical rebuild producer timed out waiting for first durable budget promotion; continuing with current budgets"
                                    );
                                }
                            }
                        }
                    }

                    if enumeration_done && active_work == 0 {
                        break;
                    }

                    producer_telemetry
                        .set_producer_state(LexicalRebuildProducerParkSite::WaitingResult);
                    let page_prep_result = result_rx.recv();
                    producer_telemetry.set_producer_state(LexicalRebuildProducerParkSite::Idle);
                    match page_prep_result {
                        Ok(LexicalRebuildPagePrepResult::Prepared(prepared)) => {
                            active_work = active_work.saturating_sub(1);
                            producer_telemetry.record(
                                page_prep_worker_count,
                                active_work,
                                completed_pages.len(),
                            );
                            if prepared.sequence < next_sequence_to_emit {
                                release_lexical_rebuild_prepared_page_reservation(
                                    &prepared.page,
                                    flow_limiter.as_ref(),
                                );
                                reservation_order.close();
                                release_completed_lexical_rebuild_pages(
                                    &mut completed_pages,
                                    flow_limiter.as_ref(),
                                );
                                return Err(anyhow::anyhow!(
                                    "lexical rebuild page-prep worker returned stale sequence {} after ordered barrier {}",
                                    prepared.sequence,
                                    next_sequence_to_emit
                                ));
                            }
                            if let Some(previous) =
                                completed_pages.insert(prepared.sequence, prepared.page)
                            {
                                release_lexical_rebuild_prepared_page_reservation(
                                    &previous,
                                    flow_limiter.as_ref(),
                                );
                                reservation_order.close();
                                release_completed_lexical_rebuild_pages(
                                    &mut completed_pages,
                                    flow_limiter.as_ref(),
                                );
                                return Err(anyhow::anyhow!(
                                    "lexical rebuild page-prep worker returned duplicate sequence {}",
                                    prepared.sequence
                                ));
                            }
                            producer_telemetry.record(
                                page_prep_worker_count,
                                active_work,
                                completed_pages.len(),
                            );
                            tracing::debug!(
                                sequence = prepared.sequence,
                                active_work,
                                ordered_buffered_pages = completed_pages.len(),
                                "lexical rebuild producer received prepared page from worker"
                            );
                        }
                        Ok(LexicalRebuildPagePrepResult::Part(part)) => {
                            // A part never finishes its page, so active work
                            // and the ordered barrier are unchanged.
                            if part.sequence < next_sequence_to_emit
                                || completed_pages.contains_key(&part.sequence)
                            {
                                reservation_order.close();
                                release_completed_lexical_rebuild_pages(
                                    &mut completed_pages,
                                    flow_limiter.as_ref(),
                                );
                                return Err(anyhow::anyhow!(
                                    "lexical rebuild page-prep worker returned a part for sequence {} after that page completed (ordered barrier {})",
                                    part.sequence,
                                    next_sequence_to_emit
                                ));
                            }
                            pending_parts
                                .entry(part.sequence)
                                .or_default()
                                .push_back(part.page);
                        }
                        Ok(LexicalRebuildPagePrepResult::Error { sequence, error }) => {
                            reservation_order.close();
                            release_completed_lexical_rebuild_pages(
                                &mut completed_pages,
                                flow_limiter.as_ref(),
                            );
                            return Err(lexical_rebuild_page_prep_error(sequence, error));
                        }
                        Err(_) => {
                            reservation_order.close();
                            release_completed_lexical_rebuild_pages(
                                &mut completed_pages,
                                flow_limiter.as_ref(),
                            );
                            return Err(anyhow::anyhow!(
                                "lexical rebuild page-prep result queue closed before producer completion"
                            ));
                        }
                    }
                }

                Ok(())
            })();

            if producer_result.is_err() {
                reservation_order.close();
            }
            drop(work_tx);
            drop(result_rx);
            for worker_handle in worker_handles {
                if let Err(payload) = worker_handle.join() {
                    let panic_message = panic_payload_message(payload);
                    tracing::warn!(
                        error = %panic_message,
                        "lexical rebuild page-prep worker panicked while producer was shutting down"
                    );
                    if producer_result.is_ok() {
                        send_error(anyhow::anyhow!(
                            "lexical rebuild page-prep worker panicked: {}",
                            panic_message
                        ));
                        producer_telemetry.record(page_prep_worker_count, 0, 0);
                        storage.close_best_effort_in_place();
                        return;
                    }
                }
            }

            producer_telemetry.record(page_prep_worker_count, 0, 0);
            storage.close_best_effort_in_place();
            match producer_result {
                Ok(()) => {
                    let _ = tx.send(LexicalRebuildPipelineMessage::Done);
                }
                Err(err) => send_error(err),
            }
        })
    })
}
