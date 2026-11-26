use super::*;

#[cfg(test)]
pub(in crate::indexer) fn ingest_batch(
    storage: &FrankenStorage,
    t_index: Option<&mut TantivyIndex>,
    data_dir: &Path,
    convs: &[NormalizedConversation],
    progress: &Option<Arc<IndexingProgress>>,
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
) -> Result<CanonicalMutationCounts> {
    let outcome = ingest_batch_detailed(
        storage,
        t_index,
        data_dir,
        convs,
        progress,
        lexical_strategy,
        defer_checkpoints,
        None,
    )?;
    if outcome.lexical_update_deferred {
        anyhow::bail!(
            "incremental lexical update ran out of memory after SQLite ingest; rerun with CASS_DEFER_LEXICAL_UPDATES=1 or rebuild derived lexical assets"
        );
    }
    Ok(outcome.canonical_mutations)
}

#[allow(clippy::too_many_arguments)]
/// gh373/oeu5a RAII guard: marks `IndexingProgress::persist_in_progress` for
/// the duration of one batched-persist call so the stall watchdog can grant
/// an active giant-batch persist the finalize-class abort grace instead of
/// killing it at the ordinary 300s no-progress threshold. Cleared on drop —
/// including the error path — so a failed persist never leaves the grace
/// stuck on.
pub(in crate::indexer) struct PersistInProgressGuard<'a>(Option<&'a IndexingProgress>);

impl<'a> PersistInProgressGuard<'a> {
    pub(in crate::indexer) fn engage(progress: Option<&'a IndexingProgress>) -> Self {
        if let Some(p) = progress {
            p.persist_in_progress.store(true, Ordering::Relaxed);
        }
        Self(progress)
    }
}

impl Drop for PersistInProgressGuard<'_> {
    fn drop(&mut self) {
        if let Some(p) = self.0 {
            p.persist_in_progress.store(false, Ordering::Relaxed);
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn ingest_batch_detailed(
    storage: &FrankenStorage,
    t_index: Option<&mut TantivyIndex>,
    data_dir: &Path,
    convs: &[NormalizedConversation],
    progress: &Option<Arc<IndexingProgress>>,
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
    progress_bump: Option<&Arc<AtomicI64>>,
) -> Result<NonWatchIngestOutcome> {
    ingest_batch_detailed_with_source_completion(
        storage,
        t_index,
        data_dir,
        convs,
        progress,
        lexical_strategy,
        defer_checkpoints,
        progress_bump,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn ingest_batch_detailed_with_source_completion(
    storage: &FrankenStorage,
    t_index: Option<&mut TantivyIndex>,
    data_dir: &Path,
    convs: &[NormalizedConversation],
    progress: &Option<Arc<IndexingProgress>>,
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
    progress_bump: Option<&Arc<AtomicI64>>,
    completion: Option<&crate::storage::sqlite::SourceIngestLedgerEntry>,
) -> Result<NonWatchIngestOutcome> {
    let trace_span =
        robot_trace_ingest_start("ingest_batch", convs, lexical_strategy, defer_checkpoints);
    // Persistence now uses short-lived writer connections internally so the
    // long-lived watch/session handle does not accumulate retained connection
    // state.
    //
    // gh373/oeu5a: thread the work-liveness heartbeat into the persist call
    // (per-message ticks during redaction/map, per-chunk ticks during write)
    // and flag `persist_in_progress` for the watchdog's defense-in-depth
    // grace — one giant conversation legitimately persists for minutes.
    let persist_in_progress = PersistInProgressGuard::engage(progress.as_deref());
    let batch_result = persist::persist_conversations_batched_inner(
        storage,
        t_index,
        convs,
        lexical_strategy,
        defer_checkpoints,
        false,
        Some(data_dir),
        persist::PersistHeartbeat::new(progress.as_deref(), progress_bump),
        completion,
    );
    let batch_outcome = match batch_result {
        Ok(batch_outcome) => batch_outcome,
        Err(error) => {
            robot_trace_ingest_finish(trace_span, "error", 0, 0, Some(&error));
            return Err(error);
        }
    };
    if batch_outcome.lexical_update_deferred {
        tracing::warn!(
            error = ?batch_outcome.lexical_update_error,
            workspace_changes = batch_outcome.workspace_changes,
            "SQLite ingest succeeded but inline lexical update was deferred; scheduling authoritative lexical rebuild"
        );
    }

    // GH#320: bound the bulk-ingest WAL (and the pager memory plus finalize
    // checkpoint debt that scale with it) at a safe between-transactions
    // boundary.
    persist::maybe_checkpoint_bulk_ingest_wal(storage, defer_checkpoints);
    // #425: a threshold-sized TRUNCATE can spend minutes in synchronous
    // engine work without an observable counter. Keep the watchdog's bounded
    // persist/finalize grace through the checkpoint, then clear it before the
    // normal progress publication below.
    drop(persist_in_progress);

    // Update progress counter for all conversations at once
    if let Some(p) = progress {
        p.current.fetch_add(convs.len(), Ordering::Relaxed);
        p.tick_activity();
    }
    bump_index_run_lock_progress_if_present(progress_bump);
    robot_trace_ingest_finish(
        trace_span,
        "ok",
        batch_outcome.inserted_conversations,
        batch_outcome.inserted_messages,
        None,
    );
    Ok(NonWatchIngestOutcome {
        canonical_mutations: CanonicalMutationCounts {
            inserted_conversations: batch_outcome.inserted_conversations,
            inserted_messages: batch_outcome.inserted_messages,
            updated_messages: batch_outcome.updated_messages,
        },
        quarantined_conversations: 0,
        lexical_update_deferred: batch_outcome.lexical_update_deferred,
        scanned_connectors: BTreeSet::new(),
        scan_had_errors: false,
        deferred_sources: BTreeSet::new(),
    })
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn ingest_non_watch_batch_with_oom_split(
    storage: &FrankenStorage,
    mut t_index: Option<&mut TantivyIndex>,
    data_dir: &Path,
    convs: &[NormalizedConversation],
    progress: &Option<Arc<IndexingProgress>>,
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
    progress_bump: Option<&Arc<AtomicI64>>,
) -> Result<NonWatchIngestOutcome> {
    if let Some(progress) = progress {
        progress.check_stop()?;
    }
    if convs.is_empty() {
        return Ok(NonWatchIngestOutcome::default());
    }

    let chunk_size = non_watch_ingest_chunk_size();
    if convs.len() > chunk_size {
        let mut merged = NonWatchIngestOutcome::default();
        for chunk in convs.chunks(chunk_size) {
            let outcome = ingest_non_watch_batch_with_oom_split(
                storage,
                t_index.as_deref_mut(),
                data_dir,
                chunk,
                progress,
                lexical_strategy,
                defer_checkpoints,
                progress_bump,
            )?;
            // #332: each persisted chunk is live work even while the outer
            // batch's `current` bump waits for the whole batch to finish.
            if let Some(p) = progress {
                p.tick_activity();
            }
            merged = merged.accumulate(outcome);
        }
        return Ok(merged);
    }

    let first_attempt = ingest_non_watch_batch_once(
        storage,
        t_index,
        data_dir,
        convs,
        progress,
        lexical_strategy,
        defer_checkpoints,
        progress_bump,
    );

    let outcome = match first_attempt {
        Ok(outcome) => Ok(outcome),
        Err(error) if error_is_out_of_memory(&error) => ingest_non_watch_oom_retry_or_quarantine(
            storage,
            data_dir,
            convs,
            progress,
            lexical_strategy,
            defer_checkpoints,
            progress_bump,
            error,
        ),
        Err(error) => Err(error),
    }?;
    if let Some(progress) = progress {
        progress.check_stop()?;
    }
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn ingest_non_watch_batch_once(
    storage: &FrankenStorage,
    t_index: Option<&mut TantivyIndex>,
    data_dir: &Path,
    convs: &[NormalizedConversation],
    progress: &Option<Arc<IndexingProgress>>,
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
    progress_bump: Option<&Arc<AtomicI64>>,
) -> Result<NonWatchIngestOutcome> {
    if should_inject_non_watch_ingest_test_oom(convs) {
        // Use the typed `FrankenError::OutOfMemory` variant so the OOM detector
        // exercises the downcast path that real SQLite OOMs hit, instead
        // of relying on the plain-string fallback.
        return Err(anyhow::Error::new(
            crate::franken_sync::FrankenError::OutOfMemory,
        ));
    }

    let outcome = ingest_batch_detailed(
        storage,
        t_index,
        data_dir,
        convs,
        progress,
        lexical_strategy,
        defer_checkpoints,
        progress_bump,
    )?;
    clear_poison_conversations_after_successful_ingest(
        data_dir,
        INDEX_INGEST_POISON_FILE,
        "index-ingest-out-of-memory",
        convs,
    );
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn ingest_non_watch_oom_retry_or_quarantine(
    storage: &FrankenStorage,
    data_dir: &Path,
    convs: &[NormalizedConversation],
    progress: &Option<Arc<IndexingProgress>>,
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
    progress_bump: Option<&Arc<AtomicI64>>,
    error: anyhow::Error,
) -> Result<NonWatchIngestOutcome> {
    if lexical_population_strategy_requires_inline_tantivy(lexical_strategy) {
        tracing::warn!(
            conversations = convs.len(),
            error = %error,
            "non-watch ingest ran out of memory; retrying batch with deferred lexical updates before quarantine"
        );
        return match ingest_non_watch_batch_once(
            storage,
            None,
            data_dir,
            convs,
            progress,
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            defer_checkpoints,
            progress_bump,
        ) {
            Ok(mut outcome) => {
                outcome.lexical_update_deferred = true;
                Ok(outcome)
            }
            Err(retry_error) if error_is_out_of_memory(&retry_error) => {
                ingest_non_watch_oom_retry_or_quarantine(
                    storage,
                    data_dir,
                    convs,
                    progress,
                    LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
                    defer_checkpoints,
                    progress_bump,
                    retry_error,
                )
            }
            Err(retry_error) => Err(retry_error),
        };
    }

    if convs.len() > 1 {
        let split_at = convs.len() / 2;
        tracing::warn!(
            conversations = convs.len(),
            left = split_at,
            right = convs.len().saturating_sub(split_at),
            error = %error,
            "non-watch deferred ingest batch ran out of memory; retrying as smaller batches"
        );
        let mut left = ingest_non_watch_batch_with_oom_split(
            storage,
            None,
            data_dir,
            &convs[..split_at],
            progress,
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            defer_checkpoints,
            progress_bump,
        )?;
        let right = ingest_non_watch_batch_with_oom_split(
            storage,
            None,
            data_dir,
            &convs[split_at..],
            progress,
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            defer_checkpoints,
            progress_bump,
        )?;
        left = left.accumulate(right);
        left.lexical_update_deferred = true;
        return Ok(left);
    }

    let conv = &convs[0];

    // #298: mirror the watch-path plausibility gate. SQLite raises the
    // same typed `FrankenError::OutOfMemory` for per-statement bounded
    // allocation guards, so a NoMem on a small conversation with no real host
    // memory pressure is not evidence of a poisoned conversation. Defer it
    // (retried by the next index run) instead of quarantining.
    let indexed_text_bytes = conversation_indexed_text_bytes(conv);
    let small_conversation = indexed_text_bytes <= watch_oom_small_conversation_bytes();
    let real_pressure = watch_oom_under_real_memory_pressure();
    let should_quarantine = match real_pressure {
        Some(true) => true,
        // #364: ample reported memory proves the NoMem is a bounded-guard
        // artifact regardless of conversation size — defer, never poison.
        Some(false) => false,
        None => !small_conversation,
    };
    if !should_quarantine {
        tracing::warn!(
            agent = %conv.agent_slug,
            external_id = conv.external_id.as_deref().unwrap_or(""),
            source_path = %conv.source_path.display(),
            indexed_text_bytes,
            real_memory_pressure = ?real_pressure,
            error = %error,
            "single non-watch conversation hit a bounded-allocation NoMem with no real memory pressure; deferring (not quarantining) so the next index run retries it (#298)"
        );
        if let Some(progress) = progress {
            progress.current.fetch_add(1, Ordering::Relaxed);
        }
        bump_index_run_lock_progress_if_present(progress_bump);
        return Ok(NonWatchIngestOutcome {
            canonical_mutations: CanonicalMutationCounts::default(),
            quarantined_conversations: 0,
            lexical_update_deferred: true,
            scanned_connectors: BTreeSet::new(),
            scan_had_errors: true,
            deferred_sources: BTreeSet::from([conv.source_path.clone()]),
        });
    }

    record_index_poison_conversation(
        data_dir,
        conv,
        &error,
        Some(PoisonMemoryAttribution {
            real_memory_pressure: real_pressure,
            indexed_text_bytes,
        }),
    )?;
    if let Some(progress) = progress {
        progress.current.fetch_add(1, Ordering::Relaxed);
    }
    bump_index_run_lock_progress_if_present(progress_bump);
    tracing::warn!(
        agent = %conv.agent_slug,
        external_id = conv.external_id.as_deref().unwrap_or(""),
        source_path = %conv.source_path.display(),
        indexed_text_bytes,
        real_memory_pressure = ?real_pressure,
        error = %error,
        "single non-watch conversation ran out of memory after deferred lexical retry; quarantined and continuing index refresh"
    );
    Ok(NonWatchIngestOutcome {
        canonical_mutations: CanonicalMutationCounts::default(),
        quarantined_conversations: 1,
        lexical_update_deferred: true,
        scanned_connectors: BTreeSet::new(),
        scan_had_errors: true,
        deferred_sources: BTreeSet::from([conv.source_path.clone()]),
    })
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn ingest_batch_with_semantic_delta(
    storage: &FrankenStorage,
    t_index: Option<&mut TantivyIndex>,
    data_dir: &Path,
    convs: &[NormalizedConversation],
    progress: &Option<Arc<IndexingProgress>>,
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
    semantic_delta: Option<&mut WatchSemanticDelta>,
) -> Result<persist::PersistBatchOutcome> {
    let trace_span = robot_trace_ingest_start(
        "ingest_batch_with_semantic_delta",
        convs,
        lexical_strategy,
        defer_checkpoints,
    );
    // gh373/oeu5a: same heartbeat + persist_in_progress treatment as
    // `ingest_batch_detailed` — watch-session batches can also carry one
    // giant conversation. No index-run lock bump handle exists on this path.
    let heartbeat = persist::PersistHeartbeat::new(progress.as_deref(), None);
    let _persist_in_progress = PersistInProgressGuard::engage(progress.as_deref());
    let batch_result = if semantic_delta.is_some() {
        persist::persist_conversations_batched_with_semantic_delta_and_raw_mirror_links(
            storage,
            t_index,
            data_dir,
            convs,
            lexical_strategy,
            defer_checkpoints,
            heartbeat,
        )
    } else {
        persist::persist_conversations_batched_with_raw_mirror_links(
            storage,
            t_index,
            data_dir,
            convs,
            lexical_strategy,
            defer_checkpoints,
            heartbeat,
        )
    };
    drop(_persist_in_progress);
    let batch_outcome = match batch_result {
        Ok(batch_outcome) => batch_outcome,
        Err(error) => {
            robot_trace_ingest_finish(trace_span, "error", 0, 0, Some(&error));
            return Err(error);
        }
    };

    clear_poison_conversations_after_successful_ingest(
        data_dir,
        WATCH_INGEST_POISON_FILE,
        "watch-ingest-out-of-memory",
        convs,
    );

    if let Some(p) = progress {
        p.current.fetch_add(convs.len(), Ordering::Relaxed);
    }

    robot_trace_ingest_finish(
        trace_span,
        "ok",
        batch_outcome.inserted_conversations,
        batch_outcome.inserted_messages,
        None,
    );
    Ok(batch_outcome)
}

#[derive(Debug, Default)]
pub(in crate::indexer) struct WatchIngestBatchOutcome {
    pub(in crate::indexer) batch_outcome: persist::PersistBatchOutcome,
    pub(in crate::indexer) processed_conversations: usize,
    pub(in crate::indexer) quarantined_conversations: usize,
    /// Conversations skipped this pass by the #298 plausibility gate (bounded
    /// NoMem, no real pressure). Not quarantined — but the watch watermark
    /// must not advance past them or they would be silently dropped until
    /// their source file changes again.
    pub(in crate::indexer) deferred_conversations: usize,
    pub(in crate::indexer) max_payload_watermark_ms: Option<i64>,
}

impl WatchIngestBatchOutcome {
    pub(in crate::indexer) fn merge(&mut self, other: Self) {
        self.batch_outcome.merge(other.batch_outcome);
        self.processed_conversations = self
            .processed_conversations
            .saturating_add(other.processed_conversations);
        self.quarantined_conversations = self
            .quarantined_conversations
            .saturating_add(other.quarantined_conversations);
        self.deferred_conversations = self
            .deferred_conversations
            .saturating_add(other.deferred_conversations);
        if let Some(ts) = other.max_payload_watermark_ms {
            self.max_payload_watermark_ms = Some(
                self.max_payload_watermark_ms
                    .map_or(ts, |current| current.max(ts)),
            );
        }
    }
}

/// How a watch ingest call should treat a single-conversation OOM (#298).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) enum WatchOomIngestMode {
    /// Normal batch ingest: the typed-OOM test injection may fire, and a
    /// single-conversation OOM goes through the solo isolate-retry plus the
    /// pressure/size gate before any quarantine decision.
    Standard,
    /// Solo isolate-retry re-entry: test injection is disabled and a
    /// single-conversation OOM propagates as `Err` so the Standard-mode
    /// caller's gate — not this re-entry — decides between deferring and
    /// quarantining (and its logs stay truthful about what happened).
    SoloIsolatedRetry,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) enum WatchLexicalTestFault {
    PublicationAfter(usize),
    SplitAt(usize),
    DebtWrite,
    DebtClear,
    Commit,
}

#[cfg(test)]
std::thread_local! {
    pub(in crate::indexer) static WATCH_LEXICAL_TEST_FAULT: std::cell::Cell<Option<WatchLexicalTestFault>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(in crate::indexer) fn inject_watch_lexical_test_failure(
    expected: WatchLexicalTestFault,
) -> Result<()> {
    if WATCH_LEXICAL_TEST_FAULT.with(|fault| fault.get() == Some(expected)) {
        anyhow::bail!("injected watch lexical persistence failure");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn ingest_watch_batch_with_oom_split(
    storage: &FrankenStorage,
    t_index: &mut TantivyIndex,
    data_dir: &Path,
    convs: &[NormalizedConversation],
    progress: &Option<Arc<IndexingProgress>>,
    defer_checkpoints: bool,
    capture_semantic_delta: bool,
) -> Result<WatchIngestBatchOutcome> {
    ingest_watch_batch_with_oom_split_inner(
        storage,
        t_index,
        data_dir,
        convs,
        progress,
        defer_checkpoints,
        capture_semantic_delta,
        WatchOomIngestMode::Standard,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn ingest_watch_batch_with_oom_split_inner(
    storage: &FrankenStorage,
    t_index: &mut TantivyIndex,
    data_dir: &Path,
    convs: &[NormalizedConversation],
    progress: &Option<Arc<IndexingProgress>>,
    defer_checkpoints: bool,
    capture_semantic_delta: bool,
    mode: WatchOomIngestMode,
    replay_canonical: bool,
) -> Result<WatchIngestBatchOutcome> {
    debug_assert!(!convs.is_empty());

    let batch_result = if matches!(mode, WatchOomIngestMode::Standard)
        && should_inject_watch_ingest_test_oom(convs)
    {
        // Use the typed `FrankenError::OutOfMemory` variant so the OOM detector
        // exercises the downcast path that real SQLite OOMs hit, instead
        // of relying on the plain-string fallback.
        Err(anyhow::Error::new(
            crate::franken_sync::FrankenError::OutOfMemory,
        ))
    } else {
        let mut semantic_delta = WatchSemanticDelta::default();
        ingest_batch_with_semantic_delta(
            storage,
            Some(t_index),
            data_dir,
            convs,
            progress,
            if replay_canonical {
                // Preserve mutation accounting and semantic deltas while
                // replaying selected canonical identities below, including
                // outcomes whose unchanged persistence wrote no rows.
                LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild
            } else {
                LexicalPopulationStrategy::IncrementalInline
            },
            defer_checkpoints,
            capture_semantic_delta.then_some(&mut semantic_delta),
        )
    };

    match batch_result {
        Ok(batch_outcome) => {
            clear_watch_nomem_deferrals(data_dir, convs);
            Ok(WatchIngestBatchOutcome {
                batch_outcome,
                processed_conversations: convs.len(),
                quarantined_conversations: 0,
                deferred_conversations: 0,
                max_payload_watermark_ms: conversations_payload_watermark_ms(convs),
            })
        }
        Err(error) if error_is_out_of_memory(&error) && convs.len() > 1 => {
            let split_at = convs.len() / 2;
            tracing::warn!(
                conversations = convs.len(),
                left = split_at,
                right = convs.len().saturating_sub(split_at),
                error = %error,
                "watch ingest batch ran out of memory; retrying as smaller batches"
            );
            let mut merged = ingest_watch_batch_with_oom_split_inner(
                storage,
                t_index,
                data_dir,
                &convs[..split_at],
                progress,
                defer_checkpoints,
                capture_semantic_delta,
                mode,
                replay_canonical,
            )?;
            let right = ingest_watch_batch_with_oom_split_inner(
                storage,
                t_index,
                data_dir,
                &convs[split_at..],
                progress,
                defer_checkpoints,
                capture_semantic_delta,
                mode,
                replay_canonical,
            )?;
            merged.merge(right);
            Ok(merged)
        }
        Err(error) if error_is_out_of_memory(&error) => {
            // Solo isolate-retry re-entry: propagate the OOM so the
            // Standard-mode caller's pressure/size gate decides between
            // deferring and quarantining. Quarantining here would bypass that
            // gate and make the caller's "ingested cleanly solo" log lie.
            if matches!(mode, WatchOomIngestMode::SoloIsolatedRetry) {
                return Err(error);
            }

            let conv = &convs[0];

            // #298: a typed `FrankenError::OutOfMemory` (NoMem) on the batch
            // path is NOT proof of real host memory exhaustion — SQLite
            // raises the same typed error for per-statement bounded allocation
            // guards. Before quarantining a single small conversation that may
            // well ingest fine in isolation, retry it solo (which the reporter
            // confirmed succeeds) and only quarantine on a genuine, repeated
            // failure under real memory pressure or for a genuinely large
            // conversation.
            let indexed_text_bytes = conversation_indexed_text_bytes(conv);
            let small_conversation = indexed_text_bytes <= watch_oom_small_conversation_bytes();
            let real_pressure = watch_oom_under_real_memory_pressure();

            // Isolate-retry the single conversation solo. The retry runs in
            // `SoloIsolatedRetry` mode so the typed-OOM test injection cannot
            // re-fire and a repeated OOM comes back as `Err` for the gate
            // below. Tests that need to model a *genuinely* poisoned
            // conversation (solo ingest also OOMs) set
            // `CASS_TEST_WATCH_SOLO_RETRY_OOM=1`.
            let solo_retry = if should_force_watch_solo_retry_oom() {
                Err(anyhow::Error::new(
                    crate::franken_sync::FrankenError::OutOfMemory,
                ))
            } else {
                ingest_watch_batch_with_oom_split_inner(
                    storage,
                    t_index,
                    data_dir,
                    std::slice::from_ref(conv),
                    progress,
                    defer_checkpoints,
                    capture_semantic_delta,
                    WatchOomIngestMode::SoloIsolatedRetry,
                    replay_canonical,
                )
            };

            match solo_retry {
                Ok(outcome) => {
                    // Solo ingest succeeded — the batch-path NoMem was a
                    // bounded-guard false positive. Do not quarantine.
                    tracing::info!(
                        agent = %conv.agent_slug,
                        external_id = conv.external_id.as_deref().unwrap_or(""),
                        source_path = %conv.source_path.display(),
                        indexed_text_bytes,
                        "watch conversation hit a bounded-allocation NoMem in batch but ingested cleanly solo; not quarantining (#298)"
                    );
                    Ok(outcome)
                }
                Err(solo_error) if !error_is_out_of_memory(&solo_error) => {
                    // The solo retry surfaced a *different* (non-OOM) error
                    // — propagate it rather than mislabel it as OOM.
                    Err(solo_error)
                }
                Err(solo_error) => {
                    // Solo retry still hit OOM. Only quarantine if this is
                    // real memory pressure or a genuinely large
                    // conversation; otherwise keep it eligible for a later
                    // retry instead of poisoning a small, replayable
                    // conversation on a transient bounded-guard hit.
                    let should_quarantine = match real_pressure {
                        // Real host pressure: quarantine is justified.
                        Some(true) => true,
                        // Ample memory reported: the NoMem is a transient or
                        // bounded-allocation artifact by definition (#364) —
                        // conversation size makes a guard more likely to trip
                        // but cannot turn it into host exhaustion. Defer so a
                        // later scan retries; quarantining here turned a
                        // healthy, replayable 36 MB source into a
                        // same-version-irreducible coverage hole.
                        Some(false) => false,
                        // Pressure unknowable (e.g. macOS): fall back to the
                        // size gate — never quarantine a small conversation
                        // on a typed NoMem we cannot attribute to real
                        // pressure.
                        None => !small_conversation,
                    };
                    if !should_quarantine {
                        // GH#510: back off instead of replaying this on every
                        // tick, and warn only on the 1st, 2nd, 4th, 8th, ...
                        // deferral rather than once per tick.
                        let deferral = record_watch_nomem_deferral(
                            data_dir,
                            conv,
                            indexed_text_bytes,
                            FrankenStorage::now_millis(),
                        );
                        let retry_in_secs = deferral
                            .next_retry_at_ms
                            .saturating_sub(deferral.last_deferred_at_ms)
                            / 1_000;
                        if deferral.deferrals.is_power_of_two() {
                            tracing::warn!(
                                agent = %conv.agent_slug,
                                external_id = conv.external_id.as_deref().unwrap_or(""),
                                source_path = %conv.source_path.display(),
                                indexed_text_bytes,
                                real_memory_pressure = ?real_pressure,
                                deferrals = deferral.deferrals,
                                retry_in_secs,
                                error = %solo_error,
                                "watch conversation hit repeated bounded-allocation NoMem with no real memory pressure; deferring (not quarantining) with backoff (#298, GH#510). `cass quarantine list --json` lists deferred conversations"
                            );
                        } else {
                            tracing::debug!(
                                agent = %conv.agent_slug,
                                source_path = %conv.source_path.display(),
                                deferrals = deferral.deferrals,
                                retry_in_secs,
                                "watch conversation deferred again after bounded-allocation NoMem (GH#510)"
                            );
                        }
                        if let Some(progress) = progress {
                            progress.current.fetch_add(1, Ordering::Relaxed);
                        }
                        return Ok(WatchIngestBatchOutcome {
                            batch_outcome: persist::PersistBatchOutcome::default(),
                            processed_conversations: 1,
                            quarantined_conversations: 0,
                            deferred_conversations: 1,
                            max_payload_watermark_ms: None,
                        });
                    }
                    // Quarantine with the solo error so the record reflects
                    // the genuine repeated failure.
                    quarantine_single_watch_conversation(
                        data_dir,
                        conv,
                        &solo_error,
                        progress,
                        indexed_text_bytes,
                        real_pressure,
                    )
                }
            }
        }
        Err(error) => Err(error),
    }
}

/// Record a single watch conversation as poison and quarantine it, advancing
/// watch progress so the watcher does not stall on a poison file.
pub(in crate::indexer) fn quarantine_single_watch_conversation(
    data_dir: &Path,
    conv: &NormalizedConversation,
    error: &anyhow::Error,
    progress: &Option<Arc<IndexingProgress>>,
    indexed_text_bytes: usize,
    real_memory_pressure: Option<bool>,
) -> Result<WatchIngestBatchOutcome> {
    record_watch_poison_conversation(
        data_dir,
        conv,
        error,
        Some(PoisonMemoryAttribution {
            real_memory_pressure,
            indexed_text_bytes,
        }),
    )?;
    if let Some(progress) = progress {
        progress.current.fetch_add(1, Ordering::Relaxed);
    }
    tracing::warn!(
        agent = %conv.agent_slug,
        external_id = conv.external_id.as_deref().unwrap_or(""),
        source_path = %conv.source_path.display(),
        indexed_text_bytes,
        real_memory_pressure = ?real_memory_pressure,
        error = %error,
        "single watch conversation ran out of memory; quarantined and advancing watch progress"
    );
    Ok(WatchIngestBatchOutcome {
        batch_outcome: persist::PersistBatchOutcome::default(),
        processed_conversations: 1,
        quarantined_conversations: 1,
        deferred_conversations: 0,
        max_payload_watermark_ms: None,
    })
}

pub(in crate::indexer) fn conversations_payload_watermark_ms(
    convs: &[NormalizedConversation],
) -> Option<i64> {
    convs
        .iter()
        .filter_map(conversation_payload_watermark_ms)
        .max()
}

#[cfg(test)]
pub(in crate::indexer) fn sort_watch_conversations_for_watermark(
    convs: &mut [NormalizedConversation],
) {
    convs.sort_by(|left, right| {
        conversation_payload_watermark_ms(left)
            .cmp(&conversation_payload_watermark_ms(right))
            .then_with(|| left.source_path.cmp(&right.source_path))
            .then_with(|| left.external_id.cmp(&right.external_id))
    });
}

pub(in crate::indexer) fn conversation_payload_watermark_ms(
    conv: &NormalizedConversation,
) -> Option<i64> {
    conv.started_at
        .into_iter()
        .chain(conv.ended_at)
        .chain(
            conv.messages
                .iter()
                .filter_map(|message| message.created_at),
        )
        .max()
}

pub(in crate::indexer) fn save_watch_state_watermark(
    data_dir: &Path,
    state: &Mutex<HashMap<ConnectorKind, i64>>,
    kind: ConnectorKind,
    ts_val: i64,
) -> Result<()> {
    let mut guard = state
        .lock()
        .map_err(|_| anyhow::anyhow!("state lock poisoned"))?;
    let entry = guard.entry(kind).or_insert(ts_val);
    *entry = (*entry).max(ts_val);
    save_watch_state(data_dir, &guard)?;
    Ok(())
}

pub(in crate::indexer) fn error_is_out_of_memory(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if cause
            .downcast_ref::<crate::franken_sync::FrankenError>()
            .is_some_and(|err| matches!(err, crate::franken_sync::FrankenError::OutOfMemory))
        {
            return true;
        }
        error_message_is_exact_out_of_memory(&cause.to_string())
    })
}

/// Default ceiling, in bytes of *indexed text*, under which a watch-ingest
/// `NoMem` is treated as a bounded-allocation-guard false positive rather than
/// real host memory exhaustion (#298). SQLite raises the same typed
/// `FrankenError::OutOfMemory` for per-statement bounded allocation limits
/// (VDBE frame/register/pager guards) as it does for real OS pressure, so a
/// small conversation that ingests fine solo must never be quarantined just
/// because it tripped a bounded guard under concurrent batch load.
pub(in crate::indexer) const WATCH_OOM_SMALL_CONVERSATION_DEFAULT_BYTES: usize = 8 * 1024 * 1024;

/// Default host-available-memory reserve (bytes). If the OS reports *less* than
/// this much available memory when a watch `NoMem` fires, we treat it as real
/// pressure and allow quarantine; otherwise the `NoMem` is a bounded-guard hit.
pub(in crate::indexer) const WATCH_OOM_REAL_PRESSURE_RESERVE_DEFAULT_BYTES: u64 = 512 * 1024 * 1024;

/// Indexed-text byte size of a single conversation — the sum of message content
/// lengths. This is the right "size" signal for #298: the false-positive
/// victims are multi-MB raw files (tool_result/thinking bulk) with only tens of
/// KB of *indexed* text, so the materialized-file size is irrelevant here.
pub(in crate::indexer) fn conversation_indexed_text_bytes(conv: &NormalizedConversation) -> usize {
    conv.messages
        .iter()
        .map(|message| message.content.len())
        .fold(0usize, |acc, len| acc.saturating_add(len))
}

pub(in crate::indexer) fn watch_oom_small_conversation_bytes() -> usize {
    dotenvy::var("CASS_WATCH_OOM_SMALL_CONVERSATION_BYTES")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .unwrap_or(WATCH_OOM_SMALL_CONVERSATION_DEFAULT_BYTES)
}

pub(in crate::indexer) fn watch_oom_real_pressure_reserve_bytes() -> u64 {
    dotenvy::var("CASS_WATCH_OOM_REAL_PRESSURE_RESERVE_BYTES")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(WATCH_OOM_REAL_PRESSURE_RESERVE_DEFAULT_BYTES)
}

/// Whether a watch-ingest `NoMem` should be treated as *real* host memory
/// pressure. Returns:
/// - `Some(true)`  — host reports available memory below the configured reserve
///   (real pressure; quarantine is justified after the solo retry fails).
/// - `Some(false)` — host reports ample available memory (the `NoMem` is a
///   bounded-allocation-guard false positive).
/// - `None`        — memory pressure is unknowable on this platform
///   (e.g. macOS has no `/proc/meminfo`); callers must fall back to the size
///   gate / solo-retry result and must NOT assume pressure.
pub(in crate::indexer) fn watch_oom_under_real_memory_pressure() -> Option<bool> {
    let available = responsiveness::available_memory_bytes()?;
    Some(available <= watch_oom_real_pressure_reserve_bytes())
}

/// GH#510: a bounded-guard NoMem that recurs deterministically is deferred,
/// never quarantined (#298/#364). The deferral holds the watch watermark, so
/// without a ledger every tick replays the batch split, the solo retry and the
/// deferral forever. The ledger records each deferral and the next time the
/// conversation may be attempted again.
pub(crate) const WATCH_NOMEM_DEFERRALS_FILE: &str = "watch_nomem_deferrals.json";
pub(in crate::indexer) const WATCH_NOMEM_DEFERRAL_BASE_BACKOFF_MS: i64 = 60_000;
pub(in crate::indexer) const WATCH_NOMEM_DEFERRAL_MAX_BACKOFF_MS: i64 = 3_600_000;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct WatchNomemDeferrals {
    #[serde(default)]
    pub(crate) entries: BTreeMap<String, WatchNomemDeferral>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct WatchNomemDeferral {
    pub(crate) agent: String,
    pub(crate) source_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) external_id: Option<String>,
    pub(crate) deferrals: u32,
    pub(crate) first_deferred_at_ms: i64,
    pub(crate) last_deferred_at_ms: i64,
    pub(crate) next_retry_at_ms: i64,
    pub(crate) indexed_text_bytes: usize,
}

impl WatchNomemDeferrals {
    pub(crate) fn path(data_dir: &Path) -> PathBuf {
        data_dir.join("quarantine").join(WATCH_NOMEM_DEFERRALS_FILE)
    }

    /// A missing or unreadable ledger is empty: losing it costs one more
    /// attempt per conversation, never a conversation.
    pub(crate) fn load(data_dir: &Path) -> Self {
        fs::read(Self::path(data_dir))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    pub(in crate::indexer) fn save(&self, data_dir: &Path) {
        let path = Self::path(data_dir);
        if let Err(error) = write_json_pretty_atomically(&path, self) {
            tracing::warn!(
                path = %path.display(),
                error = %error,
                "failed to persist the watch NoMem deferral ledger; deferred conversations retry on the next tick"
            );
        }
    }
}

pub(in crate::indexer) fn watch_nomem_deferral_key(conv: &NormalizedConversation) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        conv.agent_slug,
        conv.source_path.display(),
        conv.external_id.as_deref().unwrap_or("")
    )
}

/// 1 min after the first deferral, doubling up to 1 h.
pub(in crate::indexer) fn watch_nomem_deferral_backoff_ms(deferrals: u32) -> i64 {
    let doublings = deferrals.saturating_sub(1).min(16);
    WATCH_NOMEM_DEFERRAL_BASE_BACKOFF_MS
        .saturating_mul(1_i64 << doublings)
        .min(WATCH_NOMEM_DEFERRAL_MAX_BACKOFF_MS)
}

/// Count one more deferral of `conv` and schedule its next attempt.
pub(in crate::indexer) fn record_watch_nomem_deferral(
    data_dir: &Path,
    conv: &NormalizedConversation,
    indexed_text_bytes: usize,
    now_ms: i64,
) -> WatchNomemDeferral {
    let mut ledger = WatchNomemDeferrals::load(data_dir);
    let entry = ledger
        .entries
        .entry(watch_nomem_deferral_key(conv))
        .or_insert_with(|| WatchNomemDeferral {
            agent: conv.agent_slug.clone(),
            source_path: conv.source_path.display().to_string(),
            external_id: conv.external_id.clone(),
            deferrals: 0,
            first_deferred_at_ms: now_ms,
            last_deferred_at_ms: now_ms,
            next_retry_at_ms: now_ms,
            indexed_text_bytes,
        });
    entry.deferrals = entry.deferrals.saturating_add(1);
    entry.last_deferred_at_ms = now_ms;
    entry.indexed_text_bytes = indexed_text_bytes;
    entry.next_retry_at_ms =
        now_ms.saturating_add(watch_nomem_deferral_backoff_ms(entry.deferrals));
    let recorded = entry.clone();
    ledger.save(data_dir);
    recorded
}

/// Forget deferrals for conversations that just ingested.
pub(in crate::indexer) fn clear_watch_nomem_deferrals(
    data_dir: &Path,
    convs: &[NormalizedConversation],
) {
    if !WatchNomemDeferrals::path(data_dir).exists() {
        return;
    }
    let mut ledger = WatchNomemDeferrals::load(data_dir);
    let before = ledger.entries.len();
    for conv in convs {
        ledger.entries.remove(&watch_nomem_deferral_key(conv));
    }
    if ledger.entries.len() != before {
        ledger.save(data_dir);
    }
}

/// A held conversation still counts as deferred, so the watch watermark
/// stays behind it. Load the ledger once per scan, not once per payload.
pub(in crate::indexer) fn watch_conversation_in_nomem_backoff(
    ledger: &WatchNomemDeferrals,
    conv: &NormalizedConversation,
    now_ms: i64,
) -> bool {
    !ledger.entries.is_empty()
        && ledger
            .entries
            .get(&watch_nomem_deferral_key(conv))
            .is_some_and(|entry| entry.next_retry_at_ms > now_ms)
}

pub(in crate::indexer) fn error_message_is_exact_out_of_memory(message: &str) -> bool {
    matches!(
        message.trim().to_ascii_lowercase().as_str(),
        "out of memory" | "not enough memory"
    )
}

/// Render the full `anyhow::Error` chain as ` | `-joined causes so quarantine
/// records preserve which subsystem produced the error. The top-level
/// `error.to_string()` only shows the outermost cause, which for typed
/// `FrankenError::OutOfMemory` is the bare literal "out of memory" — useless
/// for triage.
pub(in crate::indexer) fn format_error_chain(error: &anyhow::Error) -> String {
    let mut parts: Vec<String> = Vec::new();
    for cause in error.chain() {
        let rendered = cause.to_string();
        let trimmed = rendered.trim();
        if trimmed.is_empty() {
            continue;
        }
        if parts.last().is_some_and(|prev| prev == trimmed) {
            continue;
        }
        parts.push(trimmed.to_string());
    }
    if parts.is_empty() {
        return error.to_string();
    }
    parts.join(" | ")
}
