use super::*;

#[cfg(test)]
pub(in crate::indexer) fn reset_storage(storage: &FrankenStorage) -> Result<()> {
    // Wrap the canonical-table reset in an immediate write transaction so
    // partial clears roll back, then recreate the derived FTS table.
    let mut tx = storage
        .raw()
        .transaction_with_behavior(crate::franken_sync::compat::TransactionBehavior::Immediate)?;
    tx.execute_batch(
        "DELETE FROM usage_models_daily;
         DELETE FROM usage_daily;
         DELETE FROM usage_hourly;
         DELETE FROM token_daily_stats;
         DELETE FROM daily_stats;
         DELETE FROM message_metrics;
         DELETE FROM token_usage;
         DELETE FROM snippets;
         DELETE FROM messages;
         DELETE FROM conversations;
         DELETE FROM agents;
         DELETE FROM workspaces;
         DELETE FROM tags;
         DELETE FROM conversation_tags;
         DELETE FROM meta WHERE key = 'last_scan_ts';",
    )?;
    tx.commit()?;
    storage.rebuild_fts()?;
    Ok(())
}

/// Reindex paths and return the total number of conversations indexed.
///
/// Returns `Ok(count)` where count is the number of conversations successfully indexed.
/// This count is used by the stale detector to track indexing activity.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(in crate::indexer) fn reindex_paths(
    opts: &IndexOptions,
    paths: Vec<PathBuf>,
    roots: &[(ConnectorKind, ScanRoot)],
    state: &Mutex<HashMap<ConnectorKind, i64>>,
    storage: &Mutex<FrankenStorage>,
    t_index: &Mutex<Option<TantivyIndex>>,
    index_path: &Path,
    force_full: bool,
) -> Result<usize> {
    reindex_paths_with_semantic_delta(
        opts,
        paths,
        roots,
        state,
        storage,
        t_index,
        index_path,
        force_full,
        None,
        &ActiveSessionSourceFilter::new(
            opts.watch && opts.watch_once_paths.as_ref().is_none_or(Vec::is_empty),
        ),
    )
    .map(|outcome| outcome.indexed)
}

#[derive(Default)]
pub(in crate::indexer) struct WatchReindexOutcome {
    pub(in crate::indexer) indexed: usize,
    /// Conversations whose new/revised content or recovery actually reached
    /// a committed lexical generation. Parsing old packets is not ingestion.
    pub(in crate::indexer) published_conversations: usize,
    pub(in crate::indexer) retry_sources: BTreeSet<PathBuf>,
}

pub(in crate::indexer) fn reindex_full_watch_roots(
    opts: &IndexOptions,
    roots: &[(ConnectorKind, ScanRoot)],
    state: &Mutex<HashMap<ConnectorKind, i64>>,
    storage: &Mutex<FrankenStorage>,
    t_index: &Mutex<Option<TantivyIndex>>,
    index_path: &Path,
    active_source_filter: &ActiveSessionSourceFilter,
) -> Result<WatchReindexOutcome> {
    // A forced census ignores old watermarks in memory. Persist replacement
    // watermarks only at the existing successful-publication boundaries: a
    // blocked or failed census must not erase the durable restart position.
    state
        .lock()
        .map_err(|_| anyhow::anyhow!("state lock poisoned"))?
        .clear();
    reindex_paths_with_semantic_delta(
        opts,
        roots.iter().map(|(_, root)| root.path.clone()).collect(),
        roots,
        state,
        storage,
        t_index,
        index_path,
        true,
        None,
        active_source_filter,
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn reindex_paths_with_semantic_delta(
    opts: &IndexOptions,
    paths: Vec<PathBuf>,
    roots: &[(ConnectorKind, ScanRoot)],
    state: &Mutex<HashMap<ConnectorKind, i64>>,
    storage: &Mutex<FrankenStorage>,
    t_index: &Mutex<Option<TantivyIndex>>,
    index_path: &Path,
    force_full: bool,
    semantic_delta: Option<&mut WatchSemanticDelta>,
    active_source_filter: &ActiveSessionSourceFilter,
) -> Result<WatchReindexOutcome> {
    // DO NOT lock storage/index here for the whole duration.
    // We only need them for the ingest phase, not the scan phase.
    if let Some(progress) = &opts.progress {
        progress.check_stop()?;
    }

    let explicit = opts
        .watch_once_paths
        .as_ref()
        .is_some_and(|paths| !paths.is_empty());
    if explicit {
        warn_unclaimed_explicit_watch_once_paths(&paths, roots);
    }
    let mut paths = paths;
    let absent_roots = if !explicit {
        promote_missing_watch_replay_paths(&mut paths, roots, storage, opts.progress.as_ref())?
    } else {
        Vec::new()
    };
    let mut triggers = classify_paths(paths, roots, explicit);
    triggers.retain(|(kind, root, _, _)| {
        !absent_roots.iter().any(|(absent_kind, absent)| {
            kind == absent_kind && root.origin == absent.origin && root.path == absent.path
        })
    });
    let mut outcome = match recover_absent_watch_roots(
        opts,
        &absent_roots,
        storage,
        t_index,
        index_path,
        active_source_filter,
    ) {
        Ok(outcome) => outcome,
        Err(error) => {
            reset_progress_to_idle(opts.progress.as_ref());
            return Err(error);
        }
    };
    if triggers.is_empty() {
        reset_progress_to_idle(opts.progress.as_ref());
        return Ok(outcome);
    }

    let raw_source_exclusions = {
        let storage = storage
            .lock()
            .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
        RawSourceExclusions::load(&storage)?
    };
    let mut first_source_scan_error = None;

    let mut semantic_delta = semantic_delta;
    let preserve_watch_watermark = scan_path_exclusions_active();

    for (kind, root, min_ts, max_ts) in triggers {
        if let Some(progress) = &opts.progress {
            progress.check_stop()?;
        }
        if opts.watch && !explicit {
            let metadata = fs::metadata(&root.path);
            if let Some(progress) = &opts.progress {
                progress.tick_activity();
            }
            if metadata.is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound) {
                // The selected root can disappear after classification. Keep
                // its retry so canonical-only recovery gets the next callback;
                // do not let a connector fall back to unrelated default roots.
                outcome.retry_sources.insert(root.path.clone());
                continue;
            }
        }
        let conn = kind.create_connector(Some(&opts.data_dir));
        let detect = conn.detect();
        if !detect.detected && root.origin.kind == SourceKind::Local && !root.path.exists() {
            // For local roots, if detection fails and the root is gone, skip.
            // For remote roots, detection might fail but we should still try scanning
            // if it's a brute-force attempt.
            continue;
        }

        // Update phase to scanning
        if let Some(p) = &opts.progress {
            p.phase.store(1, Ordering::Relaxed);
        }

        let explicit_watch_once = opts
            .watch_once_paths
            .as_ref()
            .is_some_and(|paths| !paths.is_empty());
        let lexical_strategy_reason = if explicit_watch_once {
            "watch_once_targeted_reindex_applies_inline_lexical_updates_for_changed_paths"
        } else {
            "watch_reindex_applies_inline_lexical_updates_for_changed_paths"
        };
        record_lexical_population_strategy_if_unset(
            opts.progress.as_ref(),
            LexicalPopulationStrategy::IncrementalInline,
            lexical_strategy_reason,
        );
        let pending_lexical_debt = {
            let storage = storage
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
            watch_lexical_replay_debt_for_root(&storage, kind, &root)?
        };
        let replay_canonical = pending_lexical_debt.iter().any(|debt| !debt.discovery_only);
        let source_scoped = conn.supports_source_boundaries();
        // Semantic watch-once must reconcile and attest an unchanged source,
        // even when the caller no longer captures callback-local deltas.
        if explicit_watch_once && !force_full && !opts.semantic && semantic_delta.is_none() {
            let unchanged = {
                let storage = storage
                    .lock()
                    .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
                explicit_watch_once_root_unchanged_after_last_index(&storage, kind, &root)?
            };
            if unchanged {
                tracing::info!(
                    ?kind,
                    scan_root = %root.path.display(),
                    "skipping unchanged explicit watch-once root already covered by last_indexed_at"
                );
                continue;
            }
        }

        let since_ts = if force_full || explicit_watch_once || replay_canonical {
            None
        } else {
            let guard = state
                .lock()
                .map_err(|_| anyhow::anyhow!("state lock poisoned"))?;
            let previous_ts = guard.get(&kind).copied();
            match (previous_ts, min_ts) {
                // No previous watermark and no trigger timestamp: full scan this root.
                (None, None) => None,
                // Only one side available: use it.
                (Some(ts), None) | (None, Some(ts)) => Some(ts.saturating_sub(1)),
                // Use the older timestamp so out-of-order file events are not skipped.
                (Some(prev), Some(batch_min)) => Some(prev.min(batch_min).saturating_sub(1)),
            }
        };

        let since_ts = watch_scan_lower_bound(kind, since_ts);

        if root.path.is_file()
            && should_skip_active_session_source(active_source_filter, root.origin.kind, &root.path)
        {
            tracing::debug!(
                ?kind,
                scan_root = %root.path.display(),
                "skipping active explicit watch source without advancing watermarks"
            );
            continue;
        }

        // Use explicit root context
        let mut ctx = crate::connectors::ScanContext::with_roots(
            root.path.clone(),
            vec![root.clone()],
            since_ts,
        );
        if source_scoped {
            // Per-source receipts, rather than a provider timestamp, decide
            // reuse. Backdated imports and restart gaps remain discoverable.
            ctx.since_ts = None;
        }
        if let Some(progress) = &opts.progress {
            let progress = Arc::clone(progress);
            ctx = ctx.with_progress_tick(Arc::new(move || progress.tick_activity()));
        }

        let (mut ingest_diagnostics, preparse_active_source_skipped) =
            capture_connector_sources_before_parse(
                conn.as_ref(),
                &ctx,
                &opts.data_dir,
                kind.slug(),
                std::slice::from_ref(&root),
                since_ts,
                active_source_filter,
                &raw_source_exclusions,
            );
        if let Some(progress) = &opts.progress {
            progress.check_stop()?;
        }

        // SCAN PHASE: IO-heavy, no locks held
        let scan_start = Instant::now();
        let ingest_chunk_size = watch_ingest_chunk_size();
        let mut convs = WatchConversationSpool::new(&opts.data_dir, ingest_chunk_size);
        let mut active_sources_skipped = 0usize;
        let mut nomem_backoff_held = 0usize;
        let nomem_deferrals = if explicit_watch_once {
            WatchNomemDeferrals::default()
        } else {
            WatchNomemDeferrals::load(&opts.data_dir)
        };
        let now_ms = FrankenStorage::now_millis();
        let saved_source_ledger = {
            let storage = storage
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
            watch_source_cache::load_bounded_watch_source_ledger(
                &storage,
                opts.progress.as_deref(),
            )?
        };
        let mut source_ledger = watch_source_ledger::WatchSourceLedger::default();
        let scan_error = match source_ledger.scan(
            conn.as_ref(),
            kind.slug(),
            &ctx,
            &saved_source_ledger,
            !explicit_watch_once && !force_full,
            replay_canonical,
            active_source_filter,
            opts.progress.as_deref(),
            &mut |mut conversation| {
                if let Some(progress) = &opts.progress {
                    progress.check_stop()?;
                    progress.tick_activity();
                }
                if should_skip_active_session_source(
                    active_source_filter,
                    root.origin.kind,
                    &conversation.source_path,
                ) {
                    active_sources_skipped = active_sources_skipped.saturating_add(1);
                    return Ok(false);
                }
                if should_skip_subagent(&conversation)
                    || should_skip_excluded_source(&conversation.source_path)
                {
                    return Ok(false);
                }
                ingest_diagnostics.observe_conversation(&mut conversation);
                // Prepare sort-key identity now, but copy raw bytes only for the
                // bounded batch about to commit. A mirror of every conversation
                // before the first commit starved watch progress on large roots.
                prepare_conversation_metadata_for_ingest(
                    kind.slug(),
                    &root.origin,
                    Some(&root),
                    &mut conversation,
                );
                if watch_conversation_in_nomem_backoff(&nomem_deferrals, &conversation, now_ms) {
                    nomem_backoff_held = nomem_backoff_held.saturating_add(1);
                    return Ok(false);
                }
                convs.push(conversation, opts.progress.as_deref())?;
                Ok(true)
            },
        ) {
            Ok(()) => None,
            Err(error) => {
                ingest_diagnostics.observe_connector_scan_error(&root.path, &error);
                let message = format!("{error:#}");
                tracing::warn!(
                    ?kind,
                    scan_root = %root.path.display(),
                    "watch scan failed: {message}"
                );
                first_source_scan_error.get_or_insert(error);
                Some(message)
            }
        };
        drop(saved_source_ledger);
        if let Some(progress) = &opts.progress {
            progress.check_stop()?;
        }
        active_sources_skipped =
            active_sources_skipped.saturating_add(source_ledger.active_sources_skipped);
        // A canonical identity can outlive a source's last emitted packet.
        // Both empty and nonempty recovery scans need source-level evidence;
        // a sibling packet alone cannot certify an existing zero-output file.
        // Completed-source evidence becomes publication proof only after all
        // admitted canonical/lexical chunks succeed below.
        let recovery_source_coverage = (source_scoped && replay_canonical && !explicit_watch_once)
            .then(|| WatchRecoverySourceCoverage::capture(&source_ledger, opts.progress.as_deref()))
            .transpose()?;
        let mut unproven_scanned_source = false;
        if let Some(coverage) = &recovery_source_coverage {
            for entry in &convs.entries {
                if let Some(progress) = &opts.progress {
                    progress.check_stop()?;
                }
                let path = crate::normalize_path_identity(&entry.key.source_path);
                if let Some(progress) = &opts.progress {
                    progress.tick_activity();
                }
                if !coverage.contains_normalized(&path) {
                    unproven_scanned_source = true;
                    break;
                }
            }
        }
        ingest_diagnostics
            .observe_reused_sources(std::mem::take(&mut source_ledger.reused_sources));
        let scan_ms = scan_start.elapsed().as_millis() as u64;
        // Publish the source error before persistence can fail for a separate
        // reason, including the all-bad case that has no ingest phase below.
        let failed_stats_index = if let Some(error) = &scan_error
            && let Some(progress) = &opts.progress
            && let Ok(mut stats) = progress.stats.lock()
        {
            stats.scan_had_errors = true;
            stats.scan_ms = stats.scan_ms.saturating_add(scan_ms);
            let index = stats.connectors.len();
            stats.connectors.push(ConnectorStats {
                name: kind.slug().to_string(),
                conversations: 0,
                messages: 0,
                scan_ms,
                error: Some(error.clone()),
            });
            if !stats
                .agents_discovered
                .iter()
                .any(|name| name == kind.slug())
            {
                stats.agents_discovered.push(kind.slug().to_string());
            }
            Some(index)
        } else {
            None
        };

        if active_sources_skipped > 0 {
            tracing::debug!(
                ?kind,
                scan_root = %root.path.display(),
                active_sources_skipped,
                "skipped active watch sources and preserved watermarks for retry"
            );
        }
        let preserve_this_watch_watermark = preserve_watch_watermark
            || preparse_active_source_skipped
            || active_sources_skipped > 0
            || scan_error.is_some()
            || source_ledger.unstable_sources
            || unproven_scanned_source;
        // A reused source is already certified by its unchanged publication
        // receipt. Preparse may defer its optional mirror capture while an
        // unchanged writer still holds it open; that must not strand replay
        // debt belonging to another, successfully recovered source.
        let recovery_scan_complete = (if source_scoped {
            !preserve_watch_watermark
                && active_sources_skipped == 0
                && scan_error.is_none()
                && !source_ledger.unstable_sources
                && !unproven_scanned_source
        } else {
            !preserve_this_watch_watermark
        }) && (!replay_canonical
            || (!skip_subagents_active() && WorkspaceExclusions::from_env()?.patterns.is_empty()));
        if source_scoped && (scan_error.is_some() || source_ledger.unstable_sources) {
            let storage = storage
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
            persist_watch_lexical_replay_debt(
                &storage,
                &WatchLexicalReplayDebt::for_discovery(kind, &root),
            )?;
            outcome.retry_sources.insert(root.path.clone());
        }

        record_connector_ingest_report(opts.progress.as_ref(), ingest_diagnostics.finish());
        if nomem_backoff_held > 0 {
            tracing::debug!(
                held = nomem_backoff_held,
                "watch conversations are waiting out their NoMem deferral backoff (GH#510)"
            );
        }

        // Update total and phase to indexing
        if let Some(p) = &opts.progress {
            p.total.fetch_add(convs.len(), Ordering::Relaxed);
            // Watch batches accumulate the total as sessions arrive; it is
            // never a stable denominator.
            p.total_is_final.store(false, Ordering::Relaxed);
            p.phase.store(2, Ordering::Relaxed);
            p.tick_activity();
        }

        let conv_count = convs.len();
        if explicit_watch_once {
            tracing::warn!(
                ?kind,
                scan_root = %root.path.display(),
                conversations = conv_count,
                since_ts,
                "watch_once_scan"
            );
        } else {
            tracing::info!(?kind, conversations = conv_count, since_ts, "watch_scan");
        }

        // Root replay debt can outlive its raw transcript. Its canonical rows
        // are still authoritative, even when discovery no longer emits a
        // packet carrying their IDs. Recover those rows before a clean census
        // may discharge the old obligation, including a nonempty sibling scan.
        // Waiting for a complete census avoids repeatedly replaying absent
        // history while an active or excluded sibling keeps the root open.
        let absent_source_recovery = if source_scoped
            && replay_canonical
            && recovery_scan_complete
            && nomem_backoff_held == 0
            && !explicit_watch_once
            && !persist::defer_lexical_updates_enabled()
        {
            let storage = storage
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
            recover_absent_watch_source_publications(
                opts,
                kind,
                &root,
                &pending_lexical_debt,
                recovery_source_coverage.as_ref(),
                None,
                &storage,
                t_index,
                index_path,
            )?
        } else {
            WatchAbsentSourceRecovery::default()
        };
        // An empty canonical identity or an existing source without completion
        // evidence cannot certify publication. Successful siblings must not
        // advance this incomplete root's watermarks or completion timestamp.
        let preserve_this_watch_watermark =
            preserve_this_watch_watermark || absent_source_recovery.unresolved_source;

        // An ordinary empty scan has nothing to ingest or publish (#194).
        // The recovery above can nevertheless have published canonical rows
        // whose raw source disappeared; only that actual work counts here.
        if conv_count == 0 {
            if nomem_backoff_held > 0 || absent_source_recovery.retry_unproven_source {
                // Holding the watermark is not enough: no source mutation
                // need follow a transient OOM or omitted source completion.
                // Keep the cooldown-driven retry for existing canonical work;
                // an empty canonical identity alone does not request a loop.
                outcome.retry_sources.insert(root.path.clone());
            }
            if scan_error.is_some() && !source_scoped {
                // A later successful sibling may advance the shared provider
                // watermark. Keep this source eligible for an unfiltered retry
                // even when it had no valid conversation to publish today.
                let storage = storage
                    .lock()
                    .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
                persist_watch_lexical_replay_debt(
                    &storage,
                    &WatchLexicalReplayDebt::for_root(kind, &root),
                )?;
            }
            if recovery_scan_complete {
                let storage = storage
                    .lock()
                    .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
                let selected_path = crate::normalize_path_identity(&root.path);
                for debt in pending_lexical_debt.iter().filter(|debt| {
                    debt.discovery_only
                        && crate::normalize_path_identity(&debt.path).starts_with(&selected_path)
                }) {
                    clear_watch_lexical_replay_debt(&storage, debt)?;
                }
                for index in &absent_source_recovery.completed_debts {
                    clear_watch_lexical_replay_debt(&storage, &pending_lexical_debt[*index])?;
                }
                if absent_source_recovery.published_conversations > 0
                    && watch_lexical_replay_debt_for_root(&storage, kind, &root)?.is_empty()
                {
                    persist::with_ephemeral_writer(
                        &storage,
                        false,
                        "recording recovered watch publication progress",
                        |writer| writer.set_last_indexed_at(FrankenStorage::now_millis()),
                    )?;
                }
            }
            outcome.indexed = outcome
                .indexed
                .saturating_add(absent_source_recovery.published_conversations);
            outcome.published_conversations = outcome
                .published_conversations
                .saturating_add(absent_source_recovery.published_conversations);
            continue;
        }

        // INGEST PHASE: Acquire locks briefly. Live watch mode persists in
        // bounded chunks so a large backlog can make durable forward progress
        // instead of retrying the same oversized transaction forever.
        let index_start = Instant::now();
        let mut inserted_messages = 0usize;
        let mut processed_conversations = 0usize;
        let mut quarantined_conversations = 0usize;
        let mut deferred_conversations = nomem_backoff_held;
        let mut lexical_replay_deferred = false;
        let mut published_conversations = absent_source_recovery.published_conversations;
        let mut remaining_conversations = conv_count;
        let current_debt = WatchLexicalReplayDebt::for_root(kind, &root);
        {
            let storage = storage
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
            let mut t_index_guard = t_index
                .lock()
                .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
            // Establish recovery debt BEFORE any canonical transaction. It
            // covers the whole selected source, including earlier successful
            // additions lost when a later addition discards this writer.
            source_ledger.begin_publication(&storage, &current_debt, None)?;
            if replay_canonical {
                // A prior failed callback may still own uncommitted additions.
                // Reconciliation must start from the published generation.
                *t_index_guard = None;
            }
        }
        let capture_semantic_delta = semantic_delta.is_some();
        let mut convs = convs.into_sorted_batches()?;
        let mut connector_name = None;
        let mut first_admitted_chunk = true;
        loop {
            let chunk_progress = if first_admitted_chunk {
                first_admitted_chunk = false;
                None
            } else {
                opts.progress.as_deref()
            };
            let mut chunk = convs.next_batch(chunk_progress)?;
            if chunk.is_empty() {
                break;
            }
            remaining_conversations = remaining_conversations.saturating_sub(chunk.len());
            if connector_name.is_none() {
                connector_name = chunk.first().map(|conv| conv.agent_slug.clone());
            }
            for conv in &mut chunk {
                capture_prepared_conversation(&opts.data_dir, conv, &raw_source_exclusions);
                if let Some(progress) = &opts.progress {
                    progress.tick_activity();
                }
            }
            let storage = storage
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
            let mut t_index_guard = t_index
                .lock()
                .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
            if let Some(pending) = load_lexical_rebuild_state(index_path)?
                && pending.is_incomplete()
                && pending.version == LEXICAL_REBUILD_STATE_VERSION
                && pending.schema_hash == crate::search::tantivy::SCHEMA_HASH
                && lexical_rebuild_page_size_is_compatible(pending.page_size)
                && lexical_rebuild_db_paths_match(
                    &pending.db.db_path,
                    &crate::normalize_path_identity(&opts.db_path).to_string_lossy(),
                )
            {
                // A previous metadata transaction may already have landed.
                // Its repeated packet then has zero mutations; the durable
                // debt, rather than this packet's counts, requires repair.
                *t_index_guard = None;
                rebuild_tantivy_from_db_deferred_startup_with_options(
                    &opts.db_path,
                    &opts.data_dir,
                    count_total_conversations_exact(&storage)?,
                    opts.progress.clone(),
                    None,
                )?;
            }
            if t_index_guard.is_none() {
                tracing::info!(
                    index_path = %index_path.display(),
                    "opening Tantivy lazily for watch ingest"
                );
                *t_index_guard = Some(open_lexical_index_with_heartbeat(
                    index_path,
                    opts.progress.as_ref(),
                )?);
            }
            let mut chunk_outcome = {
                let t_index = t_index_guard
                    .as_mut()
                    .expect("lazy watch index must be open before ingest");
                ingest_watch_batch_with_oom_split_inner(
                    &storage,
                    t_index,
                    &opts.data_dir,
                    &chunk,
                    &opts.progress,
                    !opts.watch,
                    capture_semantic_delta,
                    WatchOomIngestMode::Standard,
                    replay_canonical,
                )?
            };
            if persist::defer_lexical_updates_enabled() {
                chunk_outcome
                    .batch_outcome
                    .record_deferred_lexical_update(&anyhow::anyhow!(
                        "watch lexical publication is disabled by configuration"
                    ));
            } else if replay_canonical {
                // Reconcile once after the splitter has combined all IDs.
                // Overlapping packets must not stage the same replacement
                // twice against Quill's still-uncommitted snapshot.
                let replay = chunk_outcome
                    .batch_outcome
                    .canonical_conversation_ids
                    .iter()
                    .try_for_each(|id| {
                        persist::replay_watch_canonical_conversation(
                            &storage,
                            t_index_guard.as_mut().expect("watch writer is open"),
                            *id,
                        )
                    });
                if let Err(error) = replay {
                    if error_is_out_of_memory(&error) {
                        chunk_outcome
                            .batch_outcome
                            .record_deferred_lexical_update(&error);
                    } else {
                        return Err(error);
                    }
                }
            }
            inserted_messages =
                inserted_messages.saturating_add(chunk_outcome.batch_outcome.inserted_messages);
            processed_conversations =
                processed_conversations.saturating_add(chunk_outcome.processed_conversations);
            quarantined_conversations =
                quarantined_conversations.saturating_add(chunk_outcome.quarantined_conversations);
            deferred_conversations =
                deferred_conversations.saturating_add(chunk_outcome.deferred_conversations);
            if let Some(delta) = semantic_delta.as_deref_mut() {
                delta.extend_from_batch(
                    chunk_outcome.batch_outcome.semantic_delta_inputs,
                    chunk_outcome.batch_outcome.semantic_delta_max_message_id,
                );
            }

            // Commit each successful chunk before advancing the partial
            // watch watermark. A crash after this point replays at worst
            // the next unfinished chunk, not the entire backlog.
            let mut lexical_update_deferred = chunk_outcome.batch_outcome.lexical_update_deferred;
            let chunk_needs_publication = replay_canonical
                || chunk_outcome.batch_outcome.inserted_conversations > 0
                || chunk_outcome.batch_outcome.inserted_messages > 0
                || chunk_outcome.batch_outcome.updated_messages > 0
                || chunk_outcome.batch_outcome.workspace_changes > 0;
            if chunk_outcome.batch_outcome.workspace_changes > 0 {
                // Do not leave a live watcher serving old workspace filters.
                // The pre-mutation checkpoint remains pending if this fails.
                *t_index_guard = None;
                rebuild_tantivy_from_db_deferred_startup_with_options(
                    &opts.db_path,
                    &opts.data_dir,
                    count_total_conversations_exact(&storage)?,
                    opts.progress.clone(),
                    None,
                )?;
                *t_index_guard = Some(open_lexical_index_with_heartbeat(
                    index_path,
                    opts.progress.as_ref(),
                )?);
                lexical_update_deferred = false;
            }
            if lexical_update_deferred {
                lexical_replay_deferred = true;
                tracing::warn!(
                    error = ?chunk_outcome.batch_outcome.lexical_update_error,
                    "dropping uncommitted watch Tantivy writer after deferred lexical update"
                );
                *t_index_guard = None;
            } else {
                if chunk_needs_publication {
                    #[cfg(test)]
                    inject_watch_lexical_test_failure(WatchLexicalTestFault::Commit)?;
                    if let Err(error) = t_index_guard
                        .as_mut()
                        .expect("watch Tantivy writer must still be open before commit")
                        .commit_with_region_doc_stamps(storage.raw())
                    {
                        // A failed commit leaves every staged document —
                        // canonical AND region — inside this writer's
                        // uncommitted generation. Dropping the writer is the
                        // only way a later commit cannot certify work this
                        // chunk never shipped; replay debt already covers
                        // the republish (see 34507 / 34644 for the same
                        // discard pattern).
                        *t_index_guard = None;
                        return Err(error);
                    }
                    published_conversations = published_conversations.saturating_add(
                        chunk_outcome.batch_outcome.canonical_conversation_ids.len(),
                    );
                }
                if chunk_outcome.quarantined_conversations == 0
                    && chunk_outcome.deferred_conversations == 0
                {
                    // Each receipt follows all of its source's canonical and
                    // lexical commits. A failed earlier chunk cannot certify
                    // a multi-conversation source from its successful tail.
                    let selected_path = crate::normalize_path_identity(&root.path);
                    let publication_complete = !lexical_replay_deferred
                        && quarantined_conversations == 0
                        && deferred_conversations == 0
                        && !absent_source_recovery.unresolved_source
                        && (source_scoped || scan_error.is_none())
                        && (!replay_canonical || recovery_scan_complete);
                    let completed_debts: Vec<_> = if remaining_conversations == 0
                        && !lexical_replay_deferred
                        && quarantined_conversations == 0
                        && deferred_conversations == 0
                    {
                        pending_lexical_debt
                            .iter()
                            .chain(std::iter::once(&current_debt))
                            .filter(|debt| {
                                crate::normalize_path_identity(&debt.path)
                                    .starts_with(&selected_path)
                                    && if debt.discovery_only {
                                        recovery_scan_complete
                                    } else {
                                        publication_complete
                                    }
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                    source_ledger.publish_batch(
                        &storage,
                        &chunk,
                        &completed_debts,
                        // The canonical census can retain this known unresolved
                        // source even when every healthy sibling reuses its
                        // receipt. Do not force a sibling's history through
                        // publication again merely to keep that debt alive.
                        remaining_conversations == 0
                            && !publication_complete
                            && !absent_source_recovery.unresolved_source,
                    )?;
                }
            }

            // Conversations are sorted by watermark, so a clean later
            // chunk must not advance past an earlier (or backed-off)
            // deferred one, which would drop it from every later scan:
            // the cumulative deferral count gates this. Quarantined
            // conversations keep the per-chunk gate: they have no backoff,
            // so holding the watermark for them would re-attempt them on
            // every tick.
            if !explicit_watch_once
                && !preserve_this_watch_watermark
                && !lexical_replay_deferred
                && chunk_outcome.quarantined_conversations == 0
                && deferred_conversations == 0
                && let Some(ts_val) = chunk_outcome.max_payload_watermark_ms
            {
                save_watch_state_watermark(&opts.data_dir, state, kind, ts_val)?;
            } else if chunk_outcome.quarantined_conversations > 0
                || chunk_outcome.deferred_conversations > 0
            {
                tracing::info!(
                    ?kind,
                    quarantined_conversations = chunk_outcome.quarantined_conversations,
                    deferred_conversations = chunk_outcome.deferred_conversations,
                    "preserving partial watch watermark so quarantined/deferred source can be retried"
                );
            } else if preserve_this_watch_watermark {
                tracing::debug!(
                    ?kind,
                    active_sources_skipped,
                    "preserving partial watch watermark because a source failed or was excluded"
                );
            }
            if let Some(progress) = &opts.progress {
                progress.tick_activity();
                progress.check_stop()?;
            }
        }
        {
            let storage = storage
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
            if published_conversations > 0
                && !lexical_replay_deferred
                && scan_error.is_none()
                && quarantined_conversations == 0
                && deferred_conversations == 0
                && !preserve_this_watch_watermark
            {
                persist::with_ephemeral_writer(
                    &storage,
                    false,
                    "recording watch publication progress",
                    |writer| writer.set_last_indexed_at(FrankenStorage::now_millis()),
                )?;
            }
        }
        let index_ms = index_start.elapsed().as_millis() as u64;

        if let Some(p) = &opts.progress
            && let Ok(mut stats) = p.stats.lock()
        {
            let connector_name =
                connector_name.unwrap_or_else(|| format!("{kind:?}").to_ascii_lowercase());
            if failed_stats_index.is_none() {
                stats.scan_ms = stats.scan_ms.saturating_add(scan_ms);
            }
            stats.index_ms = stats.index_ms.saturating_add(index_ms);
            stats.total_conversations = stats.total_conversations.saturating_add(conv_count);
            stats.total_messages = stats.total_messages.saturating_add(inserted_messages);
            let connector_stats = ConnectorStats {
                name: connector_name.clone(),
                conversations: conv_count,
                messages: inserted_messages,
                scan_ms,
                error: scan_error,
            };
            if let Some(index) = failed_stats_index {
                stats.connectors[index] = connector_stats;
            } else {
                stats.connectors.push(connector_stats);
            }
            if !stats
                .agents_discovered
                .iter()
                .any(|name| name == &connector_name)
            {
                stats.agents_discovered.push(connector_name);
            }
        }

        if quarantined_conversations > 0 {
            tracing::warn!(
                ?kind,
                quarantined_conversations,
                "watch ingest skipped poison conversations after OOM bisection"
            );
        }

        // Track total processed for stale detection. This intentionally
        // follows the old return shape (scanned conversations, not newly
        // inserted rows) while including quarantined single-conversation OOMs
        // as forward progress so watch mode does not wedge on a poison file.
        outcome.indexed = outcome
            .indexed
            .saturating_add(processed_conversations)
            .saturating_add(absent_source_recovery.published_conversations);
        outcome.published_conversations = outcome
            .published_conversations
            .saturating_add(published_conversations);
        if lexical_replay_deferred
            || deferred_conversations > 0
            || (absent_source_recovery.retry_unproven_source && quarantined_conversations == 0)
        {
            // An Ok callback consumes its triggering event. Retained durable
            // debt must also remain in the live pending queue, or a transient
            // publication/NoMem failure waits forever for an unrelated event.
            // Exclusions and quarantines alone do not schedule retry loops.
            outcome.retry_sources.insert(root.path.clone());
        }

        // Explicit watch-once imports are one-shot recovery/replay work, not
        // live daemon watch progress. Do not let them advance the persistent
        // watch_state high-water marks that steady-state watch mode consumes.
        if !explicit_watch_once
            && conv_count > 0
            && !preserve_this_watch_watermark
            && quarantined_conversations == 0
            && deferred_conversations == 0
            && !lexical_replay_deferred
            && let Some(ts_val) = max_ts
        {
            // Once every chunk for this trigger has persisted without poison
            // quarantine, advance to the filesystem event high-water mark.
            // Partial per-chunk updates above deliberately use payload
            // timestamps so an unexpected later failure cannot hide
            // unprocessed conversations that share the same source file.
            save_watch_state_watermark(&opts.data_dir, state, kind, ts_val)?;
        } else if !explicit_watch_once
            && (quarantined_conversations > 0 || deferred_conversations > 0)
        {
            tracing::info!(
                ?kind,
                quarantined_conversations,
                deferred_conversations,
                "preserving final watch watermark so quarantined/deferred source can be retried"
            );
        } else if !explicit_watch_once && conv_count > 0 && preserve_this_watch_watermark {
            tracing::info!(
                ?kind,
                active_sources_skipped,
                "preserving final watch watermark because a source failed or was excluded"
            );
        }
    }

    // Reset phase to idle if progress exists
    reset_progress_to_idle(opts.progress.as_ref());

    match first_source_scan_error {
        Some(error) => Err(ConnectorScanFailure(error).into()),
        None => Ok(outcome),
    }
}

pub(in crate::indexer) fn explicit_watch_once_root_unchanged_after_last_index(
    storage: &FrankenStorage,
    kind: ConnectorKind,
    root: &ScanRoot,
) -> Result<bool> {
    if !watch_lexical_replay_debt_for_root(storage, kind, root)?.is_empty() {
        return Ok(false);
    }
    let metadata = match fs::metadata(&root.path) {
        Ok(metadata) if metadata.is_file() => metadata,
        _ => return Ok(false),
    };
    let Some(modified_at_ms) = metadata
        .modified()
        .ok()
        .and_then(system_time_to_epoch_millis)
    else {
        return Ok(false);
    };
    let Some(last_indexed_at) = storage.get_last_indexed_at()? else {
        return Ok(false);
    };
    if modified_at_ms > last_indexed_at {
        return Ok(false);
    }
    // A database-backed source commits into its -wal (or rollback -journal)
    // without touching the main file, so the main file's mtime alone cannot
    // prove the source unchanged (GH #502). -shm is excluded: readers rewrite
    // it, and it holds no committed data.
    for suffix in ["-wal", "-journal"] {
        let sidecar = database_path_with_suffix(&root.path, suffix);
        let sidecar_modified_at_ms = match fs::metadata(&sidecar) {
            Ok(metadata) => metadata
                .modified()
                .ok()
                .and_then(system_time_to_epoch_millis),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Ok(false),
        };
        match sidecar_modified_at_ms {
            Some(sidecar_modified_at_ms) if sidecar_modified_at_ms <= last_indexed_at => {}
            _ => return Ok(false),
        }
    }

    let source_path = root.path.to_string_lossy();
    let matches: Vec<i64> = storage
        .raw()
        .query_map_collect(
            "SELECT id
             FROM conversations
             WHERE source_id = ?1 AND source_path = ?2
             LIMIT 1",
            &[
                ParamValue::from(root.origin.source_id.as_str()),
                ParamValue::from(source_path.as_ref()),
            ],
            |row| row.get_typed(0),
        )
        .with_context(|| {
            format!(
                "checking indexed source path freshness for {}",
                root.path.display()
            )
        })?;
    Ok(!matches.is_empty())
}
