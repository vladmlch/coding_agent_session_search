use super::*;
use rayon::prelude::*;

pub(in crate::indexer) type ConnectorFactory = fn() -> Box<dyn Connector + Send>;

pub(in crate::indexer) fn configured_connector_factories() -> Vec<(&'static str, ConnectorFactory)>
{
    filter_disabled_connector_factories(get_connector_factories())
}

pub(in crate::indexer) fn filter_disabled_connector_factories<F>(
    connector_factories: Vec<(&'static str, F)>,
) -> Vec<(&'static str, F)> {
    if dotenvy::var("CASS_IGNORE_SOURCES_CONFIG").is_ok() {
        return connector_factories;
    }

    let config = match SourcesConfig::load() {
        Ok(config) => config,
        Err(error) => {
            tracing::debug!(
                error = %error,
                "failed to load sources config while filtering disabled connectors"
            );
            return connector_factories;
        }
    };

    let disabled_agents = config.configured_disabled_agents();
    if disabled_agents.is_empty() {
        return connector_factories;
    }

    let filtered = connector_factories
        .into_iter()
        .filter(|(name, _)| !config.is_agent_disabled(name))
        .collect::<Vec<_>>();

    tracing::info!(
        disabled_agents = ?disabled_agents,
        enabled_connectors = filtered.len(),
        "skipping disabled connectors from indexing configuration"
    );

    filtered
}

/// Whether the incremental REGION_VERSION re-projection pass is enabled.
/// Off for watch runs (their chunks converge per event) and for
/// `--watch-once` paths; `CASS_REGION_REPROJECT=0` is the explicit
/// operator off switch.
pub(in crate::indexer) fn region_reproject_enabled(opts: &IndexOptions) -> bool {
    if opts.watch || opts.watch_once_paths.is_some() {
        return false;
    }
    dotenvy::var("CASS_REGION_REPROJECT")
        .map(|value| value.trim() != "0")
        .unwrap_or(true)
}

/// Run the incremental extension-version re-projection lane.
///
/// After a REGION_VERSION (or facts-extractor / projection) bump, the
/// sessions below the current versions converge by re-projecting them
/// from the PERSISTED archive through the same ordered per-conversation
/// lane the bridge backfill uses ([`backfill::backfill_viewer_extensions`]):
/// canonical conversation+message+snippet hydration (message extras
/// included — raw tool records, response parts and other identity
/// provenance live in the extras columns), facts extraction, projection,
/// region build and region-doc skip-key publication — all bounded
/// (one savepoint per conversation, batched commits) and idempotent
/// (the marks-aware generation skip-key republishes only what changed).
///
/// NO source re-scan is involved: the stale set comes from one indexed
/// detection query over the extension tables (marks/facts/projection
/// below the current versions), so a session whose source file is gone
/// converges exactly like a live one and the Stage-1 marks gate lifts
/// for the whole archive.
///
/// The lane runs BEFORE the scan-side drain decisions and converges
/// its OWN region docs: the caller drains when `reprojected > 0`.
/// A crash between the lane and that drain leaves honest
/// DOCS_PENDING marks that the next run's detection (marks at the
/// current version are not stale) plus the existing drain machinery
/// converge.
///
/// `Ok(None)` when the lane cannot run: watch, env-disabled, or an
/// archive without the extension profile (a fresh archive's first
/// ingest installs it — nothing to converge toward yet). An EMPTY
/// stale set still returns `Some(RegionReprojectStats::default())` so
/// a steady run reports the zero-work plan.
pub(in crate::indexer) fn run_region_reprojection_lane(
    storage: &FrankenStorage,
    opts: &IndexOptions,
) -> Result<Option<RegionReprojectStats>> {
    if !region_reproject_enabled(opts) {
        return Ok(None);
    }
    let db = storage.raw();
    if !crate::viewer_extensions::search_regions::has_region_marks_table(db)? {
        return Ok(None);
    }
    let stale_ids =
        crate::viewer_extensions::search_regions::region_reproject_stale_session_ids(db)?;
    if stale_ids.is_empty() {
        return Ok(Some(RegionReprojectStats::default()));
    }
    tracing::info!(
        stale_sessions = stale_ids.len(),
        "region re-projection lane: replaying stale sessions from the persisted archive"
    );
    let backfill_stats =
        persist::with_ephemeral_writer(storage, true, "region re-projection lane", |writer| {
            let connection = writer.raw();
            // Additive idempotent schema ensure: an archive whose
            // profile predates the current extension schema upgrades
            // here, before any publication depends on the new columns.
            crate::viewer_extensions::schema::ensure_profile_for_ingest(connection)?;
            let stats = crate::viewer_extensions::backfill::backfill_viewer_extensions(
                connection,
                &crate::viewer_extensions::backfill::BackfillOptions {
                    selection:
                        crate::viewer_extensions::backfill::BackfillSelection::ConversationIds(
                            stale_ids.clone(),
                        ),
                    ..crate::viewer_extensions::backfill::BackfillOptions::default()
                },
                |stats| {
                    tracing::info!(
                        scanned = stats.conversations_scanned,
                        published = stats.facts_published,
                        skipped = stats.facts_skipped_unchanged,
                        failed = stats.failures,
                        "region re-projection lane progress"
                    );
                },
            )?;
            // One coherent post-publication journal pass, mirroring the
            // bridge backfill entry's publication contract.
            crate::viewer_extensions::changes::notify_committed_change_best_effort(connection);
            Ok(stats)
        })?;
    Ok(Some(RegionReprojectStats {
        stale_sessions: stale_ids.len(),
        reprojected: backfill_stats.generations_republished,
        // The outer receipt+markers skip — conversations the lane
        // visited without republishing anything (a receipt current
        // while a row drifted). Failures already counted above.
        skipped_unchanged: backfill_stats
            .conversations_scanned
            .saturating_sub(backfill_stats.generations_republished)
            .saturating_sub(backfill_stats.failures),
        failed: backfill_stats.failures,
    }))
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn run_streaming_index_with_connector_factories<F>(
    storage: &FrankenStorage,
    t_index: Option<&mut TantivyIndex>,
    opts: &IndexOptions,
    since_ts: Option<i64>,
    lexical_strategy: LexicalPopulationStrategy,
    additional_scan_roots: Vec<ScanRoot>,
    local_connector_roots: LocalConnectorRootsOverride,
    connector_factories: Vec<(&'static str, F)>,
    scan_start_ts: Option<i64>,
    progress_bump: Option<&Arc<AtomicI64>>,
) -> Result<NonWatchIngestOutcome>
where
    F: Fn() -> Box<dyn Connector + Send> + Send + Sync + 'static,
{
    if connector_factories.is_empty() {
        tracing::warn!("no enabled connectors are configured for indexing; skipping scan");
        if let Some(p) = &opts.progress {
            p.phase.store(1, Ordering::Relaxed);
            p.total.store(0, Ordering::Relaxed);
            p.current.store(0, Ordering::Relaxed);
            p.discovered_agents.store(0, Ordering::Relaxed);
            if let Ok(mut names) = p.discovered_agent_names.lock() {
                names.clear();
            }
        }
        return Ok(NonWatchIngestOutcome::default());
    }

    let buffered_connectors: Vec<&'static str> = connector_factories
        .iter()
        .filter_map(|(name, factory)| {
            let connector = factory();
            (!connector.supports_streaming_scan()).then_some(*name)
        })
        .collect();
    let num_connectors = connector_factories.len();

    if !buffered_connectors.is_empty() {
        tracing::warn!(
            connectors = ?buffered_connectors,
            "streaming index still has buffered connectors that do not implement callback streaming"
        );
    }

    // Set up progress tracking
    if let Some(p) = &opts.progress {
        p.phase.store(1, Ordering::Relaxed); // Scanning
        p.total.store(num_connectors, Ordering::Relaxed);
        p.current.store(0, Ordering::Relaxed);
        // The connector count is a stable denominator for the scan phase.
        p.total_is_final.store(true, Ordering::Relaxed);
        p.discovered_agents.store(0, Ordering::Relaxed);
        if let Ok(mut names) = p.discovered_agent_names.lock() {
            names.clear();
        }
    }

    // Create bounded channel for backpressure
    let (tx, rx) = bounded::<IndexMessage>(STREAMING_CHANNEL_SIZE);
    let producer_config = StreamingProducerConfig {
        raw_source_exclusions: Arc::new(RawSourceExclusions::load(storage)?),
        source_ledger: Arc::new(
            if matches!(
                lexical_strategy,
                LexicalPopulationStrategy::InlineRebuildFromScan
            ) {
                HashMap::new()
            } else {
                storage.source_ingest_ledger_entries()?
            },
        ),
        flow_limiter: Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
        data_dir: opts.data_dir.clone(),
        additional_scan_roots: additional_scan_roots.clone(),
        local_connector_roots: local_connector_roots.clone(),
        since_ts,
        local_since_ts_by_connector: Arc::new(connector_local_scan_since_ts_map(
            storage,
            since_ts,
            &connector_factories,
        )?),
        progress: opts.progress.clone(),
        active_source_filter: Arc::new(ActiveSessionSourceFilter::new(
            opts.watch && opts.watch_once_paths.as_ref().is_none_or(Vec::is_empty),
        )),
    };

    // Spawn producer threads for each connector
    let handles: Vec<(&'static str, JoinHandle<()>)> = connector_factories
        .into_iter()
        .map(|(name, factory)| {
            (
                name,
                spawn_connector_producer(name, factory, tx.clone(), producer_config.clone()),
            )
        })
        .collect();

    // Drop our copy of the sender so channel closes when all producers finish
    drop(tx);

    // Run consumer on main thread
    let consumer_result = run_streaming_consumer(
        rx,
        num_connectors,
        storage,
        &opts.data_dir,
        t_index,
        producer_config.flow_limiter.clone(),
        &opts.progress,
        lexical_strategy,
        // The scan entry's lane gate, verbatim: watch keeps per-event
        // region-doc convergence because a watch run has no final drain.
        !opts.watch && opts.watch_once_paths.is_none(),
        scan_start_ts,
        progress_bump,
    );

    if consumer_result.is_err() {
        producer_config.flow_limiter.close();
    }

    let mut join_errors = Vec::new();
    for (name, handle) in handles {
        if let Err(payload) = handle.join() {
            let panic_message = panic_payload_message(payload);
            tracing::error!(connector = name, panic = %panic_message, "streaming producer panicked");
            join_errors.push(format!("{name}: {panic_message}"));
        }
    }

    if let Err(error) = consumer_result {
        if !join_errors.is_empty() {
            let combined = format!(
                "{error}; streaming producer thread panicked: {}",
                join_errors.join("; ")
            );
            set_progress_last_error(opts.progress.as_ref(), Some(combined.clone()));
            return Err(anyhow::anyhow!(combined));
        }
        set_progress_last_error(opts.progress.as_ref(), Some(error.to_string()));
        return Err(error);
    }

    if !join_errors.is_empty() {
        let error = format!(
            "streaming producer thread panicked: {}",
            join_errors.join("; ")
        );
        set_progress_last_error(opts.progress.as_ref(), Some(error.clone()));
        return Err(anyhow::anyhow!(error));
    }

    let (discovered_names, mut ingest_outcome) = match consumer_result {
        Ok(result) => result,
        Err(_) => unreachable!("handled above"),
    };
    let active_deferred = producer_config.active_source_filter.take_deferred_sources();
    record_deferred_active_sources(opts.progress.as_ref(), &active_deferred);
    ingest_outcome.deferred_sources.extend(active_deferred);

    // Update discovered agent names in progress tracker
    if let Some(p) = &opts.progress
        && let Ok(mut names) = p.discovered_agent_names.lock()
    {
        names.extend(discovered_names);
    }

    Ok(ingest_outcome)
}

/// Run indexing using original batch collection architecture.
///
/// This uses rayon's par_iter to scan all connectors in parallel, collecting
/// all conversations into memory before ingesting. This is the fallback when
/// streaming is disabled via CASS_STREAMING_INDEX=0.
#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn run_batch_index(
    storage: &FrankenStorage,
    t_index: Option<&mut TantivyIndex>,
    opts: &IndexOptions,
    since_ts: Option<i64>,
    lexical_strategy: LexicalPopulationStrategy,
    additional_scan_roots: Vec<ScanRoot>,
    local_connector_roots: LocalConnectorRootsOverride,
    scan_start_ts: Option<i64>,
    progress_bump: Option<&Arc<AtomicI64>>,
) -> Result<NonWatchIngestOutcome> {
    run_batch_index_with_connector_factories(
        storage,
        t_index,
        opts,
        since_ts,
        lexical_strategy,
        additional_scan_roots,
        local_connector_roots,
        filter_disabled_connector_factories(
            crate::connectors::get_connector_factories_for_archive(&opts.data_dir),
        ),
        scan_start_ts,
        progress_bump,
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn run_batch_index_with_connector_factories<F>(
    storage: &FrankenStorage,
    mut t_index: Option<&mut TantivyIndex>,
    opts: &IndexOptions,
    since_ts: Option<i64>,
    lexical_strategy: LexicalPopulationStrategy,
    additional_scan_roots: Vec<ScanRoot>,
    local_connector_roots: LocalConnectorRootsOverride,
    connector_factories: Vec<(&'static str, F)>,
    scan_start_ts: Option<i64>,
    progress_bump: Option<&Arc<AtomicI64>>,
) -> Result<NonWatchIngestOutcome>
where
    F: Fn() -> Box<dyn Connector + Send> + Send + Sync + 'static,
{
    let (boundary, fallback): (Vec<_>, Vec<_>) = connector_factories
        .into_iter()
        .partition(|(_, factory)| factory().supports_source_boundaries());
    if !boundary.is_empty() {
        let completed = run_streaming_index_with_connector_factories(
            storage,
            t_index.as_deref_mut(),
            opts,
            since_ts,
            lexical_strategy,
            additional_scan_roots.clone(),
            local_connector_roots.clone(),
            boundary,
            scan_start_ts,
            progress_bump,
        )?;
        if fallback.is_empty() {
            return Ok(completed);
        }
        // The fallback pass replaces its phase-local progress statistics.
        // Retain only the fields it replaces: source outcomes, diagnostics,
        // quarantine counts and deferred flags already accumulate in place.
        let boundary_stats = opts.progress.as_ref().and_then(|progress| {
            let stats = progress.stats.lock().ok()?;
            Some((
                stats.scan_ms,
                stats.index_ms,
                stats.total_conversations,
                stats.total_messages,
                stats.connectors.clone(),
                stats.agents_discovered.clone(),
                progress.current.load(Ordering::Relaxed),
            ))
        });
        let remaining = run_batch_index_with_connector_factories(
            storage,
            t_index,
            opts,
            since_ts,
            lexical_strategy,
            additional_scan_roots,
            local_connector_roots,
            fallback,
            scan_start_ts,
            progress_bump,
        );
        if let (
            Some(progress),
            Some((
                scan_ms,
                index_ms,
                conversations,
                messages,
                connectors,
                agents,
                completed_current,
            )),
        ) = (opts.progress.as_ref(), boundary_stats)
        {
            let merged_names = if let Ok(mut stats) = progress.stats.lock() {
                // On error the fallback has not published its final stats;
                // these fields still contain the boundary pass. Adding them
                // again would double-count already committed work.
                if remaining.is_ok() {
                    stats.scan_ms = stats.scan_ms.saturating_add(scan_ms);
                    stats.index_ms = stats.index_ms.saturating_add(index_ms);
                    stats.total_conversations =
                        stats.total_conversations.saturating_add(conversations);
                    stats.total_messages = stats.total_messages.saturating_add(messages);
                    stats.connectors.extend(connectors);
                    stats
                        .connectors
                        .sort_by(|left, right| left.name.cmp(&right.name));
                    stats.agents_discovered.extend(agents);
                    stats.agents_discovered.sort();
                    stats.agents_discovered.dedup();
                }
                Some((stats.agents_discovered.clone(), stats.total_conversations))
            } else {
                None
            };
            if let Some((names, total)) = merged_names {
                if let Ok(mut discovered) = progress.discovered_agent_names.lock() {
                    if remaining.is_ok() {
                        *discovered = names;
                    } else {
                        discovered.extend(names);
                        discovered.sort();
                        discovered.dedup();
                    }
                    progress
                        .discovered_agents
                        .store(discovered.len(), Ordering::Relaxed);
                }
                if remaining.is_ok() {
                    progress
                        .phase
                        .store(INDEX_PHASE_LEXICAL_INDEXING, Ordering::Relaxed);
                    progress.total.store(total, Ordering::Relaxed);
                    progress
                        .current
                        .fetch_add(completed_current, Ordering::Relaxed);
                    progress.total_is_final.store(true, Ordering::Relaxed);
                } else {
                    // Keep a failing scan phase in connector units. Only an
                    // ingest-phase counter can include prior conversations.
                    if progress.phase.load(Ordering::Relaxed) == INDEX_PHASE_LEXICAL_INDEXING {
                        progress.total.fetch_add(conversations, Ordering::Relaxed);
                        progress
                            .current
                            .fetch_add(completed_current, Ordering::Relaxed);
                    }
                    progress.total_is_final.store(false, Ordering::Relaxed);
                }
            }
        }
        return remaining.map(|remaining| completed.accumulate(remaining));
    }
    let connector_factories = fallback;
    let scan_start = std::time::Instant::now();

    // First pass: Scan all to get counts if we have progress tracker
    // Use parallel iteration for faster agent discovery
    if let Some(p) = &opts.progress {
        p.phase.store(1, Ordering::Relaxed); // Scanning
        // Track connector scan progress during discovery.
        p.total.store(connector_factories.len(), Ordering::Relaxed);
        p.current.store(0, Ordering::Relaxed);
        // The connector count is a stable denominator for the scan phase.
        p.total_is_final.store(true, Ordering::Relaxed);
        p.discovered_agents.store(0, Ordering::Relaxed);
        if let Ok(mut names) = p.discovered_agent_names.lock() {
            names.clear();
        }
    }

    // Run connector detection and scanning in parallel using rayon
    // Optimization 2.2: Eliminate mutex lock contention on discovered_agent_names
    // by collecting names after the parallel phase instead of locking inside par_iter.
    use rayon::prelude::*;

    struct PendingBatchScan {
        name: &'static str,
        convs: Vec<NormalizedConversation>,
        is_discovered: bool,
        scan_succeeded: bool,
        active_source_skipped: bool,
        scan_error: Option<String>,
    }

    let progress_ref = opts.progress.as_ref();
    if let Some(progress) = progress_ref {
        progress.check_stop()?;
    }
    let data_dir = opts.data_dir.clone();
    let raw_source_exclusions = RawSourceExclusions::load(storage)?;
    let active_source_filter = Arc::new(ActiveSessionSourceFilter::new(
        opts.watch && opts.watch_once_paths.as_ref().is_none_or(Vec::is_empty),
    ));
    let local_since_ts_by_connector = Arc::new(connector_local_scan_since_ts_map(
        storage,
        since_ts,
        &connector_factories,
    )?);
    let scan_progress_tick: Option<Arc<dyn Fn() + Send + Sync>> =
        opts.progress.as_ref().map(|progress| {
            let progress = Arc::clone(progress);
            Arc::new(move || progress.tick_activity()) as Arc<dyn Fn() + Send + Sync>
        });

    // Keep scan completion state with each connector so watermarks are only
    // advanced for connectors whose full scan scope completed successfully.
    let pending_batches: Vec<PendingBatchScan> =
        connector_factories
            .into_par_iter()
            .filter_map(|(name, factory)| {
                let _raw_mirror_io = observe_index_raw_mirror_io(progress_ref);
                if progress_ref.is_some_and(|progress| progress.stop_requested()) {
                    return None;
                }
                let conn = factory();
                let detect = detect_for_local_scan(&local_connector_roots, || conn.detect());
                let was_detected = detect.as_ref().is_some_and(|result| result.detected);
                let mut convs = Vec::new();
                let mut is_discovered = false;
                let mut scan_succeeded = true;
                let mut active_source_skipped = false;
                let mut scan_errors = Vec::new();
                let with_scan_tick = |ctx: crate::connectors::ScanContext| {
                    match &scan_progress_tick {
                        Some(tick) => ctx.with_progress_tick(Arc::clone(tick)),
                        None => ctx,
                    }
                };

                // qu81y closed-world override: see the streaming lane — same
                // semantics for the batch path.
                let override_roots: Option<Vec<ScanRoot>> = local_connector_roots
                    .as_ref()
                    .map(|map| {
                        map.get(name)
                            .map(|roots| roots.iter().cloned().map(ScanRoot::local).collect())
                            .unwrap_or_default()
                    });
                let scan_local = match &override_roots {
                    Some(roots) => !roots.is_empty(),
                    None => detect.as_ref().is_some_and(|result| result.detected),
                };
                if scan_local {
                    // Update discovered agents count immediately when detected
                    // This gives fast UI feedback during the discovery phase
                    // Note: AtomicUsize has no contention, only the mutex was problematic
                    if let Some(p) = progress_ref {
                        p.discovered_agents.fetch_add(1, Ordering::Relaxed);
                    }
                    is_discovered = true;

                    let local_since_ts = local_since_ts_by_connector
                        .get(name)
                        .copied()
                        .unwrap_or(since_ts);
                    let ctx = with_scan_tick(match &override_roots {
                        Some(roots) => crate::connectors::ScanContext::with_roots(
                            data_dir.clone(),
                            roots.clone(),
                            local_since_ts,
                        ),
                        None => crate::connectors::ScanContext::local_default(
                            data_dir.clone(),
                            local_since_ts,
                        ),
                    });
                    let fallback_roots: Vec<ScanRoot> = match &override_roots {
                        Some(roots) => roots.clone(),
                        None => detect
                            .as_ref()
                            .map(|result| {
                                result
                                    .root_paths
                                    .iter()
                                    .cloned()
                                    .map(ScanRoot::local)
                                    .collect()
                            })
                            .unwrap_or_default(),
                    };
                    let (mut ingest_diagnostics, preparse_active_source_skipped) =
                        capture_connector_sources_before_parse(
                        conn.as_ref(),
                        &ctx,
                        &data_dir,
                        name,
                        &fallback_roots,
                        local_since_ts,
                        active_source_filter.as_ref(),
                        &raw_source_exclusions,
                    );
                    active_source_skipped |= preparse_active_source_skipped;
                    let local_origin = Origin::local();
                    // GH #511: the callback can yield good chats before
                    // reporting another transcript's failure. Keep those
                    // chats and retain the failure to hold the watermark.
                    if let Err(e) = conn.scan_with_callback(&ctx, &mut |mut conversation| {
                        if should_skip_active_session_source(
                            active_source_filter.as_ref(),
                            SourceKind::Local,
                            &conversation.source_path,
                        ) {
                            active_source_skipped = true;
                            return Ok(());
                        }
                        if should_skip_subagent(&conversation)
                            || should_skip_excluded_source(&conversation.source_path)
                        {
                            return Ok(());
                        }
                        ingest_diagnostics.observe_conversation(&mut conversation);
                        prepare_conversation_for_ingest(
                            &data_dir,
                            name,
                            &local_origin,
                            None,
                            &mut conversation,
                            &raw_source_exclusions,
                        );
                        convs.push(conversation);
                        Ok(())
                    }) {
                        // Note: agent was counted as discovered but scan failed
                        // This is acceptable as detection succeeded (agent exists)
                        scan_succeeded = false;
                        // Name the root the connector scanned, not the
                        // cass data dir (GH #511).
                        let scanned_root = fallback_roots
                            .first()
                            .map_or(ctx.data_dir.as_path(), |root| root.path.as_path());
                        ingest_diagnostics.observe_connector_scan_error(scanned_root, &e);
                        scan_errors.push(format!("{e:#}"));
                        tracing::warn!("scan failed for {}: {}", name, e);
                    }
                    record_connector_ingest_report(
                        progress_ref,
                        ingest_diagnostics.finish(),
                    );
                }

                if !additional_scan_roots.is_empty() {
                    for root in &additional_scan_roots {
                        if !mirrored_root_may_hold(name, root) {
                            continue;
                        }
                        let local_since_ts = local_since_ts_by_connector
                            .get(name)
                            .copied()
                            .unwrap_or(since_ts);
                        let root_since_ts =
                            explicit_scan_root_since_ts(root, &data_dir, local_since_ts);
                        if since_ts.is_some() && root_since_ts.is_none() {
                            tracing::debug!(
                                connector = name,
                                source_id = %root.origin.source_id,
                                root = %root.path.display(),
                                "configured scan root is using a full root scan so already-mirrored sessions can be promoted to canonical"
                            );
                        }
                        let ctx = with_scan_tick(crate::connectors::ScanContext::with_roots(
                            root.path.clone(),
                            vec![root.clone()],
                            root_since_ts,
                        ));
                        let (mut ingest_diagnostics, preparse_active_source_skipped) =
                            capture_connector_sources_before_parse(
                            conn.as_ref(),
                            &ctx,
                            &data_dir,
                            name,
                            std::slice::from_ref(root),
                            root_since_ts,
                            active_source_filter.as_ref(),
                            &raw_source_exclusions,
                        );
                        active_source_skipped |= preparse_active_source_skipped;
                        if let Err(e) = conn.scan_with_callback(&ctx, &mut |mut conversation| {
                            if should_skip_active_session_source(
                                active_source_filter.as_ref(),
                                root.origin.kind,
                                &conversation.source_path,
                            ) {
                                active_source_skipped = true;
                                return Ok(());
                            }
                            if should_skip_subagent(&conversation)
                                || should_skip_excluded_source(&conversation.source_path)
                            {
                                return Ok(());
                            }
                            ingest_diagnostics.observe_conversation(&mut conversation);
                            prepare_conversation_for_ingest(
                                &data_dir,
                                name,
                                &root.origin,
                                Some(root),
                                &mut conversation,
                                &raw_source_exclusions,
                            );
                            convs.push(conversation);
                            Ok(())
                        }) {
                            scan_succeeded = false;
                            ingest_diagnostics
                                .observe_connector_scan_error(&root.path, &e);
                            scan_errors.push(format!(
                                "remote scan failed for {}: {e:#}",
                                root.path.display(),
                            ));
                            tracing::warn!(
                                connector = name,
                                root = %root.path.display(),
                                "remote scan failed: {e}"
                            );
                        }
                        record_connector_ingest_report(
                            progress_ref,
                            ingest_diagnostics.finish(),
                        );
                    }
                }

                // Agent discovered via remote scan (wasn't detected locally but has conversations)
                if !was_detected && !convs.is_empty() {
                    if let Some(p) = progress_ref {
                        p.discovered_agents.fetch_add(1, Ordering::Relaxed);
                    }
                    is_discovered = true;
                }

                // Mark this connector as scanned for discovery progress.
                if let Some(p) = progress_ref {
                    p.current.fetch_add(1, Ordering::Relaxed);
                }

                if convs.is_empty() && !is_discovered && scan_succeeded {
                    return None;
                }

                tracing::info!(
                    connector = name,
                    conversations = convs.len(),
                    discovered = is_discovered,
                    scan_succeeded,
                    "batch_scan_complete"
                );
                let scan_error = (!scan_errors.is_empty()).then(|| scan_errors.join("; "));
                Some(PendingBatchScan {
                    name,
                    convs,
                    is_discovered,
                    scan_succeeded,
                    active_source_skipped,
                    scan_error,
                })
            })
            .collect();

    // A non-streaming connector may not expose cancellation while parsing.
    // Honor a signal before any of its collected conversations are persisted.
    if let Some(progress) = &opts.progress {
        progress.check_stop()?;
    }
    // Post-parallel phase: collect discovered agent names with single mutex lock
    // This eliminates O(connectors) mutex acquisitions during parallel execution
    let scan_ms = scan_start.elapsed().as_millis() as u64;

    let discovered_names: Vec<String> = pending_batches
        .iter()
        .filter(|pending| pending.is_discovered)
        .map(|pending| pending.name.to_string())
        .collect();
    let scan_had_errors = pending_batches
        .iter()
        .any(|pending| !pending.scan_succeeded);

    let total_conversations: usize = pending_batches
        .iter()
        .map(|pending| pending.convs.len())
        .sum();
    let total_messages: usize = pending_batches
        .iter()
        .map(|pending| {
            pending
                .convs
                .iter()
                .map(|c| c.messages.len())
                .sum::<usize>()
        })
        .sum();
    let connector_stats: Vec<ConnectorStats> = pending_batches
        .iter()
        .filter(|pending| !pending.convs.is_empty() || pending.scan_error.is_some())
        .map(|pending| {
            let msgs: usize = pending.convs.iter().map(|c| c.messages.len()).sum();
            ConnectorStats {
                name: pending.name.to_string(),
                conversations: pending.convs.len(),
                messages: msgs,
                scan_ms,
                error: pending.scan_error.clone(),
            }
        })
        .collect();

    if let Some(p) = &opts.progress {
        if let Ok(mut names) = p.discovered_agent_names.lock() {
            names.extend(discovered_names.clone());
        }

        p.phase.store(2, Ordering::Relaxed); // Indexing
        p.total.store(total_conversations, Ordering::Relaxed);
        p.current.store(0, Ordering::Relaxed);
        // The pre-counted conversation total is a stable denominator.
        p.total_is_final.store(true, Ordering::Relaxed);
    }

    let index_start = std::time::Instant::now();
    let mut ingest_outcome = NonWatchIngestOutcome::default();
    let mut scanned_connectors = BTreeSet::new();
    for pending in pending_batches {
        if let Some(progress) = &opts.progress {
            progress.check_stop()?;
        }
        let batch_outcome = ingest_non_watch_batch_with_oom_split(
            storage,
            t_index.as_deref_mut(),
            &opts.data_dir,
            &pending.convs,
            &opts.progress,
            lexical_strategy,
            !opts.watch,
            progress_bump,
        )?;
        let persistence_completed = !batch_outcome.scan_had_errors;
        ingest_outcome = ingest_outcome.accumulate(batch_outcome);
        // #426: this connector's complete scan has now been persisted. Advance
        // only its watermark at this transaction boundary. The former timed
        // global watermark ran while other pre-parsed connector batches were
        // still pending, so termination could permanently skip those rows.
        let connector_watermark_safe = pending.is_discovered
            && pending.scan_succeeded
            && persistence_completed
            && !pending.active_source_skipped
            && !scan_path_exclusions_active();
        if connector_watermark_safe && let Some(scan_start_ts) = scan_start_ts {
            scanned_connectors.insert(pending.name.to_string());
            if let Err(error) = persist::with_ephemeral_writer(
                storage,
                !opts.watch,
                "updating completed batch connector scan watermark",
                |writer| record_connector_scan_watermark(writer, pending.name, scan_start_ts),
            ) {
                tracing::warn!(
                    connector = pending.name,
                    error = %error,
                    "completed batch connector watermark save failed; final metadata persistence or the next run will retry"
                );
            }
        } else if pending.is_discovered && pending.scan_succeeded {
            tracing::debug!(
                connector = pending.name,
                active_source_skipped = pending.active_source_skipped,
                path_exclusions_active = scan_path_exclusions_active(),
                "preserving this completed batch connector's watermark while allowing unrelated connectors to advance"
            );
            if persistence_completed && scan_start_ts.is_some() {
                record_scan_contract_with_watermark_held(storage, pending.name, !opts.watch);
            }
        }
        tracing::info!(
            connector = pending.name,
            conversations = pending.convs.len(),
            "batch_ingest"
        );
    }
    let index_ms = index_start.elapsed().as_millis() as u64;

    // Populate structured stats for JSON output (T7.4)
    if let Some(p) = &opts.progress
        && let Ok(mut stats) = p.stats.lock()
    {
        stats.scan_ms = scan_ms;
        stats.index_ms = index_ms;
        stats.connectors = connector_stats;
        stats.agents_discovered = discovered_names.clone();
        stats.total_conversations = total_conversations;
        stats.total_messages = total_messages;
        stats.quarantined_conversations = stats
            .quarantined_conversations
            .saturating_add(ingest_outcome.quarantined_conversations);
        stats.lexical_update_deferred |= ingest_outcome.lexical_update_deferred;
        stats.scan_had_errors |= ingest_outcome.scan_had_errors || scan_had_errors;
    }

    ingest_outcome.scanned_connectors.extend(scanned_connectors);
    ingest_outcome.scan_had_errors |= scan_had_errors;
    let active_deferred = active_source_filter.take_deferred_sources();
    record_deferred_active_sources(opts.progress.as_ref(), &active_deferred);
    ingest_outcome.deferred_sources.extend(active_deferred);

    Ok(ingest_outcome)
}

pub(in crate::indexer) fn non_watch_scan_since_ts(
    full: bool,
    needs_rebuild: bool,
    retry_stale_index_ingest_quarantine: bool,
    last_scan_ts: Option<i64>,
) -> Option<i64> {
    if full || needs_rebuild || retry_stale_index_ingest_quarantine {
        None
    } else {
        last_scan_ts.map(|ts| ts.saturating_sub(1))
    }
}

pub(in crate::indexer) fn connector_local_scan_since_ts_from_state(
    fallback_since_ts: Option<i64>,
    connector_last_scan_ts: Option<i64>,
    connector_has_conversations: bool,
) -> Option<i64> {
    // A full scan or deliberate repair has no cutoff. A saved connector
    // watermark must not turn that request back into an incremental scan.
    let fallback_since_ts = fallback_since_ts?;
    if let Some(ts) = connector_last_scan_ts {
        return Some(ts.saturating_sub(1).max(0));
    }
    if connector_has_conversations {
        Some(fallback_since_ts)
    } else {
        None
    }
}

pub(in crate::indexer) fn connector_activity_time_lower_bound(
    name: &str,
    since_ts: Option<i64>,
) -> Option<i64> {
    if name == "devin" {
        // Devin persists activity in whole seconds. A commit in the watermark's
        // second must remain eligible after the existing millisecond overlap.
        // Keep this separate from watch scheduling, where delayed commits need
        // an unrestricted provider scan.
        since_ts.map(|ts| ts.div_euclid(1000).saturating_mul(1000))
    } else {
        since_ts
    }
}

pub(in crate::indexer) fn connector_local_scan_since_ts_map<F>(
    storage: &FrankenStorage,
    fallback_since_ts: Option<i64>,
    connector_factories: &[(&'static str, F)],
) -> Result<HashMap<&'static str, Option<i64>>> {
    let connector_names = connector_factories
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>();
    let connector_states = storage.connector_scan_states(&connector_names)?;
    let workspace_policy = WorkspaceExclusions::from_env()?;
    let saved_workspace_policies = workspace_exclusion_scan_policies(storage)?;

    connector_factories
        .iter()
        .map(|(name, _)| {
            let state = connector_states.get(*name).copied().unwrap_or_default();
            // OMP is newly split from the legacy Pi Agent identity. The
            // archive upgrade can legitimately create `omp` rows before the
            // dedicated connector has ever completed a scan; do one full OMP
            // scan until its own watermark exists so older profile/XDG roots
            // are not skipped by the global Pi-era cutoff.
            let workspace_policy_key = format!("{WORKSPACE_EXCLUSION_SCAN_PREFIX}{name}");
            let saved_workspace_policy = saved_workspace_policies
                .get(&workspace_policy_key)
                .map(String::as_str)
                .unwrap_or_default();
            let local_since_ts = if saved_workspace_policy != workspace_policy.signature {
                // A removed or changed exclusion can admit sessions older than
                // the previous successful scan. Revisit once, then save the new
                // policy alongside this connector's ordinary advancing watermark.
                None
            } else if *name == "omp" && state.last_scan_ts.is_none() {
                None
            } else if state.scan_contract_pending {
                // A source form this build reads can predate any cutoff (GH
                // #513: compressed Codex rollouts keep old modification times).
                None
            } else {
                connector_local_scan_since_ts_from_state(
                    fallback_since_ts,
                    state.last_scan_ts,
                    state.has_conversations,
                )
            };
            Ok((
                *name,
                connector_activity_time_lower_bound(name, local_since_ts),
            ))
        })
        .collect()
}

/// A successful scan under a connector's current scan contract read every
/// source form the contract names, even when path exclusions or a skipped
/// active source withheld its watermark (whose write otherwise records the
/// contract). Without this such a connector would never get a cutoff again.
pub(in crate::indexer) fn record_scan_contract_with_watermark_held(
    storage: &FrankenStorage,
    connector_name: &str,
    defer_checkpoints: bool,
) {
    if crate::storage::sqlite::connector_scan_contract(connector_name).is_none() {
        return;
    }
    if let Err(error) = persist::with_ephemeral_writer(
        storage,
        defer_checkpoints,
        "recording connector scan contract",
        |writer| writer.record_connector_scan_contract(connector_name),
    ) {
        tracing::warn!(
            connector = connector_name,
            error = %error,
            "connector scan contract save failed; the next run scans this connector without a cutoff again"
        );
    }
}

/// The connectors that own the store a remote root's configured path names.
///
/// A remote mirror's scan root is its opaque `<path>_<hash>` container, and
/// connectors that walk a root recursively claimed whatever transcripts they
/// found under any container: each synced Pi, Prime, Factory or Gemini
/// session was also indexed as a Claude conversation, and each Prime session
/// as a Pi one. The deepest known store component in the configured path
/// decides (`~/.gemini/antigravity` is Antigravity's, not Gemini's). A path
/// that names no known store, and any local root, keeps every connector.
pub(crate) fn mirrored_root_owners(root: &ScanRoot) -> Option<&'static [&'static str]> {
    root.mirrored_source_path
        .as_deref()?
        .split(['/', '\\'])
        .rev()
        .find_map(|component| match component {
            ".claude" => Some(&["claude"][..]),
            ".codex" => Some(&["codex"][..]),
            ".pi" => Some(&["pi_agent", "pi_durable"][..]),
            ".prime" => Some(&["prime_agent"][..]),
            ".omp" | "omp" => Some(&["omp"][..]),
            ".factory" => Some(&["factory"][..]),
            ".gemini" => Some(&["gemini", "antigravity"][..]),
            "antigravity" | "antigravity-cli" => Some(&["antigravity"][..]),
            ".vibe" => Some(&["vibe"][..]),
            ".continue" => Some(&["continue"][..]),
            ".goose" => Some(&["goose"][..]),
            "opencode" => Some(&["opencode"][..]),
            ".aider.chat.history.md" => Some(&["aider"][..]),
            "saoudrizwan.claude-dev" | "rooveterinaryinc.roo-cline" => Some(&["cline"][..]),
            "Cursor" => Some(&["cursor", "cline"][..]),
            _ => None,
        })
}

/// Whether `connector` scans `root`; see [`mirrored_root_owners`].
pub(in crate::indexer) fn mirrored_root_may_hold(connector: &str, root: &ScanRoot) -> bool {
    mirrored_root_owners(root).is_none_or(|owners| owners.contains(&connector))
}

pub(in crate::indexer) fn explicit_scan_root_since_ts(
    root: &ScanRoot,
    built_in_local_root: &Path,
    fallback_since_ts: Option<i64>,
) -> Option<i64> {
    // Configured local roots are local history, including opt-in backup
    // directories. They must honor the incremental cutoff; otherwise a normal
    // `cass index` can replay multi-GB backup archives on every run.
    if root.origin.kind == SourceKind::Local || root.path == built_in_local_root {
        fallback_since_ts
    } else {
        None
    }
}

pub fn run_index(
    opts: IndexOptions,
    event_channel: Option<(Sender<IndexerEvent>, Receiver<IndexerEvent>)>,
) -> Result<()> {
    run_index_inner(opts, event_channel, None, false)
}

/// Preserve the existing explicit `cass index` admission for automatic raw
/// retention. Refresh, doctor, TUI and library indexing do not acquire it just
/// because maintenance now lives inside the observed index owner.
pub(crate) fn run_cli_index(opts: IndexOptions) -> Result<()> {
    run_index_inner(opts, None, None, true)
}

/// qu81y class-B injection seam: run one index pass with the connectors'
/// LOCAL default-detection lane replaced by an explicit CLOSED-WORLD set of
/// per-connector scan roots. Connectors absent from the map are skipped
/// entirely — detection never consults process-global environment (HOME,
/// CODEX_HOME, ...), so in-process tests can drive `run_index` against
/// fixture roots without mutating env vars shared across libtest threads.
/// Explicitly configured `sources.toml` roots and the watch path are
/// unaffected; production callers keep using [`run_index`].
pub fn run_index_with_local_connector_roots(
    opts: IndexOptions,
    local_connector_roots: HashMap<String, Vec<PathBuf>>,
    event_channel: Option<(Sender<IndexerEvent>, Receiver<IndexerEvent>)>,
) -> Result<()> {
    run_index_inner(
        opts,
        event_channel,
        Some(Arc::new(local_connector_roots)),
        false,
    )
}

/// Per-connector local scan roots override (qu81y): `Some(map)` means a
/// closed world — scan exactly `map[connector]` per connector, skip
/// connectors without an entry, and never run env-based default detection.
pub(in crate::indexer) type LocalConnectorRootsOverride =
    Option<Arc<HashMap<String, Vec<PathBuf>>>>;

pub(in crate::indexer) fn detect_for_local_scan(
    local_connector_roots: &LocalConnectorRootsOverride,
    detect: impl FnOnce() -> crate::connectors::DetectionResult,
) -> Option<crate::connectors::DetectionResult> {
    local_connector_roots.is_none().then(detect)
}

/// GH #489: `CASS_EXCLUDE_PATHS` is comma/newline-delimited, but a PATH-style
/// list (`/a/x:/b/y`) is an easy mistake. The scan policy
/// (`connectors::codex::path_policy`) parses it as one nonexistent path that
/// excludes nothing, so the sources the operator meant to skip are still
/// scanned. Name the entries that look like that. An entry only qualifies when
/// every colon-separated piece is itself an absolute or home-relative path and
/// the whole entry does not exist, so Windows drive paths (`C:\x`) and real
/// names containing `:` never warn.
pub(in crate::indexer) fn colon_separated_exclusion_warning(value: &str) -> Option<String> {
    let suspicious: Vec<&str> = value
        .split([',', '\n'])
        .map(str::trim)
        .filter(|entry| {
            let mut pieces = entry.split(':');
            let looks_like_list = entry.contains(':')
                && pieces.all(|piece| piece.starts_with('/') || piece.starts_with("~/"));
            looks_like_list && !Path::new(entry).exists()
        })
        .collect();
    (!suspicious.is_empty()).then(|| {
        format!(
            "CASS_EXCLUDE_PATHS separates entries with commas or newlines, not colons; \
             these entries do not exist and exclude nothing: {}. Use a comma, e.g. {}",
            suspicious.join(", "),
            suspicious[0].replace(':', ",")
        )
    })
}

#[cfg(test)]
pub(in crate::indexer) mod colon_separated_exclusion_warning_tests {
    use super::colon_separated_exclusion_warning;

    #[test]
    fn colon_separated_exclusions_are_named_with_a_comma_suggestion() {
        let warning = colon_separated_exclusion_warning(
            "/data/old-codex/a.jsonl:/data/old-codex/b.jsonl,~/x:~/y",
        )
        .expect("PATH-style lists must warn");
        assert!(warning.contains("/data/old-codex/a.jsonl:/data/old-codex/b.jsonl"));
        assert!(warning.contains("~/x:~/y"));
        assert!(warning.contains("/data/old-codex/a.jsonl,/data/old-codex/b.jsonl"));
    }

    #[test]
    fn well_formed_and_drive_style_exclusions_do_not_warn() -> std::io::Result<()> {
        assert_eq!(colon_separated_exclusion_warning(""), None);
        assert_eq!(colon_separated_exclusion_warning("/a/x,/b/y\n~/z"), None);
        // Windows drive paths and a relative piece are not colon-separated lists.
        assert_eq!(
            colon_separated_exclusion_warning(r"C:\Users\me\.codex"),
            None
        );
        assert_eq!(colon_separated_exclusion_warning("/a/x:relative"), None);
        // A real path whose name contains ':' exists, so it is not a mistake.
        #[cfg(unix)]
        {
            let root = tempfile::tempdir()?;
            let odd = root.path().join("a:");
            std::fs::create_dir(&odd)?;
            let entry = format!("{}/b", odd.display());
            std::fs::create_dir(&entry)?;
            assert_eq!(colon_separated_exclusion_warning(&entry), None);
        }
        Ok(())
    }
}

pub(in crate::indexer) fn run_index_inner(
    opts: IndexOptions,
    event_channel: Option<(Sender<IndexerEvent>, Receiver<IndexerEvent>)>,
    local_connector_roots: LocalConnectorRootsOverride,
    maintain_raw_mirror: bool,
) -> Result<()> {
    let progress = opts.progress.clone();
    let result = run_index_with_storage_ownership(
        opts,
        event_channel,
        local_connector_roots,
        maintain_raw_mirror,
    );
    // Some archive diagnostics intentionally turn engine errors into reason
    // strings. Once the owner has drained, a requested stop must not escape
    // as a spurious corruption or migration failure merely because an Abort
    // lost its concrete type while passing through those diagnostics.
    if progress
        .as_ref()
        .is_some_and(|progress| progress.stop_requested())
    {
        if let Err(error) = &result {
            tracing::debug!(error = %error, "index owner drained after cancellation");
        }
        return Err(IndexInterrupted.into());
    }
    result
}
