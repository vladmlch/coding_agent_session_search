use super::*;

#[cfg(test)]
pub(in crate::indexer) fn send_conversation_batches(
    tx: &Sender<IndexMessage>,
    connector_name: &'static str,
    conversations: Vec<NormalizedConversation>,
    is_discovered: bool,
) {
    let mut sender = StreamingBatchSender::new(
        tx,
        Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
        connector_name,
        is_discovered,
    );
    for conversation in conversations {
        sender
            .push(conversation)
            .expect("test batch sender should deliver to in-memory receiver");
    }
    sender
        .flush()
        .expect("test batch sender should flush to in-memory receiver");
}

/// Check if streaming indexing is enabled via environment variable.
///
/// Set `CASS_STREAMING_INDEX=0` to disable streaming and use batch mode.
/// Streaming is enabled by default.
pub fn streaming_index_enabled() -> bool {
    dotenvy::var("CASS_STREAMING_INDEX")
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(true)
}

pub(in crate::indexer) fn scan_path_exclusions_value_active(value: Option<&str>) -> bool {
    value.is_some_and(|raw| {
        raw.split([',', '\n'])
            .map(str::trim)
            .any(|part| !part.is_empty())
    })
}

pub(in crate::indexer) fn scan_path_exclusions_active() -> bool {
    scan_path_exclusions_value_active(dotenvy::var("CASS_EXCLUDE_PATHS").ok().as_deref())
}

pub(in crate::indexer) fn active_session_source_skip_observed() -> bool {
    ACTIVE_SESSION_SOURCE_SKIP_OBSERVED.load(Ordering::Relaxed)
}

pub(in crate::indexer) fn scan_watermark_preservation_active() -> bool {
    scan_path_exclusions_active() || active_session_source_skip_observed()
}

pub(in crate::indexer) fn panic_payload_message(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_string(),
            Err(_) => "non-string panic payload".to_string(),
        },
    }
}

#[derive(Debug)]
pub(in crate::indexer) struct StreamingConsumerDisconnected {
    pub(in crate::indexer) connector_name: &'static str,
}

impl std::fmt::Display for StreamingConsumerDisconnected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "streaming consumer disconnected while sending batch for {}",
            self.connector_name
        )
    }
}

impl std::error::Error for StreamingConsumerDisconnected {}

pub(in crate::indexer) fn is_streaming_consumer_disconnected(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<StreamingConsumerDisconnected>()
        .is_some()
}

#[derive(Clone)]
pub(in crate::indexer) struct StreamingProducerConfig {
    pub(in crate::indexer) source_ledger: Arc<HashMap<String, String>>,
    pub(in crate::indexer) raw_source_exclusions: Arc<RawSourceExclusions>,
    pub(in crate::indexer) flow_limiter: Arc<StreamingByteLimiter>,
    pub(in crate::indexer) data_dir: PathBuf,
    pub(in crate::indexer) additional_scan_roots: Vec<ScanRoot>,
    /// qu81y closed-world override; see [`LocalConnectorRootsOverride`].
    pub(in crate::indexer) local_connector_roots: LocalConnectorRootsOverride,
    pub(in crate::indexer) since_ts: Option<i64>,
    pub(in crate::indexer) local_since_ts_by_connector: Arc<HashMap<&'static str, Option<i64>>>,
    pub(in crate::indexer) progress: Option<Arc<IndexingProgress>>,
    pub(in crate::indexer) active_source_filter: Arc<ActiveSessionSourceFilter>,
}

/// Spawn a producer thread that scans a connector and sends batches through the channel.
///
/// Each connector runs in its own thread, scanning the built-in local roots plus
/// any explicitly configured additional roots.
/// Conversations are sent through the channel as they're discovered, providing
/// backpressure when the consumer (indexer) falls behind.
pub(in crate::indexer) fn spawn_connector_producer<F>(
    name: &'static str,
    factory: F,
    tx: Sender<IndexMessage>,
    config: StreamingProducerConfig,
) -> JoinHandle<()>
where
    F: Fn() -> Box<dyn Connector + Send> + Send + 'static,
{
    thread::spawn(move || {
        let _raw_mirror_io = observe_index_raw_mirror_io(config.progress.as_ref());
        let scan_start = std::time::Instant::now();
        let conn = factory();
        let detect = detect_for_local_scan(&config.local_connector_roots, || conn.detect());
        let was_detected = detect.as_ref().is_some_and(|result| result.detected);
        let mut is_discovered = false;
        let mut scan_succeeded = true;
        let mut active_source_skipped = false;

        // #373 Variant A: give the connector a liveness tick so a long internal
        // scan (e.g. decoding a large opencode.db) advances the watchdog's
        // `activity` signal BEFORE the first conversation is yielded — otherwise
        // a multi-minute pre-first-yield scan false-fires as a stall. Wired to
        // the same per-conversation `tick_activity` the streaming consumer uses.
        let scan_progress_tick: Option<Arc<dyn Fn() + Send + Sync>> =
            config.progress.as_ref().map(|progress| {
                let progress = Arc::clone(progress);
                Arc::new(move || progress.tick_activity()) as Arc<dyn Fn() + Send + Sync>
            });
        let with_scan_tick = |ctx: crate::connectors::ScanContext| match &scan_progress_tick {
            Some(tick) => ctx.with_progress_tick(Arc::clone(tick)),
            None => ctx,
        };

        // qu81y closed-world override: when a per-connector roots map is
        // supplied, scan exactly those roots and never consult env-based
        // default detection; connectors absent from the map are skipped.
        let override_roots: Option<Vec<ScanRoot>> =
            config.local_connector_roots.as_ref().map(|map| {
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
            if let Some(p) = &config.progress {
                p.discovered_agents.fetch_add(1, Ordering::Relaxed);
            }
            is_discovered = true;

            let local_since_ts = config
                .local_since_ts_by_connector
                .get(name)
                .copied()
                .unwrap_or(config.since_ts);

            // Scan local sources
            let ctx = with_scan_tick(match &override_roots {
                Some(roots) => crate::connectors::ScanContext::with_roots(
                    config.data_dir.clone(),
                    roots.clone(),
                    local_since_ts,
                ),
                None => crate::connectors::ScanContext::local_default(
                    config.data_dir.clone(),
                    local_since_ts,
                ),
            });
            let local_origin = Origin::local();
            let mut batch_sender =
                StreamingBatchSender::new(&tx, config.flow_limiter.clone(), name, is_discovered);
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
                    &config.data_dir,
                    name,
                    &fallback_roots,
                    local_since_ts,
                    config.active_source_filter.as_ref(),
                    &config.raw_source_exclusions,
                );
            active_source_skipped |= preparse_active_source_skipped;
            match scan_with_durable_source_boundaries(
                conn.as_ref(),
                &ctx,
                &config,
                &mut batch_sender,
                |mut conversation| {
                    if should_skip_active_session_source(
                        config.active_source_filter.as_ref(),
                        SourceKind::Local,
                        &conversation.source_path,
                    ) {
                        active_source_skipped = true;
                        return Ok(None);
                    }
                    if should_skip_subagent(&conversation)
                        || should_skip_excluded_source(&conversation.source_path)
                    {
                        return Ok(None);
                    }
                    ingest_diagnostics.observe_conversation(&mut conversation);
                    prepare_conversation_for_ingest(
                        &config.data_dir,
                        name,
                        &local_origin,
                        None,
                        &mut conversation,
                        &config.raw_source_exclusions,
                    );
                    // #332: each parsed conversation is live work even while the
                    // batch sender buffers it below publication thresholds — this
                    // tick keeps the stall watchdog from firing during a long
                    // parse of a large source artifact.
                    if let Some(p) = &config.progress {
                        p.tick_activity();
                    }
                    Ok(Some(conversation))
                },
            ) {
                Ok(()) => {
                    if let Err(error) = batch_sender.flush() {
                        if is_streaming_consumer_disconnected(&error) {
                            tracing::info!(
                                connector = name,
                                "streaming consumer disconnected; stopping producer"
                            );
                            return;
                        }
                        scan_succeeded = false;
                        tracing::warn!(connector = name, "local flush failed: {}", error);
                        let _ = tx.send(IndexMessage::ScanError {
                            connector_name: name,
                            error: format!("local flush failed: {error}"),
                        });
                    }
                }
                Err(e) => {
                    if let Err(flush_error) = batch_sender.flush()
                        && !is_streaming_consumer_disconnected(&flush_error)
                    {
                        tracing::warn!(connector = name, "local flush failed: {}", flush_error);
                    }
                    if is_streaming_consumer_disconnected(&e) {
                        tracing::info!(
                            connector = name,
                            "streaming consumer disconnected; stopping producer"
                        );
                        return;
                    }
                    scan_succeeded = false;
                    // Name the root the connector scanned, not the cass data
                    // dir (GH #511).
                    let scanned_root = fallback_roots
                        .first()
                        .map_or(ctx.data_dir.as_path(), |root| root.path.as_path());
                    ingest_diagnostics.observe_connector_scan_error(scanned_root, &e);
                    tracing::warn!(connector = name, "local scan failed: {}", e);
                    let _ = tx.send(IndexMessage::ScanError {
                        connector_name: name,
                        error: format!("{e:#}"),
                    });
                }
            }
            ingest_diagnostics
                .observe_reused_sources(std::mem::take(&mut batch_sender.reused_sources));
            record_connector_ingest_report(config.progress.as_ref(), ingest_diagnostics.finish());
        }

        // Scan explicitly configured additional roots. These may be true remote
        // mirrors or machine-local backup directories wired through sources.toml.
        for root in &config.additional_scan_roots {
            if !mirrored_root_may_hold(name, root) {
                continue;
            }
            let local_since_ts = config
                .local_since_ts_by_connector
                .get(name)
                .copied()
                .unwrap_or(config.since_ts);
            let root_since_ts = explicit_scan_root_since_ts(root, &config.data_dir, local_since_ts);
            if config.since_ts.is_some() && root_since_ts.is_none() {
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
            let mut batch_sender =
                StreamingBatchSender::new(&tx, config.flow_limiter.clone(), name, is_discovered);
            let (mut ingest_diagnostics, preparse_active_source_skipped) =
                capture_connector_sources_before_parse(
                    conn.as_ref(),
                    &ctx,
                    &config.data_dir,
                    name,
                    std::slice::from_ref(root),
                    root_since_ts,
                    config.active_source_filter.as_ref(),
                    &config.raw_source_exclusions,
                );
            active_source_skipped |= preparse_active_source_skipped;
            match scan_with_durable_source_boundaries(
                conn.as_ref(),
                &ctx,
                &config,
                &mut batch_sender,
                |mut conversation| {
                    if should_skip_active_session_source(
                        config.active_source_filter.as_ref(),
                        root.origin.kind,
                        &conversation.source_path,
                    ) {
                        active_source_skipped = true;
                        return Ok(None);
                    }
                    if should_skip_subagent(&conversation)
                        || should_skip_excluded_source(&conversation.source_path)
                    {
                        return Ok(None);
                    }
                    ingest_diagnostics.observe_conversation(&mut conversation);
                    prepare_conversation_for_ingest(
                        &config.data_dir,
                        name,
                        &root.origin,
                        Some(root),
                        &mut conversation,
                        &config.raw_source_exclusions,
                    );

                    if !was_detected && !is_discovered {
                        if let Some(p) = &config.progress {
                            p.discovered_agents.fetch_add(1, Ordering::Relaxed);
                        }
                        is_discovered = true;
                    }

                    // #332: parsed-conversation liveness tick (see the local-scan
                    // callback above).
                    if let Some(p) = &config.progress {
                        p.tick_activity();
                    }
                    Ok(Some(conversation))
                },
            ) {
                Ok(()) => {
                    if let Err(error) = batch_sender.flush() {
                        if is_streaming_consumer_disconnected(&error) {
                            tracing::info!(
                                connector = name,
                                "streaming consumer disconnected; stopping producer"
                            );
                            return;
                        }
                        scan_succeeded = false;
                        tracing::warn!(
                            connector = name,
                            root = %root.path.display(),
                            "remote flush failed: {}",
                            error
                        );
                        let _ = tx.send(IndexMessage::ScanError {
                            connector_name: name,
                            error: format!(
                                "remote flush failed for {}: {}",
                                root.path.display(),
                                error
                            ),
                        });
                    }
                }
                Err(e) => {
                    if let Err(flush_error) = batch_sender.flush()
                        && !is_streaming_consumer_disconnected(&flush_error)
                    {
                        tracing::warn!(
                            connector = name,
                            root = %root.path.display(),
                            "remote flush failed: {}",
                            flush_error
                        );
                    }
                    if is_streaming_consumer_disconnected(&e) {
                        tracing::info!(
                            connector = name,
                            "streaming consumer disconnected; stopping producer"
                        );
                        return;
                    }
                    scan_succeeded = false;
                    ingest_diagnostics.observe_connector_scan_error(&root.path, &e);
                    tracing::warn!(
                        connector = name,
                        root = %root.path.display(),
                        "remote scan failed: {}", e
                    );
                    let _ = tx.send(IndexMessage::ScanError {
                        connector_name: name,
                        error: format!("remote scan failed for {}: {e:#}", root.path.display()),
                    });
                }
            }
            ingest_diagnostics
                .observe_reused_sources(std::mem::take(&mut batch_sender.reused_sources));
            record_connector_ingest_report(config.progress.as_ref(), ingest_diagnostics.finish());
        }

        let scan_ms = scan_start.elapsed().as_millis() as u64;
        tracing::info!(
            connector = name,
            discovered = is_discovered,
            scan_ms,
            "streaming_scan_complete"
        );

        // Signal completion with timing
        let _ = tx.send(IndexMessage::Done {
            connector_name: name,
            scan_ms,
            is_discovered,
            scan_succeeded,
            active_source_skipped,
        });
    })
}

/// Base commit cadence for the streaming indexing consumer. Default is 5s;
/// can be overridden at process start via `CASS_STREAMING_CONSUMER_COMMIT_SECS`.
/// Must return at least 1s to avoid pathological tight-loop commits even if
/// the env var is misconfigured.
pub(in crate::indexer) fn streaming_consumer_commit_base_secs() -> u64 {
    dotenvy::var("CASS_STREAMING_CONSUMER_COMMIT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(5)
}

/// Effective commit interval for the streaming consumer. On an idle host this
/// is the base value; under responsiveness pressure it scales with the
/// governor capacity so commits happen more often, keeping the WAL and
/// in-memory buffers from piling up. Clamped to at least 1s to avoid
/// pathological tight-loop commits under an aggressive capacity floor.
pub(in crate::indexer) fn streaming_consumer_commit_interval() -> Duration {
    let base = streaming_consumer_commit_base_secs();
    let capacity = responsiveness::current_capacity_pct().clamp(1, 100) as u64;
    let scaled = base.saturating_mul(capacity) / 100;
    Duration::from_secs(scaled.max(1))
}

/// Minimum spacing between segment-fold attempts inside one streaming run.
pub(in crate::indexer) const STREAMING_FOLD_ATTEMPT_INTERVAL: Duration = Duration::from_secs(60);

/// The next streaming commit waits at least this many times as long as the
/// last one took, so publishing costs at most about a fifth of consumer time.
pub(in crate::indexer) const STREAMING_COMMIT_COST_MULTIPLE: u32 = 4;

/// Upper bound on the cost-derived commit interval, so a slow publish cannot
/// hold freshly ingested sessions out of search for long.
pub(in crate::indexer) const STREAMING_COMMIT_MAX_PACED_INTERVAL: Duration =
    Duration::from_secs(120);

/// Effective streaming commit interval given what the last commit cost.
///
/// Every Quill publish re-verifies each live segment, including an xxh3 pass
/// over its full bytes, so a commit costs time proportional to the whole
/// lexical index, not to what it adds. On a 2-8M-message archive a commit
/// took 3-12 s, and with the fixed 5 s interval the consumer spent most of a
/// catch-up re-verifying unchanged segments while producers waited on
/// backpressure. Pacing by measured cost keeps small indexes at the base
/// cadence (their commits take milliseconds) and bounds the verification
/// share on large ones.
pub(in crate::indexer) fn paced_streaming_commit_interval(
    base: Duration,
    last_commit_cost: Duration,
) -> Duration {
    base.max(
        last_commit_cost
            .saturating_mul(STREAMING_COMMIT_COST_MULTIPLE)
            .min(STREAMING_COMMIT_MAX_PACED_INTERVAL),
    )
}

/// Commit cadence and segment-fold bookkeeping for one streaming run.
pub(in crate::indexer) struct StreamingCommitPacer {
    pub(in crate::indexer) last_commit: std::time::Instant,
    pub(in crate::indexer) last_commit_cost: Duration,
    pub(in crate::indexer) last_fold_attempt: Option<std::time::Instant>,
}

impl StreamingCommitPacer {
    pub(in crate::indexer) fn new() -> Self {
        Self {
            last_commit: std::time::Instant::now(),
            last_commit_cost: Duration::ZERO,
            last_fold_attempt: None,
        }
    }

    /// Whether enough ingest time has passed since the last commit ended.
    pub(in crate::indexer) fn due(&self) -> bool {
        self.last_commit.elapsed()
            >= paced_streaming_commit_interval(
                streaming_consumer_commit_interval(),
                self.last_commit_cost,
            )
    }

    /// Commit, then fold small segments when due, then restart the interval.
    /// The fold is a publish too, so its time counts toward the pacing cost.
    /// `db` lets a successful commit flush the region-doc marks stamps its
    /// staged documents earned (`None` leaves them queued for a later
    /// stamp-aware commit — never stamped pre-commit).
    pub(in crate::indexer) fn commit(
        &mut self,
        t_index: Option<&mut TantivyIndex>,
        db: Option<&crate::franken_sync::Connection>,
    ) {
        if let Some(t_index) = t_index {
            let started = std::time::Instant::now();
            match t_index.commit() {
                Ok(()) => {
                    tracing::debug!(
                        commit_ms = started.elapsed().as_millis() as u64,
                        "incremental commit completed"
                    );
                    if let Some(db) = db
                        && let Err(error) = t_index.commit_region_doc_stamps(db)
                    {
                        // A stamp failure leaves the marks honestly
                        // pending; the next stamp-aware commit retries.
                        tracing::warn!(%error, "region-doc marks stamp failed");
                    }
                    self.fold_small_segments(t_index);
                }
                Err(error) => tracing::warn!(%error, "incremental commit failed"),
            }
            self.last_commit_cost = started.elapsed();
        }
        self.last_commit = std::time::Instant::now();
    }

    /// Fold the small segment tail during a long streaming run.
    ///
    /// Every commit publishes a few small segments, and every publish
    /// re-verifies each live segment's term dictionary. A long catch-up that
    /// only folded after the run ended grew one generation past 3,000 segments,
    /// so each publish got slower as the run went on and every search opened
    /// during it paid for the whole pile. This runs at most one merge
    /// (`fold_largest_small_run`), gated by the segment threshold and a 300 s
    /// cooldown, so it costs about one extra publish a few times per long run;
    /// the full fold still runs after the run ends. A pass that folds nothing
    /// does not restart that cooldown, so attempts are also spaced by
    /// [`STREAMING_FOLD_ATTEMPT_INTERVAL`].
    pub(in crate::indexer) fn fold_small_segments(&mut self, t_index: &mut TantivyIndex) {
        if lexical_post_run_maintenance_skipped_for_test()
            || self
                .last_fold_attempt
                .is_some_and(|at| at.elapsed() < STREAMING_FOLD_ATTEMPT_INTERVAL)
        {
            return;
        }
        self.last_fold_attempt = Some(std::time::Instant::now());
        match t_index.fold_largest_small_run() {
            Ok(true) => tracing::info!(
                segments = t_index.segment_count(),
                "folded lexical segments during streaming ingest"
            ),
            Ok(false) => {}
            Err(error) => tracing::warn!(
                error = %format!("{error:#}"),
                "segment merge during streaming ingest failed; continuing with the unmerged generation"
            ),
        }
    }
}

/// Flat-combining drain in the streaming consumer (Card 3 / `§14.2 Flat
/// Combining` in the alien graveyard). When enabled, the consumer drains
/// up to `streaming_combine_max_messages` pending `Batch` messages in one
/// sweep and folds them into a single `ingest_batch` call. Reduces
/// consumer-thread CPU under multi-connector producers; single-producer
/// scans bypass the drain entirely (the `active_producers > 1` gate).
///
/// **Default: ON.** Set `CASS_STREAMING_CONSUMER_COMBINE=0` to revert.
pub(in crate::indexer) fn streaming_combine_enabled() -> bool {
    match dotenvy::var("CASS_STREAMING_CONSUMER_COMBINE")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .as_deref()
    {
        // Explicit opt-out keywords.
        Some("0" | "false" | "no" | "off") => false,
        // Anything else (unset, `1`, `true`, garbage value) = enabled.
        _ => true,
    }
}

/// Max pending channel messages folded into a single combined drain.
/// Oversizing hurts first-visibility latency without increasing
/// throughput (we still commit at the same cadence); undersizing wastes
/// the consumer's CPU. Default 64 matches `ALIEN-ARTIFACT-CARD3-SPEC.md`
/// §2.B derivation from Little's Law. Clamped to [1, 1024].
pub(in crate::indexer) fn streaming_combine_max_messages() -> usize {
    dotenvy::var("CASS_STREAMING_COMBINE_MAX")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|v| v.clamp(1, 1024))
        .unwrap_or(64)
}

/// Max combined-drain byte budget. Half the streaming in-flight cap by
/// default so combining cannot push a single drain over the global
/// producer-backpressure limit. Override with
/// `CASS_STREAMING_COMBINE_MAX_BYTES` (clamped to
/// `[1 MiB, STREAMING_MAX_BYTES_IN_FLIGHT]`).
pub(in crate::indexer) fn streaming_combine_max_bytes() -> usize {
    dotenvy::var("CASS_STREAMING_COMBINE_MAX_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|v| v.clamp(1024 * 1024, STREAMING_MAX_BYTES_IN_FLIGHT))
        .unwrap_or(STREAMING_MAX_BYTES_IN_FLIGHT / 2)
}

/// Run the streaming indexing consumer.
///
/// Receives batches from producer threads and ingests them into storage.
/// Processes batches as they arrive, providing early feedback and reducing
/// peak memory usage compared to batch collection.
#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn run_streaming_consumer(
    rx: Receiver<IndexMessage>,
    num_producers: usize,
    storage: &FrankenStorage,
    data_dir: &Path,
    mut t_index: Option<&mut TantivyIndex>,
    flow_limiter: Arc<StreamingByteLimiter>,
    progress: &Option<Arc<IndexingProgress>>,
    lexical_strategy: LexicalPopulationStrategy,
    // Whether this consumer run may latch the whole-archive region-doc
    // republish deferral at its prologue. Production passes the scan
    // entry's lane gate (`!opts.watch && opts.watch_once_paths.is_none()`):
    // a watch run has no final drain, so latching its startup scan would
    // strand every mark the watch loop persists at `pending` for the life
    // of the process.
    allow_region_doc_republish_deferral: bool,
    scan_start_ts: Option<i64>,
    progress_bump: Option<&Arc<AtomicI64>>,
) -> Result<(Vec<String>, NonWatchIngestOutcome)> {
    use std::collections::HashMap;

    let mut active_producers = num_producers;
    let mut discovered_names: Vec<String> = Vec::new();
    let mut total_conversations = 0usize;
    let mut total_messages = 0usize;
    let mut switched_to_indexing = false;
    let mut commit_pacer = StreamingCommitPacer::new();
    // Whole-archive republish deferral: latch before the first batch when the
    // stale-version marks a REGION_VERSION bump left exceed the deferral
    // threshold, so this consumer's per-batch region-doc prelude stages
    // nothing and its final commit drains the accumulated pending set once
    // (bounded, one handful of publishes). Idempotent with the scan entry's
    // latch; the strategy gate keeps the rebuild flavors staging per batch —
    // a fresh snapshot re-materializes every session and additive upserts do
    // not republish. The caller's lane flag mirrors the scan entry's
    // `!opts.watch && opts.watch_once_paths.is_none()` gate: a watch run
    // has no final drain, so latching its startup scan would strand every
    // mark the watch loop persists at `pending` for the life of the
    // process — watch keeps per-event convergence (tantivy.rs: the latch is
    // "never latched by watch").
    if allow_region_doc_republish_deferral
        && lexical_strategy == LexicalPopulationStrategy::IncrementalInline
        && let Some(index) = t_index.as_deref_mut()
    {
        persist::maybe_defer_region_doc_convergence_for_republish(storage.raw(), index)?;
    }
    let index_start = std::time::Instant::now();
    let mut ingest_outcome = NonWatchIngestOutcome::default();
    // Streaming ingest intentionally defers connection-local WAL
    // auto-checkpoints until the enclosing index/watch close path restores
    // steady-state policy and runs the final checkpoint. Keep incidental
    // progress writes on the same policy so slow startup batches do not flip
    // the long-lived handle back to steady-state mid-ingest.
    let defer_streaming_checkpoints = true;

    // Per-connector stats tracking (T7.4)
    let mut connector_stats: HashMap<String, ConnectorStats> = HashMap::new();
    let mut failed_scan_connectors = BTreeSet::new();
    let source_commit_limit = dotenvy::var("CASS_INDEX_MAX_SOURCE_COMMITS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|limit| *limit > 0);
    let mut source_commits = 0usize;

    // Card 3 (flat combining, §14.2): when enabled and at least one
    // additional producer is live, we opportunistically drain pending
    // `Batch` messages from the channel after receiving the first one,
    // fold them into a single `ingest_batch` call, and commit once.
    // This amortises the per-message overhead of persist + Tantivy add.
    // Disabled by default; opt in via `CASS_STREAMING_CONSUMER_COMBINE=1`.
    //
    // Correctness is preserved by:
    //  * concat order: combined.conversations = first.conversations ++ next1 ++ next2 ...,
    //  * per-message byte_reservation release tracked exactly once,
    //  * non-`Batch` messages (Done/ScanError) seen mid-drain are deferred
    //    into `deferred_non_batch` and processed in-order *after* the
    //    combined ingest, so producer-Done accounting stays accurate.
    let combine_enabled = streaming_combine_enabled();
    let max_combine_messages = streaming_combine_max_messages();
    let max_combine_bytes = streaming_combine_max_bytes();
    let mut deferred_non_batch: VecDeque<IndexMessage> = VecDeque::new();

    loop {
        if progress
            .as_ref()
            .is_some_and(|progress| progress.stop_requested())
        {
            if let Some(index) = t_index.as_deref_mut() {
                index.commit_with_region_doc_stamps(storage.raw())?;
            }
            best_effort_abort_wal_checkpoint_for_db(&storage.database_path()?);
            return Err(anyhow::Error::new(IndexInterrupted));
        }
        // Drain any deferred messages from a prior combine-drain first, in
        // the exact order they arrived on the channel. This preserves the
        // invariant that every message is handled exactly once, in order,
        // regardless of whether combining was active.
        let next_message = if let Some(m) = deferred_non_batch.pop_front() {
            Ok(m)
        } else {
            rx.recv()
        };
        match next_message {
            Ok(IndexMessage::Batch {
                connector_name,
                conversations,
                is_discovered,
                message_count,
                byte_reservation,
            }) => {
                // Accumulators start with the first-received batch.
                let mut combined_conversations: Vec<NormalizedConversation> = conversations;
                let mut combined_connector_ranges =
                    vec![(connector_name, 0..combined_conversations.len())];
                let mut combined_message_count = message_count;
                let mut combined_byte_reservation = byte_reservation;
                let mut combined_batch_size = combined_conversations.len();
                total_conversations += combined_batch_size;
                total_messages += combined_message_count;

                // Update per-connector stats for the FIRST batch. Extra
                // batches coalesced during drain get their own stats
                // updates in the inner loop below so per-connector
                // accounting stays identical to the per-message path.
                {
                    let stats = connector_stats
                        .entry(connector_name.to_string())
                        .or_insert_with(|| ConnectorStats {
                            name: connector_name.to_string(),
                            ..Default::default()
                        });
                    stats.conversations += combined_batch_size;
                    stats.messages += message_count;
                }

                // Switch to indexing phase on first batch (reset total/current for accurate progress)
                if !switched_to_indexing {
                    if let Some(p) = progress {
                        p.phase.store(2, Ordering::Relaxed); // Indexing
                        p.total.store(0, Ordering::Relaxed); // Reset - will accumulate as batches arrive
                        p.current.store(0, Ordering::Relaxed);
                        // #332: this total is "discovered so far", not a
                        // stable denominator — flag it so ETA is suppressed
                        // instead of regressing as discovery expands it.
                        p.total_is_final.store(false, Ordering::Relaxed);
                    }
                    switched_to_indexing = true;
                }

                // Update progress total (we learn about sizes as batches arrive)
                if let Some(p) = progress {
                    p.total.fetch_add(combined_batch_size, Ordering::Relaxed);
                    // #332: a received batch is proof of live work even before
                    // it is persisted and `current` advances.
                    p.tick_activity();
                }

                // Track discovered agent names
                if is_discovered {
                    remember_discovered_connector(&mut discovered_names, connector_name);
                }

                // Combine drain: pull additional pending Batch messages up
                // to the configured caps. Stop on:
                //   - channel empty (try_recv returns Err),
                //   - non-Batch message (defer it for the next loop iteration),
                //   - MAX messages or MAX bytes cap reached.
                if combine_enabled && active_producers > 1 {
                    let mut combined_messages_so_far = 1usize;
                    while combined_messages_so_far < max_combine_messages
                        && combined_byte_reservation < max_combine_bytes
                    {
                        match rx.try_recv() {
                            Ok(IndexMessage::Batch {
                                connector_name: cname2,
                                conversations: extra_convs,
                                is_discovered: extra_discovered,
                                message_count: extra_msg_count,
                                byte_reservation: extra_byte_reservation,
                            }) => {
                                let extra_size = extra_convs.len();
                                // Per-connector stats for the extra batch.
                                let stats = connector_stats
                                    .entry(cname2.to_string())
                                    .or_insert_with(|| ConnectorStats {
                                        name: cname2.to_string(),
                                        ..Default::default()
                                    });
                                stats.conversations += extra_size;
                                stats.messages += extra_msg_count;
                                if extra_discovered {
                                    remember_discovered_connector(&mut discovered_names, cname2);
                                }
                                if let Some(p) = progress {
                                    p.total.fetch_add(extra_size, Ordering::Relaxed);
                                    p.tick_activity();
                                }
                                let start = combined_conversations.len();
                                combined_conversations.extend(extra_convs);
                                combined_connector_ranges
                                    .push((cname2, start..combined_conversations.len()));
                                combined_message_count += extra_msg_count;
                                combined_byte_reservation += extra_byte_reservation;
                                combined_batch_size += extra_size;
                                total_conversations += extra_size;
                                total_messages += extra_msg_count;
                                combined_messages_so_far += 1;
                            }
                            Ok(other) => {
                                // Non-Batch message: defer and stop draining.
                                // Ordering is preserved because we'll process
                                // `other` as the very next message on the
                                // main loop.
                                deferred_non_batch.push_back(other);
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                }

                // Ingest the combined batch (== original single batch when
                // combine_enabled = false or no extras were drained).
                let batch_outcome = ingest_non_watch_batch_with_oom_split(
                    storage,
                    t_index.as_deref_mut(),
                    data_dir,
                    &combined_conversations,
                    progress,
                    lexical_strategy,
                    defer_streaming_checkpoints,
                    progress_bump,
                );
                // The reservation belongs to these retained conversation
                // trees. Drop them before waking blocked producers so the
                // queue's byte counter never advertises memory that is still
                // live in this consumer arm (#320).
                if let Ok(outcome) = &batch_outcome {
                    for (name, range) in combined_connector_ranges {
                        if combined_conversations[range].iter().any(|conversation| {
                            outcome.deferred_sources.contains(&conversation.source_path)
                        }) {
                            failed_scan_connectors.insert(name.to_string());
                        }
                    }
                }
                drop(combined_conversations);
                flow_limiter.release(combined_byte_reservation);
                ingest_outcome = ingest_outcome.accumulate(batch_outcome?);

                // For tracing parity with the per-message path, use the
                // first batch's connector_name + the combined totals.
                let message_count = combined_message_count;
                let batch_size = combined_batch_size;

                // Periodic commit to make results visible incrementally.
                // The base interval is 5s (tunable via
                // CASS_STREAMING_CONSUMER_COMMIT_SECS); under responsiveness
                // pressure it is scaled down so the writer hold time and
                // buffered memory both shrink in lockstep with the rest of
                // the pipeline. It never drops below a multiple of what the
                // last commit cost (see `paced_streaming_commit_interval`).
                if commit_pacer.due() {
                    commit_pacer.commit(t_index.as_deref_mut(), Some(storage.raw()));
                    // Do not advance the legacy global `last_scan_ts` from a
                    // partial streaming run: a later connector scan error would
                    // make that global watermark unsafe for legacy fallback.
                    // Successful connector-specific markers are persisted when
                    // each producer's Done message is processed.
                    if scan_watermark_preservation_active() {
                        tracing::debug!(
                            "preserving streaming incremental last_scan_ts because scan exclusions or active source skips are active"
                        );
                    }
                }

                tracing::info!(
                    connector = connector_name,
                    conversations = batch_size,
                    messages = message_count,
                    "streaming_ingest"
                );
            }
            Ok(IndexMessage::SourceComplete {
                connector_name,
                conversations,
                completion,
                byte_reservation,
            }) => {
                let count = conversations.len();
                let messages = conversations
                    .iter()
                    .map(|conv| conv.messages.len())
                    .sum::<usize>();
                if !switched_to_indexing {
                    if let Some(p) = progress {
                        p.phase
                            .store(INDEX_PHASE_LEXICAL_INDEXING, Ordering::Relaxed);
                        p.total.store(0, Ordering::Relaxed);
                        p.current.store(0, Ordering::Relaxed);
                        p.total_is_final.store(false, Ordering::Relaxed);
                    }
                    switched_to_indexing = true;
                }
                if let Some(p) = progress {
                    p.total.fetch_add(count, Ordering::Relaxed);
                    p.tick_activity();
                }
                let mut completion_written = !failed_scan_connectors.contains(connector_name);
                let first = if should_inject_non_watch_ingest_test_oom(&conversations) {
                    Err(anyhow::Error::new(
                        crate::franken_sync::FrankenError::OutOfMemory,
                    ))
                } else {
                    ingest_batch_detailed_with_source_completion(
                        storage,
                        t_index.as_deref_mut(),
                        data_dir,
                        &conversations,
                        progress,
                        lexical_strategy,
                        defer_streaming_checkpoints,
                        progress_bump,
                        completion_written.then_some(&completion),
                    )
                };
                let result = match first {
                    Err(error) if error_is_out_of_memory(&error) => {
                        // Preserve the ordinary split/defer/quarantine recovery.
                        // A partial retry must never certify this source as complete.
                        completion_written = false;
                        ingest_non_watch_oom_retry_or_quarantine(
                            storage,
                            data_dir,
                            &conversations,
                            progress,
                            lexical_strategy,
                            defer_streaming_checkpoints,
                            progress_bump,
                            error,
                        )
                    }
                    result => result,
                };
                if let Ok(outcome) = &result
                    && conversations.iter().any(|conversation| {
                        outcome.deferred_sources.contains(&conversation.source_path)
                    })
                {
                    failed_scan_connectors.insert(connector_name.to_string());
                }
                drop(conversations);
                flow_limiter.release(byte_reservation);
                ingest_outcome = ingest_outcome.accumulate(result?);
                total_conversations += count;
                total_messages += messages;
                remember_discovered_connector(&mut discovered_names, connector_name);
                let stats = connector_stats
                    .entry(connector_name.to_string())
                    .or_insert_with(|| ConnectorStats {
                        name: connector_name.to_string(),
                        ..Default::default()
                    });
                stats.conversations += count;
                stats.messages += messages;
                if commit_pacer.due() {
                    commit_pacer.commit(t_index.as_deref_mut(), Some(storage.raw()));
                }
                if completion_written {
                    tracing::info!(
                        connector = connector_name,
                        conversations = count,
                        "source_ingest_committed"
                    );
                    source_commits += 1;
                    if source_commit_limit.is_some_and(|limit| source_commits >= limit)
                        && let Some(progress) = progress
                    {
                        progress.request_stop();
                    }
                }
            }
            Ok(IndexMessage::ScanError {
                connector_name,
                error,
            }) => {
                ingest_outcome.scan_had_errors = true;
                failed_scan_connectors.insert(connector_name.to_string());
                // Record error in connector stats
                let stats = connector_stats
                    .entry(connector_name.to_string())
                    .or_insert_with(|| ConnectorStats {
                        name: connector_name.to_string(),
                        ..Default::default()
                    });
                stats.error = Some(error.clone());

                tracing::warn!(
                    connector = connector_name,
                    error = %error,
                    "streaming_scan_error"
                );
                // Continue processing - scan errors are non-fatal
            }
            Ok(IndexMessage::Done {
                connector_name,
                scan_ms,
                is_discovered,
                scan_succeeded,
                active_source_skipped,
            }) => {
                active_producers -= 1;
                let effective_scan_succeeded =
                    scan_succeeded && !failed_scan_connectors.contains(connector_name);

                // Record scan timing in connector stats
                let stats = connector_stats
                    .entry(connector_name.to_string())
                    .or_insert_with(|| ConnectorStats {
                        name: connector_name.to_string(),
                        ..Default::default()
                    });
                stats.scan_ms = scan_ms;

                if is_discovered {
                    remember_discovered_connector(&mut discovered_names, connector_name);
                }
                if is_discovered && effective_scan_succeeded {
                    let connector_watermark_safe = scan_start_ts.is_some()
                        && !scan_path_exclusions_active()
                        && !active_source_skipped;
                    if connector_watermark_safe {
                        ingest_outcome
                            .scanned_connectors
                            .insert(connector_name.to_string());
                    }
                    if let Some(ts) = scan_start_ts
                        && connector_watermark_safe
                        && let Err(e) = persist::with_ephemeral_writer(
                            storage,
                            true,
                            "updating streaming connector scan watermark",
                            |writer| record_connector_scan_watermark(writer, connector_name, ts),
                        )
                    {
                        tracing::warn!(
                            connector = connector_name,
                            "streaming connector last_scan_ts save failed: {}",
                            e
                        );
                    } else if !connector_watermark_safe {
                        tracing::debug!(
                            connector = connector_name,
                            active_source_skipped,
                            path_exclusions_active = scan_path_exclusions_active(),
                            "preserving this connector's scan watermark while allowing unrelated connectors to advance"
                        );
                    }
                    if scan_start_ts.is_some() && !connector_watermark_safe {
                        record_scan_contract_with_watermark_held(storage, connector_name, true);
                    }
                } else if !effective_scan_succeeded {
                    ingest_outcome.scan_had_errors = true;
                }

                // If we haven't switched to indexing phase yet, this Done message represents
                // a completed scan step. Increment current to show scanning progress.
                if !switched_to_indexing && let Some(p) = progress {
                    p.current.fetch_add(1, Ordering::Relaxed);
                }

                tracing::debug!(
                    connector = connector_name,
                    scan_ms,
                    remaining = active_producers,
                    "streaming_producer_done"
                );
                if active_producers == 0 {
                    break;
                }
            }
            Err(_) => {
                let error = format!(
                    "streaming indexing aborted: channel closed with {active_producers} producers still active"
                );
                tracing::warn!(remaining = active_producers, error = %error);
                set_progress_last_error(progress.as_ref(), Some(error.clone()));
                return Err(anyhow::anyhow!(error));
            }
        }
    }

    // Final commit to ensure all data is persisted
    if let Some(t_index) = t_index
        && !ingest_outcome.lexical_update_deferred
    {
        // Whole-archive republish deferral: this run's persist batches
        // staged no region documents (the per-batch prelude skipped on the
        // latched writer); converge everything they left `pending` in ONE
        // bounded pass now — a handful of upserts and publishes for the
        // whole run — and let the commit below stamp them all `published`
        // together. Not draining here would leave the run's region marks
        // pending; the drain is a no-op when the latch was never set.
        if t_index.region_doc_convergence_deferred_to_final_drain() {
            t_index.converge_pending_search_region_docs(storage.raw(), i64::MAX)?;
        }
        // Final streaming commit seals every region document staged
        // during the run — the queued marks stamps land `published`
        // only after the commit succeeds.
        t_index.commit_with_region_doc_stamps(storage.raw())?;
    } else if ingest_outcome.lexical_update_deferred {
        tracing::warn!(
            "skipping final streaming Tantivy commit because lexical updates were deferred; authoritative DB rebuild will replace derived lexical assets"
        );
    }

    let index_ms = index_start.elapsed().as_millis() as u64;

    // Calculate total scan time (max of all connector scan times since they run in parallel)
    let scan_ms = connector_stats
        .values()
        .map(|s| s.scan_ms)
        .max()
        .unwrap_or(0);

    // Update progress with final stats (T7.4)
    if let Some(p) = progress
        && let Ok(mut stats) = p.stats.lock()
    {
        stats.scan_ms = scan_ms;
        stats.index_ms = index_ms;
        stats.connectors = connector_stats.values().cloned().collect();
        stats.agents_discovered = discovered_names.clone();
        stats.total_conversations = total_conversations;
        stats.total_messages = total_messages;
        stats.quarantined_conversations = stats
            .quarantined_conversations
            .saturating_add(ingest_outcome.quarantined_conversations);
        stats.lexical_update_deferred |= ingest_outcome.lexical_update_deferred;
        stats.scan_had_errors |= ingest_outcome.scan_had_errors;
    }

    tracing::info!(
        total_conversations,
        total_messages,
        scan_ms,
        index_ms,
        discovered = discovered_names.len(),
        "streaming_indexing_complete"
    );

    Ok((discovered_names, ingest_outcome))
}

/// Run indexing using streaming architecture with backpressure.
///
/// This spawns producer threads for each connector that send batches through
/// a bounded channel. The consumer receives and ingests batches as they arrive,
/// providing backpressure when indexing falls behind scanning.
#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn run_streaming_index(
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
    run_streaming_index_with_connector_factories(
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
