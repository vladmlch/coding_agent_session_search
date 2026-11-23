use super::*;

pub(in crate::indexer) fn is_database_watch_root(kind: ConnectorKind, root: &ScanRoot) -> bool {
    is_database_watch_path(kind, &root.path) && root.path.is_file()
}

pub(in crate::indexer) fn is_database_watch_path(kind: ConnectorKind, path: &Path) -> bool {
    kind == ConnectorKind::Devin && path.extension().is_some_and(|extension| extension == "db")
}

/// Devin takes an explicit file root as its database and fails the
/// whole scan on anything else. So `--watch-once <file>` under a directory root
/// hands it the file only when it is a SQLite database. An unhinted Codex
/// rollout under a shared configured source used to fail the run (GH #524).
pub(in crate::indexer) fn explicit_watch_once_file_may_be_database(
    kind: ConnectorKind,
    path: &Path,
) -> bool {
    if kind != ConnectorKind::Devin || !path.is_file() {
        return true;
    }
    let mut header = [0_u8; 16];
    std::fs::File::open(path)
        .and_then(|mut file| std::io::Read::read_exact(&mut file, &mut header))
        .is_ok_and(|()| &header == b"SQLite format 3\0")
}

/// FSEvents (macOS) reports a write through a file descriptor that stays open
/// only when that descriptor closes, and SQLite keeps a provider's `-wal` open
/// for the life of its connection, so a running Devin or Shelley's WAL-only
/// commits never arrive as events there (s1nqh). Poll each database root's
/// directory at the watch interval as well, queueing only the database and its
/// sidecars. The native watcher stays authoritative: a poll error is logged,
/// never escalated to a full-root rescan.
pub(in crate::indexer) fn database_root_poller(
    databases: &BTreeSet<PathBuf>,
    interval: Duration,
    backlog: Arc<WatchNotificationMailbox>,
    rescan: Arc<AtomicBool>,
    wake: Sender<()>,
) -> (Option<notify::PollWatcher>, bool) {
    if databases.is_empty() {
        return (None, false);
    }
    let files: HashSet<PathBuf> = databases
        .iter()
        .flat_map(|database| {
            let [wal, shm] = database_sidecar_paths(database);
            [database.clone(), wal, shm]
        })
        .collect();
    let directories: BTreeSet<&Path> = databases
        .iter()
        .map(|database| database.parent().unwrap_or(database.as_path()))
        .collect();
    // notify 8.2 reports failed initial directory scans synchronously through
    // the callback, yet PollWatcher::watch still returns Ok. Only these root
    // errors invalidate registration; an unrelated child must not rearm it.
    let registration_directories: HashSet<PathBuf> =
        directories.iter().map(|path| path.to_path_buf()).collect();
    let callback_registration_failed = Arc::new(AtomicBool::new(false));
    let failed_for_callback = Arc::clone(&callback_registration_failed);
    let mut poller = match notify::PollWatcher::new(
        move |result: notify::Result<notify::Event>| match result {
            Ok(mut event) => {
                event.paths.retain(|path| files.contains(path));
                enqueue_watch_notification(Ok(event), &backlog, &rescan, &wake);
            }
            Err(error) => {
                if error
                    .paths
                    .iter()
                    .any(|path| registration_directories.contains(path))
                {
                    failed_for_callback.store(true, Ordering::Release);
                }
                tracing::debug!(%error, "database root poll failed");
            }
        },
        notify::Config::default().with_poll_interval(interval),
    ) {
        Ok(poller) => poller,
        Err(error) => {
            tracing::warn!(%error, "failed to start polling database roots");
            return (None, true);
        }
    };
    let mut retry_registration = false;
    for directory in directories {
        if let Err(error) = poller.watch(directory, RecursiveMode::NonRecursive) {
            tracing::warn!("failed to poll {}: {}", directory.display(), error);
            retry_registration = true;
        } else {
            tracing::info!("polling {} for database commits", directory.display());
        }
    }
    // This is an initial-registration snapshot. Later polling errors retain
    // their existing watches and remain diagnostic, without retry/rescan churn.
    retry_registration |= callback_registration_failed.load(Ordering::Acquire);
    (Some(poller), retry_registration)
}

pub(in crate::indexer) fn watch_scan_lower_bound(
    kind: ConnectorKind,
    since_ts: Option<i64>,
) -> Option<i64> {
    if kind == ConnectorKind::Devin {
        // Database providers filter by activity time, which can precede
        // the WAL commit that triggered this scan. Filesystem event times cannot
        // bound it, even after rounding to seconds. Re-read the changed store and
        // let idempotent ingestion skip unchanged sessions; retain event
        // watermarks separately for scheduling and provenance.
        None
    } else {
        since_ts
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer) struct WatchRootIdentity {
    pub(in crate::indexer) is_directory: bool,
    #[cfg(unix)]
    pub(in crate::indexer) device: u64,
    #[cfg(unix)]
    pub(in crate::indexer) inode: u64,
    #[cfg(not(unix))]
    pub(in crate::indexer) created_at: Option<SystemTime>,
}

impl WatchRootIdentity {
    pub(in crate::indexer) fn from_metadata(metadata: &fs::Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;

        Self {
            is_directory: metadata.is_dir(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(not(unix))]
            created_at: metadata.created().ok(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(in crate::indexer) struct WatchSourceBinding {
    pub(in crate::indexer) resolved_path: PathBuf,
    pub(in crate::indexer) identity: WatchRootIdentity,
}

pub(in crate::indexer) struct WatchRootRegistration {
    pub(in crate::indexer) identity: WatchRootIdentity,
    pub(in crate::indexer) recursive: bool,
    pub(in crate::indexer) sources: BTreeMap<PathBuf, WatchSourceBinding>,
}

#[derive(Default)]
pub(in crate::indexer) struct WatchRootTarget {
    pub(in crate::indexer) recursive: bool,
    pub(in crate::indexer) sources: BTreeSet<PathBuf>,
}

#[derive(Default)]
pub(in crate::indexer) struct WatchRootRefresh {
    pub(in crate::indexer) reconcile: BTreeSet<PathBuf>,
    pub(in crate::indexer) database_roots: BTreeSet<PathBuf>,
}

/// Native backends can share one watch descriptor between aliases and nested
/// recursive registrations. Removing either registration then removes the
/// other's coverage too. Canonical targets must have disjoint recursive
/// coverage, while their source paths retain the configured provenance.
pub(in crate::indexer) fn coalesce_native_watch_targets(
    targets: BTreeMap<PathBuf, WatchRootTarget>,
) -> BTreeMap<PathBuf, WatchRootTarget> {
    let mut kept: BTreeMap<PathBuf, WatchRootTarget> = BTreeMap::new();
    // Path ordering places every ancestor before its descendants.
    for (path, target) in targets {
        let covering = kept.iter_mut().find_map(|(ancestor, existing)| {
            if existing.recursive && path.starts_with(ancestor) {
                Some(&mut existing.sources)
            } else {
                None
            }
        });
        if let Some(sources) = covering {
            sources.extend(target.sources);
        } else {
            kept.insert(path, target);
        }
    }
    kept
}

/// Native watches follow filesystem objects, not a permanently live pathname.
/// A failed initial registration or a replaced root needs another registration
/// and a census after it. Inspect only distinct roots, never their descendants;
/// unchanged healthy registrations do not request a scan.
pub(in crate::indexer) fn refresh_watch_root_registrations<W: Watcher>(
    watcher: &mut W,
    roots: &[(ConnectorKind, ScanRoot)],
    registered: &mut BTreeMap<PathBuf, WatchRootRegistration>,
    progress: Option<&Arc<IndexingProgress>>,
) -> Result<WatchRootRefresh> {
    let mut observations = BTreeMap::new();
    let mut source_bindings = BTreeMap::new();
    let mut refreshed = WatchRootRefresh::default();
    for (_, root) in roots {
        if let std::collections::btree_map::Entry::Vacant(entry) =
            observations.entry(root.path.clone())
        {
            if let Some(progress) = progress {
                progress.check_stop()?;
            }
            let observed = fs::metadata(&root.path);
            if let Some(progress) = progress {
                progress.tick_activity();
                progress.check_stop()?;
            }
            if let Ok(metadata) = &observed {
                let resolved_path =
                    dunce::canonicalize(&root.path).unwrap_or_else(|_| root.path.clone());
                if let Some(progress) = progress {
                    progress.tick_activity();
                }
                source_bindings.insert(
                    root.path.clone(),
                    WatchSourceBinding {
                        resolved_path,
                        identity: WatchRootIdentity::from_metadata(metadata),
                    },
                );
            }
            if let Err(error) = &observed
                && error.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(path = %root.path.display(), %error, "watch root is temporarily unavailable; retrying discovery");
                refreshed.reconcile.insert(root.path.clone());
            }
            entry.insert(observed);
        }
    }

    // A database watches its parent for WAL creation. Several connectors or
    // provenances can share the same native path; install the strongest mode
    // once and retain every source root for the subsequent ordinary census.
    let mut targets: BTreeMap<PathBuf, WatchRootTarget> = BTreeMap::new();
    for (kind, root) in roots {
        // Missing roots stay in `roots` for the next metadata probe. Do not
        // enroll them in an existing ancestor's coverage yet: their later
        // appearance must request a census even if it produces no event.
        let Some(Ok(metadata)) = observations.get(&root.path) else {
            continue;
        };
        let database_file = is_database_watch_path(*kind, &root.path) && metadata.is_file();
        let watch_path = if database_file {
            refreshed.database_roots.insert(root.path.clone());
            root.path.parent().unwrap_or(&root.path)
        } else {
            &root.path
        };
        let target = targets.entry(watch_path.to_path_buf()).or_default();
        target.recursive |= !database_file;
        target.sources.insert(root.path.clone());
    }
    let mut native_targets: BTreeMap<PathBuf, WatchRootTarget> = BTreeMap::new();
    for (path, target) in targets {
        if let Some(progress) = progress {
            progress.check_stop()?;
        }
        let native_path = if let Some(binding) = source_bindings.get(&path) {
            binding.resolved_path.clone()
        } else {
            let native_path = dunce::canonicalize(&path).unwrap_or(path);
            if let Some(progress) = progress {
                progress.tick_activity();
            }
            native_path
        };
        let native_target = native_targets.entry(native_path).or_default();
        native_target.recursive |= target.recursive;
        native_target.sources.extend(target.sources);
    }
    let targets = coalesce_native_watch_targets(native_targets);
    let mut native_registration_changed = false;
    let obsolete: Vec<_> = registered
        .keys()
        .filter(|path| !targets.contains_key(*path))
        .cloned()
        .collect();
    for path in obsolete {
        if let Some(progress) = progress {
            progress.check_stop()?;
        }
        native_registration_changed = true;
        let _ = watcher.unwatch(&path);
        registered.remove(&path);
        if let Some(progress) = progress {
            progress.tick_activity();
        }
    }

    for (path, WatchRootTarget { recursive, sources }) in targets {
        if let Some(progress) = progress {
            progress.check_stop()?;
        }
        let observed = observations.entry(path.clone()).or_insert_with(|| {
            let observed = fs::metadata(&path);
            if let Some(progress) = progress {
                progress.tick_activity();
            }
            observed
        });
        let identity = match observed {
            Ok(metadata) => WatchRootIdentity::from_metadata(metadata),
            Err(error) => {
                if error.kind() == std::io::ErrorKind::NotFound {
                    if registered.remove(&path).is_some() {
                        native_registration_changed = true;
                        let _ = watcher.unwatch(&path);
                        if let Some(progress) = progress {
                            progress.tick_activity();
                        }
                    }
                } else {
                    tracing::warn!(path = %path.display(), %error, "watch root is temporarily unavailable; retrying discovery");
                    refreshed.reconcile.extend(sources);
                }
                continue;
            }
        };
        let bindings: BTreeMap<_, _> = sources
            .iter()
            .filter_map(|source| {
                source_bindings
                    .get(source)
                    .map(|binding| (source.clone(), binding.clone()))
            })
            .collect();
        if let Some(registration) = registered.get_mut(&path)
            && registration.identity == identity
            && registration.recursive == recursive
        {
            // A new source or a retargeted alias can stay within an existing
            // parent's native coverage. Its configured identity still needs
            // a census; a symlink change outside that parent emits no event
            // there. Source bindings ignore ordinary directory mtime changes.
            for (source, binding) in &bindings {
                if registration.sources.get(source) != Some(binding) {
                    refreshed.reconcile.insert(source.clone());
                }
            }
            registration.sources = bindings;
            continue;
        }
        if registered.remove(&path).is_some() {
            let _ = watcher.unwatch(&path);
            if let Some(progress) = progress {
                progress.tick_activity();
                progress.check_stop()?;
            }
        }
        let mode = if recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };
        native_registration_changed = true;
        match watcher.watch(&path, mode) {
            Ok(()) => {
                tracing::info!(path = %path.display(), "registered watch root; reconciling source snapshot");
                registered.insert(
                    path,
                    WatchRootRegistration {
                        identity,
                        recursive,
                        sources: bindings,
                    },
                );
            }
            Err(error) => {
                // While registration remains unavailable, ordinary source
                // receipts provide bounded fallback discovery at the existing
                // watch cooldown. A warning alone would strand future writes.
                tracing::warn!(path = %path.display(), %error, "failed to watch root; retrying registration and source discovery");
            }
        }
        if let Some(progress) = progress {
            progress.tick_activity();
            progress.check_stop()?;
        }
        refreshed.reconcile.extend(sources);
    }
    if native_registration_changed && W::kind() == notify::WatcherKind::Fsevent {
        // notify 8.2 restarts its entire FSEvents stream with SinceNow for
        // every watch/unwatch attempt, even an unsuccessful one. Writes to
        // other roots in that gap need an ordinary post-registration census.
        // Unchanged registrations never trigger this reconciliation.
        refreshed.reconcile.extend(source_bindings.into_keys());
    }
    Ok(refreshed)
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn watch_sources<F>(
    watch_once_paths: Option<Vec<PathBuf>>,
    roots: Vec<(ConnectorKind, ScanRoot)>,
    event_channel: Option<(Sender<IndexerEvent>, Receiver<IndexerEvent>)>,
    stale_detector: Arc<StaleDetector>,
    progress: Option<&Arc<IndexingProgress>>,
    watch_interval_secs: u64,
    deferred_sources: BTreeSet<PathBuf>,
    callback: F,
) -> Result<()>
where
    F: Fn(Vec<PathBuf>, &[(ConnectorKind, ScanRoot)], bool) -> Result<BTreeSet<PathBuf>>,
{
    if let Some(paths) = watch_once_paths {
        if !paths.is_empty() {
            callback(paths, &roots, false)?;
        }
        return Ok(());
    }

    let (_control_sender, controls) = event_channel
        .map(|(sender, receiver)| (Some(sender), receiver))
        .unwrap_or_else(|| (None, never()));
    let (wake, notifications) = bounded(1);
    let notification_backlog = Arc::new(WatchNotificationMailbox::default());
    let notification_rescan = Arc::new(AtomicBool::new(false));
    let backlog_for_watcher = Arc::clone(&notification_backlog);
    let rescan_for_watcher = Arc::clone(&notification_rescan);
    let wake_for_watcher = wake.clone();
    let mut watcher = recommended_watcher(move |result| {
        enqueue_watch_notification(
            result,
            &backlog_for_watcher,
            &rescan_for_watcher,
            &wake_for_watcher,
        );
    })?;

    let mut registered_roots = BTreeMap::new();
    let initial_roots =
        refresh_watch_root_registrations(&mut watcher, &roots, &mut registered_roots, progress)?;
    let mut polled_database_roots = initial_roots.database_roots;
    let (mut database_poller, mut retry_database_registration) = database_root_poller(
        &polled_database_roots,
        Duration::from_secs(watch_interval_secs.max(1)),
        Arc::clone(&notification_backlog),
        Arc::clone(&notification_rescan),
        wake.clone(),
    );
    let mut last_registration_check = Instant::now();

    let debounce = Duration::from_secs(2);
    let max_wait = Duration::from_secs(5);
    // Minimum interval between scan cycles to prevent tight-loop CPU burn
    // when filesystem events arrive continuously. Default: 30s. (Issue #129)
    let min_scan_interval = Duration::from_secs(watch_interval_secs.max(1));
    // Stale check interval: check every 5 minutes for quicker detection
    let stale_check_interval = Duration::from_secs(300);
    let mut pending = WatchEventBacklog::default();
    // Register notifications before reconciling the startup snapshot. A file
    // written after startup discovery but before watch registration produces
    // no event for this watcher; source receipts make this second census cheap.
    let mut pending_rescan = !pending.extend(
        deferred_sources
            .into_iter()
            .chain(roots.iter().map(|(_, root)| root.path.clone())),
    );
    let mut first_event = (pending_rescan || !pending.paths.is_empty()).then(Instant::now);
    let mut last_stale_check = Instant::now();
    // Initialize to the past so the first scan can fire immediately.
    // Use checked_sub to avoid panic if system uptime < min_scan_interval
    // (e.g., --watch-interval 999999 on a freshly booted system).
    // If the full interval won't fit, try smaller values so the first scan
    // still fires quickly rather than waiting the full cooldown.
    let mut last_scan = [
        min_scan_interval,
        Duration::from_secs(60),
        Duration::from_secs(1),
    ]
    .iter()
    .find_map(|d| Instant::now().checked_sub(*d))
    .unwrap_or_else(Instant::now);

    tracing::info!(
        watch_interval_secs,
        "watch mode: minimum interval between scan cycles"
    );

    loop {
        if let Some(progress) = progress {
            progress.check_stop()?;
        }
        if last_registration_check.elapsed() >= min_scan_interval {
            let mut refreshed = refresh_watch_root_registrations(
                &mut watcher,
                &roots,
                &mut registered_roots,
                progress,
            )?;
            if refreshed.database_roots != polled_database_roots
                || retry_database_registration
                || !refreshed.reconcile.is_disjoint(&refreshed.database_roots)
            {
                // A newly created database may share an already-watched
                // parent. Its native events alone do not cover macOS writes
                // through a long-lived WAL descriptor; enroll it in polling
                // even when no native parent registration needed replacement.
                drop(database_poller.take());
                polled_database_roots = refreshed.database_roots;
                (database_poller, retry_database_registration) = database_root_poller(
                    &polled_database_roots,
                    min_scan_interval,
                    Arc::clone(&notification_backlog),
                    Arc::clone(&notification_rescan),
                    wake.clone(),
                );
                // A replacement poller treats current WAL contents as its
                // initial snapshot. Reconcile the selected databases after
                // every attempt, including partial failure, or healthy peers
                // can lose writes while another directory keeps retrying.
                refreshed
                    .reconcile
                    .extend(polled_database_roots.iter().cloned());
            }
            last_registration_check = Instant::now();
            let recovered = refreshed.reconcile;
            if !recovered.is_empty() {
                if !pending_rescan && pending.paths.is_empty() {
                    first_event = Some(Instant::now());
                }
                if !pending_rescan && !pending.extend(recovered) {
                    pending_rescan = true;
                }
            }
        }
        // How much cooldown remains before we may fire the next callback.
        // Using this as recv_timeout lets us keep accumulating events
        // instead of blocking with thread::sleep (which would drop events
        // if the inotify buffer fills up).
        let cooldown_remaining = min_scan_interval.saturating_sub(last_scan.elapsed());

        // Calculate timeout: use stale check interval when idle, debounce when active
        let timeout = if !pending_rescan && pending.paths.is_empty() {
            // Wake for root registration only. Healthy registrations do not
            // create scan work, and an active batch retains its existing
            // debounce/cooldown behavior.
            stale_check_interval
                .min(min_scan_interval.saturating_sub(last_registration_check.elapsed()))
        } else {
            let now = Instant::now();
            let elapsed = now.duration_since(first_event.unwrap_or(now));
            if elapsed >= max_wait {
                if cooldown_remaining.is_zero() {
                    // Cooldown elapsed and max_wait exceeded: fire now.
                    dispatch_pending_watch_callback(
                        &mut pending,
                        &mut pending_rescan,
                        &roots,
                        &callback,
                    );
                    last_scan = Instant::now();
                    first_event = (pending_rescan || !pending.paths.is_empty()).then(Instant::now);
                    continue;
                }
                // max_wait exceeded but cooldown still active: wait for
                // the remaining cooldown while accumulating new events.
                cooldown_remaining
            } else {
                let remaining = max_wait.saturating_sub(elapsed);
                // Use the larger of (debounce, cooldown_remaining) to ensure
                // we never fire the callback faster than min_scan_interval.
                debounce.min(remaining).max(cooldown_remaining)
            }
        };

        // Idle watchers still honor cooperative shutdown. This poll does not
        // tick activity: it must never hide a stuck scan from the watchdog.
        match wait_watch_loop_event(&controls, &notifications, timeout, progress)? {
            WatchLoopEvent::Filesystem => {
                if !pending_rescan && pending.paths.is_empty() {
                    first_event = Some(Instant::now());
                }
                collect_watch_notifications(
                    &notification_backlog,
                    &notification_rescan,
                    &mut pending,
                    &mut pending_rescan,
                );
            }
            WatchLoopEvent::Control(IndexerEvent::Notify(paths)) => {
                if !pending_rescan && pending.paths.is_empty() {
                    first_event = Some(Instant::now());
                }
                if !pending_rescan && !pending.extend(paths) {
                    pending_rescan = true;
                }
            }
            WatchLoopEvent::Control(IndexerEvent::Command(cmd)) => match cmd {
                ReindexCommand::Full => {
                    // Full rebuild commands bypass cooldown for responsive
                    // operator-initiated rebuilds. Drain notifications that
                    // precede the scan; later events remain queued for retry.
                    collect_watch_notifications(
                        &notification_backlog,
                        &notification_rescan,
                        &mut pending,
                        &mut pending_rescan,
                    );
                    pending_rescan = true;
                    dispatch_pending_watch_callback(
                        &mut pending,
                        &mut pending_rescan,
                        &roots,
                        &callback,
                    );
                    last_scan = Instant::now();
                    first_event = (pending_rescan || !pending.paths.is_empty()).then(Instant::now);
                }
            },
            WatchLoopEvent::Timeout => {
                // Process pending events only if cooldown has elapsed
                if (pending_rescan || !pending.paths.is_empty())
                    && last_scan.elapsed() >= min_scan_interval
                {
                    dispatch_pending_watch_callback(
                        &mut pending,
                        &mut pending_rescan,
                        &roots,
                        &callback,
                    );
                    last_scan = Instant::now();
                    first_event = (pending_rescan || !pending.paths.is_empty()).then(Instant::now);
                }

                // Periodic stale check
                let now = Instant::now();
                if now.duration_since(last_stale_check) >= stale_check_interval {
                    last_stale_check = now;

                    if let Some(action) = stale_detector.check_stale() {
                        let stats = stale_detector.stats();
                        match action {
                            StaleAction::Warn => {
                                tracing::warn!(
                                    consecutive_zero_scans = stats.consecutive_zero_scans,
                                    seconds_since_last_ingest = ?stats.seconds_since_last_ingest,
                                    total_ingests = stats.total_ingests,
                                    "watch daemon appears stale: no conversations indexed recently"
                                );
                                tracing::info!(
                                    "hint: run 'cass index --full' to rebuild, or set \
                                     CASS_WATCH_STALE_ACTION=rebuild for auto-recovery"
                                );
                            }
                            StaleAction::Rebuild => {
                                tracing::warn!(
                                    consecutive_zero_scans = stats.consecutive_zero_scans,
                                    seconds_since_last_ingest = ?stats.seconds_since_last_ingest,
                                    "stale state detected, triggering automatic full rebuild"
                                );
                                // Trigger full rebuild
                                collect_watch_notifications(
                                    &notification_backlog,
                                    &notification_rescan,
                                    &mut pending,
                                    &mut pending_rescan,
                                );
                                pending_rescan = true;
                                dispatch_pending_watch_callback(
                                    &mut pending,
                                    &mut pending_rescan,
                                    &roots,
                                    &callback,
                                );
                                last_scan = Instant::now();
                                first_event = (pending_rescan || !pending.paths.is_empty())
                                    .then(Instant::now);
                            }
                            StaleAction::None => {
                                // Stale detection disabled, should not reach here
                            }
                        }
                    }
                }
            }
            WatchLoopEvent::Disconnected => break,
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
pub(in crate::indexer) mod watch_registration_tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    fn wait_for_native_write(events: &Receiver<notify::Result<notify::Event>>, path: &Path) {
        let expected = dunce::canonicalize(path).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let event = events
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("native write notification must arrive")
                .expect("native watcher must remain available");
            if watch_event_should_trigger_reindex(&event)
                && event.paths.iter().any(|observed| observed == &expected)
            {
                return;
            }
        }
    }

    struct NativeWatch {
        _temp: TempDir,
        root: PathBuf,
        observed: Receiver<Option<String>>,
        progress: Arc<IndexingProgress>,
        worker: Option<JoinHandle<Result<()>>>,
    }

    impl NativeWatch {
        fn start(root_exists: bool) -> Self {
            let temp = TempDir::new().unwrap();
            let root = temp.path().join("sessions");
            if root_exists {
                fs::create_dir(&root).unwrap();
                fs::write(root.join("rollout.jsonl"), "baseline").unwrap();
            }
            let progress = Arc::new(IndexingProgress::default());
            let worker_progress = Arc::clone(&progress);
            let worker_root = root.clone();
            let (observed_tx, observed) = crossbeam_channel::unbounded();
            let worker = thread::spawn(move || {
                watch_sources(
                    None,
                    vec![(ConnectorKind::Codex, ScanRoot::local(worker_root.clone()))],
                    None,
                    Arc::new(StaleDetector::new(StaleConfig {
                        action: StaleAction::None,
                        ..StaleConfig::default()
                    })),
                    Some(&worker_progress),
                    1,
                    BTreeSet::new(),
                    move |paths, roots, force_full| {
                        assert!(
                            !force_full,
                            "registration recovery is an ordinary source census"
                        );
                        assert_eq!(roots.len(), 1);
                        let native_root = dunce::canonicalize(&worker_root).ok();
                        assert!(paths.iter().any(|path| {
                            path.starts_with(&worker_root)
                                || native_root
                                    .as_ref()
                                    .is_some_and(|root| path.starts_with(root))
                        }));
                        observed_tx
                            .send(fs::read_to_string(worker_root.join("rollout.jsonl")).ok())?;
                        Ok(BTreeSet::new())
                    },
                )
            });
            Self {
                _temp: temp,
                root,
                observed,
                progress,
                worker: Some(worker),
            }
        }

        fn wait_for(&self, content: Option<&str>) {
            let deadline = Instant::now() + Duration::from_secs(12);
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let observed = self
                    .observed
                    .recv_timeout(remaining)
                    .unwrap_or_else(|error| {
                        panic!(
                            "native watch did not revisit {content:?} after root recovery: {error}"
                        )
                    });
                if observed.as_deref() == content {
                    return;
                }
            }
        }
    }

    impl Drop for NativeWatch {
        fn drop(&mut self) {
            self.progress.request_stop();
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }

    #[test]
    fn gh538_native_watch_retries_failed_root_registration() {
        let watch = NativeWatch::start(false);
        // The initial registration fails on a real absent directory. Wait for
        // the initial census to finish before creating it; no existing parent
        // watch or later operator command can deliver this change for us.
        watch.wait_for(None);
        fs::create_dir(&watch.root).unwrap();
        fs::write(watch.root.join("rollout.jsonl"), "first arrival").unwrap();
        watch.wait_for(Some("first arrival"));
        fs::write(watch.root.join("rollout.jsonl"), "successive append").unwrap();
        watch.wait_for(Some("successive append"));
    }

    #[test]
    fn gh538_native_watch_rearms_replaced_root() {
        let watch = NativeWatch::start(true);
        watch.wait_for(Some("baseline"));
        // Preserve the old directory and its source. Replacing a watched
        // pathname gives the new directory a different inode; the old native
        // registration alone cannot observe writes to that replacement.
        fs::rename(&watch.root, watch.root.with_file_name("preserved-sessions")).unwrap();
        fs::create_dir(&watch.root).unwrap();
        fs::write(watch.root.join("rollout.jsonl"), "replacement").unwrap();
        watch.wait_for(Some("replacement"));
        fs::write(watch.root.join("rollout.jsonl"), "replacement append").unwrap();
        watch.wait_for(Some("replacement append"));
        assert_eq!(
            fs::read_to_string(
                watch
                    .root
                    .with_file_name("preserved-sessions")
                    .join("rollout.jsonl")
            )
            .unwrap(),
            "baseline"
        );
    }

    #[test]
    fn gh538_healthy_watch_registration_does_not_schedule_periodic_scans() {
        let watch = NativeWatch::start(true);
        watch.wait_for(Some("baseline"));
        assert!(
            watch.observed.recv_timeout(Duration::from_secs(3)).is_err(),
            "healthy root checks must not manufacture indexing work"
        );
    }

    #[test]
    fn gh538_native_watch_retargets_alias_without_losing_shared_root() {
        let temp = TempDir::new().unwrap();
        let original = temp.path().join("original");
        let replacement = temp.path().join("new-target");
        let alias = temp.path().join("alias");
        fs::create_dir(&original).unwrap();
        fs::create_dir(&replacement).unwrap();
        symlink(&original, &alias).unwrap();
        let roots = vec![
            (ConnectorKind::Codex, ScanRoot::local(original.clone())),
            (ConnectorKind::Codex, ScanRoot::local(alias.clone())),
        ];
        let (events_tx, events) = crossbeam_channel::unbounded();
        let mut watcher = recommended_watcher(move |event| {
            let _ = events_tx.send(event);
        })
        .unwrap();
        let mut registered = BTreeMap::new();
        refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None).unwrap();
        assert_eq!(registered.len(), 1, "aliases must share one native watch");
        let before = original.join("before.jsonl");
        fs::write(&before, "original target").unwrap();
        wait_for_native_write(&events, &before);

        let link = temp.path().join("replacement-link");
        symlink(&replacement, &link).unwrap();
        fs::rename(link, &alias).unwrap();
        let refreshed =
            refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None).unwrap();
        let expected = if notify::RecommendedWatcher::kind() == notify::WatcherKind::Fsevent {
            roots.iter().map(|(_, root)| root.path.clone()).collect()
        } else {
            BTreeSet::from([alias])
        };
        assert_eq!(refreshed.reconcile, expected);
        assert_eq!(registered.len(), 2);
        let moved = replacement.join("rearmed.jsonl");
        fs::write(&moved, "retargeted alias").unwrap();
        wait_for_native_write(&events, &moved);

        // notify's inotify backend shares descriptors by inode. Unwatching
        // a separately registered alias would also disable this unchanged
        // root, even though its metadata still matched the registration.
        let kept = original.join("kept-after.jsonl");
        fs::write(&kept, "new conversation in unchanged root").unwrap();
        wait_for_native_write(&events, &kept);
        fs::write(&before, "successive write in unchanged root").unwrap();
        wait_for_native_write(&events, &before);
    }

    #[test]
    fn gh538_native_watch_coalesces_nested_alias_coverage() {
        let temp = TempDir::new().unwrap();
        let parent = temp.path().join("sessions");
        let child = parent.join("project");
        let arriving = parent.join("arriving");
        let replacement = temp.path().join("new-target");
        let alias = temp.path().join("child-alias");
        fs::create_dir_all(&child).unwrap();
        fs::create_dir(&replacement).unwrap();
        symlink(&child, &alias).unwrap();
        let roots = vec![
            (ConnectorKind::Codex, ScanRoot::local(parent.clone())),
            (ConnectorKind::Devin, ScanRoot::local(child.clone())),
            (ConnectorKind::Devin, ScanRoot::local(arriving.clone())),
            (ConnectorKind::Codex, ScanRoot::local(alias.clone())),
        ];
        let (events_tx, events) = crossbeam_channel::unbounded();
        let mut watcher = recommended_watcher(move |event| {
            let _ = events_tx.send(event);
        })
        .unwrap();
        let mut registered = BTreeMap::new();
        let initial =
            refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None).unwrap();
        assert_eq!(
            registered.len(),
            1,
            "recursive native coverage must not overlap"
        );
        assert_eq!(
            initial.reconcile,
            BTreeSet::from([parent, child.clone(), alias.clone()])
        );
        let existing = child.join("existing.jsonl");
        fs::write(&existing, "nested baseline").unwrap();
        wait_for_native_write(&events, &existing);

        // An already-watched ancestor must not count an absent configured
        // child as enrolled: appearance still needs its own first census.
        fs::create_dir(&arriving).unwrap();
        assert_eq!(
            refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None)
                .unwrap()
                .reconcile,
            BTreeSet::from([arriving])
        );
        let link = temp.path().join("replacement-link");
        symlink(&replacement, &link).unwrap();
        fs::rename(link, &alias).unwrap();
        let refreshed =
            refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None).unwrap();
        let expected = if notify::RecommendedWatcher::kind() == notify::WatcherKind::Fsevent {
            roots.iter().map(|(_, root)| root.path.clone()).collect()
        } else {
            BTreeSet::from([alias])
        };
        assert_eq!(refreshed.reconcile, expected);
        assert_eq!(registered.len(), 2);
        let moved = replacement.join("rearmed.jsonl");
        fs::write(&moved, "new alias target").unwrap();
        wait_for_native_write(&events, &moved);
        fs::write(&existing, "append under retained recursive ancestor").unwrap();
        wait_for_native_write(&events, &existing);
        assert!(
            refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None)
                .unwrap()
                .reconcile
                .is_empty(),
            "healthy coverage must not schedule another census"
        );
    }

    #[test]
    fn gh538_native_watch_retargets_alias_within_registered_ancestor() {
        let temp = TempDir::new().unwrap();
        let parent = temp.path().join("sessions");
        let first = parent.join("first-project");
        let second = parent.join("second-project");
        let alias = temp.path().join("external-alias");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir(&second).unwrap();
        symlink(&first, &alias).unwrap();
        let roots = vec![
            (ConnectorKind::Codex, ScanRoot::local(parent)),
            (ConnectorKind::Devin, ScanRoot::local(alias.clone())),
        ];
        let (events_tx, events) = crossbeam_channel::unbounded();
        let mut watcher = recommended_watcher(move |event| {
            let _ = events_tx.send(event);
        })
        .unwrap();
        let mut registered = BTreeMap::new();
        refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None).unwrap();
        assert_eq!(registered.len(), 1);
        assert!(
            refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None)
                .unwrap()
                .reconcile
                .is_empty()
        );

        // Both physical targets were already covered. The link changes
        // outside the native parent, so no event there can select this
        // configured source's new snapshot for its distinct connector.
        let link = temp.path().join("replacement-link");
        symlink(&second, &link).unwrap();
        fs::rename(link, &alias).unwrap();
        assert_eq!(
            refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None)
                .unwrap()
                .reconcile,
            BTreeSet::from([alias])
        );
        assert_eq!(registered.len(), 1, "native coverage remains unchanged");
        for (directory, text) in [(first, "old target"), (second, "new target")] {
            let path = directory.join("rollout.jsonl");
            fs::write(&path, text).unwrap();
            wait_for_native_write(&events, &path);
        }
    }

    #[test]
    fn gh538_registration_coalesces_paths_without_using_directory_mtime() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("sessions.db");
        fs::write(&database, b"SQLite format 3\0").unwrap();
        let roots = vec![
            (
                ConnectorKind::Codex,
                ScanRoot::local(temp.path().to_path_buf()),
            ),
            (ConnectorKind::Devin, ScanRoot::local(database.clone())),
        ];
        let mut watcher = recommended_watcher(|_| {}).unwrap();
        let mut registered = BTreeMap::new();
        let pending =
            refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None).unwrap();
        assert_eq!(pending.database_roots, BTreeSet::from([database.clone()]));
        assert_eq!(
            pending.reconcile,
            BTreeSet::from([temp.path().to_path_buf(), database])
        );
        assert_eq!(
            registered.len(),
            1,
            "one native registration per shared path"
        );
        assert!(
            registered[&dunce::canonicalize(temp.path()).unwrap()].recursive,
            "preserve the strongest watch mode"
        );

        // A directory's mtime changes during normal writes. That is an event,
        // not a replaced native watch, and must not force another whole-root
        // census after the corresponding event already completed.
        File::open(temp.path())
            .unwrap()
            .set_modified(UNIX_EPOCH + Duration::from_secs(1_600_000_000))
            .unwrap();
        assert!(
            refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None)
                .unwrap()
                .reconcile
                .is_empty()
        );
    }

    #[test]
    fn gh538_database_poller_retries_callback_registration_errors() {
        let temp = TempDir::new().unwrap();
        let parent = temp.path().join("provider");
        let database = parent.join("sessions.db");
        fs::create_dir(&parent).unwrap();
        fs::write(&database, b"SQLite format 3\0").unwrap();
        let roots = vec![(ConnectorKind::Devin, ScanRoot::local(database.clone()))];
        let mut watcher = recommended_watcher(|_| {}).unwrap();
        let mut registered = BTreeMap::new();
        let selected =
            refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None).unwrap();
        assert_eq!(selected.database_roots, BTreeSet::from([database.clone()]));

        // Reproduce the real registration race after discovery selected an
        // existing database. notify 8.2 reports this through the callback but
        // still returns Ok from PollWatcher::watch, leaving no poll enrolled.
        let preserved = temp.path().join("preserved-provider");
        fs::rename(&parent, &preserved).unwrap();
        let backlog = Arc::new(WatchNotificationMailbox::default());
        let rescan = Arc::new(AtomicBool::new(false));
        let (wake, notifications) = bounded(1);
        let (poller, retry) = database_root_poller(
            &selected.database_roots,
            Duration::from_millis(50),
            Arc::clone(&backlog),
            Arc::clone(&rescan),
            wake.clone(),
        );
        assert!(poller.is_some(), "the backend creates the poller itself");
        assert!(
            retry,
            "a callback registration error must request re-enrollment"
        );
        assert!(backlog.backlog.lock().unwrap().paths.is_empty());
        assert!(backlog.deferred_rx.is_empty());
        assert_eq!(backlog.retained.load(Ordering::Acquire), 0);
        assert!(!rescan.load(Ordering::Acquire));
        assert!(notifications.try_recv().is_err());
        drop(poller);

        // Restore the same inode before the next metadata probe: native root
        // identity alone cannot reveal that the poll registration was missed.
        fs::rename(&preserved, &parent).unwrap();
        let unrelated = parent.join("unrelated-broken-link");
        symlink(parent.join("not-present"), &unrelated).unwrap();
        let (poller, retry) = database_root_poller(
            &selected.database_roots,
            Duration::from_millis(50),
            Arc::clone(&backlog),
            Arc::clone(&rescan),
            wake,
        );
        let _poller = poller.expect("the restored parent can be polled");
        assert!(
            !retry,
            "an unrelated child error must not rearm the database poller"
        );

        let wal = database_sidecar_paths(&database)[0].clone();
        let mut provider = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal)
            .unwrap();
        provider
            .write_all(b"commit through a descriptor that stays open")
            .unwrap();
        provider.sync_all().unwrap();
        let mut pending = WatchEventBacklog::default();
        let mut pending_rescan = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pending.paths.contains(&wal) {
            notifications
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap();
            collect_watch_notifications(&backlog, &rescan, &mut pending, &mut pending_rescan);
        }
        assert!(
            !pending_rescan,
            "registration repair stays scoped to the database"
        );
        assert!(!pending.paths.contains(&unrelated));
        assert!(!rescan.load(Ordering::Acquire));
        drop(provider);
        drop(_poller);

        // Rebuilding a poller for a newly selected database also resets its
        // unchanged peer's snapshot. The real watch loop must request both
        // source censuses, even when the peer emitted no filesystem event.
        let watch_temp = TempDir::new().unwrap();
        let first = watch_temp.path().join("first.db");
        let arriving = watch_temp.path().join("arriving.db");
        fs::write(&first, "unchanged peer").unwrap();
        let roots = vec![
            (ConnectorKind::Devin, ScanRoot::local(first.clone())),
            (ConnectorKind::Devin, ScanRoot::local(arriving.clone())),
        ];
        let progress = Arc::new(IndexingProgress::default());
        let worker_progress = Arc::clone(&progress);
        let worker_arriving = arriving.clone();
        let (observed_tx, observed) = crossbeam_channel::unbounded();
        let worker = thread::spawn(move || {
            watch_sources(
                None,
                roots,
                None,
                Arc::new(StaleDetector::new(StaleConfig {
                    action: StaleAction::None,
                    ..StaleConfig::default()
                })),
                Some(&worker_progress),
                1,
                BTreeSet::new(),
                move |paths: Vec<PathBuf>, _, force_full: bool| {
                    assert!(!force_full, "poller repair uses an ordinary census");
                    let content = if paths.contains(&first) && paths.contains(&worker_arriving) {
                        fs::read_to_string(&worker_arriving).ok()
                    } else {
                        None
                    };
                    observed_tx.send(content)?;
                    Ok(BTreeSet::new())
                },
            )
        });
        let watch = NativeWatch {
            root: watch_temp.path().to_path_buf(),
            _temp: watch_temp,
            observed,
            progress,
            worker: Some(worker),
        };
        watch.wait_for(None);
        fs::write(&arriving, "late database arrival").unwrap();
        watch.wait_for(Some("late database arrival"));
    }

    #[test]
    fn gh538_late_database_under_registered_parent_joins_poll_discovery() {
        let temp = TempDir::new().unwrap();
        let existing = temp.path().join("existing.db");
        let arriving = temp.path().join("arriving.db");
        fs::write(&existing, b"SQLite format 3\0").unwrap();
        let roots = vec![
            (ConnectorKind::Devin, ScanRoot::local(existing.clone())),
            (ConnectorKind::Devin, ScanRoot::local(arriving.clone())),
        ];
        let mut watcher = recommended_watcher(|_| {}).unwrap();
        let mut registered = BTreeMap::new();
        let before =
            refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None).unwrap();
        assert_eq!(before.database_roots, BTreeSet::from([existing.clone()]));

        fs::write(&arriving, b"SQLite format 3\0").unwrap();
        let after =
            refresh_watch_root_registrations(&mut watcher, &roots, &mut registered, None).unwrap();
        assert_eq!(
            after.reconcile,
            BTreeSet::from([arriving.clone()]),
            "a new source needs a census even when its native parent is already registered"
        );
        assert_eq!(
            after.database_roots,
            BTreeSet::from([existing, arriving.clone()])
        );
        assert_ne!(before.database_roots, after.database_roots);

        let backlog = Arc::new(WatchNotificationMailbox::default());
        let rescan = Arc::new(AtomicBool::new(false));
        let (wake, notifications) = bounded(1);
        let (_poller, retry) = database_root_poller(
            &after.database_roots,
            Duration::from_millis(100),
            Arc::clone(&backlog),
            Arc::clone(&rescan),
            wake,
        );
        assert!(_poller.is_some() && !retry);
        let wal = database_sidecar_paths(&arriving)[0].clone();
        let mut held_writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal)
            .unwrap();
        held_writer
            .write_all(b"committed while the descriptor stays open")
            .unwrap();
        let mut pending = WatchEventBacklog::default();
        let mut pending_rescan = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pending.paths.contains(&wal) {
            notifications
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap();
            collect_watch_notifications(&backlog, &rescan, &mut pending, &mut pending_rescan);
            assert!(
                !pending_rescan,
                "a WAL poll must remain scoped to its database"
            );
        }
        drop(held_writer);
    }
}
