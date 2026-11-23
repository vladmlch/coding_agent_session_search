use super::*;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::indexer) enum ConnectorKind {
    #[serde(rename = "cx", alias = "Codex")]
    Codex,
    #[serde(rename = "gm", alias = "Gemini")]
    Gemini,
    #[serde(rename = "ag", alias = "Antigravity")]
    Antigravity,
    #[serde(rename = "cd", alias = "Claude")]
    Claude,
    #[serde(rename = "oc", alias = "OpenCode")]
    OpenCode,
    #[serde(rename = "cu", alias = "Cursor")]
    Cursor,
    #[serde(rename = "cg", alias = "ChatGpt")]
    ChatGpt,
    #[serde(rename = "qw", alias = "Qwen")]
    Qwen,
    #[serde(rename = "dv", alias = "Devin")]
    Devin,
    #[serde(rename = "qd", alias = "Qoder")]
    Qoder,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub(in crate::indexer) struct WatchState {
    #[serde(rename = "v", default, skip_serializing_if = "is_zero_u8")]
    pub(in crate::indexer) version: u8,
    #[serde(rename = "m", default, skip_serializing_if = "HashMap::is_empty")]
    pub(in crate::indexer) map: HashMap<ConnectorKind, i64>,
}

pub(in crate::indexer) fn is_zero_u8(value: &u8) -> bool {
    *value == 0
}

pub(in crate::indexer) fn state_path(data_dir: &Path) -> PathBuf {
    data_dir.join("watch_state.json")
}

pub(in crate::indexer) fn load_watch_state(data_dir: &Path) -> HashMap<ConnectorKind, i64> {
    let path = state_path(data_dir);
    let Ok(bytes) = fs::read(&path) else {
        return HashMap::new();
    };

    if let Ok(state) = serde_json::from_slice::<WatchState>(&bytes) {
        return state.map;
    }

    if let Ok(map) = serde_json::from_slice::<HashMap<ConnectorKind, i64>>(&bytes) {
        return map;
    }
    HashMap::new()
}

pub(in crate::indexer) fn replace_file_from_temp(
    temp_path: &Path,
    final_path: &Path,
) -> Result<()> {
    #[cfg(windows)]
    {
        match fs::rename(temp_path, final_path) {
            Ok(()) => {
                sync_parent_directory(final_path)?;
                Ok(())
            }
            Err(first_err)
                if final_path.exists()
                    && matches!(
                        first_err.kind(),
                        std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
                    ) =>
            {
                let backup_path = unique_replace_backup_path(final_path);
                fs::rename(final_path, &backup_path).map_err(|backup_err| {
                    let _ = fs::remove_file(temp_path);
                    anyhow::anyhow!(
                        "failed preparing backup {} before replacing {}: first error: {}; backup error: {}",
                        backup_path.display(),
                        final_path.display(),
                        first_err,
                        backup_err
                    )
                })?;
                match fs::rename(temp_path, final_path) {
                    Ok(()) => {
                        sync_parent_directory(final_path)?;
                        let _ = fs::remove_file(&backup_path);
                        Ok(())
                    }
                    Err(second_err) => {
                        let restore_result = fs::rename(&backup_path, final_path);
                        match restore_result {
                            Ok(()) => {
                                let _ = fs::remove_file(temp_path);
                                sync_parent_directory(final_path)?;
                                Err(anyhow::anyhow!(
                                    "failed replacing {} with {}: first error: {}; second error: {}; restored original file",
                                    final_path.display(),
                                    temp_path.display(),
                                    first_err,
                                    second_err
                                ))
                            }
                            Err(restore_err) => Err(anyhow::anyhow!(
                                "failed replacing {} with {}: first error: {}; second error: {}; restore error: {}; temp file retained at {}",
                                final_path.display(),
                                temp_path.display(),
                                first_err,
                                second_err,
                                restore_err,
                                temp_path.display()
                            )),
                        }
                    }
                }
            }
            Err(rename_err) => Err(rename_err.into()),
        }
    }

    #[cfg(not(windows))]
    {
        fs::rename(temp_path, final_path)?;
        sync_parent_directory(final_path)?;
        Ok(())
    }
}

pub(in crate::indexer) fn unique_atomic_temp_path(path: &Path) -> PathBuf {
    unique_atomic_sidecar_path(path, "tmp", "watch_state.json")
}

#[cfg(windows)]
pub(in crate::indexer) fn unique_replace_backup_path(path: &Path) -> PathBuf {
    unique_atomic_sidecar_path(path, "bak", "watch_state.json")
}

pub(in crate::indexer) fn unique_atomic_sidecar_path(
    path: &Path,
    suffix: &str,
    fallback_name: &str,
) -> PathBuf {
    static NEXT_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let nonce = NEXT_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(fallback_name);

    path.with_file_name(format!(
        ".{file_name}.{suffix}.{}.{}.{}",
        std::process::id(),
        timestamp,
        nonce
    ))
}

pub(in crate::indexer) fn create_new_atomic_sidecar_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

pub(in crate::indexer) fn unique_failed_seed_backup_root(
    backups_dir: &Path,
    db_name: &str,
) -> PathBuf {
    static NEXT_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let nonce = NEXT_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    backups_dir.join(format!(
        "{db_name}.{timestamp}.{}.{}.failed-baseline-seed.bak",
        std::process::id(),
        nonce
    ))
}

pub(in crate::indexer) fn save_watch_state(
    data_dir: &Path,
    state: &HashMap<ConnectorKind, i64>,
) -> Result<()> {
    let path = state_path(data_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let watch_state = WatchState {
        version: 1,
        map: state.clone(),
    };
    let json = serde_json::to_vec(&watch_state)?;
    let tmp_path = unique_atomic_temp_path(&path);
    {
        let file = create_new_atomic_sidecar_file(&tmp_path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&json)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
    }
    replace_file_from_temp(&tmp_path, &path)?;
    Ok(())
}

pub(in crate::indexer) fn set_progress_last_error(
    progress: Option<&Arc<IndexingProgress>>,
    error: Option<String>,
) {
    let Some(progress) = progress else {
        return;
    };

    match progress.last_error.lock() {
        Ok(mut guard) => *guard = error,
        Err(poisoned) => *poisoned.into_inner() = error,
    }
}

pub(in crate::indexer) fn finalize_watch_reindex_result(
    result: Result<usize>,
    detector: &StaleDetector,
    progress: Option<&Arc<IndexingProgress>>,
    context: &str,
) -> Result<usize> {
    match result {
        Ok(indexed) => {
            set_progress_last_error(progress, None);
            detector.record_scan(indexed);
            Ok(indexed)
        }
        Err(error) => {
            // ERROR (not WARN) with the full chain so watch-cycle failures are
            // visible: a failed cycle leaves the per-connector since_ts watermark
            // un-advanced, so the next cycle re-scans the same backlog and fails
            // again — the crash-loop symptom in issue #250
            // (coding_agent_session_search-u7r1z).
            tracing::error!(
                context,
                "watch reindex failed; since_ts not advanced this cycle: {error:#}"
            );
            reset_progress_to_idle(progress);
            set_progress_last_error(progress, Some(format!("{context}: {error}")));
            detector.record_scan(0);
            // Let the watch loop retain its pending sources. Reporting zero
            // here would consume the only retry for a previously active file.
            Err(error)
        }
    }
}

pub(in crate::indexer) fn finalize_watch_once_reindex_result(
    result: Result<usize>,
    detector: &StaleDetector,
    progress: Option<&Arc<IndexingProgress>>,
    context: &str,
) -> Result<usize> {
    match result {
        Ok(indexed) => {
            set_progress_last_error(progress, None);
            detector.record_scan(indexed);
            Ok(indexed)
        }
        Err(error) => {
            // ERROR (not WARN) with the full chain so watch-cycle failures are
            // visible: a failed cycle leaves the per-connector since_ts watermark
            // un-advanced, so the next cycle re-scans the same backlog and fails
            // again — the crash-loop symptom in issue #250
            // (coding_agent_session_search-u7r1z).
            tracing::error!(
                context,
                "watch reindex failed; since_ts not advanced this cycle: {error:#}"
            );
            reset_progress_to_idle(progress);
            set_progress_last_error(progress, Some(format!("{context}: {error}")));
            detector.record_scan(0);
            Err(error)
        }
    }
}

pub(in crate::indexer) fn explicit_watch_once_connector_hint(path: &Path) -> Option<ConnectorKind> {
    let components: Vec<String> = path
        .components()
        .map(|component| component.as_os_str().to_string_lossy().to_ascii_lowercase())
        .collect();

    let has_pair = |left: &str, right: &str| {
        components
            .windows(2)
            .any(|window| window[0] == left && window[1] == right)
    };

    if has_pair(".codex", "sessions") {
        Some(ConnectorKind::Codex)
    } else if has_pair(".claude", "projects") {
        Some(ConnectorKind::Claude)
    } else if has_pair(".gemini", "tmp") {
        Some(ConnectorKind::Gemini)
    } else {
        None
    }
}

pub(in crate::indexer) fn explicit_watch_once_scan_path(
    _kind: ConnectorKind,
    _requested: &Path,
    canonical: &Path,
) -> PathBuf {
    canonical.to_path_buf()
}

/// GH #478: existing explicit `--watch-once` paths that no connector claims.
/// Absent paths are excluded: the absent-path fast path deliberately treats
/// them as a no-op (agent hooks name transcripts that may not exist yet).
pub(in crate::indexer) fn unclaimed_explicit_watch_once_paths(
    paths: &[PathBuf],
    roots: &[(ConnectorKind, ScanRoot)],
) -> Vec<PathBuf> {
    paths
        .iter()
        .filter(|path| {
            path.exists() && classify_paths(vec![(*path).clone()], roots, true).is_empty()
        })
        .cloned()
        .collect()
}

/// A targeted `--watch-once` that indexes nothing looks exactly like one
/// that indexed everything: exit 0, zero work. Name the paths that no
/// connector claims so a backfill that did nothing is visible.
pub(in crate::indexer) fn warn_unclaimed_explicit_watch_once_paths(
    paths: &[PathBuf],
    roots: &[(ConnectorKind, ScanRoot)],
) {
    let unclaimed = unclaimed_explicit_watch_once_paths(paths, roots);
    if unclaimed.is_empty() {
        return;
    }
    let listed: Vec<String> = unclaimed
        .iter()
        .map(|path| path.display().to_string())
        .collect();
    tracing::warn!(
        unclaimed = unclaimed.len(),
        requested = paths.len(),
        paths = %listed.join(", "),
        "watch-once: no enabled connector claims these existing paths, so nothing was indexed for them; check the path against `cass sources agents list --json` and the connector's session root"
    );
}

pub(in crate::indexer) fn classify_paths(
    paths: Vec<PathBuf>,
    roots: &[(ConnectorKind, ScanRoot)],
    prefer_explicit_paths: bool,
) -> Vec<(ConnectorKind, ScanRoot, Option<i64>, Option<i64>)> {
    // Key -> (Root, MinTS, MaxTS)
    let mut batch_map: BatchClassificationMap = HashMap::new();
    // Keep provenance on the original roots while comparing both sides in
    // the same path namespace, including macOS /var aliases and linked roots.
    let match_root_paths: Vec<PathBuf> = roots
        .iter()
        .map(|(_, root)| {
            if prefer_explicit_paths {
                dunce::canonicalize(&root.path).unwrap_or_else(|_| root.path.clone())
            } else {
                root.path.clone()
            }
        })
        .collect();
    // A live watch keeps scanning the root as configured, but its events can
    // arrive in either spelling: macOS FSEvents reports canonical paths
    // (/private/var/... for a root under /var, the target of a symlinked
    // root), inotify reports them as watched. Match the canonical spelling
    // too, or every event under such a root is dropped.
    let alternate_root_paths: Vec<Option<PathBuf>> = roots
        .iter()
        .map(|(_, root)| {
            (!prefer_explicit_paths)
                .then(|| dunce::canonicalize(&root.path).ok())
                .flatten()
                .filter(|canonical| canonical != &root.path)
        })
        .collect();

    for requested in paths {
        let hinted_kind = prefer_explicit_paths
            .then(|| explicit_watch_once_connector_hint(&requested))
            .flatten();
        // dunce keeps Windows paths in the plain spelling a scan records, so
        // a session indexed by --watch-once and by a scan has one identity
        // (9v9xi). Elsewhere it is std::fs::canonicalize.
        let canonical = prefer_explicit_paths.then(|| {
            dunce::canonicalize(&requested).unwrap_or_else(|error| {
                tracing::warn!(
                    path = %requested.display(),
                    %error,
                    "watch-once path could not be canonicalized; it may not match a connector scan root (issue #377)"
                );
                requested.clone()
            })
        });
        let p = canonical.as_ref().unwrap_or(&requested);
        let hinted_kind = hinted_kind.or_else(|| {
            prefer_explicit_paths
                .then(|| explicit_watch_once_connector_hint(p))
                .flatten()
        });
        if let Ok(meta) = std::fs::metadata(p)
            && let Ok(time) = meta.modified()
            && let Ok(dur) = time.duration_since(std::time::UNIX_EPOCH)
        {
            let ts = Some(i64::try_from(dur.as_millis()).unwrap_or(i64::MAX));

            // A connector can report nested roots for one physical store (OMP
            // profiles are the motivating example). Scanning every containing
            // root derives different root-relative external IDs for the same
            // transcript, so retain only the deepest root per connector and
            // source provenance. Distinct sources remain distinct scans.
            let mut matching_roots: Vec<(ConnectorKind, &ScanRoot, &PathBuf, usize)> = Vec::new();
            for (((kind, root), match_root_path), alternate_root_path) in roots
                .iter()
                .zip(&match_root_paths)
                .zip(&alternate_root_paths)
            {
                if let Some(hinted_kind) = hinted_kind
                    && *kind != hinted_kind
                {
                    continue;
                }
                if prefer_explicit_paths
                    && !is_database_watch_root(*kind, root)
                    && !explicit_watch_once_file_may_be_database(*kind, p)
                {
                    continue;
                }
                let under = |candidate: &PathBuf| {
                    p.starts_with(candidate)
                        || (is_database_watch_root(*kind, root)
                            && database_sidecar_paths(candidate).contains(p))
                };
                if under(match_root_path) || alternate_root_path.as_ref().is_some_and(under) {
                    // Aliases can make a physical descendant's configured
                    // spelling shorter than its ancestor's (or vice versa).
                    // Compare their resolved coverage, but scan the original
                    // configured root so source identity stays consistent.
                    let depth = alternate_root_path
                        .as_ref()
                        .unwrap_or(match_root_path)
                        .components()
                        .count();
                    if let Some(index) =
                        matching_roots
                            .iter()
                            .position(|(selected_kind, selected_root, _, _)| {
                                *selected_kind == *kind && selected_root.origin == root.origin
                            })
                    {
                        if depth > matching_roots[index].3 {
                            matching_roots[index] = (*kind, root, match_root_path, depth);
                        }
                    } else {
                        matching_roots.push((*kind, root, match_root_path, depth));
                    }
                }
            }
            let matched_root = !matching_roots.is_empty();
            for (kind, root, match_root_path, _) in matching_roots {
                let scan_path = if prefer_explicit_paths && !is_database_watch_root(kind, root) {
                    explicit_watch_once_scan_path(kind, &requested, p)
                } else {
                    match_root_path.clone()
                };
                let mut scan_root = root.clone();
                scan_root.path = scan_path.clone();
                let key = (
                    kind,
                    scan_root.origin.kind,
                    scan_root.origin.source_id.clone(),
                    scan_root.origin.host.clone(),
                    scan_path,
                );
                let entry = batch_map.entry(key).or_insert((scan_root, None, None));

                // Update MinTS (for scan window start)
                entry.1 = match (entry.1, ts) {
                    (Some(prev), Some(cur)) => Some(prev.min(cur)),
                    (None, Some(cur)) => Some(cur),
                    _ => entry.1,
                };

                // Update MaxTS (for state high-water mark)
                entry.2 = match (entry.2, ts) {
                    (Some(prev), Some(cur)) => Some(prev.max(cur)),
                    (None, Some(cur)) => Some(cur),
                    _ => entry.2,
                };
            }
            if prefer_explicit_paths
                && !matched_root
                && let Some(hinted_kind) = hinted_kind
            {
                let scan_path = explicit_watch_once_scan_path(hinted_kind, &requested, p);
                let scan_root = ScanRoot::local(scan_path.clone());
                let entry = batch_map
                    .entry((
                        hinted_kind,
                        scan_root.origin.kind,
                        scan_root.origin.source_id.clone(),
                        scan_root.origin.host.clone(),
                        scan_path,
                    ))
                    .or_insert((scan_root, None, None));
                entry.1 = match (entry.1, ts) {
                    (Some(prev), Some(cur)) => Some(prev.min(cur)),
                    (None, Some(cur)) => Some(cur),
                    _ => entry.1,
                };
                entry.2 = match (entry.2, ts) {
                    (Some(prev), Some(cur)) => Some(prev.max(cur)),
                    (None, Some(cur)) => Some(cur),
                    _ => entry.2,
                };
            }
        }
    }

    batch_map
        .into_iter()
        .map(|((kind, _, _, _, _), (root, min_ts, max_ts))| (kind, root, min_ts, max_ts))
        .collect()
}

pub(in crate::indexer) fn watch_event_should_trigger_reindex(event: &notify::Event) -> bool {
    match event.kind {
        notify::event::EventKind::Access(AccessKind::Close(AccessMode::Write)) => true,
        notify::event::EventKind::Access(_) => false,
        notify::event::EventKind::Create(_)
        | notify::event::EventKind::Any
        | notify::event::EventKind::Other => true,
        // Incremental watch indexing is append-only today: once a path is gone,
        // classify_paths() cannot derive a scan window from it and the ingest
        // path cannot delete the stale conversation rows it previously indexed.
        // Treat remove events as noise until delete-aware rebuilds exist.
        notify::event::EventKind::Remove(_) => false,
        notify::event::EventKind::Modify(ModifyKind::Metadata(MetadataKind::AccessTime)) => false,
        notify::event::EventKind::Modify(_) => true,
    }
}

pub(in crate::indexer) fn sync_sources_config_to_db(storage: &FrankenStorage) {
    if dotenvy::var("CASS_IGNORE_SOURCES_CONFIG").is_ok() {
        return;
    }
    let config = match SourcesConfig::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            tracing::debug!("sources config load failed: {e}");
            return;
        }
    };

    let records: Vec<Source> = config
        .sources
        .iter()
        .map(|source| {
            let platform = source.platform.map(|p| match p {
                Platform::Macos => "macos".to_string(),
                Platform::Linux => "linux".to_string(),
                Platform::Windows => "windows".to_string(),
            });

            let config_json = serde_json::json!({
                "paths": source.paths.clone(),
                "path_mappings": source.path_mappings.clone(),
                "sync_schedule": source.sync_schedule,
            });

            Source {
                id: source.name.clone(),
                kind: source.source_type,
                host_label: source.host.clone(),
                machine_id: None,
                platform,
                config_json: Some(config_json),
                created_at: None,
                updated_at: None,
            }
        })
        .collect();

    if let Err(err) =
        persist::with_ephemeral_writer(storage, false, "syncing configured sources", |writer| {
            for record in &records {
                if let Err(upsert_err) = writer.upsert_source(record) {
                    tracing::warn!(
                        source_id = %record.id,
                        error = %upsert_err,
                        "failed to upsert configured source into db"
                    );
                }
            }
            Ok(())
        })
    {
        tracing::warn!(
            error = %err,
            "failed to sync configured sources with a short-lived writer"
        );
    }
}

pub(in crate::indexer) fn expand_local_scan_root_path(path: &str) -> PathBuf {
    if let Some(stripped) = path.strip_prefix("~/")
        && let Some(home) = dirs::home_dir()
    {
        return home.join(stripped);
    }
    if path == "~"
        && let Some(home) = dirs::home_dir()
    {
        return home;
    }
    PathBuf::from(path)
}

/// Build a list of scan roots for multi-root indexing.
///
/// This function collects both:
/// 1. Local default roots (from watch_roots() or standard locations)
/// 2. Remote mirror roots (from registered sources in the database)
///
/// Part of P2.2 - Indexer multi-root orchestration.
pub(in crate::indexer) fn additional_scan_roots_for_scan_or_watch(
    storage: &FrankenStorage,
    data_dir: &Path,
    retain_missing: bool,
) -> Vec<ScanRoot> {
    // Source-config syncing and scan-root discovery can be expensive on large
    // machines with many historical bundles and configured mirrors. Defer that
    // work until a source scan or watch session actually needs it.
    sync_sources_config_to_db(storage);
    build_scan_roots_with_missing(Some(storage), data_dir, retain_missing)
        .into_iter()
        .filter(|root| !(root.origin.source_id == LOCAL_SOURCE_ID && root.path == data_dir))
        .collect()
}
