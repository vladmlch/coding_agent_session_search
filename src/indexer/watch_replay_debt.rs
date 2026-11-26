use super::*;

pub(in crate::indexer) const WATCH_LEXICAL_REPLAY_DEBT_PREFIX: &str = "watch_lexical_replay_v1:";

/// One connector's selected source needs an unfiltered retry: it may have
/// canonical rows newer than its published lexical documents, or a failed
/// scan that left some source content unread. Failed scans retain this marker
/// even when no conversation parsed, so a successful sibling cannot hide an
/// older repaired source by advancing the shared provider watermark.
/// Shared scan roots serve several connectors; completing one connector must
/// not discharge another connector's debt. Keep this separate from archive-wide
/// rebuild state.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(in crate::indexer) struct WatchLexicalReplayDebt {
    pub(in crate::indexer) connector: ConnectorKind,
    pub(in crate::indexer) source_id: String,
    pub(in crate::indexer) path: PathBuf,
    /// Discovery failed without leaving unpublished canonical mutations.
    /// A complete all-reused census can discharge this marker. Old records
    /// omit the field and retain their conservative publication semantics.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(in crate::indexer) discovery_only: bool,
}

impl WatchLexicalReplayDebt {
    pub(in crate::indexer) fn for_root(kind: ConnectorKind, root: &ScanRoot) -> Self {
        Self {
            connector: kind,
            source_id: root.origin.source_id.clone(),
            path: crate::normalize_path_identity(&root.path),
            discovery_only: false,
        }
    }

    pub(in crate::indexer) fn for_discovery(kind: ConnectorKind, root: &ScanRoot) -> Self {
        Self {
            discovery_only: true,
            ..Self::for_root(kind, root)
        }
    }

    pub(in crate::indexer) fn metadata(&self) -> Result<(String, String)> {
        let value = serde_json::to_string(self)?;
        let key = format!(
            "{WATCH_LEXICAL_REPLAY_DEBT_PREFIX}{}",
            blake3::hash(value.as_bytes()).to_hex()
        );
        Ok((key, value))
    }
}

pub(in crate::indexer) fn load_watch_lexical_replay_debt(
    storage: &FrankenStorage,
) -> Result<Vec<WatchLexicalReplayDebt>> {
    let rows: Vec<String> = storage.raw().query_map_collect(
        "SELECT value FROM meta WHERE key >= ?1 AND key < ?2",
        &[
            ParamValue::from(WATCH_LEXICAL_REPLAY_DEBT_PREFIX),
            ParamValue::from("watch_lexical_replay_v1;"),
        ],
        |row| row.get_typed(0),
    )?;
    rows.into_iter()
        .map(|row| serde_json::from_str(&row).context("reading durable watch lexical replay debt"))
        .collect()
}

/// Recover interrupted callbacks even when their source has stopped changing.
/// Only current connector/source roots may authorize replay; a removed source
/// must not be resurrected merely because an older run left a debt record.
pub(in crate::indexer) fn watch_lexical_replay_sources(
    storage: &FrankenStorage,
    roots: &[(ConnectorKind, ScanRoot)],
) -> Result<BTreeSet<PathBuf>> {
    let debts = load_watch_lexical_replay_debt(storage)?;
    if debts.is_empty() {
        return Ok(BTreeSet::new());
    }
    let roots: Vec<_> = roots
        .iter()
        .map(|(kind, root)| (kind, root, crate::normalize_path_identity(&root.path)))
        .collect();
    let mut pending = BTreeSet::new();
    for debt in debts {
        let debt_path = crate::normalize_path_identity(&debt.path);
        for (kind, root, root_path) in &roots {
            if debt.connector == **kind
                && debt.source_id == root.origin.source_id
                && (debt_path.starts_with(root_path) || root_path.starts_with(&debt_path))
            {
                pending.insert(root.path.clone());
            }
        }
    }
    Ok(pending)
}

pub(in crate::indexer) fn watch_lexical_replay_debt_for_root(
    storage: &FrankenStorage,
    kind: ConnectorKind,
    root: &ScanRoot,
) -> Result<Vec<WatchLexicalReplayDebt>> {
    let selected_path = crate::normalize_path_identity(&root.path);
    let mut pending = Vec::new();
    for debt in load_watch_lexical_replay_debt(storage)? {
        // Re-normalize the saved path: an older build (or another alias of the
        // same root) may have stored a different spelling, e.g. Windows' \\?\
        // form before txc9h. The debt itself is returned as stored, so clearing
        // deletes exactly the saved key.
        let debt_path = crate::normalize_path_identity(&debt.path);
        if debt.connector == kind
            && debt.source_id == root.origin.source_id
            && (debt_path.starts_with(&selected_path) || selected_path.starts_with(&debt_path))
        {
            pending.push(debt);
        }
    }
    Ok(pending)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) enum WatchReplayRootStatus {
    Directory,
    Absent,
    Other,
}

/// Only NotFound proves the whole configured root is absent. A failed lookup
/// through a non-directory, a permission denial, or a loop is not that proof.
pub(in crate::indexer) fn watch_replay_root_status(
    metadata: std::io::Result<fs::Metadata>,
) -> std::io::Result<WatchReplayRootStatus> {
    match metadata {
        Ok(metadata) if metadata.is_dir() => Ok(WatchReplayRootStatus::Directory),
        Ok(_) => Ok(WatchReplayRootStatus::Other),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(WatchReplayRootStatus::Absent)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotADirectory => {
            Ok(WatchReplayRootStatus::Other)
        }
        Err(error) => Err(error),
    }
}

/// A failed callback retains its notification paths. If a transcript is moved
/// before retry, classification can no longer stat that leaf to select its
/// owning root. Prefer the ordinary census under an extant current root. Only
/// when none covers that connector/origin may a strictly absent current root
/// authorize a separate canonical-only publication of its durable debt.
pub(in crate::indexer) fn promote_missing_watch_replay_paths(
    paths: &mut Vec<PathBuf>,
    roots: &[(ConnectorKind, ScanRoot)],
    storage: &Mutex<FrankenStorage>,
    progress: Option<&Arc<IndexingProgress>>,
) -> Result<Vec<(ConnectorKind, ScanRoot)>> {
    let mut missing = BTreeSet::new();
    for path in paths.iter() {
        if let Some(progress) = progress {
            progress.check_stop()?;
        }
        let metadata = fs::metadata(path);
        if let Some(progress) = progress {
            progress.tick_activity();
        }
        if metadata.is_err_and(|error| {
            matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            )
        }) {
            missing.insert(crate::normalize_path_identity(path));
        }
    }
    if missing.is_empty() {
        return Ok(Vec::new());
    }
    let debts = {
        let storage = storage
            .lock()
            .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
        load_watch_lexical_replay_debt(&storage)?
    };
    if debts.is_empty() {
        return Ok(Vec::new());
    }
    let debts: Vec<_> = debts
        .into_iter()
        .map(|debt| {
            let path = crate::normalize_path_identity(&debt.path);
            (debt.connector, debt.source_id, path, debt.discovery_only)
        })
        .collect();
    let roots: Vec<_> = roots
        .iter()
        .map(|(kind, root)| (*kind, root, crate::normalize_path_identity(&root.path)))
        .collect();
    let mut observed_roots = BTreeMap::new();
    let mut promoted = BTreeSet::new();
    let mut absent_roots: Vec<(ConnectorKind, ScanRoot, PathBuf)> = Vec::new();
    for missing_path in missing {
        if let Some(progress) = progress {
            progress.check_stop()?;
        }
        let mut selected: Vec<(ConnectorKind, &ScanRoot, &PathBuf)> = Vec::new();
        let mut absent: Vec<(ConnectorKind, &ScanRoot, &PathBuf)> = Vec::new();
        for (kind, root, root_path) in &roots {
            let matches_debt =
                |(debt_kind, source_id, debt_path, _): &(ConnectorKind, String, PathBuf, bool)| {
                    debt_kind == kind
                        && source_id == &root.origin.source_id
                        && (missing_path.starts_with(debt_path)
                            || debt_path.starts_with(&missing_path))
                };
            if !missing_path.starts_with(root_path) || !debts.iter().any(matches_debt) {
                continue;
            }
            if let std::collections::btree_map::Entry::Vacant(entry) =
                observed_roots.entry(root.path.clone())
            {
                if let Some(progress) = progress {
                    progress.check_stop()?;
                }
                let metadata = fs::metadata(&root.path);
                if let Some(progress) = progress {
                    progress.tick_activity();
                }
                let status = watch_replay_root_status(metadata).with_context(|| {
                    format!("checking watch replay root {}", root.path.display())
                })?;
                entry.insert(status);
            }
            let candidates = match observed_roots[&root.path] {
                WatchReplayRootStatus::Directory => &mut selected,
                WatchReplayRootStatus::Absent
                    if debts.iter().any(|debt| matches_debt(debt) && !debt.3) =>
                {
                    &mut absent
                }
                _ => continue,
            };
            if let Some(previous) =
                candidates
                    .iter_mut()
                    .find(|(selected_kind, selected_root, _)| {
                        *selected_kind == *kind && selected_root.origin == root.origin
                    })
            {
                if root_path.components().count() > previous.2.components().count() {
                    *previous = (*kind, *root, root_path);
                }
            } else {
                candidates.push((*kind, *root, root_path));
            }
        }
        for (kind, root, root_path) in absent {
            if selected.iter().any(|(extant_kind, extant, _)| {
                *extant_kind == kind && extant.origin == root.origin
            }) {
                // The existing ancestor census already recovers absent
                // children; do not publish the same canonical identities twice.
                continue;
            }
            let same_family = |candidate: &(ConnectorKind, ScanRoot, PathBuf)| {
                candidate.0 == kind
                    && candidate.1.origin == root.origin
                    && candidate.1.platform == root.platform
            };
            if absent_roots
                .iter()
                .any(|candidate| same_family(candidate) && root_path.starts_with(&candidate.2))
            {
                continue;
            }
            absent_roots.retain(|candidate| {
                !(same_family(candidate) && candidate.2.starts_with(root_path))
            });
            absent_roots.push((kind, root.clone(), root_path.clone()));
        }
        promoted.extend(selected.into_iter().map(|(_, root, _)| root.path.clone()));
    }
    paths.extend(promoted);
    Ok(absent_roots
        .into_iter()
        .map(|(kind, root, _)| (kind, root))
        .collect())
}

/// Publish already-admitted canonical rows when their entire current root is
/// absent. This is not a source census: it never detects or scans a connector,
/// writes a source receipt, or advances source/completion watermarks.
pub(in crate::indexer) fn recover_absent_watch_roots(
    opts: &IndexOptions,
    roots: &[(ConnectorKind, ScanRoot)],
    storage: &Mutex<FrankenStorage>,
    t_index: &Mutex<Option<TantivyIndex>>,
    index_path: &Path,
    active_source_filter: &ActiveSessionSourceFilter,
) -> Result<WatchReindexOutcome> {
    let mut outcome = WatchReindexOutcome::default();
    if roots.is_empty() || !opts.watch {
        return Ok(outcome);
    }
    crate::connectors::codex::path_policy::ScanExclusions::from_env().validate()?;
    if scan_path_exclusions_active()
        || skip_subagents_active()
        || !WorkspaceExclusions::from_env()?.patterns.is_empty()
        || persist::defer_lexical_updates_enabled()
    {
        // These are intentional restrictions, not a transient filesystem gap.
        // Leave debt durable without manufacturing periodic source scans.
        return Ok(outcome);
    }
    let nomem_deferrals = WatchNomemDeferrals::load(&opts.data_dir);
    let now_ms = FrankenStorage::now_millis();
    for (kind, root) in roots {
        if let Some(progress) = &opts.progress {
            progress.check_stop()?;
        }
        let metadata = fs::metadata(&root.path);
        if let Some(progress) = &opts.progress {
            progress.tick_activity();
        }
        if watch_replay_root_status(metadata)
            .with_context(|| format!("checking absent watch replay root {}", root.path.display()))?
            != WatchReplayRootStatus::Absent
        {
            // It returned after selection. Let the next ordinary callback
            // establish current source evidence instead of falling through to
            // a connector whose missing-root behavior may use default paths.
            outcome.retry_sources.insert(root.path.clone());
            continue;
        }
        if !kind
            .create_connector(Some(&opts.data_dir))
            .supports_source_boundaries()
        {
            continue;
        }
        let storage = storage
            .lock()
            .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
        let debts = watch_lexical_replay_debt_for_root(&storage, *kind, root)?;
        if !debts.iter().any(|debt| !debt.discovery_only) {
            continue;
        }
        let selected_path = crate::normalize_path_identity(&root.path);
        let provider = crate::connectors::archive_provider_slug(kind.slug());
        let held = nomem_deferrals.entries.values().any(|entry| {
            if entry.next_retry_at_ms <= now_ms
                || !(entry.agent == kind.slug()
                    || entry.agent == provider
                    || (*kind == ConnectorKind::Claude && entry.agent == "claude-code"))
            {
                return false;
            }
            let path = crate::normalize_path_identity(Path::new(&entry.source_path));
            path.starts_with(&selected_path)
                && debts.iter().any(|debt| {
                    !debt.discovery_only
                        && path.starts_with(crate::normalize_path_identity(&debt.path))
                })
        });
        if held {
            outcome.retry_sources.insert(root.path.clone());
            continue;
        }
        if let Some(progress) = &opts.progress {
            progress.phase.store(2, Ordering::Relaxed);
            progress.total_is_final.store(false, Ordering::Relaxed);
        }
        let recovered = recover_absent_watch_source_publications(
            opts,
            *kind,
            root,
            &debts,
            None,
            Some(active_source_filter),
            &storage,
            t_index,
            index_path,
        )?;
        outcome.indexed = outcome
            .indexed
            .saturating_add(recovered.published_conversations);
        outcome.published_conversations = outcome
            .published_conversations
            .saturating_add(recovered.published_conversations);

        let metadata = fs::metadata(&root.path);
        if let Some(progress) = &opts.progress {
            progress.tick_activity();
            progress.check_stop()?;
        }
        if watch_replay_root_status(metadata).with_context(|| {
            format!(
                "rechecking absent watch replay root {}",
                root.path.display()
            )
        })? == WatchReplayRootStatus::Absent
        {
            // The bounded replay certifies only the selected publication
            // obligations. Discovery debt, an ancestor obligation, and empty
            // canonical identities remain unproven; no scan happened here.
            for index in recovered.completed_debts {
                clear_watch_lexical_replay_debt(&storage, &debts[index])?;
            }
        } else {
            outcome.retry_sources.insert(root.path.clone());
        }
        if recovered.retry_unproven_source {
            outcome.retry_sources.insert(root.path.clone());
        }
    }
    Ok(outcome)
}

#[derive(Default)]
pub(in crate::indexer) struct WatchAbsentSourceRecovery {
    pub(in crate::indexer) published_conversations: usize,
    pub(in crate::indexer) completed_debts: Vec<usize>,
    pub(in crate::indexer) unresolved_source: bool,
    pub(in crate::indexer) retry_unproven_source: bool,
}

/// Evidence that an existing source was either reused from a publication
/// receipt or delivered a stable, complete stream in the current scan. The
/// latter is conditional on the caller committing every admitted batch.
pub(in crate::indexer) struct WatchRecoverySourceCoverage {
    pub(in crate::indexer) fingerprints: Vec<[u8; 32]>,
}

impl WatchRecoverySourceCoverage {
    pub(in crate::indexer) fn capture(
        ledger: &watch_source_ledger::WatchSourceLedger,
        progress: Option<&IndexingProgress>,
    ) -> Result<Self> {
        // Each bounded ledger can retain at most this many reused sources and
        // completions. Fixed-size native-path digests avoid duplicating long
        // normalized paths: the additional payload is at most four MiB.
        // Any evidence omitted by a budget remains conservatively uncovered.
        const MAX_FINGERPRINTS: usize = 2 * watch_source_cache::MAX_WATCH_SOURCE_LEDGER_ENTRIES;
        let paths = ledger
            .completed_source_paths()
            .chain(ledger.reused_sources.iter())
            .take(MAX_FINGERPRINTS);
        let mut fingerprints = Vec::with_capacity(paths.size_hint().1.unwrap_or(MAX_FINGERPRINTS));
        for path in paths {
            if let Some(progress) = progress {
                progress.check_stop()?;
            }
            let normalized = crate::normalize_path_identity(path);
            fingerprints.push(Self::fingerprint(&normalized));
            if let Some(progress) = progress {
                progress.tick_activity();
            }
        }
        fingerprints.sort_unstable();
        fingerprints.dedup();
        if let Some(progress) = progress {
            progress.check_stop()?;
        }
        Ok(Self { fingerprints })
    }

    pub(in crate::indexer) fn fingerprint(path: &Path) -> [u8; 32] {
        // Keep non-Unicode paths distinct; these fingerprints never persist
        // across platforms and use no lossy path-to-string conversion.
        *blake3::hash(path.as_os_str().as_encoded_bytes()).as_bytes()
    }

    pub(in crate::indexer) fn contains_normalized(&self, path: &Path) -> bool {
        path.ancestors().any(|candidate| {
            self.fingerprints
                .binary_search(&Self::fingerprint(candidate))
                .is_ok()
        })
    }
}

/// A successful raw-source census cannot prove that every older canonical
/// mutation has published: its transcript may have been rotated away after
/// the canonical transaction. Visit only this connector/source's identities in
/// bounded pages and reconcile absent sources from the canonical archive.
/// Existing files remain the ordinary source-ingest path's responsibility.
/// An older ancestor obligation permits replay only within the current root;
/// that narrower census never certifies the whole ancestor debt as complete.
///
/// Every census supplies bounded source coverage. An existing source without
/// a reused publication receipt or complete delivery cannot be certified by a
/// successful sibling. Codex and Claude omit completion for zero-output files,
/// so those identities retain debt until a later scan supplies the evidence.
/// A partial evidence cache can withhold clearance, never certify omitted work.
#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn recover_absent_watch_source_publications(
    opts: &IndexOptions,
    kind: ConnectorKind,
    root: &ScanRoot,
    debts: &[WatchLexicalReplayDebt],
    source_coverage: Option<&WatchRecoverySourceCoverage>,
    active_source_filter: Option<&ActiveSessionSourceFilter>,
    storage: &FrankenStorage,
    t_index: &Mutex<Option<TantivyIndex>>,
    index_path: &Path,
) -> Result<WatchAbsentSourceRecovery> {
    let selected_path = crate::normalize_path_identity(&root.path);
    // (input index, effective scope, saw absent source, existing coverage,
    //  whole debt is inside this current root and may be discharged)
    let mut scopes: Vec<_> = debts
        .iter()
        .enumerate()
        .filter(|(_, debt)| {
            !debt.discovery_only
                && debt.connector == kind
                && debt.source_id == root.origin.source_id
        })
        .filter_map(|(index, debt)| {
            let path = crate::normalize_path_identity(&debt.path);
            if path.starts_with(&selected_path) {
                Some((index, path, false, true, true))
            } else if selected_path.starts_with(&path) {
                // A narrowed configuration may retain older parent debt.
                // Recover only the current child's canonical rows; its census
                // cannot certify or authorize replay of any former siblings.
                Some((index, selected_path.clone(), false, true, false))
            } else {
                None
            }
        })
        .collect();
    if scopes.is_empty() {
        return Ok(WatchAbsentSourceRecovery::default());
    }
    let provider = crate::connectors::archive_provider_slug(kind.slug());
    let legacy_provider = if kind == ConnectorKind::Claude {
        "claude-code"
    } else {
        provider
    };
    let page_rows = watch_ingest_chunk_size().clamp(1, 256);
    let mut after = 0_i64;
    let mut writer_reset = false;
    let mut recovered = WatchAbsentSourceRecovery::default();
    let mut index = t_index
        .lock()
        .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
    loop {
        if let Some(progress) = &opts.progress {
            progress.check_stop()?;
        }
        // Compare native path identities in Rust, including legacy spellings
        // through a symlinked existing parent of a now-missing transcript.
        // SQL still scopes the scan to the actual canonical source/provider;
        // unrelated providers or source identities cannot be published here.
        let page: Vec<(i64, String)> = storage.raw().query_map_collect(
            "SELECT c.id, c.source_path FROM conversations c \
             JOIN agents a ON a.id = c.agent_id \
             WHERE c.source_id = ?1 AND c.id > ?2 \
               AND (a.slug = ?3 OR a.slug = ?4 OR a.slug = ?5) \
             ORDER BY c.id LIMIT ?6",
            &[
                ParamValue::from(root.origin.source_id.as_str()),
                ParamValue::from(after),
                ParamValue::from(kind.slug()),
                ParamValue::from(provider),
                ParamValue::from(legacy_provider),
                ParamValue::from(i64::try_from(page_rows)?),
            ],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )?;
        if let Some(progress) = &opts.progress {
            progress.tick_activity();
            progress.check_stop()?;
        }
        if page.is_empty() {
            break;
        }
        let page_len = page.len();
        let mut page_replayed = 0_usize;
        for (conversation_id, source_path) in page {
            anyhow::ensure!(
                conversation_id > after,
                "watch recovery cursor did not advance"
            );
            after = conversation_id;
            if let Some(progress) = &opts.progress {
                progress.check_stop()?;
            }
            let path = crate::normalize_path_identity(Path::new(&source_path));
            if !scopes
                .iter()
                .any(|(_, scope, _, _, _)| path.starts_with(scope))
            {
                continue;
            }
            let missing = match fs::metadata(&path) {
                Ok(_) => false,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                    ) =>
                {
                    true
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("checking canonical watch source {}", path.display())
                    });
                }
            };
            if let Some(progress) = &opts.progress {
                progress.tick_activity();
            }
            if missing
                && active_source_filter.is_some_and(|filter| {
                    should_skip_active_session_source(filter, root.origin.kind, &path)
                })
            {
                recovered.unresolved_source = true;
                for (_, scope, _, existing_covered, _) in &mut scopes {
                    if path.starts_with(scope) {
                        *existing_covered = false;
                    }
                }
                continue;
            }
            let source_is_covered = missing
                || source_coverage.is_some_and(|coverage| coverage.contains_normalized(&path));
            if !source_is_covered {
                recovered.unresolved_source = true;
                tracing::debug!(
                    conversation_id,
                    source_path = %path.display(),
                    "watch source has no complete publication evidence; retaining replay debt"
                );
            }
            for (_, scope, saw_absent, existing_covered, _) in &mut scopes {
                if path.starts_with(scope) {
                    if missing {
                        *saw_absent = true;
                    } else if !source_is_covered {
                        *existing_covered = false;
                    }
                }
            }
            if !missing && source_is_covered {
                continue;
            }
            let messages: Vec<i64> = storage.raw().query_map_collect(
                "SELECT id FROM messages WHERE conversation_id = ?1 LIMIT 1",
                &[ParamValue::from(conversation_id)],
                |row| row.get_typed(0),
            )?;
            if messages.is_empty() {
                // The ordinary canonical replay has no deletion selector for
                // an empty conversation. Do not claim that skipping it proved
                // there are no stale lexical documents under its identity.
                recovered.unresolved_source = true;
                for (_, scope, _, existing_covered, _) in &mut scopes {
                    if path.starts_with(scope) {
                        *existing_covered = false;
                    }
                }
                tracing::warn!(
                    conversation_id,
                    source_path = %path.display(),
                    "empty canonical watch source cannot certify lexical recovery; retaining debt"
                );
                continue;
            }
            if !missing {
                // An extant source with canonical messages but no completion
                // evidence may recover on another read without another event.
                // Preserve that live retry separately from empty canonical
                // identities, for which this path cannot prove a repair.
                recovered.retry_unproven_source = true;
                continue;
            }
            if !writer_reset {
                // A failed previous callback can retain an uncommitted
                // writer. Start from the last published generation exactly
                // as ordinary durable watch replay does.
                *index = None;
                *index = Some(open_lexical_index_with_heartbeat(
                    index_path,
                    opts.progress.as_ref(),
                )?);
                writer_reset = true;
            }
            persist::replay_watch_canonical_conversation(
                storage,
                index
                    .as_mut()
                    .expect("canonical watch replay writer is open"),
                conversation_id,
            )?;
            page_replayed = page_replayed.saturating_add(1);
            if let Some(progress) = &opts.progress {
                progress.tick_activity();
                progress.check_stop()?;
            }
        }
        if page_replayed > 0 {
            #[cfg(test)]
            inject_watch_lexical_test_failure(WatchLexicalTestFault::Commit)?;
            if let Err(error) = index
                .as_mut()
                .expect("canonical watch replay writer is open")
                .commit_with_region_doc_stamps(storage.raw())
            {
                // Same discard contract as the chunk commit: the failed
                // commit leaves staged replay documents in this writer's
                // uncommitted generation — drop it so no later commit can
                // certify them.
                *index = None;
                return Err(error);
            }
            recovered.published_conversations = recovered
                .published_conversations
                .saturating_add(page_replayed);
            if let Some(progress) = &opts.progress {
                progress.total.fetch_add(page_replayed, Ordering::Relaxed);
                progress.current.fetch_add(page_replayed, Ordering::Relaxed);
                progress.tick_activity();
            }
        }
        if page_len < page_rows {
            break;
        }
    }
    recovered.completed_debts = scopes
        .into_iter()
        .filter_map(|(index, _, saw_absent, existing_covered, can_clear)| {
            (can_clear && saw_absent && existing_covered).then_some(index)
        })
        .collect();
    Ok(recovered)
}

pub(in crate::indexer) fn persist_watch_lexical_replay_debt(
    storage: &FrankenStorage,
    debt: &WatchLexicalReplayDebt,
) -> Result<()> {
    #[cfg(test)]
    inject_watch_lexical_test_failure(WatchLexicalTestFault::DebtWrite)?;
    let (key, value) = debt.metadata()?;
    persist::with_ephemeral_writer(
        storage,
        false,
        "recording watch lexical replay debt",
        |writer| {
            writer.raw().execute_compat(
                "INSERT OR IGNORE INTO meta(key, value) VALUES(?1, ?2)",
                &[ParamValue::from(key), ParamValue::from(value)],
            )?;
            Ok(())
        },
    )
}

pub(in crate::indexer) fn clear_watch_lexical_replay_debt(
    storage: &FrankenStorage,
    debt: &WatchLexicalReplayDebt,
) -> Result<()> {
    #[cfg(test)]
    inject_watch_lexical_test_failure(WatchLexicalTestFault::DebtClear)?;
    let (key, value) = debt.metadata()?;
    persist::with_ephemeral_writer(
        storage,
        false,
        "completing watch lexical replay debt",
        |writer| {
            writer.raw().execute_compat(
                "DELETE FROM meta WHERE key = ?1 AND value = ?2",
                &[ParamValue::from(key), ParamValue::from(value)],
            )?;
            Ok(())
        },
    )
}

/// #372: fingerprint the current on-disk state of a remote mirror directory as
/// `(relative_path, size, mtime)` over every regular file / symlink under
/// `root_path`, hashed in a stable order.
///
/// This is compared against the fingerprint recorded at the last error-free
/// scan to decide whether an unchanged mirror can skip its (deliberately
/// full — see [`explicit_scan_root_since_ts`]) re-scan. It is intentionally
/// self-referential: it compares the mirror to a prior snapshot of *itself*,
/// never to a wall clock, so rsync's preservation of the *remote* mtime cannot
/// cause a clock-skew false match. It also mirrors rsync's own size+mtime
/// change-detection, so "rsync did not transfer" ⇒ "fingerprint unchanged".
///
/// Returns `None` on any doubt (missing path, walk/stat error, unreadable
/// mtime) so the caller falls through to a full scan — never a false skip.
pub(in crate::indexer) fn remote_mirror_scan_fingerprint(root_path: &Path) -> Option<String> {
    if !root_path.exists() {
        return None;
    }
    let mut entries: Vec<(String, u64, i64)> = Vec::new();
    for entry in walkdir::WalkDir::new(root_path)
        .min_depth(1)
        .follow_links(false)
    {
        let entry = entry.ok()?;
        let file_type = entry.file_type();
        if !(file_type.is_file() || file_type.is_symlink()) {
            continue;
        }
        let metadata = entry.metadata().ok()?;
        let modified_ms = metadata
            .modified()
            .ok()
            .and_then(system_time_to_epoch_millis)?;
        let relative = entry.path().strip_prefix(root_path).ok()?;
        entries.push((
            relative.to_string_lossy().into_owned(),
            metadata.len(),
            modified_ms,
        ));
    }
    // Directory-walk order is not guaranteed stable across runs/platforms.
    entries.sort();
    let mut hasher = blake3::Hasher::new();
    for (relative, size, modified_ms) in &entries {
        hasher.update(relative.as_bytes());
        hasher.update(&[0]);
        hasher.update(&size.to_le_bytes());
        hasher.update(&modified_ms.to_le_bytes());
        hasher.update(b"\n");
    }
    // Include the file count so a missing dir, an empty dir, and a populated
    // dir never collide on the empty-hash.
    Some(format!(
        "mirrorfp-v1:{}:{}",
        entries.len(),
        hasher.finalize().to_hex()
    ))
}

/// #372: has this mirror source already produced at least one canonical
/// conversation row? Guards the skip so a never-indexed mirror is always
/// scanned even if a stale fingerprint somehow matched.
pub(in crate::indexer) fn remote_mirror_source_already_indexed(
    storage: &FrankenStorage,
    source_id: &str,
) -> Result<bool> {
    let matches: Vec<i64> = storage
        .raw()
        .query_map_collect(
            "SELECT id FROM conversations WHERE source_id = ?1 LIMIT 1",
            &[ParamValue::from(source_id)],
            |row| row.get_typed(0),
        )
        .with_context(|| format!("checking indexed rows for mirror source {source_id}"))?;
    Ok(!matches.is_empty())
}

/// #372: partition additional scan roots into the ones to actually scan and the
/// remote-mirror fingerprints to persist on a clean scan.
///
/// A remote (ssh) mirror root gets `since_ts = None` (a full re-scan every run)
/// so freshly-synced old-content sessions still promote to canonical. When the
/// mirror is byte-identical to the last clean scan, that full re-scan is pure
/// waste (issue #372: 2.9 h for a 2.67 GB `opencode.db`). Such a root is
/// dropped from the returned scan set; the returned fingerprint list is stored
/// by the caller *only after an error-free scan*.
///
/// Fail-safe: local roots are never touched (they already honor the incremental
/// cutoff); `--full` / `--force-rebuild` never skip; and any fingerprint doubt
/// falls through to a full scan.
pub(in crate::indexer) fn plan_remote_mirror_scan_skips(
    storage: &FrankenStorage,
    opts: &IndexOptions,
    roots: Vec<ScanRoot>,
) -> (Vec<ScanRoot>, Vec<(String, String)>) {
    let skip_enabled = !opts.full && !opts.force_rebuild;
    let workspace_policy = WorkspaceExclusions::from_env().ok();
    let mut scanned = Vec::with_capacity(roots.len());
    let mut fingerprints_to_store: Vec<(String, String)> = Vec::new();
    let mut skipped = 0usize;
    for root in roots {
        if root.origin.kind != SourceKind::Ssh {
            scanned.push(root);
            continue;
        }
        let fingerprint_now = remote_mirror_scan_fingerprint(&root.path).and_then(|fingerprint| {
            workspace_policy
                .as_ref()
                .map(|policy| policy.bind_contract(&fingerprint))
        });
        let stored = storage
            .read_mirror_scan_fingerprint(&root.origin.source_id)
            .ok()
            .flatten();
        // A clean scan can deliberately exclude every session. Its saved
        // fingerprint still proves completion under this exact policy even
        // though no canonical conversation exists to prove prior admission.
        let unchanged = skip_enabled
            && fingerprint_now.is_some()
            && stored.is_some()
            && stored.as_deref() == fingerprint_now.as_deref()
            && (remote_mirror_source_already_indexed(storage, &root.origin.source_id)
                .unwrap_or(false)
                || workspace_policy
                    .as_ref()
                    .is_some_and(|policy| !policy.patterns.is_empty()));
        if unchanged {
            skipped += 1;
            tracing::info!(
                source_id = %root.origin.source_id,
                root = %root.path.display(),
                "skipping unchanged remote mirror scan; fingerprint matches last clean scan (#372)"
            );
            continue;
        }
        if let Some(fingerprint) = fingerprint_now {
            fingerprints_to_store.push((root.origin.source_id.clone(), fingerprint));
        }
        scanned.push(root);
    }
    if skipped > 0 {
        tracing::info!(
            skipped_mirror_roots = skipped,
            "#372: skipped unchanged remote mirror roots to avoid a redundant full re-scan"
        );
    }
    (scanned, fingerprints_to_store)
}

#[cfg(test)]
pub(in crate::indexer) mod mirror_scan_skip_tests {
    use super::*;
    use tempfile::TempDir;

    fn base_opts(db_path: &Path, data_dir: &Path) -> IndexOptions {
        IndexOptions {
            full: false,
            watch: false,
            force_rebuild: false,
            watch_once_paths: None,
            db_path: db_path.to_path_buf(),
            data_dir: data_dir.to_path_buf(),
            semantic: false,
            build_hnsw: false,
            embedder: "fastembed".to_string(),
            progress: None,
            watch_interval_secs: 30,
        }
    }

    fn ssh_root(mirror: &Path, source_id: &str) -> ScanRoot {
        ScanRoot::remote(
            mirror.to_path_buf(),
            Origin {
                source_id: source_id.to_string(),
                kind: SourceKind::Ssh,
                host: Some(source_id.to_string()),
            },
            None,
        )
    }

    fn insert_conv_with_source(storage: &FrankenStorage, source_id: &str, source_path: &Path) {
        let agent = crate::model::types::Agent {
            id: None,
            slug: "tester".into(),
            name: "Tester".into(),
            version: None,
            kind: crate::model::types::AgentKind::Cli,
        };
        let agent_id = storage.ensure_agent(&agent).unwrap();
        let conv = crate::model::types::Conversation {
            id: None,
            agent_slug: "tester".into(),
            workspace: None,
            external_id: Some(format!("ext-{source_id}")),
            title: Some("t".into()),
            source_path: source_path.to_path_buf(),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_000_000),
            approx_tokens: None,
            metadata_json: serde_json::json!({}),
            messages: vec![crate::model::types::Message {
                id: None,
                idx: 0,
                role: crate::model::types::MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_000),
                content: "hi".into(),
                extra_json: serde_json::json!({}),
                snippets: Vec::new(),
            }],
            source_id: source_id.to_string(),
            origin_host: None,
        };
        storage
            .insert_conversation_tree(agent_id, None, &conv)
            .unwrap();
    }

    #[test]
    fn remote_mirror_fingerprint_detects_changes() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("mirror");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("opencode.db"), b"hello").unwrap();

        let fp1 = remote_mirror_scan_fingerprint(&dir).expect("fingerprint");
        // Stable across repeated calls on an unchanged directory.
        assert_eq!(
            remote_mirror_scan_fingerprint(&dir).as_deref(),
            Some(fp1.as_str())
        );
        // Content growth (size changes) -> different fingerprint.
        std::fs::write(dir.join("opencode.db"), b"hello world").unwrap();
        let fp2 = remote_mirror_scan_fingerprint(&dir).expect("fingerprint");
        assert_ne!(fp1, fp2);
        // A new file -> different fingerprint.
        std::fs::write(dir.join("opencode.db-wal"), b"x").unwrap();
        let fp3 = remote_mirror_scan_fingerprint(&dir).expect("fingerprint");
        assert_ne!(fp2, fp3);
        // Missing directory -> None (never skip).
        assert_eq!(
            remote_mirror_scan_fingerprint(&tmp.path().join("absent")),
            None
        );
    }

    #[test]
    fn mirror_scan_fingerprint_meta_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&tmp.path().join("db.sqlite")).unwrap();
        assert_eq!(storage.read_mirror_scan_fingerprint("css").unwrap(), None);
        storage
            .record_mirror_scan_fingerprint("css", "fp-abc")
            .unwrap();
        assert_eq!(
            storage.read_mirror_scan_fingerprint("css").unwrap(),
            Some("fp-abc".to_string())
        );
        storage
            .record_mirror_scan_fingerprint("css", "fp-def")
            .unwrap();
        assert_eq!(
            storage.read_mirror_scan_fingerprint("css").unwrap(),
            Some("fp-def".to_string())
        );
        // Distinct source ids do not collide.
        assert_eq!(storage.read_mirror_scan_fingerprint("csd").unwrap(), None);
    }

    #[test]
    fn plan_skips_only_unchanged_indexed_remote_mirrors() {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("db.sqlite");
        let storage = FrankenStorage::open(&db_path).unwrap();

        let mirror = tmp.path().join("remotes/css/mirror");
        std::fs::create_dir_all(&mirror).unwrap();
        let mirror_db = mirror.join("opencode.db");
        std::fs::write(&mirror_db, b"data").unwrap();

        let remote = ssh_root(&mirror, "css");
        let local = ScanRoot::local(tmp.path().join("localdir"));
        let opts = base_opts(&db_path, tmp.path());

        // 1. No stored fingerprint yet -> remote scanned + fp captured; local
        //    root is never a skip candidate.
        let (scanned, fps) =
            plan_remote_mirror_scan_skips(&storage, &opts, vec![remote.clone(), local.clone()]);
        assert_eq!(
            scanned.len(),
            2,
            "nothing skipped without a stored fingerprint"
        );
        assert_eq!(fps.len(), 1, "one remote fingerprint captured for storing");
        assert_eq!(fps[0].0, "css");
        storage
            .record_mirror_scan_fingerprint(&fps[0].0, &fps[0].1)
            .unwrap();

        // 2. Fingerprint matches but the source was never indexed -> still scan.
        let (scanned2, _) = plan_remote_mirror_scan_skips(&storage, &opts, vec![remote.clone()]);
        assert_eq!(
            scanned2.len(),
            1,
            "a never-indexed mirror must scan even with a matching fingerprint"
        );

        insert_conv_with_source(&storage, "css", &mirror_db);

        // 3. Fingerprint matches AND the source is indexed -> skip.
        let (scanned3, fps3) = plan_remote_mirror_scan_skips(&storage, &opts, vec![remote.clone()]);
        assert!(
            scanned3.is_empty(),
            "unchanged indexed mirror should be skipped"
        );
        assert!(
            fps3.is_empty(),
            "a skipped mirror stores no new fingerprint"
        );

        // 3b. --full never skips, even when unchanged + indexed.
        let mut full_opts = base_opts(&db_path, tmp.path());
        full_opts.full = true;
        let (scanned_full, _) =
            plan_remote_mirror_scan_skips(&storage, &full_opts, vec![remote.clone()]);
        assert_eq!(scanned_full.len(), 1, "--full must never skip a mirror");

        // 4. Mirror content changes -> re-scanned and a fresh fingerprint captured.
        std::fs::write(&mirror_db, b"data plus more").unwrap();
        let (scanned4, fps4) = plan_remote_mirror_scan_skips(&storage, &opts, vec![remote.clone()]);
        assert_eq!(scanned4.len(), 1, "a changed mirror must re-scan");
        assert_eq!(fps4.len(), 1, "changed mirror captures a new fingerprint");
        assert_ne!(fps4[0].1, fps[0].1, "fingerprint changed with content");
    }

    #[test]
    fn recovery_source_coverage_matches_logical_subpaths() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("sessions.db");
        std::fs::write(&db, b"db").unwrap();
        let normalized_db = crate::normalize_path_identity(&db);
        let coverage = WatchRecoverySourceCoverage {
            fingerprints: vec![WatchRecoverySourceCoverage::fingerprint(&normalized_db)],
        };
        assert!(coverage.contains_normalized(&normalized_db));
        assert!(coverage.contains_normalized(&crate::normalize_path_identity(&db.join("kept"))));
        assert!(
            !coverage.contains_normalized(&crate::normalize_path_identity(
                &tmp.path().join("other.db")
            ))
        );
    }
}
