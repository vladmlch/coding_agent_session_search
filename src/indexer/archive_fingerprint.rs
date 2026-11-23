use super::*;
#[cfg(test)]
use crate::franken_sync::compat::{
    Transaction as FrankenTransaction, TransactionExt as FrankenTransactionExt,
};
#[cfg(test)]
use crate::storage::sqlite::{DailyStatsRebuildResult, StatsAggregator, StatsDelta};

pub(crate) fn load_lexical_rebuild_checkpoint(
    index_path: &Path,
) -> Result<Option<LexicalRebuildCheckpoint>> {
    let Some(state) = load_lexical_rebuild_state(index_path)? else {
        return Ok(None);
    };
    let processed_conversations = state.reported_processed_conversations();
    let committed_conversation_id = state.reported_committed_conversation_id();
    let indexed_docs = state.reported_indexed_docs();

    Ok(Some(LexicalRebuildCheckpoint {
        db_path: state.db.db_path,
        total_conversations: state.db.total_conversations,
        storage_fingerprint: state.db.storage_fingerprint,
        committed_offset: state.committed_offset,
        committed_conversation_id,
        processed_conversations,
        indexed_docs,
        indexed_region_docs: state.indexed_region_docs,
        schema_hash: state.schema_hash,
        page_size: state.page_size,
        completed: state.completed,
        updated_at_ms: state.updated_at_ms,
    }))
}

pub(crate) fn load_active_lexical_rebuild_pipeline_runtime(
    index_path: &Path,
    db_path: &Path,
) -> Result<Option<LexicalRebuildPipelineRuntimeSnapshot>> {
    let Some(state) = load_lexical_rebuild_state(index_path)? else {
        return Ok(None);
    };

    if state.completed || !crate::stored_path_identity_matches(&state.db.db_path, db_path) {
        return Ok(None);
    }

    if !state.runtime.is_observed() {
        return Ok(None);
    }

    Ok(Some(state.runtime))
}

pub(crate) fn lexical_storage_fingerprint_for_db(db_path: &Path) -> Result<String> {
    lexical_rebuild_storage_fingerprint(db_path)
}

/// Compute the lexical fingerprint without invoking any recovery-capable read
/// opener. Used by strict read-only search so a dirty WAL or duplicate schema
/// is surfaced rather than repaired as a side effect of freshness validation.
pub(crate) fn lexical_storage_fingerprint_for_db_strict(db_path: &Path) -> Result<String> {
    let storage = FrankenStorage::open_strict_readonly(db_path).with_context(|| {
        format!(
            "strictly opening readonly storage to compute lexical fingerprint for {}",
            db_path.display()
        )
    })?;
    let total_conversations = count_total_conversations_exact(&storage)?;
    let fingerprint = lexical_rebuild_content_fingerprint(&storage, total_conversations)?;
    storage.close_without_checkpoint().with_context(|| {
        format!(
            "closing strict readonly storage without checkpoint for {}",
            db_path.display()
        )
    })?;
    Ok(fingerprint)
}

/// Sidecar (inside the lexical index directory, next to the rebuild
/// checkpoint) that memoizes the canonical archive fingerprint against the
/// archive's physical identity. It exists for the one-shot CLI: every
/// `cass search --robot` is a fresh process, so an in-process memo alone
/// never hits on the path that matters most.
pub(crate) const ARCHIVE_FINGERPRINT_CACHE_FILE: &str = ".archive-fingerprint-cache.json";

pub(in crate::indexer) const ARCHIVE_FINGERPRINT_CACHE_SCHEMA_VERSION: u32 = 1;

/// Physical identity of a canonical archive for fingerprint memoization: the
/// size and mtime of the main database file and of its WAL sidecar. Any
/// committed write touches at least one of them (a WAL append changes the WAL
/// length and mtime; a checkpoint changes the main file), so an unchanged key
/// means the strict fingerprint computed last time is still the current one.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(in crate::indexer) struct ArchivePhysicalIdentity {
    pub(in crate::indexer) db_len: u64,
    pub(in crate::indexer) db_mtime_secs: u64,
    pub(in crate::indexer) db_mtime_nanos: u32,
    pub(in crate::indexer) wal_len: u64,
    pub(in crate::indexer) wal_mtime_secs: u64,
    pub(in crate::indexer) wal_mtime_nanos: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(in crate::indexer) struct ArchiveFingerprintCache {
    pub(in crate::indexer) schema_version: u32,
    pub(in crate::indexer) identity: ArchivePhysicalIdentity,
    pub(in crate::indexer) fingerprint: String,
    pub(in crate::indexer) computed_at_ms: i64,
}

pub(in crate::indexer) fn lexical_storage_physical_identity(
    db_path: &Path,
) -> Option<ArchivePhysicalIdentity> {
    fn stamp(path: &Path) -> Option<(u64, u64, u32)> {
        match fs::metadata(path) {
            Ok(meta) => {
                let modified = meta
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(UNIX_EPOCH).ok())?;
                Some((meta.len(), modified.as_secs(), modified.subsec_nanos()))
            }
            // An absent WAL sidecar is a legitimate, stable state.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Some((0, 0, 0)),
            Err(_) => None,
        }
    }
    let (db_len, db_mtime_secs, db_mtime_nanos) = stamp(db_path)?;
    if db_len == 0 {
        // No archive (or an empty placeholder): nothing worth memoizing.
        return None;
    }
    let wal_path = crate::storage::sqlite::database_sidecar_path(db_path, "-wal");
    let (wal_len, wal_mtime_secs, wal_mtime_nanos) = stamp(&wal_path)?;
    Some(ArchivePhysicalIdentity {
        db_len,
        db_mtime_secs,
        db_mtime_nanos,
        wal_len,
        wal_mtime_secs,
        wal_mtime_nanos,
    })
}

pub(in crate::indexer) fn write_archive_fingerprint_cache_for_identity(
    identity: &ArchivePhysicalIdentity,
    cache_dir: &Path,
    fingerprint: &str,
) {
    if !fingerprint.starts_with("content-v1:") || !cache_dir.is_dir() {
        return;
    }
    let cache_path = cache_dir.join(ARCHIVE_FINGERPRINT_CACHE_FILE);
    let payload = ArchiveFingerprintCache {
        schema_version: ARCHIVE_FINGERPRINT_CACHE_SCHEMA_VERSION,
        identity: identity.clone(),
        fingerprint: fingerprint.to_string(),
        computed_at_ms: semantic_indexing_now_ms(),
    };
    // Per-process temp name: concurrent one-shot searches may all miss at once.
    let tmp_path = cache_dir.join(format!(
        "{ARCHIVE_FINGERPRINT_CACHE_FILE}.{}.tmp",
        std::process::id()
    ));
    let written = serde_json::to_vec(&payload)
        .map_err(std::io::Error::other)
        .and_then(|bytes| fs::write(&tmp_path, bytes))
        .and_then(|()| fs::rename(&tmp_path, &cache_path));
    if let Err(err) = written {
        tracing::debug!(
            error = %err,
            cache = %cache_path.display(),
            "archive fingerprint cache write skipped"
        );
        let _ = fs::remove_file(&tmp_path);
    }
}

pub(crate) fn write_archive_fingerprint_cache_best_effort(
    db_path: &Path,
    cache_dir: &Path,
    fingerprint: &str,
) {
    let Some(identity) = lexical_storage_physical_identity(db_path) else {
        return;
    };
    write_archive_fingerprint_cache_for_identity(&identity, cache_dir, fingerprint);
}

pub(crate) fn prime_archive_fingerprint_cache_from_completed_checkpoint(
    db_path: &Path,
    index_path: &Path,
) {
    let Ok(Some(checkpoint)) = load_lexical_rebuild_checkpoint(index_path) else {
        return;
    };
    if checkpoint.completed && crate::stored_path_identity_matches(&checkpoint.db_path, db_path) {
        write_archive_fingerprint_cache_best_effort(
            db_path,
            index_path,
            &checkpoint.storage_fingerprint,
        );
    }
}

/// The on-disk layer of [`lexical_storage_fingerprint_for_db_cached`]: serve
/// the sidecar's fingerprint when it was computed for exactly this physical
/// identity, otherwise compute the strict fingerprint and rewrite the sidecar
/// (best effort, tmp + rename, never fatal). The sidecar is never consulted
/// by the indexer's own checkpoint logic, which always recomputes.
pub(in crate::indexer) fn lexical_storage_fingerprint_for_db_disk_cached(
    db_path: &Path,
    identity: &ArchivePhysicalIdentity,
    cache_dir: &Path,
) -> Result<String> {
    let cache_path = cache_dir.join(ARCHIVE_FINGERPRINT_CACHE_FILE);
    if let Ok(raw) = fs::read(&cache_path)
        && let Ok(cached) = serde_json::from_slice::<ArchiveFingerprintCache>(&raw)
        && cached.schema_version == ARCHIVE_FINGERPRINT_CACHE_SCHEMA_VERSION
        && &cached.identity == identity
        && cached.fingerprint.starts_with("content-v1:")
    {
        tracing::debug!(
            archive_fingerprint_cache = "hit",
            cache = %cache_path.display(),
            "archive fingerprint served from the identity sidecar"
        );
        return Ok(cached.fingerprint);
    }
    tracing::debug!(
        archive_fingerprint_cache = "miss",
        cache = %cache_path.display(),
        "archive fingerprint recomputed from the canonical database"
    );
    let fingerprint = lexical_storage_fingerprint_for_db_strict(db_path)?;
    write_archive_fingerprint_cache_for_identity(identity, cache_dir, &fingerprint);
    Ok(fingerprint)
}

/// [`lexical_storage_fingerprint_for_db_strict`], memoized on the archive's
/// physical identity (`lexical_storage_physical_identity`) and the lexical
/// index it is being validated against — first in this process, then in the
/// on-disk sidecar inside `index_path`.
///
/// The strict variant opens a synchronous read-only handle and replays the
/// WAL; on a large archive that cost every default `cass search` seconds
/// (measured: 201 MB of WAL read in 97k calls, 3.5–4 s) before the engine
/// query even ran, and a one-shot CLI process cannot amortize it in memory.
/// A search-time freshness check only needs the answer to change when the
/// archive does, and any committed write moves the database or WAL
/// size/mtime, so both layers are invalidated exactly when they must be. When
/// the identity cannot be read the strict path runs uncached.
pub(crate) fn lexical_storage_fingerprint_for_db_cached(
    db_path: &Path,
    index_path: &Path,
) -> Result<String> {
    type MemoKey = (PathBuf, PathBuf, ArchivePhysicalIdentity);
    static MEMO: std::sync::OnceLock<Mutex<HashMap<MemoKey, String>>> = std::sync::OnceLock::new();
    let Some(identity) = lexical_storage_physical_identity(db_path) else {
        return lexical_storage_fingerprint_for_db_strict(db_path);
    };
    let key: MemoKey = (
        db_path.to_path_buf(),
        index_path.to_path_buf(),
        identity.clone(),
    );
    let memo = MEMO.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(fingerprint) = memo.lock().ok().and_then(|guard| guard.get(&key).cloned()) {
        return Ok(fingerprint);
    }
    let fingerprint =
        lexical_storage_fingerprint_for_db_disk_cached(db_path, &identity, index_path)?;
    if let Ok(mut guard) = memo.lock() {
        // One entry per archive/index pair: a changed identity replaces the
        // stale one rather than accumulating history.
        guard.retain(|(db, index, _), _| !(db == &key.0 && index == &key.1));
        guard.insert(key, fingerprint.clone());
    }
    Ok(fingerprint)
}

/// GH #353: the sidecar-cached archive fingerprint WITHOUT opening the
/// database. The watermark health lane and other `open_skipped` surfaces
/// promise a mutation-free probe with no archive open (k2k20: a readonly
/// open on a large dirty-WAL archive can take minutes), but they must still
/// compare the SAME storage fingerprint `search` defers repair on. Served
/// from the identity-keyed sidecar only; `None` when the sidecar does not
/// describe the current physical identity — an honest "not computed", never
/// a guess. Search primes that sidecar after every real fingerprint
/// computation, so a status that follows any search sees what search saw.
pub(crate) fn lexical_storage_fingerprint_for_db_cached_readonly(
    db_path: &Path,
    index_path: &Path,
) -> Option<String> {
    let identity = lexical_storage_physical_identity(db_path)?;
    let cache_path = index_path.join(ARCHIVE_FINGERPRINT_CACHE_FILE);
    let raw = fs::read(cache_path).ok()?;
    let cached = serde_json::from_slice::<ArchiveFingerprintCache>(&raw).ok()?;
    (cached.schema_version == ARCHIVE_FINGERPRINT_CACHE_SCHEMA_VERSION
        && cached.identity == identity
        && cached.fingerprint.starts_with("content-v1:"))
    .then_some(cached.fingerprint)
}

/// Same fingerprint as [`lexical_storage_fingerprint_for_db`], computed on an
/// already-open read-only handle so callers that have paid the archive open
/// cost (semantic context loading, which opens the DB for filter maps anyway)
/// do not pay it a second time just to learn whether their vector assets
/// still describe the current database (GH #404).
pub(crate) fn lexical_storage_fingerprint_for_storage(storage: &FrankenStorage) -> Result<String> {
    let total_conversations = count_total_conversations_exact(storage)?;
    lexical_rebuild_content_fingerprint(storage, total_conversations)
}

/// True iff a *completed* lexical-rebuild checkpoint exists for `db_path` whose
/// recorded storage fingerprint matches the current canonical DB. Used to
/// recognize an already-published, still-valid lexical generation and skip a
/// redundant from-scratch rebuild that would otherwise loop on every restart
/// after an OOM-killed shard merge (issue #248,
/// coding_agent_session_search-raoug). Conservative by construction: any missing
/// checkpoint, incomplete rebuild, path-identity mismatch, changed data
/// (different fingerprint), or read error returns `false`, preserving the
/// existing rebuild behavior.
pub(in crate::indexer) fn published_lexical_index_validated_for_current_data(
    index_path: &Path,
    db_path: &Path,
) -> bool {
    let checkpoint = match load_lexical_rebuild_checkpoint(index_path) {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => return false,
        Err(err) => {
            tracing::debug!(
                error = %err,
                "could not load lexical rebuild checkpoint while validating published index"
            );
            return false;
        }
    };
    if !checkpoint.completed || !crate::stored_path_identity_matches(&checkpoint.db_path, db_path) {
        return false;
    }
    match lexical_storage_fingerprint_for_db(db_path) {
        Ok(current_fingerprint) => current_fingerprint == checkpoint.storage_fingerprint,
        Err(err) => {
            tracing::debug!(
                error = %format!("{err:#}"),
                "could not compute current storage fingerprint while validating published index"
            );
            false
        }
    }
}

/// Whether independent coverage validation certified the completed checkpoint.
pub(in crate::indexer) fn persist_completed_lexical_rebuild_checkpoint_from_observations(
    storage: &FrankenStorage,
    index_path: &Path,
    db_state: LexicalRebuildDbState,
    total_messages: usize,
    max_conversation_id: Option<i64>,
    observed_tantivy_docs: Option<usize>,
    allow_full_expected_doc_scan: bool,
) -> Result<bool> {
    let total_conversations = db_state.total_conversations;
    let observed_tantivy_docs = match observed_tantivy_docs {
        Some(observed_tantivy_docs) => observed_tantivy_docs,
        None => {
            let Some(observed_tantivy_docs) = live_tantivy_doc_count(index_path)? else {
                return Ok(false);
            };
            observed_tantivy_docs
        }
    };
    // Task 5: live documents include converged search-region documents
    // alongside canonical message documents. Subtract the persisted marks
    // bound (region rows + queued stale deletes) so the coverage gate below
    // compares the canonical-message share exactly like it did before
    // region documents existed. A pending or partial region publication
    // under-counts live region docs against the bound, which only makes
    // this refresh skip — conservative, never a false certification.
    let region_doc_bound =
        crate::viewer_extensions::search_regions::live_region_doc_bound(storage.raw()).unwrap_or(0);
    let observed_message_docs = observed_tantivy_docs.saturating_sub(region_doc_bound as usize);
    // GH #435: the lexical sink intentionally drops hard-noise messages
    // (empty content / tool-acks via `is_hard_message_noise`), so on such
    // corpora a HEALTHY live index always has fewer docs than the raw
    // `COUNT(*) FROM messages`. Gating this refresh on the raw count made the
    // metadata-only checkpoint refresh (#353) skip forever, leaving the stale
    // fingerprint that drives repeated `--force-rebuild` churn (also #433).
    // Gate on the same noise-adjusted expectation the rebuild planner uses
    // (#317). The per-conversation content scan is only paid when the cheap
    // raw equality does not already prove coverage.
    let expected_docs = if observed_message_docs == total_messages {
        total_messages
    } else {
        let Some(expected_docs) = expected_live_lexical_doc_count_cached_with_scan_policy(
            storage,
            index_path,
            &db_state.db_path,
            total_messages,
            None,
            allow_full_expected_doc_scan,
        )?
        else {
            tracing::debug!(
                path = %index_path.display(),
                "deferring watch checkpoint refresh until full expected-document verification"
            );
            return Ok(false);
        };
        expected_docs
    };
    if observed_message_docs != expected_docs {
        tracing::debug!(
            path = %index_path.display(),
            observed_tantivy_docs,
            expected_lexical_docs = expected_docs,
            canonical_messages = total_messages,
            "skipping lexical checkpoint refresh because the live Tantivy index does not match the noise-adjusted expected document count"
        );
        return Ok(false);
    }

    let committed_meta_fingerprint = index_meta_fingerprint(index_path)?;
    let db_path_string = db_state.db_path.clone();
    let mut state = match load_lexical_rebuild_state(index_path)? {
        Some(state)
            if state.version == LEXICAL_REBUILD_STATE_VERSION
                && state.schema_hash == crate::search::tantivy::SCHEMA_HASH
                && lexical_rebuild_db_paths_match(&state.db.db_path, &db_path_string) =>
        {
            state
        }
        Some(state) => {
            tracing::info!(
                path = %index_path.display(),
                existing_db_path = %state.db.db_path,
                existing_completed = state.completed,
                "replacing stale lexical rebuild checkpoint from the live Tantivy index"
            );
            LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE)
        }
        None => {
            tracing::info!(
                path = %index_path.display(),
                total_conversations,
                total_messages,
                "bootstrapping missing lexical rebuild checkpoint from the live Tantivy index"
            );
            LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE)
        }
    };
    let target_committed_offset = i64::try_from(total_conversations).unwrap_or(i64::MAX);
    if state.db == db_state
        && state.page_size == LEXICAL_REBUILD_PAGE_SIZE
        && state.committed_offset == target_committed_offset
        && state.committed_conversation_id == max_conversation_id
        && state.processed_conversations == total_conversations
        && state.indexed_docs == observed_tantivy_docs
        && state.indexed_region_docs == region_doc_bound as usize
        && state.committed_meta_fingerprint == committed_meta_fingerprint
        && state.pending.is_none()
        && state.completed
        && state.effective_execution_mode() == LexicalRebuildExecutionMode::SharedWriter
        && state.runtime == LexicalRebuildPipelineRuntimeSnapshot::default()
    {
        tracing::debug!(
            path = %index_path.display(),
            total_conversations,
            total_messages,
            "skipping lexical checkpoint rewrite because the completed state already matches the live Tantivy index"
        );
        return Ok(true);
    }

    state.db = db_state;
    state.page_size = LEXICAL_REBUILD_PAGE_SIZE;
    state.committed_offset = target_committed_offset;
    state.committed_conversation_id = max_conversation_id;
    state.processed_conversations = total_conversations;
    // GH #435: record the OBSERVED live doc count, not the raw canonical
    // message count — on noisy corpora those legitimately differ, and readers
    // like `should_skip_post_full_scan_authoritative_rebuild` compare the
    // checkpoint's indexed_docs against live Tantivy observations.
    state.indexed_docs = observed_tantivy_docs;
    state.indexed_region_docs = region_doc_bound as usize;
    state.pending = None;
    state.completed = true;
    state.committed_meta_fingerprint = committed_meta_fingerprint;
    state.updated_at_ms = FrankenStorage::now_millis();
    persist_lexical_rebuild_state(index_path, &state)?;
    Ok(true)
}

/// 5ajer: the final checkpoint refresh runs after the scan has parked every
/// counter at `current == total`, and it can recount the expected lexical docs
/// (database work that reports no progress). Grant it the finalize-class grace
/// the other post-publish steps get, so the stall watchdog cannot kill a run
/// whose ingest has already finished.
pub(in crate::indexer) fn grant_final_checkpoint_refresh_grace(
    progress: Option<&Arc<IndexingProgress>>,
) {
    if let Some(progress) = progress {
        progress.finalizing.store(true, Ordering::Relaxed);
        progress.tick_activity();
    }
}

pub(in crate::indexer) fn refresh_completed_lexical_rebuild_checkpoint(
    storage: &FrankenStorage,
    db_path: &Path,
    data_dir: &Path,
) -> Result<()> {
    let total_conversations = count_total_conversations_exact(storage)?;
    let index_path = index_dir(data_dir)?;
    let total_messages = count_total_messages_exact(storage)?;
    let max_conversation_id = max_conversation_id_exact(storage)?;
    let db_state = lexical_rebuild_db_state_with_exact_totals(
        storage,
        db_path,
        total_conversations,
        total_messages,
    )?;
    persist_completed_lexical_rebuild_checkpoint_from_observations(
        storage,
        &index_path,
        db_state,
        total_messages,
        max_conversation_id,
        None,
        true,
    )
    .map(|_| ())
}

pub(in crate::indexer) fn refresh_completed_watch_lexical_checkpoint(
    storage: &FrankenStorage,
    index: &TantivyIndex,
    db_path: &Path,
    index_path: &Path,
) -> Result<bool> {
    // An absent, invalidated or expired expectation must not trigger a scan
    // of old message bodies on the live callback. Ordinary indexing performs
    // that verification; until then this checkpoint stays uncertified.
    // A successful source can publish while another source still owes a
    // replay. Never certify that partially published canonical archive.
    if !load_watch_lexical_replay_debt(storage)?.is_empty() {
        return Ok(false);
    }
    let observed_docs = usize::try_from(index.doc_count()?)?;
    let total_conversations = count_total_conversations_exact(storage)?;
    let total_messages = count_total_messages_exact(storage)?;
    let max_conversation_id = max_conversation_id_exact(storage)?;
    let db_state = lexical_rebuild_db_state_with_exact_totals(
        storage,
        db_path,
        total_conversations,
        total_messages,
    )?;
    // Use the committed writer's verified count rather than opening a new
    // reader and hashing every lexical segment on each watch callback. The
    // existing independent, noise-adjusted coverage check still applies.
    persist_completed_lexical_rebuild_checkpoint_from_observations(
        storage,
        index_path,
        db_state,
        total_messages,
        max_conversation_id,
        Some(observed_docs),
        false,
    )
}

pub(in crate::indexer) fn refresh_completed_lexical_rebuild_checkpoint_for_final_state(
    storage: &mut FrankenStorage,
    db_path: &Path,
    data_dir: &Path,
    keep_storage_open: bool,
    exact_counts: Option<(usize, usize)>,
) -> Result<()> {
    if let Some((total_conversations, total_messages)) = exact_counts {
        // This run already knows the exact canonical row counts, so avoid
        // re-opening or force-closing the large database during finalization.
        // The lexical fingerprint is content-based (`COUNT/MAX(id)`), so the
        // live canonical handle already has the authoritative values.
        let max_conversation_id = max_conversation_id_exact(storage)?;
        let db_state = lexical_rebuild_db_state_with_exact_totals(
            storage,
            db_path,
            total_conversations,
            total_messages,
        )?;
        let index_path = index_dir(data_dir)?;
        // GH #435: pass no fabricated observation — read the live Tantivy doc
        // count so the checkpoint records what the index actually holds (on
        // noisy corpora that is legitimately below `total_messages`), and so
        // the refresh stays fail-closed when the index is genuinely behind.
        return persist_completed_lexical_rebuild_checkpoint_from_observations(
            storage,
            &index_path,
            db_state,
            total_messages,
            max_conversation_id,
            None,
            true,
        )
        .map(|_| ());
    }

    if keep_storage_open {
        return refresh_completed_lexical_rebuild_checkpoint(storage, db_path, data_dir);
    }

    // Fallback for callers that do not already know the exact canonical row
    // counts. This reopens the DB once, derives the settled counts + content
    // fingerprint from that fresh snapshot, and avoids a redundant second open
    // just to rebuild the same lexical storage fingerprint.
    storage.close_best_effort_in_place();
    let mut settled = FrankenStorage::open_readonly(db_path).with_context(|| {
        format!(
            "reopening readonly storage to refresh settled lexical checkpoint for {}",
            db_path.display()
        )
    })?;
    let total_conversations = count_total_conversations_exact(&settled)?;
    let total_messages = count_total_messages_exact(&settled)?;
    let max_conversation_id = max_conversation_id_exact(&settled)?;
    let db_state = lexical_rebuild_db_state_with_exact_totals(
        &settled,
        db_path,
        total_conversations,
        total_messages,
    )?;
    let index_path = index_dir(data_dir)?;
    // Keep `settled` open through the persist call: the doc-count gate may
    // need it to compute the noise-adjusted expectation (GH #435).
    let result = persist_completed_lexical_rebuild_checkpoint_from_observations(
        &settled,
        &index_path,
        db_state,
        total_messages,
        max_conversation_id,
        None,
        true,
    );
    settled.close_best_effort_in_place();
    result.map(|_| ())
}

pub(in crate::indexer) fn persist_final_index_run_metadata(
    storage: &FrankenStorage,
    db_path: &Path,
    performed_scan: bool,
    scan_start_ts: i64,
    now_ms: i64,
) -> Result<()> {
    persist_final_index_run_metadata_with_writer(
        db_path,
        performed_scan,
        scan_start_ts,
        now_ms,
        || {
            persist::with_concurrent_retry(persist::begin_concurrent_retry_limit(), || {
                persist::with_ephemeral_writer(
                    storage,
                    false,
                    "updating final index run metadata",
                    |writer| {
                        if performed_scan {
                            writer.set_last_scan_ts(scan_start_ts)?;
                        }
                        writer.set_last_indexed_at(now_ms)
                    },
                )
            })
        },
    )
}

/// Bead zz8ni: the expensive index + lexical rebuild work above this call
/// has already been committed durably. The `last_indexed_at` /
/// `last_scan_ts` markers are status-display metadata — losing the writer
/// race to a peer `cass` process MUST NOT fail the whole run and discard a
/// multi-minute rebuild. A subsequent incremental `cass index` will
/// rewrite the markers once contention clears.
///
/// Split out with an injectable `writer_fn` so the log-and-swallow
/// behavior can be exercised by unit tests without shelling out to a real
/// locked database.
pub(in crate::indexer) fn persist_final_index_run_metadata_with_writer<F>(
    db_path: &Path,
    performed_scan: bool,
    scan_start_ts: i64,
    now_ms: i64,
    writer_fn: F,
) -> Result<()>
where
    F: FnOnce() -> Result<()>,
{
    match writer_fn() {
        Ok(()) => {
            if performed_scan {
                tracing::info!(
                    scan_start_ts,
                    "updated last_scan_ts for incremental indexing"
                );
            } else {
                tracing::info!(
                    db_path = %db_path.display(),
                    "preserving last_scan_ts because this run only resumed the lexical rebuild"
                );
            }
            tracing::info!(now_ms, "updated last_indexed_at for status display");
            Ok(())
        }
        Err(err) => {
            tracing::warn!(
                db_path = %db_path.display(),
                performed_scan,
                scan_start_ts,
                now_ms,
                error = %format!("{err:#}"),
                "deferred final index-run metadata update after retries exhausted; \
                 index and lexical artifacts are committed, status markers will be \
                 rewritten on the next incremental run once peer contention clears"
            );
            Ok(())
        }
    }
}

pub(in crate::indexer) fn persist_connector_scan_watermarks(
    storage: &FrankenStorage,
    db_path: &Path,
    scanned_connectors: &BTreeSet<String>,
    scan_start_ts: i64,
) -> Result<()> {
    persist_connector_scan_watermarks_with_writer(
        db_path,
        scanned_connectors.len(),
        scan_start_ts,
        || {
            persist::with_concurrent_retry(persist::begin_concurrent_retry_limit(), || {
                persist::with_ephemeral_writer(
                    storage,
                    false,
                    "updating connector scan watermarks",
                    |writer| {
                        for connector_name in scanned_connectors {
                            record_connector_scan_watermark(writer, connector_name, scan_start_ts)?;
                        }
                        Ok(())
                    },
                )
            })
        },
    )
}

pub(in crate::indexer) fn persist_connector_scan_watermarks_with_writer<F>(
    db_path: &Path,
    connector_count: usize,
    scan_start_ts: i64,
    writer_fn: F,
) -> Result<()>
where
    F: FnOnce() -> Result<()>,
{
    if connector_count == 0 {
        return Ok(());
    }

    match writer_fn() {
        Ok(()) => {
            tracing::info!(
                connector_count,
                scan_start_ts,
                "updated connector last_scan_ts watermarks for incremental indexing"
            );
            Ok(())
        }
        Err(err) => {
            tracing::warn!(
                db_path = %db_path.display(),
                connector_count,
                scan_start_ts,
                error = %format!("{err:#}"),
                "deferred connector scan-watermark update after retries exhausted; \
                 index and lexical artifacts are committed, per-connector markers \
                 will be rewritten on the next incremental run once peer contention clears"
            );
            Ok(())
        }
    }
}

#[cfg(test)]
pub(crate) fn load_lexical_rebuild_snapshot(
    index_path: &Path,
    db_path: &Path,
) -> Result<Option<LexicalRebuildSnapshot>> {
    let Some(state) = load_lexical_rebuild_state(index_path)? else {
        return Ok(None);
    };

    if state.completed || !crate::stored_path_identity_matches(&state.db.db_path, db_path) {
        return Ok(None);
    }
    let processed_conversations = state.reported_processed_conversations();
    let committed_conversation_id = state.reported_committed_conversation_id();
    let indexed_docs = state.reported_indexed_docs();

    Ok(Some(LexicalRebuildSnapshot {
        db_path: state.db.db_path,
        total_conversations: state.db.total_conversations,
        storage_fingerprint: state.db.storage_fingerprint,
        committed_offset: state.committed_offset,
        committed_conversation_id,
        processed_conversations,
        indexed_docs,
        completed: state.completed,
        updated_at_ms: state.updated_at_ms,
    }))
}

pub(in crate::indexer) fn repair_daily_stats_if_drifted(
    storage: &FrankenStorage,
    db_path: &Path,
    known_archive_fingerprint: Option<&str>,
) -> Result<DailyStatsRepairOutcome> {
    if let Some(archive_fingerprint) = known_archive_fingerprint
        && storage.daily_stats_is_known_healthy_for_archive_fingerprint(archive_fingerprint)?
    {
        return Ok(DailyStatsRepairOutcome::SkippedKnownHealthyForFingerprint {
            archive_fingerprint: archive_fingerprint.to_string(),
        });
    }

    let health = storage.daily_stats_health().with_context(|| {
        format!(
            "checking daily_stats health before index planning for {}",
            db_path.display()
        )
    })?;

    if health.populated && health.drift == 0 && !storage.daily_stats_content_repair_required()? {
        if let Some(archive_fingerprint) = known_archive_fingerprint {
            storage.record_daily_stats_archive_fingerprint(archive_fingerprint)?;
        }
        return Ok(DailyStatsRepairOutcome::AlreadyHealthy);
    }

    tracing::warn!(
        db_path = %db_path.display(),
        populated = health.populated,
        row_count = health.row_count,
        conversation_count = health.conversation_count,
        materialized_total = health.materialized_total,
        drift = health.drift,
        "daily_stats is missing or drifted; rebuilding from canonical conversations"
    );

    // #329: the packet projection rebuild held one write transaction for the
    // entire archive and materialized canonical message batches before the
    // nominally bounded fallback even started. On the 967k-message field
    // corpus that first path reached an internal OOM, then the fallback did the
    // same. Production now has one authoritative implementation: the
    // failure-atomic staging rebuild whose canonical message pages are driven
    // by one prepared, row-streaming `(conversation_id, idx)` statement.
    let rebuilt = storage.rebuild_daily_stats().with_context(|| {
        format!(
            "rebuilding daily_stats with streaming canonical scan before index planning for {}",
            db_path.display()
        )
    })?;

    tracing::info!(
        db_path = %db_path.display(),
        rows_created = rebuilt.rows_created,
        total_sessions = rebuilt.total_sessions,
        "rebuilt daily_stats before index planning"
    );

    if let Some(archive_fingerprint) = known_archive_fingerprint {
        storage.record_daily_stats_archive_fingerprint(archive_fingerprint)?;
    }

    Ok(DailyStatsRepairOutcome::Rebuilt {
        rows_created: rebuilt.rows_created,
        total_sessions: rebuilt.total_sessions,
    })
}

#[cfg(test)]
pub(in crate::indexer) const PACKET_DAILY_STATS_REBUILD_BATCH_SIZE: i64 = 256;

#[cfg(test)]
pub(in crate::indexer) fn packet_daily_stats_provenance(
    conversation: &crate::storage::sqlite::LexicalRebuildConversationRow,
) -> LexicalRebuildPacketProvenance {
    let source_kind = if conversation.source_id == LOCAL_SOURCE_ID {
        SourceKind::Local
    } else {
        SourceKind::Ssh
    };
    LexicalRebuildPacketProvenance {
        source_id: conversation.source_id.clone(),
        origin_kind: source_kind.as_str().to_string(),
        origin_host: conversation.origin_host.clone(),
    }
}

#[cfg(test)]
pub(in crate::indexer) fn packet_daily_stats_message_count(
    projections: &ConversationPacketSinkProjections,
) -> i64 {
    let analytics = &projections.analytics;
    i64::try_from(
        analytics.user_messages
            + analytics.assistant_messages
            + analytics.tool_messages
            + analytics.system_messages
            + analytics.other_messages,
    )
    .unwrap_or(i64::MAX)
}

#[cfg(test)]
pub(in crate::indexer) fn packet_daily_stats_total_chars(
    projections: &ConversationPacketSinkProjections,
) -> i64 {
    i64::try_from(projections.lexical.total_content_bytes).unwrap_or(i64::MAX)
}

#[cfg(test)]
pub(in crate::indexer) fn packet_update_daily_stats_batched_in_tx(
    tx: &FrankenTransaction<'_>,
    entries: &[(i64, String, String, StatsDelta)],
) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }

    let now = FrankenStorage::now_millis();
    let mut total_affected = 0usize;
    for (day_id, agent_slug, source_id, delta) in entries {
        total_affected += tx.execute_compat(
            "INSERT INTO daily_stats (day_id, agent_slug, source_id, session_count, message_count, total_chars, last_updated)
             VALUES(?1,?2,?3,?4,?5,?6,?7)
             ON CONFLICT(day_id, agent_slug, source_id) DO UPDATE SET
                 session_count = session_count + excluded.session_count,
                 message_count = message_count + excluded.message_count,
                 total_chars = total_chars + excluded.total_chars,
                 last_updated = excluded.last_updated",
            &[
                ParamValue::from(*day_id),
                ParamValue::from(agent_slug.as_str()),
                ParamValue::from(source_id.as_str()),
                ParamValue::from(delta.session_count_delta),
                ParamValue::from(delta.message_count_delta),
                ParamValue::from(delta.total_chars_delta),
                ParamValue::from(now),
            ],
        )?;
    }

    Ok(total_affected)
}

#[cfg(test)]
pub(in crate::indexer) fn rebuild_daily_stats_from_conversation_packets(
    storage: &FrankenStorage,
    db_path: &Path,
) -> Result<DailyStatsRebuildResult> {
    #[cfg(test)]
    if dotenvy::var("CASS_TEST_PACKET_DAILY_STATS_REBUILD_OOM").is_ok() {
        anyhow::bail!("out of memory");
    }

    let (agent_slugs, workspace_paths) = storage
        .build_lexical_rebuild_lookups()
        .with_context(|| format!("building packet rebuild lookups for {}", db_path.display()))?;
    let mut tx = storage.raw().transaction().with_context(|| {
        format!(
            "opening packet daily_stats rebuild transaction for {}",
            db_path.display()
        )
    })?;
    tx.execute("DELETE FROM daily_stats").with_context(|| {
        format!(
            "clearing daily_stats before packet rebuild for {}",
            db_path.display()
        )
    })?;

    let mut last_conversation_id = 0_i64;
    let mut conversation_batches = 0usize;
    let mut conversations_processed = 0usize;
    let mut messages_projected = 0usize;
    let mut raw_entries_flushed = 0usize;
    let mut expanded_entries_flushed = 0usize;

    loop {
        let conversation_rows = storage
            .list_conversations_for_lexical_rebuild_after_id(
                PACKET_DAILY_STATS_REBUILD_BATCH_SIZE,
                last_conversation_id,
                &agent_slugs,
                &workspace_paths,
            )
            .with_context(|| {
                format!(
                    "listing canonical conversations for packet daily_stats rebuild after id {}",
                    last_conversation_id
                )
            })?;
        if conversation_rows.is_empty() {
            break;
        }

        let conversation_ids = conversation_rows
            .iter()
            .map(|conversation| {
                conversation.id.ok_or_else(|| {
                    anyhow::anyhow!(
                        "packet daily_stats rebuild encountered conversation without id after {}",
                        last_conversation_id
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut grouped_messages = storage
            .fetch_messages_for_lexical_rebuild_batch(&conversation_ids, None, None)
            .with_context(|| {
                format!(
                    "fetching canonical message batch for packet daily_stats rebuild after id {}",
                    last_conversation_id
                )
            })?;

        let mut aggregate = StatsAggregator::new();
        for conversation in conversation_rows {
            let conversation_id = conversation.id.ok_or_else(|| {
                anyhow::anyhow!(
                    "packet daily_stats rebuild encountered conversation without id after {}",
                    last_conversation_id
                )
            })?;
            last_conversation_id = conversation_id;

            let canonical_messages = grouped_messages
                .remove(&conversation_id)
                .unwrap_or_default();
            let provenance = packet_daily_stats_provenance(&conversation);
            let packet = lexical_rebuild_contract_from_canonical_messages(
                &conversation,
                &provenance,
                canonical_messages,
            );
            let message_count = packet_daily_stats_message_count(&packet.projections);
            let total_chars = packet_daily_stats_total_chars(&packet.projections);
            let day_id = conversation
                .started_at
                .map(FrankenStorage::day_id_from_millis)
                .unwrap_or(0);
            aggregate.record(
                &conversation.agent_slug,
                &conversation.source_id,
                day_id,
                message_count,
                total_chars,
            );
            conversations_processed += 1;
            messages_projected = messages_projected.saturating_add(message_count.max(0) as usize);
        }

        conversation_batches += 1;
        raw_entries_flushed += aggregate.raw_entry_count();
        let entries = aggregate.expand();
        expanded_entries_flushed += entries.len();
        if !entries.is_empty() {
            packet_update_daily_stats_batched_in_tx(&tx, &entries)?;
        }

        if conversation_batches.is_multiple_of(25) {
            tracing::info!(
                target: "cass::perf::daily_stats",
                conversation_batches,
                batch_size = PACKET_DAILY_STATS_REBUILD_BATCH_SIZE,
                last_conversation_id,
                conversations_processed,
                messages_projected,
                "packet daily_stats rebuild progress"
            );
        }
    }

    let rows_created: i64 = tx.query_row_map(
        "SELECT COUNT(*) FROM daily_stats",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    let total_sessions: i64 = tx.query_row_map(
        "SELECT COALESCE(SUM(session_count), 0) FROM daily_stats WHERE agent_slug = 'all' AND source_id = 'all'",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;

    tx.commit()?;

    tracing::info!(
        target: "cass::perf::daily_stats",
        db_path = %db_path.display(),
        rows_created,
        total_sessions,
        conversation_batches,
        batch_size = PACKET_DAILY_STATS_REBUILD_BATCH_SIZE,
        conversations_processed,
        messages_projected,
        raw_entries_flushed,
        expanded_entries_flushed,
        "daily_stats rebuilt from canonical ConversationPacket projections"
    );

    Ok(DailyStatsRebuildResult {
        rows_created,
        total_sessions,
    })
}

pub(in crate::indexer) fn should_repair_daily_stats_after_historical_salvage(
    checked_pre_scan: bool,
    full_refresh: bool,
    rebuild_from_canonical_only: bool,
    salvage_messages_imported: usize,
) -> bool {
    salvage_messages_imported > 0
        || (full_refresh && !rebuild_from_canonical_only && !checked_pre_scan)
}
