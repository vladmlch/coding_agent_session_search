use super::*;

pub(in crate::indexer) fn write_json_pretty_atomically<T: serde::Serialize>(
    path: &Path,
    value: &T,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating parent directory for {}", path.display()))?;
    }
    let temp_path = unique_atomic_temp_path(path);
    {
        let file = create_new_atomic_sidecar_file(&temp_path)
            .with_context(|| format!("creating temporary file {}", temp_path.display()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, value)
            .with_context(|| format!("serializing {}", path.display()))?;
        writer
            .flush()
            .with_context(|| format!("flushing temporary file {}", temp_path.display()))?;
        writer
            .get_ref()
            .sync_all()
            .with_context(|| format!("syncing temporary file {}", temp_path.display()))?;
    }
    replace_file_from_temp(&temp_path, path)
        .with_context(|| format!("replacing {} from temp file", path.display()))
}

#[cfg(not(windows))]
pub(in crate::indexer) fn sync_parent_directory(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let directory = File::open(parent)
        .with_context(|| format!("opening parent directory {} for sync", parent.display()))?;
    directory
        .sync_all()
        .with_context(|| format!("syncing parent directory {}", parent.display()))
}

#[cfg(windows)]
pub(in crate::indexer) fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}

/// How many conversations the published lexical generation was certified
/// against: its completed rebuild checkpoint for this database, if any.
pub(in crate::indexer) fn completed_lexical_checkpoint_conversations(
    index_path: &Path,
    db_path: &Path,
) -> Option<usize> {
    let state = load_lexical_rebuild_state(index_path).ok().flatten()?;
    (state.completed && crate::stored_path_identity_matches(&state.db.db_path, db_path))
        .then_some(state.db.total_conversations)
}

pub(in crate::indexer) fn load_lexical_rebuild_state(
    index_path: &Path,
) -> Result<Option<LexicalRebuildState>> {
    let path = lexical_rebuild_state_path(index_path);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("reading lexical rebuild state {}", path.display()));
        }
    };

    match serde_json::from_slice::<LexicalRebuildState>(&bytes) {
        Ok(state) => Ok(Some(state)),
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "ignoring malformed lexical rebuild checkpoint"
            );
            Ok(None)
        }
    }
}

pub(in crate::indexer) fn persist_lexical_rebuild_state(
    index_path: &Path,
    state: &LexicalRebuildState,
) -> Result<()> {
    let path = lexical_rebuild_state_path(index_path);
    write_json_pretty_atomically(&path, state)
}

// Rebuild progress and publication authority have different lifetimes. A staged
// rebuild overwrites .lexical-rebuild-state.json in the LIVE directory before
// touching its documents. Keep a separate, immutable-until-publication receipt
// so readers need not join (or restart) that rebuild to search the prior index.
pub(in crate::indexer) const LEXICAL_PUBLISHED_STATE_FILE: &str = ".lexical-published-state.json";
pub(in crate::indexer) const LEXICAL_PUBLISHED_STATE_MAX_BYTES: u64 = 1024 * 1024;

pub(in crate::indexer) fn lexical_state_can_certify_publication(
    state: &LexicalRebuildState,
) -> bool {
    state.version == LEXICAL_REBUILD_STATE_VERSION
        && state.completed
        && state.pending.is_none()
        && state.schema_hash == crate::search::tantivy::SCHEMA_HASH
        && lexical_rebuild_page_size_is_compatible(state.page_size)
        && state.processed_conversations == state.db.total_conversations
        && usize::try_from(state.committed_offset).ok() == Some(state.db.total_conversations)
        // Task 5: `indexed_docs` totals canonical message documents PLUS
        // converged search-region documents. The message-share bound is
        // preserved exactly by subtracting the recorded region share —
        // the live-count match in `lexical_publication_receipt_matches_live`
        // still compares the full total against the index.
        && state.indexed_region_docs <= state.indexed_docs
        && state.indexed_docs.saturating_sub(state.indexed_region_docs)
            <= state.db.total_messages
        && completed_lexical_storage_fingerprint_matches_total(
            &state.db.storage_fingerprint,
            state.db.total_conversations,
        )
        && state
            .committed_meta_fingerprint
            .as_ref()
            .is_some_and(|hash| !hash.is_empty())
}

pub(in crate::indexer) fn lexical_publication_receipt_matches_live(
    index_path: &Path,
    state: &LexicalRebuildState,
) -> Result<bool> {
    if !lexical_state_can_certify_publication(state) {
        return Ok(false);
    }
    let before = index_meta_fingerprint(index_path)?;
    if before != state.committed_meta_fingerprint {
        return Ok(false);
    }
    let count_matches = crate::search::tantivy::searchable_index_live_doc_count(index_path)
        .is_some_and(|docs| u128::from(docs) == state.indexed_docs as u128);
    // A concurrent atomic swap must not combine one generation's receipt with
    // another's count. No canonical DB open, repair, or freshness claim here.
    Ok(count_matches && index_meta_fingerprint(index_path)? == before)
}

pub(in crate::indexer) fn persist_lexical_publication_receipt(
    index_path: &Path,
    state: &LexicalRebuildState,
) -> Result<()> {
    anyhow::ensure!(
        lexical_state_can_certify_publication(state),
        "cannot publish an incomplete or inconsistent lexical receipt"
    );
    let path = index_path.join(LEXICAL_PUBLISHED_STATE_FILE);
    write_json_pretty_atomically(&path, state)?;
    sync_parent_directory(&path)
}

/// Read-only proof for serving the last completed generation during a staged
/// rebuild. This never certifies the INCOMPLETE rebuild or advances its cursor.
/// Callers must also admit the engine's readable contract before serving it.
pub(crate) fn has_usable_lexical_publication_receipt(index_path: &Path, db_path: &Path) -> bool {
    let inspect = || -> Result<bool> {
        use std::io::Read;
        let path = index_path.join(LEXICAL_PUBLISHED_STATE_FILE);
        let metadata = fs::symlink_metadata(&path)?;
        anyhow::ensure!(
            metadata.is_file() && metadata.len() <= LEXICAL_PUBLISHED_STATE_MAX_BYTES,
            "lexical publication receipt is not a bounded regular file"
        );
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // A replaced receipt must not turn this bounded read into a FIFO
            // open or a symlink traversal between metadata inspection and open.
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(&path)?;
        anyhow::ensure!(
            file.metadata()?.is_file(),
            "lexical receipt is not a regular file"
        );
        let mut bytes = Vec::new();
        file.take(LEXICAL_PUBLISHED_STATE_MAX_BYTES + 1)
            .read_to_end(&mut bytes)?;
        anyhow::ensure!(
            bytes.len() as u64 <= LEXICAL_PUBLISHED_STATE_MAX_BYTES,
            "lexical publication receipt exceeds its read budget"
        );
        let state: LexicalRebuildState = serde_json::from_slice(&bytes)?;
        if !crate::stored_path_identity_matches(&state.db.db_path, db_path) {
            return Ok(false);
        }
        lexical_publication_receipt_matches_live(index_path, &state)
    };
    match inspect() {
        Ok(usable) => usable,
        Err(error) => {
            tracing::debug!(
                error = %format!("{error:#}"),
                "no usable independent lexical publication receipt"
            );
            false
        }
    }
}

pub(in crate::indexer) fn clear_lexical_rebuild_state(index_path: &Path) -> Result<()> {
    let path = lexical_rebuild_state_path(index_path);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => {
            Err(err).with_context(|| format!("removing lexical rebuild state {}", path.display()))
        }
    }
}

pub(in crate::indexer) fn index_meta_fingerprint(index_path: &Path) -> Result<Option<String>> {
    crate::search::tantivy::searchable_index_fingerprint(index_path)
}

/// GH #457: the post-publish proof that a rebuild's generation serves exactly
/// the documents it counted. A count that cannot be observed — no manifest
/// landed, or no reader can open what did — is a FAILED proof, never a
/// skipped one: the previous `if let Some(observed)` shape verified nothing
/// in exactly the cases that matter, so a generation whose MANIFEST never
/// landed could still be certified by a completed checkpoint.
pub(crate) fn verify_published_lexical_doc_count(
    index_path: &Path,
    indexed_docs: usize,
    publish_mode: &str,
) -> Result<()> {
    let summary = crate::search::tantivy::searchable_index_summary(index_path)
        .with_context(|| {
            format!(
                "{publish_mode} lexical rebuild published {indexed_docs} docs but the generation at {} \
                 cannot be opened for verification; refusing to certify it (GH #457)",
                index_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "{publish_mode} lexical rebuild published {indexed_docs} docs but no searchable \
                 generation was found at {}; refusing to certify it (GH #457)",
                index_path.display()
            )
        })?;
    if summary.docs != indexed_docs {
        return Err(LexicalDocCountMismatch {
            publish_mode: publish_mode.to_owned(),
            counted: indexed_docs,
            observed: summary.docs,
        }
        .into());
    }
    Ok(())
}

/// GH #457: a fresh reader serves a different number of live documents than
/// the run counted. Typed so a resumed run can tell a candidate it can never
/// certify from one it could not open.
#[derive(Debug)]
pub(crate) struct LexicalDocCountMismatch {
    publish_mode: String,
    pub(in crate::indexer) counted: usize,
    pub(in crate::indexer) observed: usize,
}

impl std::fmt::Display for LexicalDocCountMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} lexical rebuild published {} docs but a fresh reader sees {}; refusing to \
             certify the generation (GH #457)",
            self.publish_mode, self.counted, self.observed
        )
    }
}

impl std::error::Error for LexicalDocCountMismatch {}

/// GH #494: a resumed candidate failed certification, was retained, and the
/// live checkpoint now restarts from zero. The caller rebuilds once.
#[derive(Debug)]
pub(in crate::indexer) struct ResumedLexicalCandidateRetained {
    pub(in crate::indexer) quarantine: PathBuf,
}

impl std::fmt::Display for ResumedLexicalCandidateRetained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the resumed lexical candidate cannot be certified; retained it in {} and \
             rebuilding from zero (GH #494)",
            self.quarantine.display()
        )
    }
}

/// GH #494: a resumed staged candidate whose live documents disagree with the
/// count its checkpoint carried after the replay can never be certified by
/// resuming it again. The resume check catches an EOF checkpoint before the
/// replay; a surplus in the committed prefix, which the #440 upsert window
/// never revisits, surfaces only here. Retain the candidate with the
/// checkpoint that could not certify it, and reset the live checkpoint so even
/// an interrupted retry replays from zero. If retention fails, the original
/// refusal stands and nothing is retried.
pub(in crate::indexer) fn retain_uncertifiable_resumed_candidate(
    index_path: &Path,
    candidate: &Path,
    rebuild_state: &LexicalRebuildState,
    db_state: &LexicalRebuildDbState,
    refusal: anyhow::Error,
) -> anyhow::Error {
    let retained = (|| -> Result<PathBuf> {
        let quarantine = lexical_publish::quarantine_incomplete_candidate(candidate)?;
        persist_lexical_rebuild_state(&quarantine, rebuild_state)?;
        persist_lexical_rebuild_state(
            index_path,
            &LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE),
        )?;
        Ok(quarantine)
    })();
    match retained {
        Ok(quarantine) => {
            tracing::warn!(
                quarantine_path = %quarantine.display(),
                counted_docs = rebuild_state.indexed_docs,
                error = %format!("{refusal:#}"),
                "resumed lexical candidate failed certification; retained it and rebuilding from zero (GH #494)"
            );
            refusal.context(ResumedLexicalCandidateRetained { quarantine })
        }
        Err(retain_error) => refusal.context(format!(
            "could not retain the uncertifiable resumed lexical candidate, so it was not \
             rebuilt: {retain_error:#}"
        )),
    }
}

/// GH #457: a run must not certify a hollow generation. Every full rebuild
/// proves `live == indexed_docs` before completing its checkpoint, and the
/// pre-scan sparse check (`choose_incremental_canonical_lexical_repair_plan`)
/// rebuilds a generation that was already hollow when the run started, so at
/// the end of any run the served count (read from the MANIFEST the run just
/// committed, no engine open) must still cover what the checkpoint certified.
/// This is the backstop for a generation hollowed DURING the run: a shortfall
/// past the readiness floor is a hard failure naming the remedy, and the
/// ingest itself is already durable.
pub(in crate::indexer) fn verify_lexical_generation_not_hollow_after_run(
    index_path: &Path,
    db_path: &Path,
) -> Result<()> {
    let Some(expected_docs) =
        crate::search::asset_state::completed_lexical_checkpoint_indexed_docs(index_path, db_path)
    else {
        return Ok(());
    };
    let live_docs = crate::search::tantivy::searchable_index_live_doc_count(index_path);
    if let Some(verdict) = crate::search::asset_state::lexical_generation_hollow_verdict(
        Some(expected_docs),
        live_docs,
    ) {
        return Err(anyhow::anyhow!(
            "refusing to certify the lexical generation at {} after this run: {}",
            index_path.display(),
            verdict.reason()
        ));
    }
    Ok(())
}

pub(in crate::indexer) fn live_tantivy_doc_count(index_path: &Path) -> Result<Option<usize>> {
    match crate::search::tantivy::searchable_index_summary(index_path) {
        Ok(Some(summary)) => Ok(Some(summary.docs)),
        Ok(None) => Ok(None),
        Err(err) => {
            tracing::debug!(
                path = %index_path.display(),
                error = %err,
                "live Tantivy reader unavailable while refreshing lexical checkpoint"
            );
            Ok(None)
        }
    }
}

pub(in crate::indexer) fn validate_lexical_rebuild_shard_build_result(
    result: &LexicalRebuildShardBuildResult,
) -> Result<LexicalRebuildShardMergeArtifact> {
    crate::search::tantivy::validate_searchable_index_contract(&result.shard_index_path)
        .with_context(|| {
            format!(
                "validating built lexical rebuild shard {} at {}",
                result.shard.shard_index,
                result.shard_index_path.display()
            )
        })?;
    let observed_docs = live_tantivy_doc_count(&result.shard_index_path)?.ok_or_else(|| {
        anyhow::anyhow!(
            "built lexical rebuild shard {} at {} is not searchable",
            result.shard.shard_index,
            result.shard_index_path.display()
        )
    })?;
    if observed_docs != result.indexed_docs {
        return Err(anyhow::anyhow!(
            "built lexical rebuild shard {} reported {} docs but a fresh Tantivy reader sees {}",
            result.shard.shard_index,
            result.indexed_docs,
            observed_docs
        ));
    }
    // [coding_agent_session_search-rx1ex] When known, the shard plan's
    // `message_count` is a SQLite-side planning estimate from
    // `list_conversation_footprints_for_lexical_rebuild`, while
    // `observed_docs` is the post-filter Tantivy doc count. The
    // lexical sink legitimately drops messages via
    // `is_hard_message_noise` (empty content, hard-noise roles).
    // Pre-fix this check fired on any session where ANY message was
    // filtered — including the trivial 1-conversation/2-message
    // repro in bead rx1ex. The meaningful invariant is that the
    // sink can never produce MORE docs than there were source
    // messages; if it does, something is duplicating documents and
    // a hard error is correct. If observed_docs <= message_count,
    // the gap is filter-induced and we log the count for operator
    // audit instead of failing the rebuild.
    if lexical_shard_message_count_is_known(result.shard.message_count)
        && observed_docs > result.shard.message_count
    {
        return Err(anyhow::anyhow!(
            "built lexical rebuild shard {} indexed {} docs which EXCEEDS its shard plan's \
             {} source messages — the lexical sink should never produce more docs than \
             rows; investigate fan-out or duplicate-document bugs",
            result.shard.shard_index,
            observed_docs,
            result.shard.message_count
        ));
    }
    if lexical_shard_message_count_is_known(result.shard.message_count) {
        let filtered = result.shard.message_count.saturating_sub(observed_docs);
        if filtered > 0 {
            tracing::debug!(
                target: "cass::indexer::lexical_rebuild",
                shard_index = result.shard.shard_index,
                observed_docs,
                planned_message_count = result.shard.message_count,
                filtered_messages = filtered,
                "lexical rebuild shard indexed fewer docs than the shard plan's message count; \
                 gap is hard-noise/empty-content filtering applied by cass_document_for_message"
            );
        }
    } else {
        tracing::debug!(
            target: "cass::indexer::lexical_rebuild",
            shard_index = result.shard.shard_index,
            observed_docs,
            "lexical rebuild shard used conversation-only planning; source message count validation deferred to exact indexed-doc accounting"
        );
    }

    Ok(LexicalRebuildShardMergeArtifact {
        first_shard_index: result.shard.shard_index,
        last_shard_index: result.shard.shard_index,
        index_path: result.shard_index_path.clone(),
        docs: observed_docs,
        segments: result.segments,
    })
}

pub(in crate::indexer) fn validate_complete_lexical_rebuild_shard_artifacts(
    shard_plan: &LexicalShardPlan,
    artifacts: &[LexicalRebuildShardMergeArtifact],
) -> Result<()> {
    if artifacts.len() != shard_plan.shards.len() {
        return Err(anyhow::anyhow!(
            "staged lexical rebuild validated {} shard artifacts but planned {} shards",
            artifacts.len(),
            shard_plan.shards.len()
        ));
    }

    for (expected, artifact) in shard_plan.shards.iter().zip(artifacts.iter()) {
        if artifact.first_shard_index != expected.shard_index
            || artifact.last_shard_index != expected.shard_index
        {
            return Err(anyhow::anyhow!(
                "staged lexical rebuild shard artifact order mismatch: expected shard {} but got range {}..={}",
                expected.shard_index,
                artifact.first_shard_index,
                artifact.last_shard_index
            ));
        }
        // These counts were already checked against a fresh reader when each
        // shard build completed; reusing them avoids a redundant end-of-run
        // open of every staged shard.
        let observed_docs = artifact.docs;
        // [coding_agent_session_search-rx1ex] Mirrors the relaxation in
        // validate_lexical_rebuild_shard_build_result: tolerate
        // filter-induced doc < message gaps; only fail on the
        // impossible case where docs > raw messages. See the comment
        // there for the full rationale.
        if lexical_shard_message_count_is_known(expected.message_count)
            && observed_docs > expected.message_count
        {
            return Err(anyhow::anyhow!(
                "validated lexical rebuild shard {} has {} docs which EXCEEDS its shard plan's \
                 {} source messages — investigate fan-out or duplicate-document bugs",
                expected.shard_index,
                observed_docs,
                expected.message_count
            ));
        }
        if !lexical_shard_message_count_is_known(expected.message_count) {
            continue;
        }
        let filtered = expected.message_count.saturating_sub(observed_docs);
        if filtered > 0 {
            tracing::debug!(
                target: "cass::indexer::lexical_rebuild",
                shard_index = expected.shard_index,
                observed_docs,
                planned_message_count = expected.message_count,
                filtered_messages = filtered,
                "validated lexical rebuild shard has fewer docs than the shard plan's message \
                 count; gap is hard-noise/empty-content filtering"
            );
        }
    }

    Ok(())
}

pub(in crate::indexer) fn pending_commit_landed(
    base_meta_fingerprint: Option<&str>,
    current_meta_fingerprint: Option<&str>,
) -> bool {
    match (base_meta_fingerprint, current_meta_fingerprint) {
        (None, Some(_)) => true,
        (Some(base), Some(current)) => current != base,
        _ => false,
    }
}

/// New checkpoints bind their cursor to the physical content lane. SharedWriter
/// remains the legacy, unspecified-location case; completed receipts always
/// describe the published live tree, even when old scratch debris survives.
pub(in crate::indexer) fn lexical_rebuild_resume_content_path(
    index_path: &Path,
    state: &LexicalRebuildState,
) -> Result<PathBuf> {
    if state.completed
        || state.effective_execution_mode() == LexicalRebuildExecutionMode::LiveSingleIndex
    {
        return Ok(index_path.to_path_buf());
    }
    let scratch = staged_lexical_rebuild_scratch_path(index_path);
    if state.effective_execution_mode() == LexicalRebuildExecutionMode::StagedSingleIndex {
        let metadata = fs::symlink_metadata(&scratch).with_context(|| {
            format!(
                "checkpoint's staged lexical generation is unavailable: {}",
                scratch.display()
            )
        })?;
        anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "checkpoint's staged lexical generation is not a real directory: {}",
            scratch.display()
        );
        return Ok(scratch);
    }
    Ok(if scratch.is_dir() {
        scratch
    } else {
        index_path.to_path_buf()
    })
}

pub(in crate::indexer) fn lexical_rebuild_bound_candidate_is_missing(
    index_path: &Path,
    state: &LexicalRebuildState,
) -> Result<bool> {
    if state.completed
        || state.effective_execution_mode() != LexicalRebuildExecutionMode::StagedSingleIndex
    {
        return Ok(false);
    }
    let scratch = staged_lexical_rebuild_scratch_path(index_path);
    match fs::symlink_metadata(&scratch) {
        Ok(_) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error).with_context(|| {
            format!(
                "inspecting checkpoint's staged lexical generation {}",
                scratch.display()
            )
        }),
    }
}

pub(in crate::indexer) fn legacy_lexical_resume_lost_its_generation(
    index_path: &Path,
    state: &LexicalRebuildState,
) -> Result<bool> {
    if state.completed
        || state.effective_execution_mode() != LexicalRebuildExecutionMode::SharedWriter
        || staged_lexical_rebuild_scratch_path(index_path).is_dir()
        || (state.processed_conversations == 0 && state.pending.is_none())
    {
        return Ok(false);
    }
    // Old shared_writer checkpoints did not record a content lane. With no
    // scratch, only the exact committed live MANIFEST proves cursor ownership.
    // A different live manifest might be a landed in-place pending commit OR
    // an unrelated prior generation after staging was lost. Replay safely
    // rather than using that ambiguity as permission to skip canonical rows.
    let current = index_meta_fingerprint(index_path)?;
    Ok(state.committed_meta_fingerprint.is_none() || current != state.committed_meta_fingerprint)
}

pub(in crate::indexer) fn retain_missing_lexical_candidate_checkpoint(
    index_path: &Path,
    state: &LexicalRebuildState,
) -> Result<PathBuf> {
    let parent = index_path.parent().context("lexical index has no parent")?;
    // As with candidate quarantine, keep ownership before any fallible write.
    // A failed preservation must not remove evidence or overwrite live state.
    let quarantine = TempDirBuilder::new()
        .prefix(".lexical-rebuild-quarantine-")
        .tempdir_in(parent)
        .context("reserving missing-candidate checkpoint quarantine")?
        .keep();
    persist_lexical_rebuild_state(&quarantine, state)?;
    sync_parent_directory(&quarantine)?;
    Ok(quarantine)
}

pub(in crate::indexer) fn reconcile_pending_lexical_commit(
    index_path: &Path,
    mut state: LexicalRebuildState,
) -> Result<LexicalRebuildState> {
    let Some(pending) = state.pending.clone() else {
        return Ok(state);
    };

    // The checkpoint lives beside the old live generation, but a staged
    // rebuild commits into its scratch sibling. Comparing the pending commit
    // with the unrelated live MANIFEST can promote rows that never committed,
    // or roll back a commit that did. Recovery must observe the same content
    // directory that commit_lexical_rebuild_progress fingerprinted.
    let content_path = lexical_rebuild_resume_content_path(index_path, &state)?;
    let current_meta_fingerprint = index_meta_fingerprint(&content_path)?;
    // A staged candidate's MANIFEST counts what really committed, and that
    // count decides. A fingerprint cannot: a heartbeat's uncommitted progress
    // in a checkpoint from an older cass carries the live generation's
    // fingerprint, which always differs from the candidate's (so it was
    // promoted, and the resume check then quarantined a candidate that was
    // only behind its own heartbeat, GH #523), while the terminal checkpoint
    // is written after the folding commit, so its base already describes the
    // committed generation. The resume check applies the same count rule.
    let landed = match (content_path != index_path)
        .then(|| crate::search::tantivy::searchable_index_live_doc_count(&content_path))
        .flatten()
    {
        Some(live_docs) => u128::from(live_docs) >= pending.indexed_docs as u128,
        None => pending_commit_landed(
            pending.base_meta_fingerprint.as_deref(),
            current_meta_fingerprint.as_deref(),
        ),
    };
    if landed {
        state.finalize_commit(current_meta_fingerprint);
    } else {
        state.clear_pending();
    }
    // Runtime telemetry belongs to the prior in-flight pipeline instance. Once
    // recovery reconciles a dead run's pending commit, the next attach should
    // not see stale queue/controller state until the new producer emits it.
    state.clear_runtime();
    persist_lexical_rebuild_state(index_path, &state)?;
    Ok(state)
}

pub(in crate::indexer) fn prepare_lexical_rebuild_state_for_active_run(
    index_path: &Path,
    state: &mut LexicalRebuildState,
) -> Result<()> {
    if !state.is_incomplete() || !state.runtime.is_observed() {
        return Ok(());
    }

    // Persisted runtime telemetry belongs to a previous in-flight pipeline
    // instance. Once a new rebuild process has resumed ownership, attach/status
    // readers should not see stale queue/controller state before the new
    // producer emits its first runtime heartbeat.
    state.clear_runtime();
    persist_lexical_rebuild_state(index_path, state)
}

pub(in crate::indexer) fn persist_lexical_rebuild_state_for_active_run_start(
    index_path: &Path,
    state: &LexicalRebuildState,
) -> Result<()> {
    // Bootstrap older completed generations before replacing their only
    // checkpoint with progress for an off-live candidate. Never copy an
    // incomplete checkpoint or a receipt for different engine bytes.
    if !state.completed
        && let Some(previous) = load_lexical_rebuild_state(index_path)?
    {
        match lexical_publication_receipt_matches_live(index_path, &previous) {
            Ok(true) => persist_lexical_publication_receipt(index_path, &previous)?,
            Ok(false) => {}
            Err(error) => tracing::debug!(
                error = %format!("{error:#}"),
                "previous generation cannot be certified; continuing explicit rebuild"
            ),
        }
    }
    persist_lexical_rebuild_state(index_path, state)
}

pub(in crate::indexer) fn normalize_lexical_rebuild_state_for_current_run(
    index_path: &Path,
    state: &mut LexicalRebuildState,
) -> Result<()> {
    if state.page_size == LEXICAL_REBUILD_PAGE_SIZE {
        return Ok(());
    }
    if !lexical_rebuild_page_size_is_compatible(state.page_size) {
        anyhow::bail!(
            "refusing to normalize incompatible lexical rebuild checkpoint page_size={} at {}",
            state.page_size,
            index_path.display()
        );
    }
    state.page_size = LEXICAL_REBUILD_PAGE_SIZE;
    persist_lexical_rebuild_state(index_path, state)
}

pub(in crate::indexer) fn resolve_legacy_lexical_rebuild_conversation_id_from_offset(
    storage: &FrankenStorage,
    committed_offset: i64,
) -> Result<Option<i64>> {
    if committed_offset <= 0 {
        return Ok(None);
    }

    let rows = storage
        .list_conversations_for_lexical_rebuild_by_offset(
            1,
            committed_offset.saturating_sub(1),
            &HashMap::new(),
            &HashMap::new(),
        )
        .with_context(|| {
            format!(
                "resolving legacy lexical rebuild cursor at ordinal offset {}",
                committed_offset.saturating_sub(1)
            )
        })?;
    Ok(rows
        .into_iter()
        .next()
        .and_then(|conversation| conversation.id))
}

pub(in crate::indexer) fn upgrade_lexical_rebuild_state_resume_cursor_if_needed(
    storage: &FrankenStorage,
    index_path: &Path,
    state: &mut LexicalRebuildState,
) -> Result<()> {
    if state.completed {
        return Ok(());
    }

    let mut changed = false;
    if state.committed_offset > 0 && state.committed_conversation_id.is_none() {
        let Some(conversation_id) = resolve_legacy_lexical_rebuild_conversation_id_from_offset(
            storage,
            state.committed_offset,
        )?
        else {
            tracing::warn!(
                path = %index_path.display(),
                committed_offset = state.committed_offset,
                "discarding incomplete lexical rebuild checkpoint because the legacy ordinal cursor could not be upgraded to a stable conversation id"
            );
            *state = LexicalRebuildState::new(state.db.clone(), LEXICAL_REBUILD_PAGE_SIZE);
            persist_lexical_rebuild_state(index_path, state)?;
            return Ok(());
        };
        tracing::info!(
            path = %index_path.display(),
            committed_offset = state.committed_offset,
            committed_conversation_id = conversation_id,
            "upgraded lexical rebuild checkpoint from ordinal offset resume to stable conversation-id resume"
        );
        state.committed_conversation_id = Some(conversation_id);
        changed = true;
    }

    if let Some(pending) = state.pending.as_mut()
        && pending.next_offset > 0
        && pending.next_conversation_id.is_none()
    {
        let Some(conversation_id) = resolve_legacy_lexical_rebuild_conversation_id_from_offset(
            storage,
            pending.next_offset,
        )?
        else {
            tracing::warn!(
                path = %index_path.display(),
                next_offset = pending.next_offset,
                "dropping legacy pending lexical rebuild progress because the ordinal cursor could not be upgraded to a stable conversation id"
            );
            state.clear_pending();
            persist_lexical_rebuild_state(index_path, state)?;
            return Ok(());
        };
        tracing::info!(
            path = %index_path.display(),
            next_offset = pending.next_offset,
            next_conversation_id = conversation_id,
            "upgraded pending lexical rebuild checkpoint from ordinal offset resume to stable conversation-id resume"
        );
        pending.next_conversation_id = Some(conversation_id);
        changed = true;
    }

    if changed {
        persist_lexical_rebuild_state(index_path, state)?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn persist_pending_lexical_rebuild_progress_with_base_meta_fingerprint(
    index_path: &Path,
    state: &mut LexicalRebuildState,
    next_conversation_id: Option<i64>,
    processed_conversations: usize,
    indexed_docs: usize,
    runtime: &LexicalRebuildPipelineRuntimeSnapshot,
    base_meta_fingerprint: Option<String>,
    region_docs: usize,
) -> Result<()> {
    let checkpoint_runtime = runtime.checkpoint_snapshot();
    let current_processed_conversations = state.reported_processed_conversations();
    let current_indexed_docs = state.reported_indexed_docs();
    if processed_conversations < current_processed_conversations
        || indexed_docs < current_indexed_docs
    {
        tracing::warn!(
            requested_next_conversation_id = ?next_conversation_id,
            requested_processed_conversations = processed_conversations,
            current_processed_conversations,
            requested_indexed_docs = indexed_docs,
            current_indexed_docs,
            "ignoring stale lexical rebuild progress checkpoint update while preserving runtime telemetry"
        );
        if state.runtime != checkpoint_runtime {
            state.set_runtime(&checkpoint_runtime);
            persist_lexical_rebuild_state(index_path, state)?;
        }
        return Ok(());
    }
    let already_recorded = state.pending.as_ref().is_some_and(|pending| {
        pending.next_conversation_id == next_conversation_id
            && pending.processed_conversations == processed_conversations
            && pending.indexed_docs == indexed_docs
            && pending.base_meta_fingerprint == base_meta_fingerprint
            && pending.region_docs == region_docs
            && state.runtime == checkpoint_runtime
    });
    if already_recorded {
        return Ok(());
    }

    state.record_pending_commit(
        next_conversation_id,
        processed_conversations,
        indexed_docs,
        base_meta_fingerprint,
    );
    if let Some(pending) = state.pending.as_mut() {
        pending.region_docs = region_docs;
    }
    state.set_runtime(&checkpoint_runtime);
    persist_lexical_rebuild_state(index_path, state)
}

/// `index_path` locates the checkpoint; `content_path` is the directory the
/// next commit writes. After a crash, reconcile_pending_lexical_commit decides
/// whether the pending commit landed by comparing its base fingerprint with
/// that same content directory. A staged rebuild commits into its scratch
/// sibling, so a base taken from `index_path` (the untouched live generation)
/// always differs: every heartbeat's uncommitted counts were promoted, the
/// resume check found the candidate short, and each killed run quarantined
/// its candidate and restarted from zero (GH #523).
#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn persist_pending_lexical_rebuild_progress(
    index_path: &Path,
    content_path: &Path,
    state: &mut LexicalRebuildState,
    next_conversation_id: Option<i64>,
    processed_conversations: usize,
    indexed_docs: usize,
    runtime: &LexicalRebuildPipelineRuntimeSnapshot,
    region_docs: usize,
) -> Result<()> {
    persist_pending_lexical_rebuild_progress_with_base_meta_fingerprint(
        index_path,
        state,
        next_conversation_id,
        processed_conversations,
        indexed_docs,
        runtime,
        index_meta_fingerprint(content_path)?,
        region_docs,
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn maybe_persist_staged_lexical_rebuild_progress(
    index_path: &Path,
    state: &mut LexicalRebuildState,
    next_conversation_id: Option<i64>,
    processed_conversations: usize,
    indexed_docs: usize,
    runtime: &LexicalRebuildPipelineRuntimeSnapshot,
    base_meta_fingerprint_override: Option<&str>,
    force: bool,
    conversations_since_progress_persist: &mut usize,
    progress_heartbeat_interval_conversations: usize,
    last_progress_persist: &mut Instant,
    progress_heartbeat_interval: Duration,
    progress_bump: Option<&Arc<AtomicI64>>,
    mut perf_profile: Option<&mut LexicalRebuildPerfProfile>,
) -> Result<bool> {
    // Staged shard-build resume safety is restart-from-zero, so these persists
    // exist to keep cross-process status visibility fresh without rewriting the
    // checkpoint on every durable shard completion.
    if !force
        && !should_persist_lexical_rebuild_progress(
            *conversations_since_progress_persist,
            progress_heartbeat_interval_conversations,
            last_progress_persist.elapsed(),
            progress_heartbeat_interval,
        )
    {
        return Ok(false);
    }

    let heartbeat_progress_started = perf_profile.as_ref().map(|_| Instant::now());
    let base_meta_fingerprint = match base_meta_fingerprint_override {
        Some(base_meta_fingerprint) => Some(base_meta_fingerprint.to_string()),
        None => index_meta_fingerprint(index_path)?,
    };
    persist_pending_lexical_rebuild_progress_with_base_meta_fingerprint(
        index_path,
        state,
        next_conversation_id,
        processed_conversations,
        indexed_docs,
        runtime,
        base_meta_fingerprint,
        0,
    )?;
    bump_index_run_lock_progress_if_present(progress_bump);
    if let (Some(profile), Some(started)) = (perf_profile.as_mut(), heartbeat_progress_started) {
        profile.heartbeat_persist_count = profile.heartbeat_persist_count.saturating_add(1);
        profile.heartbeat_progress_duration += started.elapsed();
    }
    *conversations_since_progress_persist = 0;
    *last_progress_persist = Instant::now();
    Ok(true)
}

pub(in crate::indexer) fn lexical_rebuild_content_fingerprint_value(
    total_conversations: usize,
    max_conversation_id: i64,
    max_message_id: i64,
) -> String {
    format!("content-v1:{total_conversations}:{max_conversation_id}:{max_message_id}")
}

pub(in crate::indexer) fn lexical_rebuild_deferred_content_fingerprint(
    total_conversations: usize,
) -> String {
    format!("content-pending-v1:{total_conversations}")
}

pub(crate) fn completed_lexical_storage_fingerprint_matches_total(
    fingerprint: &str,
    expected_total_conversations: usize,
) -> bool {
    let mut parts = match fingerprint.strip_prefix("content-v1:") {
        Some(rest) => rest.split(':'),
        None => return false,
    };
    let parsed_total = parts.next().and_then(|part| part.parse::<usize>().ok());
    let max_conversation_id = parts.next().and_then(|part| part.parse::<i64>().ok());
    let max_message_id = parts.next().and_then(|part| part.parse::<i64>().ok());
    parsed_total == Some(expected_total_conversations)
        && max_conversation_id.is_some_and(|id| id >= 0)
        && max_message_id.is_some_and(|id| id >= 0)
        && parts.next().is_none()
}

pub(in crate::indexer) fn lexical_rebuild_content_fingerprint(
    storage: &FrankenStorage,
    total_conversations: usize,
) -> Result<String> {
    let prep_profile = std::env::var_os("CASS_PREP_PROFILE").is_some();
    let conversations_started = Instant::now();
    let max_conversation_id: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COALESCE(MAX(id), 0) FROM conversations",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .context("computing lexical rebuild conversation fingerprint")?;
    if prep_profile {
        eprintln!(
            "CASS_PREP_PROFILE step=fingerprint_conversations step_ms={}",
            conversations_started.elapsed().as_millis()
        );
    }
    let messages_started = Instant::now();
    let max_message_id: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COALESCE(MAX(id), 0) FROM messages",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .context("computing lexical rebuild message fingerprint")?;
    if prep_profile {
        eprintln!(
            "CASS_PREP_PROFILE step=fingerprint_messages step_ms={}",
            messages_started.elapsed().as_millis()
        );
    }
    Ok(lexical_rebuild_content_fingerprint_value(
        total_conversations,
        max_conversation_id,
        max_message_id,
    ))
}

pub(in crate::indexer) fn lexical_rebuild_storage_fingerprint(db_path: &Path) -> Result<String> {
    let mut storage = FrankenStorage::open_readonly(db_path).with_context(|| {
        format!(
            "opening readonly storage to compute lexical fingerprint for {}",
            db_path.display()
        )
    })?;
    let total_conversations = count_total_conversations_exact(&storage)?;
    let fingerprint = lexical_rebuild_content_fingerprint(&storage, total_conversations)?;
    storage.close_best_effort_in_place();
    Ok(fingerprint)
}

pub(in crate::indexer) fn count_total_conversations_exact(
    storage: &FrankenStorage,
) -> Result<usize> {
    let total_conversations: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM conversations",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .context("counting canonical conversations for lexical rebuild state")?;
    Ok(usize::try_from(total_conversations.max(0)).unwrap_or(usize::MAX))
}
