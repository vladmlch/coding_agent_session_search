use super::*;

pub(in crate::indexer) fn semantic_indexing_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

pub(in crate::indexer) fn system_time_to_epoch_millis(time: SystemTime) -> Option<i64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

pub(in crate::indexer) fn semantic_tier_for_embedder_id(
    embedder_id: &str,
) -> Option<SemanticTierKind> {
    match embedder_id {
        "minilm-384" | "multilingual-minilm-384" => Some(SemanticTierKind::Quality),
        "fnv1a-384" => Some(SemanticTierKind::Fast),
        _ => None,
    }
}

pub(in crate::indexer) fn semantic_identity_tier_for_requested_embedder(
    embedder: &str,
) -> SemanticIdentityTier {
    if matches!(embedder, "hash" | "fnv1a-384") {
        SemanticIdentityTier::Fast
    } else {
        SemanticIdentityTier::Quality
    }
}

pub(in crate::indexer) fn storage_semantic_identity_tier(
    tier: SemanticTierKind,
) -> SemanticIdentityTier {
    match tier {
        SemanticTierKind::Fast => SemanticIdentityTier::Fast,
        SemanticTierKind::Quality => SemanticIdentityTier::Quality,
    }
}

pub(in crate::indexer) fn semantic_model_revision_for_embedder_id(embedder_id: &str) -> String {
    if embedder_id == "fnv1a-384" {
        "hash".to_string()
    } else if crate::search::semantic_manifest::is_external_identity(embedder_id) {
        // Legacy external spaces keep their own identity as provenance.
        embedder_id.to_owned()
    } else {
        crate::search::fastembed_embedder::FastEmbedder::canonical_name(embedder_id)
            .and_then(crate::search::model_download::ModelManifest::for_embedder)
            .map(|manifest| manifest.revision)
            .unwrap_or_default()
    }
}

pub(in crate::indexer) fn semantic_vector_matches_current_contract(
    index: &FsVectorIndex,
    embedder_id: &str,
    dimension: usize,
) -> bool {
    index.embedder_id() == embedder_id
        && index.dimension() == dimension
        && crate::indexer::semantic::expected_vector_space_revision(embedder_id)
            == Some(index.embedder_revision())
}

/// A watermark proves row coverage only for the requested input contract.
/// Inspect metadata without initializing or downloading the native model.
pub(in crate::indexer) fn semantic_index_has_current_contract(
    data_dir: &Path,
    requested_embedder: &str,
) -> bool {
    let (embedder_id, dimension) = if requested_embedder == "hash" {
        ("fnv1a-384".to_string(), 384)
    } else {
        let Some(config) =
            crate::search::fastembed_embedder::FastEmbedder::config_for(requested_embedder)
        else {
            return false;
        };
        (config.embedder_id, config.dimension)
    };
    let Some(tier) = semantic_tier_for_embedder_id(&embedder_id) else {
        return false;
    };
    let Ok(Some(manifest)) = SemanticManifest::load(data_dir) else {
        return false;
    };
    let Some(artifact) = semantic_artifact_for_tier(&manifest, tier) else {
        return false;
    };
    if !artifact.ready
        || artifact.tier != tier
        || artifact.embedder_id != embedder_id
        || artifact.dimension != dimension
        || artifact.schema_version != SEMANTIC_SCHEMA_VERSION
        || artifact.chunking_version != CHUNKING_STRATEGY_VERSION
        || artifact.model_revision != semantic_model_revision_for_embedder_id(&embedder_id)
    {
        return false;
    }
    let Ok(index_path) = semantic_artifact_index_path(data_dir, artifact) else {
        return false;
    };
    if index_path != vector_index_path(data_dir, &embedder_id) {
        return false;
    }
    FsVectorIndex::open_read_only(&index_path).is_ok_and(|index| {
        semantic_vector_matches_current_contract(&index, &embedder_id, dimension)
    })
}

/// Honor explicit ANN maintenance even when no embedding work was needed.
/// The index-run lock is held by the caller. Only the published requested
/// artifact may be accelerated; constructing a model is unnecessary here.
pub(in crate::indexer) fn maintain_requested_hnsw(
    data_dir: &Path,
    requested_embedder: &str,
) -> Result<usize> {
    use crate::search::ann_index::{hnsw_index_path, maintain_hnsw_accelerator};
    use crate::search::semantic_manifest::HnswRecord;

    let Some(mut manifest) = SemanticManifest::load(data_dir)? else {
        tracing::info!(
            action = "skipped",
            reason = "no_vectors",
            "HNSW maintenance has no published semantic artifact"
        );
        return Ok(0);
    };
    anyhow::ensure!(
        semantic_index_has_current_contract(data_dir, requested_embedder),
        "cannot build HNSW: the requested semantic artifact is not published under the current input contract; run 'cass index --semantic --force-rebuild'"
    );
    let tier = if matches!(requested_embedder, "hash" | "fnv1a-384") {
        SemanticTierKind::Fast
    } else {
        SemanticTierKind::Quality
    };
    let artifact = semantic_artifact_for_tier(&manifest, tier)
        .context("requested semantic tier is absent after publication")?;
    let index_path = semantic_artifact_index_path(data_dir, artifact)?;
    let index = FsVectorIndex::open_read_only(&index_path)
        .context("opening the published semantic artifact for HNSW maintenance")?;
    let vector_count = index.record_count();
    anyhow::ensure!(
        u64::try_from(vector_count).ok() == Some(artifact.doc_count),
        "cannot build HNSW: published vector count does not match the selected semantic artifact"
    );
    let embedder_id = artifact.embedder_id.clone();
    let ann_path = hnsw_index_path(data_dir, &embedder_id);
    let relative_ann_path = ann_path
        .strip_prefix(data_dir)
        .context("HNSW path must remain inside the selected data directory")?
        .to_string_lossy()
        .into_owned();
    let maintenance = maintain_hnsw_accelerator(
        &ann_path,
        &index,
        frankensearch::index::HnswConfig::default(),
        false,
    )?;
    let already_recorded = manifest.hnsw.as_ref().is_some_and(|record| {
        record.ready
            && record.base_tier == tier
            && record.embedder_id == embedder_id
            && record.index_path == relative_ann_path
    });
    if maintenance.rebuilt || !already_recorded {
        manifest.publish_hnsw(HnswRecord {
            base_tier: tier,
            embedder_id,
            ef_search: frankensearch::index::HNSW_DEFAULT_EF_SEARCH,
            index_path: relative_ann_path,
            size_bytes: fs::metadata(&ann_path)?.len(),
            built_at_ms: semantic_indexing_now_ms(),
            ready: true,
        });
        manifest
            .save(data_dir)
            .context("recording the natively verified HNSW accelerator in the semantic manifest")?;
    }
    tracing::info!(
        action = maintenance.action(),
        reason = maintenance.reason,
        vector_count,
        manifest_published = maintenance.rebuilt || !already_recorded,
        "completed requested HNSW maintenance for the published semantic artifact"
    );
    Ok(vector_count)
}

/// Republish the semantic manifest after a direct `cass index --semantic`
/// pass so `cass status` reflects the freshly-built vector index.
///
/// The manifest-backed `cass models backfill` path already does this via
/// `manifest.publish_artifact(...)` inside `run_backfill_batch`. The
/// direct path at the call-site below previously skipped the manifest
/// update entirely, leaving status pointed at stale artifact metadata
/// even after a successful republish — see issue #203.
#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn publish_direct_semantic_artifact(
    storage: &FrankenStorage,
    data_dir: &Path,
    index_path: &Path,
    embedder_id: &str,
    embedder_dimension: usize,
    embedded_doc_count: u64,
    build_started_at_ms: i64,
) -> Result<()> {
    let Some(tier) = semantic_tier_for_embedder_id(embedder_id) else {
        tracing::debug!(
            embedder = embedder_id,
            "skipping direct semantic manifest publish: unknown embedder tier"
        );
        return Ok(());
    };

    // Compute conversation count and fingerprint from the SAME storage
    // handle so the manifest's `conversation_count` and the count
    // embedded in `db_fingerprint` (the `content-v1:N:M:K` string) can
    // never disagree by one. Also avoids re-opening the DB in
    // `lexical_storage_fingerprint_for_db`, which is a no-op cost on
    // SQLite but still pointless work.
    let total_conversations_raw = count_total_conversations_exact(storage)?;
    let db_fingerprint = lexical_rebuild_content_fingerprint(storage, total_conversations_raw)?;
    let total_conversations = u64::try_from(total_conversations_raw).unwrap_or(u64::MAX);
    let size_bytes = fs::metadata(index_path)
        .with_context(|| {
            format!(
                "stat published semantic index {} for direct manifest publish",
                index_path.display()
            )
        })?
        .len();
    let relative_index_path = index_path
        .strip_prefix(data_dir)
        .unwrap_or(index_path)
        .to_string_lossy()
        .into_owned();
    let model_revision = semantic_model_revision_for_embedder_id(embedder_id);

    let mut manifest = SemanticManifest::load_or_default(data_dir).map_err(|err| {
        anyhow::anyhow!("loading semantic manifest for direct artifact publish: {err}")
    })?;
    let now = semantic_indexing_now_ms();
    manifest.publish_artifact(ArtifactRecord {
        tier,
        embedder_id: embedder_id.to_string(),
        model_revision,
        schema_version: SEMANTIC_SCHEMA_VERSION,
        chunking_version: CHUNKING_STRATEGY_VERSION,
        dimension: embedder_dimension,
        doc_count: embedded_doc_count,
        conversation_count: total_conversations,
        db_fingerprint: db_fingerprint.clone(),
        index_path: relative_index_path,
        size_bytes,
        started_at_ms: build_started_at_ms,
        completed_at_ms: now,
        ready: true,
    });
    manifest.refresh_backlog(total_conversations, &db_fingerprint);
    manifest
        .save(data_dir)
        .map_err(|err| anyhow::anyhow!("saving semantic manifest after direct publish: {err}"))?;
    tracing::info!(
        embedder = embedder_id,
        tier = tier.as_str(),
        doc_count = embedded_doc_count,
        conversation_count = total_conversations,
        "published direct semantic artifact to manifest"
    );
    Ok(())
}

pub(in crate::indexer) fn count_total_messages_exact(storage: &FrankenStorage) -> Result<usize> {
    let total_messages: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM messages",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .context("counting canonical messages for lexical rebuild state")?;
    Ok(usize::try_from(total_messages.max(0)).unwrap_or(usize::MAX))
}

/// Count the tantivy docs a *healthy* live lexical index should hold for the
/// current canonical data: one per reachable (live-conversation) message that
/// the authoritative rebuild sink does NOT drop as hard-noise.
///
/// The lexical sink (`prebuilt_docs`) emits no doc for a hard-noise message —
/// empty content or a tool-ack (`is_hard_message_noise`) — so the true expected
/// doc count is `reachable_messages - hard_noise`, never the raw
/// `COUNT(*) FROM messages`. cass#317: comparing the observed doc count against
/// the raw message count made any corpus carrying tool-ack traffic look
/// permanently sparse, firing a full authoritative rebuild on every index/watch
/// run (the run then preserves `last_scan_ts`, so the same rebuild repeats).
///
/// Exactness comes from reusing the sink's own inputs rather than a SQL
/// approximation that could drift from Rust trim semantics:
///   - iterate live conversations only, via the rebuild's own message stream
///     (`for_each_lexical_rebuild_message`) — orphaned `messages` rows (no live
///     conversation) are never indexed by the sink, so they must not inflate the
///     expectation;
///   - that stream applies the same #290 per-message cap as the sink; there is
///     no per-conversation cap any more, so a long conversation's later
///     messages count as expected documents (bgn6s: the old cumulative cap
///     cleared them to empty, and this expectation then certified an index
///     missing them);
///   - classify with the same `is_hard_message_noise(lexical_rebuild_noise_role(
///     is_tool_role), content)` predicate and precise tool-role mapping the sink
///     uses.
///
/// Content is read one conversation at a time, so peak memory is bounded to a
/// single conversation regardless of corpus size. Only called on the
/// sparse-looking branch (observed already below the cheap raw upper bound), so
/// the extra scan is paid lazily.
///
/// This scans the live conversations with an id above `after_conversation_id`
/// in id order, adding to `expected_docs`, so an interrupted scan can resume
/// (GH #381). `after_conversation(id, docs)` runs once each conversation is
/// counted; an error from it stops the scan.
pub(in crate::indexer) fn expected_live_lexical_doc_count_after(
    storage: &FrankenStorage,
    after_conversation_id: i64,
    mut expected_docs: usize,
    mut after_conversation: impl FnMut(i64, usize) -> Result<()>,
) -> Result<usize> {
    let conversation_ids: Vec<i64> = storage
        .raw()
        .query_map_collect(
            "SELECT id FROM conversations WHERE id > ?1 ORDER BY id",
            &[ParamValue::from(after_conversation_id)],
            |row: &crate::franken_sync::Row| row.get_typed::<i64>(0),
        )
        .context("listing conversations for the noise-adjusted lexical doc expectation")?;
    for conversation_id in conversation_ids {
        // Stream: a long conversation is classified message by message, never
        // held whole (bgn6s removed the per-conversation cap that bounded it).
        storage.for_each_lexical_rebuild_message(conversation_id, None, |message| {
            let is_tool_role = matches!(message.role, crate::model::types::MessageRole::Tool);
            if !is_hard_message_noise(lexical_rebuild_noise_role(is_tool_role), &message.content) {
                expected_docs += 1;
            }
            Ok(true)
        })?;
        after_conversation(conversation_id, expected_docs)?;
    }
    Ok(expected_docs)
}

/// The whole-archive count in one uninterrupted pass: the reference the
/// memoized, delta and resumed counts must equal.
#[cfg(test)]
pub(in crate::indexer) fn expected_live_lexical_doc_count(
    storage: &FrankenStorage,
) -> Result<usize> {
    expected_live_lexical_doc_count_after(storage, 0, 0, |_, _| Ok(()))
}

/// GH #461: sidecar memoizing the noise-adjusted expected doc count for one
/// canonical content identity (see [`expected_lexical_docs_cache_path`]).
pub(in crate::indexer) const EXPECTED_LEXICAL_DOCS_CACHE_FILE: &str = ".expected-lexical-docs.json";

/// The memo describes the canonical database, not a lexical generation, so it
/// lives beside the generation directory rather than inside it. Every lexical
/// publish exchanges that directory: a memo kept inside was discarded by each
/// rebuild, and the next run re-read every message in the archive to recreate
/// it (GH #381: 900 s in `watch_startup:count_total_messages` on a 28 GB
/// archive).
pub(in crate::indexer) fn expected_lexical_docs_cache_path(index_path: &Path) -> Option<PathBuf> {
    index_path
        .parent()
        .map(|parent| parent.join(EXPECTED_LEXICAL_DOCS_CACHE_FILE))
}

/// 2: counts under the per-message cap (bgn6s). A v1 memo counted a long
/// conversation's messages past its first 8 MiB as noise and must be recounted.
pub(in crate::indexer) const EXPECTED_LEXICAL_DOCS_CACHE_SCHEMA_VERSION: u32 = 2;
/// Recount in full after this many consecutive append deltas. The identity
/// (like the lexical checkpoint fingerprint it extends) cannot see an in-place
/// content edit of an existing row; this bounds how long such an edit can skew
/// the expectation.
pub(in crate::indexer) const EXPECTED_LEXICAL_DOCS_MAX_DELTA_GENERATIONS: u32 = 32;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(in crate::indexer) struct ExpectedLexicalDocsIdentity {
    pub(in crate::indexer) total_conversations: usize,
    pub(in crate::indexer) max_conversation_id: i64,
    pub(in crate::indexer) max_message_id: i64,
    pub(in crate::indexer) total_messages: usize,
    pub(in crate::indexer) content_cap_bytes: usize,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(in crate::indexer) struct ExpectedLexicalDocsCache {
    pub(in crate::indexer) schema_version: u32,
    pub(in crate::indexer) db_path: String,
    pub(in crate::indexer) identity: ExpectedLexicalDocsIdentity,
    pub(in crate::indexer) expected_docs: usize,
    pub(in crate::indexer) delta_generations: u32,
}

pub(in crate::indexer) fn count_exact(
    storage: &FrankenStorage,
    sql: &str,
    bound: i64,
) -> Result<usize> {
    let count: i64 = storage
        .raw()
        .query_row_map(sql, &[ParamValue::from(bound)], |row| row.get_typed(0))
        .with_context(|| format!("counting rows for the expected lexical docs delta: {sql}"))?;
    Ok(usize::try_from(count.max(0)).unwrap_or(usize::MAX))
}

pub(in crate::indexer) fn expected_lexical_docs_identity(
    storage: &FrankenStorage,
    total_messages: usize,
) -> Result<ExpectedLexicalDocsIdentity> {
    let max_message_id: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COALESCE(MAX(id), 0) FROM messages",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .context("reading the max message id for the expected lexical docs identity")?;
    Ok(ExpectedLexicalDocsIdentity {
        total_conversations: count_total_conversations_exact(storage)?,
        max_conversation_id: max_conversation_id_exact(storage)?.unwrap_or(0),
        max_message_id,
        total_messages,
        // The same per-message cap the rebuild's message stream applies.
        content_cap_bytes: crate::storage::sqlite::lexical_max_message_content_bytes()
            .min(i32::MAX as usize),
    })
}

/// Extend a memoized expectation by the rows appended since it was taken,
/// or `None` when the archive changed in any way an append cannot explain.
///
/// Sound only for pure appends: no conversation or message was deleted (both
/// totals grew by exactly the rows beyond the memoized maxima) and the content
/// cap is unchanged. The cap applies to each message on its own, so appending
/// rows never changes an older message's truncation or noise classification,
/// and only the new messages need classifying, exactly as the full scan would.
pub(in crate::indexer) fn expected_live_lexical_doc_count_delta(
    storage: &FrankenStorage,
    cached: &ExpectedLexicalDocsCache,
    current: &ExpectedLexicalDocsIdentity,
) -> Result<Option<usize>> {
    if !expected_lexical_docs_identity_is_pure_append(storage, &cached.identity, current)? {
        return Ok(None);
    }
    let appended =
        expected_appended_lexical_docs(storage, cached.identity.max_message_id, |_| true)?;
    Ok(Some(cached.expected_docs.saturating_add(appended)))
}

/// Whether the archive changed from `old` to `current` only by appended rows:
/// no conversation or message was deleted (both totals grew by exactly the
/// rows beyond the old maxima) and the content cap is unchanged.
pub(in crate::indexer) fn expected_lexical_docs_identity_is_pure_append(
    storage: &FrankenStorage,
    old: &ExpectedLexicalDocsIdentity,
    current: &ExpectedLexicalDocsIdentity,
) -> Result<bool> {
    if old.content_cap_bytes != current.content_cap_bytes
        || current.max_message_id < old.max_message_id
        || current.max_conversation_id < old.max_conversation_id
        || current.total_conversations < old.total_conversations
        || current.total_messages < old.total_messages
    {
        return Ok(false);
    }
    let new_conversations = count_exact(
        storage,
        "SELECT COUNT(*) FROM conversations WHERE id > ?1",
        old.max_conversation_id,
    )?;
    let new_messages = count_exact(
        storage,
        "SELECT COUNT(*) FROM messages WHERE id > ?1",
        old.max_message_id,
    )?;
    Ok(
        old.total_conversations.checked_add(new_conversations) == Some(current.total_conversations)
            && old.total_messages.checked_add(new_messages) == Some(current.total_messages),
    )
}

/// Expected docs among the messages with an id above `after_message_id` whose
/// live conversation satisfies `include`.
pub(in crate::indexer) fn expected_appended_lexical_docs(
    storage: &FrankenStorage,
    after_message_id: i64,
    include: impl Fn(i64) -> bool,
) -> Result<usize> {
    // The per-message cap makes every message's classification independent of
    // every other, so only the rows appended since the memo need classifying:
    // a rowid-range scan over them, never a JOIN (wide join paths previously
    // materialized every message row, content included: 20+ minutes on an
    // 8M-message archive) and never the older text of a touched conversation.
    let touched: Vec<i64> = storage
        .raw()
        .query_map_collect(
            "SELECT DISTINCT conversation_id FROM messages WHERE id > ?1",
            &[ParamValue::from(after_message_id)],
            |row: &crate::franken_sync::Row| row.get_typed::<i64>(0),
        )
        .context("listing conversations touched since the expected lexical docs memo")?;
    // Live conversations only, like the full scan (orphaned rows are never
    // indexed): one point lookup per touched conversation, resolved before the
    // row stream so no query runs inside its callback.
    let mut live_conversations = HashSet::with_capacity(touched.len());
    for conversation_id in touched.into_iter().filter(|id| include(*id)) {
        let live: i64 = storage
            .raw()
            .query_row_map(
                "SELECT EXISTS(SELECT 1 FROM conversations WHERE id = ?1)",
                &[ParamValue::from(conversation_id)],
                |row: &crate::franken_sync::Row| row.get_typed(0),
            )
            .context("checking a touched conversation for the expected lexical docs memo")?;
        if live != 0 {
            live_conversations.insert(conversation_id);
        }
    }
    let mut expected_docs = 0usize;
    storage.for_each_lexical_rebuild_message_after_id(
        after_message_id,
        |conversation_id, message| {
            let is_tool_role = matches!(message.role, crate::model::types::MessageRole::Tool);
            if live_conversations.contains(&conversation_id)
                && !is_hard_message_noise(
                    lexical_rebuild_noise_role(is_tool_role),
                    &message.content,
                )
            {
                expected_docs = expected_docs.saturating_add(1);
            }
            Ok(())
        },
    )?;
    Ok(expected_docs)
}

/// GH #381 (ntxs1): where an interrupted full expected-docs scan stopped. It
/// sits beside the memo; a completed scan removes it.
pub(in crate::indexer) const EXPECTED_LEXICAL_DOCS_PARTIAL_FILE: &str =
    ".expected-lexical-docs.partial.json";
/// How often a full scan saves its position: an interruption loses at most
/// this much reading.
pub(in crate::indexer) const EXPECTED_LEXICAL_DOCS_PARTIAL_SAVE_INTERVAL: Duration =
    Duration::from_secs(2);

#[derive(serde::Serialize, serde::Deserialize)]
pub(in crate::indexer) struct ExpectedLexicalDocsPartial {
    pub(in crate::indexer) schema_version: u32,
    pub(in crate::indexer) db_path: String,
    /// The archive the cursor and count describe.
    pub(in crate::indexer) identity: ExpectedLexicalDocsIdentity,
    /// Every live conversation with an id at or below this one is counted.
    pub(in crate::indexer) last_conversation_id: i64,
    pub(in crate::indexer) expected_docs: usize,
}

/// The saved position of a running or interrupted full expected-docs scan,
/// read without opening the database, so `cass status --json` shows a long
/// first scan moving across runs (GH #381, ntxs1).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ExpectedLexicalDocsScanPosition {
    /// Every live conversation with an id at or below this one is counted.
    pub(crate) last_conversation_id: i64,
    /// The highest conversation id when the position was saved.
    pub(crate) max_conversation_id: i64,
    pub(crate) expected_docs_so_far: usize,
    /// When the position was saved (the file's mtime).
    pub(crate) saved_at_ms: Option<i64>,
}

/// The position file beside the lexical index at `index_path`, if it belongs
/// to `db_path` and the current memo schema. The next scan resumes from it
/// when the archive is unchanged or only grew, and starts over otherwise.
pub(crate) fn expected_lexical_docs_scan_position(
    index_path: &Path,
    db_path: &Path,
) -> Option<ExpectedLexicalDocsScanPosition> {
    let path = expected_lexical_docs_cache_path(index_path)?
        .with_file_name(EXPECTED_LEXICAL_DOCS_PARTIAL_FILE);
    let partial: ExpectedLexicalDocsPartial =
        serde_json::from_slice(&fs::read(&path).ok()?).ok()?;
    if partial.schema_version != EXPECTED_LEXICAL_DOCS_CACHE_SCHEMA_VERSION
        || !lexical_rebuild_db_paths_match(&partial.db_path, &db_path.to_string_lossy())
    {
        return None;
    }
    let saved_at_ms = fs::metadata(&path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok());
    Some(ExpectedLexicalDocsScanPosition {
        last_conversation_id: partial.last_conversation_id,
        max_conversation_id: partial.identity.max_conversation_id,
        expected_docs_so_far: partial.expected_docs,
        saved_at_ms,
    })
}

pub(in crate::indexer) fn write_expected_lexical_docs_sidecar<T: serde::Serialize>(
    path: &Path,
    payload: &T,
) {
    let Some(file_name) = path.file_name().map(|name| name.to_string_lossy()) else {
        return;
    };
    let tmp_path = path.with_file_name(format!("{file_name}.{}.tmp", std::process::id()));
    let written = serde_json::to_vec(payload)
        .map_err(std::io::Error::other)
        .and_then(|bytes| fs::write(&tmp_path, bytes))
        .and_then(|()| fs::rename(&tmp_path, path));
    if let Err(err) = written {
        tracing::debug!(error = %err, path = %path.display(), "expected lexical docs sidecar write skipped");
        let _ = fs::remove_file(&tmp_path);
    }
}

/// The full expected-docs count, resumable (GH #381, ntxs1). The first full
/// scan reads every message; on a 28 GB archive it outlived the caller's
/// 900 s timeout, and an interrupted scan kept nothing, so the next run
/// started over and never finished. The scan now saves its cursor and count
/// at `partial_path` every few seconds. A later run continues from there:
/// as-is when the archive is unchanged, or after counting the rows appended to
/// already-counted conversations when the archive only grew. Any other change
/// (a deletion, a cap change) starts over.
pub(in crate::indexer) fn expected_live_lexical_doc_count_resumable(
    storage: &FrankenStorage,
    partial_path: Option<&Path>,
    db_path: &str,
    current: &ExpectedLexicalDocsIdentity,
    progress: Option<&IndexingProgress>,
) -> Result<usize> {
    expected_live_lexical_doc_count_resumable_with(
        storage,
        partial_path,
        db_path,
        current,
        progress,
        EXPECTED_LEXICAL_DOCS_PARTIAL_SAVE_INTERVAL,
        |_| Ok(()),
    )
}

pub(in crate::indexer) fn expected_live_lexical_doc_count_resumable_with(
    storage: &FrankenStorage,
    partial_path: Option<&Path>,
    db_path: &str,
    current: &ExpectedLexicalDocsIdentity,
    progress: Option<&IndexingProgress>,
    save_interval: Duration,
    mut on_conversation: impl FnMut(i64) -> Result<()>,
) -> Result<usize> {
    if let Some(progress) = progress {
        progress.check_stop()?;
    }
    let saved = partial_path
        .and_then(|path| fs::read(path).ok())
        .and_then(|raw| serde_json::from_slice::<ExpectedLexicalDocsPartial>(&raw).ok())
        .filter(|partial| {
            partial.schema_version == EXPECTED_LEXICAL_DOCS_CACHE_SCHEMA_VERSION
                && lexical_rebuild_db_paths_match(&partial.db_path, db_path)
        });
    let (resume_after, resume_docs) = match saved {
        Some(partial) if partial.identity == *current => {
            (partial.last_conversation_id, partial.expected_docs)
        }
        Some(partial)
            if expected_lexical_docs_identity_is_pure_append(
                storage,
                &partial.identity,
                current,
            )? =>
        {
            // Conversations above the cursor are read whole below, appended
            // rows included; rows appended to the ones already counted are not
            // in the saved count yet.
            let cursor = partial.last_conversation_id;
            let appended = expected_appended_lexical_docs(
                storage,
                partial.identity.max_message_id,
                |conversation_id| conversation_id <= cursor,
            )?;
            (cursor, partial.expected_docs.saturating_add(appended))
        }
        _ => (0, 0),
    };
    if resume_after > 0 {
        tracing::info!(
            resume_after_conversation_id = resume_after,
            expected_docs_so_far = resume_docs,
            "resuming the interrupted expected lexical docs scan (GH #381)"
        );
    }
    let mut last_save = Instant::now();
    let expected_docs = expected_live_lexical_doc_count_after(
        storage,
        resume_after,
        resume_docs,
        |conversation_id, expected_docs| {
            if let Some(progress) = progress {
                // The stall watchdog reads activity, not phase counters.
                progress.tick_activity();
            }
            let observed = on_conversation(conversation_id);
            let stop = progress.map_or(Ok(()), IndexingProgress::check_stop);
            if let Some(path) = partial_path
                && (stop.is_err() || last_save.elapsed() >= save_interval)
            {
                write_expected_lexical_docs_sidecar(
                    path,
                    &ExpectedLexicalDocsPartial {
                        schema_version: EXPECTED_LEXICAL_DOCS_CACHE_SCHEMA_VERSION,
                        db_path: db_path.to_string(),
                        identity: current.clone(),
                        last_conversation_id: conversation_id,
                        expected_docs,
                    },
                );
                tracing::info!(
                    last_conversation_id = conversation_id,
                    max_conversation_id = current.max_conversation_id,
                    expected_docs_so_far = expected_docs,
                    "expected lexical docs scan position saved (GH #381)"
                );
                last_save = Instant::now();
            }
            observed?;
            stop
        },
    )?;
    if let Some(path) = partial_path {
        // The memo the caller writes next supersedes the partial count.
        let _ = fs::remove_file(path);
    }
    Ok(expected_docs)
}

/// The expected doc count ([`expected_live_lexical_doc_count_after`]) without
/// re-reading the whole archive on every run (GH #461). The full per-conversation content scan ran at index
/// startup and again at the post-run checkpoint refresh on any archive whose
/// sink drops tool-acks or empty messages, i.e. on essentially every
/// incremental run; a 16 MB incremental run read 9.4 GB. The count is served
/// from the sidecar when the canonical identity is unchanged, extended by an
/// append-only delta when possible, and otherwise recomputed in full.
/// The sidecar is best effort: a missing, foreign or unreadable one only
/// costs the full scan it replaces.
pub(in crate::indexer) fn expected_live_lexical_doc_count_cached(
    storage: &FrankenStorage,
    index_path: &Path,
    db_path: &str,
    total_messages: usize,
    progress: Option<&IndexingProgress>,
) -> Result<usize> {
    expected_live_lexical_doc_count_cached_with_scan_policy(
        storage,
        index_path,
        db_path,
        total_messages,
        progress,
        true,
    )?
    .context("full expected lexical document verification did not return a count")
}

pub(in crate::indexer) fn expected_live_lexical_doc_count_cached_with_scan_policy(
    storage: &FrankenStorage,
    index_path: &Path,
    db_path: &str,
    total_messages: usize,
    progress: Option<&IndexingProgress>,
    allow_full_scan: bool,
) -> Result<Option<usize>> {
    let current = expected_lexical_docs_identity(storage, total_messages)?;
    let cache_path = expected_lexical_docs_cache_path(index_path)
        .filter(|path| path.parent().is_some_and(Path::is_dir));
    let partial_path = cache_path
        .as_deref()
        .map(|path| path.with_file_name(EXPECTED_LEXICAL_DOCS_PARTIAL_FILE));
    let cached = cache_path
        .as_deref()
        .and_then(|path| fs::read(path).ok())
        .and_then(|raw| serde_json::from_slice::<ExpectedLexicalDocsCache>(&raw).ok())
        .filter(|cached| {
            cached.schema_version == EXPECTED_LEXICAL_DOCS_CACHE_SCHEMA_VERSION
                && lexical_rebuild_db_paths_match(&cached.db_path, db_path)
        });
    let full_scan = || -> Result<Option<usize>> {
        if !allow_full_scan {
            return Ok(None);
        }
        expected_live_lexical_doc_count_resumable(
            storage,
            partial_path.as_deref(),
            db_path,
            &current,
            progress,
        )
        .map(Some)
    };
    let (expected_docs, delta_generations, source) = match cached {
        Some(cached) if cached.identity == current => {
            return Ok(Some(cached.expected_docs));
        }
        Some(cached) if cached.delta_generations < EXPECTED_LEXICAL_DOCS_MAX_DELTA_GENERATIONS => {
            match expected_live_lexical_doc_count_delta(storage, &cached, &current)? {
                Some(expected) => (expected, cached.delta_generations + 1, "delta"),
                None => match full_scan()? {
                    Some(expected) => (expected, 0, "full"),
                    None => return Ok(None),
                },
            }
        }
        _ => match full_scan()? {
            Some(expected) => (expected, 0, "full"),
            None => return Ok(None),
        },
    };
    tracing::debug!(
        expected_lexical_docs = expected_docs,
        source,
        delta_generations,
        "expected lexical doc count recomputed"
    );
    if let Some(cache_path) = cache_path {
        write_expected_lexical_docs_sidecar(
            &cache_path,
            &ExpectedLexicalDocsCache {
                schema_version: EXPECTED_LEXICAL_DOCS_CACHE_SCHEMA_VERSION,
                db_path: db_path.to_string(),
                identity: current,
                expected_docs,
                delta_generations,
            },
        );
    }
    Ok(Some(expected_docs))
}

pub(in crate::indexer) fn max_conversation_id_exact(
    storage: &FrankenStorage,
) -> Result<Option<i64>> {
    let max_conversation_id: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COALESCE(MAX(id), 0) FROM conversations",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .context("computing lexical rebuild max conversation id")?;
    Ok((max_conversation_id > 0).then_some(max_conversation_id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) struct IncrementalCanonicalLexicalRepairPlan {
    pub(in crate::indexer) canonical_messages: usize,
    pub(in crate::indexer) observed_tantivy_docs: Option<usize>,
    pub(in crate::indexer) reason: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) struct IncrementalCanonicalLexicalRepairContext {
    pub(in crate::indexer) full_refresh: bool,
    pub(in crate::indexer) force_rebuild: bool,
    pub(in crate::indexer) resume_lexical_rebuild: bool,
    pub(in crate::indexer) targeted_watch_once_only: bool,
    pub(in crate::indexer) salvage_messages_imported: usize,
    pub(in crate::indexer) canonical_messages: usize,
    /// Noise-adjusted expected live tantivy doc count: the number of docs a
    /// healthy live index holds for this canonical data. The lexical rebuild
    /// sink emits NO doc for a hard-noise message (empty content / tool-ack —
    /// `is_hard_message_noise`), so a healthy index has fewer docs than
    /// `COUNT(*) FROM messages`. Comparing the observed doc count against the raw
    /// `canonical_messages` made any corpus with tool-ack traffic look
    /// permanently sparse and triggered a full authoritative rebuild on every
    /// index/watch run (cass#317). This is the correct sparseness threshold; it
    /// equals `canonical_messages` only when no reachable message is hard-noise
    /// (and when it was not separately computed because the raw count already
    /// looked covered). Always `<= canonical_messages`.
    pub(in crate::indexer) expected_lexical_docs: usize,
    pub(in crate::indexer) tantivy_requires_rebuild: bool,
    pub(in crate::indexer) observed_tantivy_docs: Option<usize>,
    /// Task 5: upper bound on live search-region documents — the
    /// non-canonical share of `observed_tantivy_docs`. The sparse gate
    /// subtracts it so converged `meta:*`/derived region documents can
    /// never mask a genuinely sparse canonical index (the same split the
    /// checkpoint refresh applies at `live_region_doc_bound`).
    pub(in crate::indexer) region_doc_bound: usize,
    /// True when a completed lexical-rebuild checkpoint records this exact
    /// canonical data (matching storage fingerprint). This is diagnostic
    /// context only: SQLite remains authoritative, and a fresh sparse live-doc
    /// observation must still repair the derived lexical index.
    pub(in crate::indexer) published_index_validated_for_current_data: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LexicalRebuildOutcome {
    /// Exact number of canonical conversations observed while streaming the
    /// authoritative database. This can differ from the startup hint used by
    /// count-free restart paths when the interrupted checkpoint is stale.
    pub observed_conversations: usize,
    pub indexed_docs: usize,
    /// Task 5: the share of `indexed_docs` that is persisted search-region
    /// documents. `indexed_docs - indexed_region_docs` is the canonical
    /// message-document share; `observed_messages` already reports the
    /// canonical count.
    pub indexed_region_docs: usize,
    pub observed_messages: Option<usize>,
    pub exact_checkpoint_persisted: bool,
    /// Equivalence proof emitted by the authoritative streaming rebuild (ibuuh.29).
    /// `None` for short-circuit paths that did not re-ingest packets (e.g., a
    /// checkpoint that was already complete when the rebuild entered).
    pub equivalence: Option<LexicalRebuildEquivalenceEvidence>,
}
