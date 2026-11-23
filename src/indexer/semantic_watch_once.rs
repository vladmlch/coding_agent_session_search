use super::*;

pub(in crate::indexer) fn semantic_role_name(role: u8) -> Option<&'static str> {
    match role {
        ROLE_USER => Some("user"),
        ROLE_ASSISTANT => Some("assistant"),
        ROLE_SYSTEM => Some("system"),
        ROLE_TOOL => Some("tool"),
        _ => None,
    }
}

#[derive(Debug, Default)]
pub(in crate::indexer) struct WatchSemanticDelta {
    pub(in crate::indexer) inputs: Vec<EmbeddingInput>,
    pub(in crate::indexer) max_message_id: Option<i64>,
}

impl WatchSemanticDelta {
    pub(in crate::indexer) fn extend_from_batch(
        &mut self,
        batch_inputs: Vec<EmbeddingInput>,
        max_message_id: Option<i64>,
    ) {
        self.inputs.extend(batch_inputs);
        if let Some(max_message_id) = max_message_id {
            self.max_message_id = Some(
                self.max_message_id
                    .map_or(max_message_id, |current| current.max(max_message_id)),
            );
        }
    }
}

/// Keep semantic work on the existing watch retry queue until a canonical
/// reconciliation has published it. Callback-local deltas cannot prove coverage
/// after cooldown skips, same-ID edits, or deletions.
pub(in crate::indexer) struct WatchSemanticMaintenance {
    pub(in crate::indexer) pending_paths: BTreeSet<PathBuf>,
    pub(in crate::indexer) pending_conversations: usize,
    pub(in crate::indexer) pre_watch_conversations: usize,
    pub(in crate::indexer) last_attempt: Instant,
}

impl WatchSemanticMaintenance {
    pub(in crate::indexer) fn new(pre_watch_conversations: usize) -> Self {
        Self {
            pending_paths: BTreeSet::new(),
            pending_conversations: 0,
            pre_watch_conversations,
            last_attempt: Instant::now(),
        }
    }

    pub(in crate::indexer) fn record_ingest(
        &mut self,
        indexed: usize,
        paths: impl IntoIterator<Item = PathBuf>,
    ) {
        if indexed > 0 {
            self.pending_conversations = self.pending_conversations.saturating_add(indexed);
            self.pending_paths.extend(paths);
        }
    }

    pub(in crate::indexer) fn reconcile_if_due(
        &mut self,
        embedder: &str,
        data_dir: &Path,
        storage: &Mutex<FrankenStorage>,
        cooldown: Duration,
        progress: Option<&Arc<IndexingProgress>>,
    ) -> Result<Option<SemanticWatchOnceStats>> {
        if self.pending_conversations == 0 || self.last_attempt.elapsed() < cooldown {
            return Ok(None);
        }
        prepare_progress_for_semantic_build(progress);
        let result = (|| {
            if self.pre_watch_conversations > 0
                && !semantic_index_has_current_contract(data_dir, embedder)
            {
                anyhow::bail!(
                    "watch semantic base is missing or incompatible; rebuild with cass index --semantic before retrying"
                );
            }
            let stats = run_targeted_semantic_watch_once_publish(
                embedder,
                data_dir,
                storage,
                self.pending_conversations,
                self.pre_watch_conversations,
            )?;
            let current_conversations = {
                let guard = storage.lock().map_err(|error| {
                    anyhow::anyhow!("lock storage after watch semantic publication: {error}")
                })?;
                count_total_conversations_exact(&guard)?
            };
            self.pre_watch_conversations = current_conversations;
            self.pending_conversations = 0;
            self.pending_paths.clear();
            Ok(Some(stats))
        })();
        // Count cooldown from completion, on success or failure. A skipped
        // callback must not move this clock and starve pending work forever.
        self.last_attempt = Instant::now();
        result
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) struct SemanticContentFingerprint {
    pub(in crate::indexer) total_conversations: usize,
    pub(in crate::indexer) max_conversation_id: i64,
    pub(in crate::indexer) max_message_id: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) enum TargetedSemanticWatchOnceMode {
    RebuildAll,
    AppendToExisting,
    ReconcileExisting,
    AlreadyCovered,
}

#[derive(Debug)]
pub(in crate::indexer) struct TargetedSemanticWatchOnceSelection {
    pub(in crate::indexer) mode: TargetedSemanticWatchOnceMode,
    pub(in crate::indexer) inputs: Vec<EmbeddingInput>,
    pub(in crate::indexer) raw_max_message_id: Option<i64>,
    pub(in crate::indexer) tier: SemanticTierKind,
    pub(in crate::indexer) index_path: PathBuf,
    pub(in crate::indexer) total_conversations: u64,
    pub(in crate::indexer) current_db_fingerprint: String,
    pub(in crate::indexer) manifest_before_db_fingerprint: Option<String>,
    pub(in crate::indexer) current_doc_ids: Option<HashSet<String>>,
    pub(in crate::indexer) reason: &'static str,
}

#[derive(Debug)]
pub(in crate::indexer) struct CanonicalSemanticDocuments {
    pub(in crate::indexer) documents: Vec<(String, EmbeddingInput)>,
    pub(in crate::indexer) doc_ids: HashSet<String>,
}

impl CanonicalSemanticDocuments {
    pub(in crate::indexer) fn from_storage(storage: &FrankenStorage) -> Result<Self> {
        Self::from_storage_missing_from(storage, None)
    }

    pub(in crate::indexer) fn from_storage_missing_from(
        storage: &FrankenStorage,
        existing_doc_ids: Option<&HashSet<String>>,
    ) -> Result<Self> {
        let mut documents = Vec::new();
        let mut doc_ids = HashSet::new();
        visit_packet_embedding_inputs_from_storage(storage, |input| {
            if is_hard_message_noise(semantic_role_name(input.role), &input.content) {
                return Ok(());
            }
            let Some(doc_id) = semantic_doc_id_for_input(&input) else {
                return Ok(());
            };
            if !doc_ids.insert(doc_id.clone()) {
                anyhow::bail!(
                    "canonical DB produced duplicate semantic document identity {doc_id}"
                );
            }
            if existing_doc_ids.is_none_or(|existing| !existing.contains(&doc_id)) {
                documents.push((doc_id, input));
            }
            Ok(())
        })?;
        Ok(Self { documents, doc_ids })
    }

    pub(in crate::indexer) fn into_inputs(self) -> Vec<EmbeddingInput> {
        self.documents.into_iter().map(|(_, input)| input).collect()
    }
}

#[derive(Debug)]
pub(in crate::indexer) struct SemanticArtifactInventory {
    pub(in crate::indexer) reusable_main_doc_ids: HashSet<String>,
    pub(in crate::indexer) main_record_count: usize,
    pub(in crate::indexer) tombstone_count: usize,
    pub(in crate::indexer) wal_record_count: usize,
    pub(in crate::indexer) duplicate_live_doc_ids: HashSet<String>,
}

impl SemanticArtifactInventory {
    pub(in crate::indexer) fn live_record_count(&self) -> usize {
        self.main_record_count
            .saturating_sub(self.tombstone_count)
            .saturating_add(self.wal_record_count)
    }

    pub(in crate::indexer) fn is_compacted_unique_main(&self) -> bool {
        self.tombstone_count == 0
            && self.wal_record_count == 0
            && self.duplicate_live_doc_ids.is_empty()
            && self.reusable_main_doc_ids.len() == self.main_record_count
    }
}

pub(in crate::indexer) fn should_run_targeted_semantic_watch_once(opts: &IndexOptions) -> bool {
    opts.semantic
        && !opts.watch
        && !opts.full
        && !opts.force_rebuild
        && opts
            .watch_once_paths
            .as_ref()
            .is_some_and(|paths| !paths.is_empty())
        // Explicit graph construction previously seeded missing vectors via
        // the broad build. Retain that path when targeted reconciliation has
        // no compatible published base to extend.
        && (!opts.build_hnsw
            || semantic_index_has_current_contract(&opts.data_dir, &opts.embedder))
}

pub(in crate::indexer) const fn should_optimize_tantivy_after_watch_once_ingest(
    indexed_conversations: usize,
    targeted_semantic_watch_once: bool,
) -> bool {
    indexed_conversations > 0 && !targeted_semantic_watch_once
}

pub(in crate::indexer) fn prepare_progress_for_semantic_build(
    progress: Option<&Arc<IndexingProgress>>,
) {
    reset_progress_to_idle(progress);
}

pub(in crate::indexer) fn set_semantic_progress_phase(
    progress: Option<&Arc<IndexingProgress>>,
    progress_bump: &Arc<AtomicI64>,
    phase: usize,
    current: usize,
    total: usize,
) {
    if let Some(progress) = progress {
        progress.set_phase_progress(phase, current, total);
    }
    bump_index_run_lock_progress_atomic(progress_bump);
}

pub(in crate::indexer) fn update_semantic_progress(
    progress: Option<&Arc<IndexingProgress>>,
    progress_bump: &Arc<AtomicI64>,
    current: usize,
    total: usize,
) {
    if let Some(progress) = progress {
        progress.update_phase_progress(current, total);
    }
    bump_index_run_lock_progress_atomic(progress_bump);
}

pub(in crate::indexer) fn parse_semantic_content_fingerprint(
    raw: &str,
) -> Option<SemanticContentFingerprint> {
    let mut parts = raw.strip_prefix("content-v1:")?.split(':');
    let total_conversations = parts.next()?.parse::<usize>().ok()?;
    let max_conversation_id = parts.next()?.parse::<i64>().ok()?;
    let max_message_id = parts.next()?.parse::<i64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(SemanticContentFingerprint {
        total_conversations,
        max_conversation_id,
        max_message_id,
    })
}

pub(in crate::indexer) fn semantic_artifact_for_tier(
    manifest: &SemanticManifest,
    tier: SemanticTierKind,
) -> Option<&ArtifactRecord> {
    match tier {
        SemanticTierKind::Fast => manifest.fast_tier.as_ref(),
        SemanticTierKind::Quality => manifest.quality_tier.as_ref(),
    }
}

pub(in crate::indexer) fn semantic_artifact_index_path(
    data_dir: &Path,
    artifact: &ArtifactRecord,
) -> Result<PathBuf> {
    let path = PathBuf::from(&artifact.index_path);
    if path.is_absolute() {
        return Ok(path);
    }

    let mut resolved = data_dir.to_path_buf();
    for component in path.components() {
        let std::path::Component::Normal(part) = component else {
            anyhow::bail!(
                "semantic watch-once cannot use unsafe relative vector artifact path {}",
                artifact.index_path
            );
        };
        resolved.push(part);
    }
    Ok(resolved)
}

pub(in crate::indexer) fn validate_semantic_watch_once_artifact(
    data_dir: &Path,
    artifact: &ArtifactRecord,
    indexer: &SemanticIndexer,
    tier: SemanticTierKind,
) -> Result<PathBuf> {
    if !artifact.ready {
        anyhow::bail!("semantic watch-once cannot reuse artifact that is not ready");
    }
    let artifact_matches_indexer = artifact.tier.eq(&tier)
        && artifact.embedder_id.as_str().eq(indexer.embedder_id())
        && artifact.dimension.eq(&indexer.embedder_dimension())
        && artifact.schema_version.eq(&SEMANTIC_SCHEMA_VERSION)
        && artifact.chunking_version.eq(&CHUNKING_STRATEGY_VERSION);
    if !artifact_matches_indexer {
        anyhow::bail!(
            "semantic watch-once cannot prove coverage from incompatible semantic artifact"
        );
    }

    let index_path = semantic_artifact_index_path(data_dir, artifact)?;
    let canonical_index_path = vector_index_path(data_dir, indexer.embedder_id());
    if !index_path.eq(&canonical_index_path) {
        anyhow::bail!(
            "semantic watch-once cannot append to non-canonical vector path {}; expected {}",
            index_path.display(),
            canonical_index_path.display()
        );
    }
    let index = FsVectorIndex::open(&index_path).map_err(|err| {
        anyhow::anyhow!(
            "semantic watch-once cannot open existing vector artifact {}: {err}",
            index_path.display()
        )
    })?;
    if !semantic_vector_matches_current_contract(
        &index,
        indexer.embedder_id(),
        indexer.embedder_dimension(),
    ) {
        anyhow::bail!("semantic watch-once cannot reuse an incompatible vector input contract");
    }
    drop(index);
    Ok(index_path)
}

pub(in crate::indexer) fn inspect_semantic_artifact(
    index_path: &Path,
) -> Result<SemanticArtifactInventory> {
    let index = FsVectorIndex::open(index_path).map_err(|err| {
        anyhow::anyhow!(
            "semantic watch-once cannot inventory vector artifact {}: {err}",
            index_path.display()
        )
    })?;
    let main_record_count = index.record_count();
    let wal_record_count = index.wal_record_count();
    let mut tombstone_count = 0usize;
    let mut duplicate_live_doc_ids = HashSet::new();
    let mut reusable_main_doc_ids = HashSet::with_capacity(main_record_count);
    for record_index in 0..main_record_count {
        if index.is_deleted(record_index) {
            tombstone_count = tombstone_count.saturating_add(1);
            continue;
        }
        let doc_id = index.doc_id_at(record_index).map_err(|err| {
            anyhow::anyhow!(
                "semantic watch-once cannot read document {record_index} from {}: {err}",
                index_path.display()
            )
        })?;
        if duplicate_live_doc_ids.contains(doc_id) {
            continue;
        }
        if !reusable_main_doc_ids.insert(doc_id.to_owned()) {
            reusable_main_doc_ids.remove(doc_id);
            duplicate_live_doc_ids.insert(doc_id.to_owned());
        }
    }
    Ok(SemanticArtifactInventory {
        reusable_main_doc_ids,
        main_record_count,
        tombstone_count,
        wal_record_count,
        duplicate_live_doc_ids,
    })
}

pub(in crate::indexer) fn semantic_artifact_is_append_only_prefix(
    storage: &FrankenStorage,
    artifact_fingerprint: SemanticContentFingerprint,
    current_fingerprint: SemanticContentFingerprint,
) -> Result<bool> {
    if artifact_fingerprint.total_conversations > current_fingerprint.total_conversations
        || artifact_fingerprint.max_conversation_id > current_fingerprint.max_conversation_id
        || artifact_fingerprint.max_message_id > current_fingerprint.max_message_id
    {
        return Ok(false);
    }

    let prefix_conversations: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*)
             FROM conversations
             WHERE id <= ?1",
            &[ParamValue::from(artifact_fingerprint.max_conversation_id)],
            |row| row.get_typed(0),
        )
        .context("checking semantic watch-once prefix conversation count")?;
    let observed_prefix_conversations =
        usize::try_from(prefix_conversations.max(0)).unwrap_or(usize::MAX);
    Ok(observed_prefix_conversations.eq(&artifact_fingerprint.total_conversations))
}

pub(in crate::indexer) fn select_targeted_semantic_watch_once_inputs(
    storage: &FrankenStorage,
    data_dir: &Path,
    indexer: &SemanticIndexer,
    pre_watch_conversations: usize,
) -> Result<TargetedSemanticWatchOnceSelection> {
    let total_conversations = count_total_conversations_exact(storage)?;
    if total_conversations == 0 {
        anyhow::bail!(
            "semantic watch-once indexed zero conversations; refusing to publish semantic success"
        );
    }
    let current_db_fingerprint = lexical_rebuild_content_fingerprint(storage, total_conversations)?;
    let current_fingerprint = parse_semantic_content_fingerprint(&current_db_fingerprint)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "semantic watch-once could not parse current DB fingerprint {current_db_fingerprint}"
            )
        })?;
    let tier = semantic_tier_for_embedder_id(indexer.embedder_id()).ok_or_else(|| {
        anyhow::anyhow!(
            "semantic watch-once cannot publish unknown embedder tier for {}",
            indexer.embedder_id()
        )
    })?;
    let manifest = SemanticManifest::load_or_default(data_dir).map_err(|err| {
        anyhow::anyhow!("loading semantic manifest for semantic watch-once: {err}")
    })?;
    let artifact = semantic_artifact_for_tier(&manifest, tier)
        .filter(|artifact| artifact.embedder_id.as_str().eq(indexer.embedder_id()))
        .cloned();
    let manifest_before_db_fingerprint = artifact
        .as_ref()
        .map(|artifact| artifact.db_fingerprint.clone());
    let raw_max_message_id =
        (current_fingerprint.max_message_id > 0).then_some(current_fingerprint.max_message_id);

    if pre_watch_conversations == 0 {
        let canonical_documents = CanonicalSemanticDocuments::from_storage(storage)?;
        return Ok(TargetedSemanticWatchOnceSelection {
            mode: TargetedSemanticWatchOnceMode::RebuildAll,
            inputs: canonical_documents.into_inputs(),
            raw_max_message_id,
            tier,
            index_path: vector_index_path(data_dir, indexer.embedder_id()),
            total_conversations: u64::try_from(total_conversations).unwrap_or(u64::MAX),
            current_db_fingerprint,
            manifest_before_db_fingerprint,
            current_doc_ids: None,
            reason: "fresh_watch_once_db",
        });
    }

    let artifact = artifact.ok_or_else(|| {
        anyhow::anyhow!(
            "semantic watch-once cannot prove bounded coverage: no existing {} artifact for populated DB",
            tier.as_str()
        )
    })?;
    let index_path = validate_semantic_watch_once_artifact(data_dir, &artifact, indexer, tier)?;
    let inventory = inspect_semantic_artifact(&index_path)?;
    let canonical_documents = CanonicalSemanticDocuments::from_storage_missing_from(
        storage,
        Some(&inventory.reusable_main_doc_ids),
    )?;
    let exact_coverage = inventory.is_compacted_unique_main()
        && inventory
            .reusable_main_doc_ids
            .eq(&canonical_documents.doc_ids);
    if exact_coverage {
        return Ok(TargetedSemanticWatchOnceSelection {
            mode: TargetedSemanticWatchOnceMode::AlreadyCovered,
            inputs: Vec::new(),
            raw_max_message_id,
            tier,
            index_path,
            total_conversations: u64::try_from(total_conversations).unwrap_or(u64::MAX),
            current_db_fingerprint,
            manifest_before_db_fingerprint,
            current_doc_ids: None,
            reason: "semantic_artifact_already_covers_db",
        });
    }

    let CanonicalSemanticDocuments { documents, doc_ids } = canonical_documents;
    let missing_inputs = documents
        .into_iter()
        .map(|(_, input)| input)
        .collect::<Vec<_>>();
    let artifact_fingerprint = parse_semantic_content_fingerprint(&artifact.db_fingerprint);
    let manifest_matches_inventory =
        artifact.doc_count == u64::try_from(inventory.live_record_count()).unwrap_or(u64::MAX);
    let append_only_prefix = if let Some(artifact_fingerprint) = artifact_fingerprint {
        let artifact_fingerprint_conversations =
            u64::try_from(artifact_fingerprint.total_conversations).unwrap_or(u64::MAX);
        artifact
            .conversation_count
            .eq(&artifact_fingerprint_conversations)
            && semantic_artifact_is_append_only_prefix(
                storage,
                artifact_fingerprint,
                current_fingerprint,
            )?
            && missing_inputs.iter().all(|input| {
                i64::try_from(input.message_id)
                    .is_ok_and(|message_id| message_id > artifact_fingerprint.max_message_id)
            })
    } else {
        false
    };
    let can_append = inventory.is_compacted_unique_main()
        && manifest_matches_inventory
        && inventory.reusable_main_doc_ids.is_subset(&doc_ids)
        && append_only_prefix;
    if can_append {
        return Ok(TargetedSemanticWatchOnceSelection {
            mode: TargetedSemanticWatchOnceMode::AppendToExisting,
            inputs: missing_inputs,
            raw_max_message_id,
            tier,
            index_path,
            total_conversations: u64::try_from(total_conversations).unwrap_or(u64::MAX),
            current_db_fingerprint,
            manifest_before_db_fingerprint,
            current_doc_ids: None,
            reason: "semantic_artifact_is_append_only_prefix",
        });
    }

    Ok(TargetedSemanticWatchOnceSelection {
        mode: TargetedSemanticWatchOnceMode::ReconcileExisting,
        inputs: missing_inputs,
        raw_max_message_id,
        tier,
        index_path,
        total_conversations: u64::try_from(total_conversations).unwrap_or(u64::MAX),
        current_db_fingerprint,
        manifest_before_db_fingerprint,
        current_doc_ids: Some(doc_ids),
        reason: "semantic_artifact_reconciled_with_current_db",
    })
}

pub(in crate::indexer) fn publish_semantic_watch_once_artifact(
    data_dir: &Path,
    indexer: &SemanticIndexer,
    selection: &TargetedSemanticWatchOnceSelection,
    doc_count: u64,
    build_started_at_ms: i64,
) -> Result<()> {
    let size_bytes = fs::metadata(&selection.index_path)
        .with_context(|| {
            format!(
                "stat semantic watch-once index {}",
                selection.index_path.display()
            )
        })?
        .len();
    let relative_index_path = selection
        .index_path
        .strip_prefix(data_dir)
        .unwrap_or(selection.index_path.as_path())
        .to_string_lossy()
        .to_string();
    let mut manifest = SemanticManifest::load_or_default(data_dir).map_err(|err| {
        anyhow::anyhow!("loading semantic manifest for semantic watch-once publish: {err}")
    })?;
    if !matches!(
        selection.mode,
        TargetedSemanticWatchOnceMode::AlreadyCovered
    ) && manifest.hnsw.as_ref().is_some_and(|hnsw| {
        hnsw.base_tier == selection.tier && hnsw.embedder_id == indexer.embedder_id()
    }) {
        // Any append/rebuild/reconciliation changes the vector row set or
        // ordering. The HNSW loader independently rejects that stale graph,
        // but the durable manifest must stop advertising it as ready too.
        manifest.hnsw = None;
    }
    manifest.publish_artifact(ArtifactRecord {
        tier: selection.tier,
        embedder_id: indexer.embedder_id().to_string(),
        model_revision: semantic_model_revision_for_embedder_id(indexer.embedder_id()),
        schema_version: SEMANTIC_SCHEMA_VERSION,
        chunking_version: CHUNKING_STRATEGY_VERSION,
        dimension: indexer.embedder_dimension(),
        doc_count,
        conversation_count: selection.total_conversations,
        db_fingerprint: selection.current_db_fingerprint.clone(),
        index_path: relative_index_path,
        size_bytes,
        started_at_ms: build_started_at_ms,
        completed_at_ms: semantic_indexing_now_ms(),
        ready: true,
    });
    manifest.refresh_backlog(
        selection.total_conversations,
        &selection.current_db_fingerprint,
    );
    manifest
        .save(data_dir)
        .map_err(|err| anyhow::anyhow!("saving semantic watch-once manifest: {err}"))
}

pub(in crate::indexer) fn run_targeted_semantic_watch_once_publish(
    embedder: &str,
    data_dir: &Path,
    storage: &Mutex<FrankenStorage>,
    indexed_conversations: usize,
    pre_watch_conversations: usize,
) -> Result<SemanticWatchOnceStats> {
    if indexed_conversations == 0 {
        anyhow::bail!(
            "semantic watch-once indexed zero conversations; refusing to publish semantic success"
        );
    }

    let indexer = SemanticIndexer::new(embedder, Some(data_dir))?;
    let selection = {
        let storage = storage.lock().map_err(|err| {
            anyhow::anyhow!("lock storage for semantic watch-once selection: {err}")
        })?;
        select_targeted_semantic_watch_once_inputs(
            &storage,
            data_dir,
            &indexer,
            pre_watch_conversations,
        )?
    };
    let selected_docs = selection.inputs.len();
    let build_started_at_ms = semantic_indexing_now_ms();
    let embedded = if selection.inputs.is_empty() {
        Vec::new()
    } else {
        indexer.embed_messages(&selection.inputs)?
    };
    let embedded_docs = embedded.len();

    let doc_count = match selection.mode {
        TargetedSemanticWatchOnceMode::AlreadyCovered => {
            let index = FsVectorIndex::open(&selection.index_path).map_err(|err| {
                anyhow::anyhow!(
                    "open already-covered semantic watch-once index {}: {err}",
                    selection.index_path.display()
                )
            })?;
            u64::try_from(index.record_count()).unwrap_or(u64::MAX)
        }
        TargetedSemanticWatchOnceMode::RebuildAll => {
            let index = indexer.build_and_save_index(embedded, data_dir)?;
            u64::try_from(index.record_count()).unwrap_or(u64::MAX)
        }
        TargetedSemanticWatchOnceMode::AppendToExisting => {
            if embedded_docs > 0 {
                let appended = indexer.append_to_index(embedded, data_dir)?;
                if !appended.eq(&embedded_docs) {
                    anyhow::bail!(
                        "semantic watch-once append count mismatch: appended {appended}, embedded {embedded_docs}"
                    );
                }
            }
            let mut index = FsVectorIndex::open(&selection.index_path).map_err(|err| {
                anyhow::anyhow!(
                    "open appended semantic watch-once index {}: {err}",
                    selection.index_path.display()
                )
            })?;
            if index.wal_record_count() > 0 {
                index.compact().map_err(|err| {
                    anyhow::anyhow!("compact appended semantic watch-once index: {err}")
                })?;
            }
            drop(index);
            let inventory = inspect_semantic_artifact(&selection.index_path)?;
            if !inventory.is_compacted_unique_main() {
                anyhow::bail!(
                    "semantic watch-once append did not publish a compact unique FSVI artifact"
                );
            }
            u64::try_from(inventory.live_record_count()).unwrap_or(u64::MAX)
        }
        TargetedSemanticWatchOnceMode::ReconcileExisting => {
            let current_doc_ids = selection.current_doc_ids.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "semantic reconciliation selection omitted canonical document identities"
                )
            })?;
            let index = indexer.reconcile_index_with_canonical_documents(
                embedded,
                data_dir,
                selection.tier,
                &selection.current_db_fingerprint,
                current_doc_ids,
            )?;
            u64::try_from(index.record_count()).unwrap_or(u64::MAX)
        }
    };

    publish_semantic_watch_once_artifact(
        data_dir,
        &indexer,
        &selection,
        doc_count,
        build_started_at_ms,
    )?;
    if let Some(raw_max_message_id) = selection.raw_max_message_id {
        update_incremental_semantic_watermark(
            storage,
            raw_max_message_id,
            "updating semantic watch-once watermark",
        )?;
    }
    {
        let guard = storage
            .lock()
            .map_err(|err| anyhow::anyhow!("lock storage after semantic watch-once: {err}"))?;
        let semantic_identity_tier = storage_semantic_identity_tier(selection.tier);
        if guard.semantic_identity_rebuild_required(semantic_identity_tier)? {
            crate::indexer::semantic::invalidate_identity_stale_semantic_shards(
                data_dir,
                indexer.embedder_id(),
            )
            .with_context(
                || "revoking identity-stale semantic shard generations after watch-once publish",
            )?;
            persist::with_ephemeral_writer(
                &guard,
                false,
                "completing semantic identity rebuild after semantic watch-once",
                |writer| writer.complete_semantic_identity_rebuild(semantic_identity_tier),
            )?;
        }
    }

    Ok(SemanticWatchOnceStats {
        published: true,
        selected_docs,
        embedded_docs,
        tier: selection.tier.as_str().to_string(),
        vector_index_path: selection.index_path.display().to_string(),
        manifest_before_db_fingerprint: selection.manifest_before_db_fingerprint,
        manifest_after_db_fingerprint: Some(selection.current_db_fingerprint),
        reason: selection.reason.to_string(),
    })
}

pub(in crate::indexer) fn update_incremental_semantic_watermark(
    storage: &Mutex<FrankenStorage>,
    raw_max_id: i64,
    context: &str,
) -> Result<()> {
    let guard = storage
        .lock()
        .map_err(|e| anyhow::anyhow!("lock storage for watermark write: {e}"))?;
    persist::with_ephemeral_writer(&guard, false, context, |writer| {
        writer.set_last_embedded_message_id(raw_max_id)
    })?;
    Ok(())
}

pub(in crate::indexer) fn embed_incremental_semantic_inputs(
    embedder: &str,
    data_dir: &Path,
    storage: &Mutex<FrankenStorage>,
    embedding_inputs: Vec<EmbeddingInput>,
    raw_max_id: i64,
    filtered_watermark_context: &str,
    success_watermark_context: &str,
) -> Result<usize> {
    // A watch callback may outlive the base generation it started with. Admit
    // the target before loading a model, embedding a delta, or certifying even
    // a filtered-only watermark. Append still validates again to catch races.
    if !semantic_index_has_current_contract(data_dir, embedder) {
        anyhow::bail!(
            "incremental semantic base is missing or incompatible; rebuild with cass index --semantic before appending"
        );
    }
    if embedding_inputs.is_empty() {
        update_incremental_semantic_watermark(storage, raw_max_id, filtered_watermark_context)?;
        return Ok(0);
    }

    let semantic_indexer = SemanticIndexer::new(embedder, Some(data_dir))?;
    let embedded = semantic_indexer.embed_messages(&embedding_inputs)?;
    let count = semantic_indexer.append_to_index(embedded, data_dir)?;

    update_incremental_semantic_watermark(storage, raw_max_id, success_watermark_context)?;
    Ok(count)
}

/// tpndx: after a one-shot delta append, make the artifact publishable —
/// compact any WAL tail, count live records, and republish the semantic
/// manifest against the current canonical DB (the same tail the targeted
/// watch-once path performs). Returns the published document count.
pub(in crate::indexer) fn finalize_one_shot_semantic_delta(
    embedder: &str,
    data_dir: &Path,
    storage: &FrankenStorage,
    build_started_at_ms: i64,
) -> Result<u64> {
    let semantic_indexer = SemanticIndexer::new(embedder, Some(data_dir))?;
    let index_path =
        crate::search::vector_index::vector_index_path(data_dir, semantic_indexer.embedder_id());
    let mut index = FsVectorIndex::open(&index_path).map_err(|err| {
        anyhow::anyhow!(
            "open appended one-shot semantic index {}: {err}",
            index_path.display()
        )
    })?;
    if index.wal_record_count() > 0 {
        index
            .compact()
            .map_err(|err| anyhow::anyhow!("compact appended one-shot semantic index: {err}"))?;
    }
    drop(index);
    let inventory = inspect_semantic_artifact(&index_path)?;
    let doc_count = u64::try_from(inventory.live_record_count()).unwrap_or(u64::MAX);
    // The append changed the vector row set, so any HNSW graph built over the
    // previous rows is stale. The loader rejects it independently, but the
    // durable manifest must stop advertising it as ready (same rule as the
    // targeted watch-once publish).
    if let Some(tier) = semantic_tier_for_embedder_id(semantic_indexer.embedder_id()) {
        let mut manifest = SemanticManifest::load_or_default(data_dir)
            .map_err(|err| anyhow::anyhow!("loading semantic manifest for delta publish: {err}"))?;
        if manifest.hnsw.as_ref().is_some_and(|hnsw| {
            hnsw.base_tier == tier && hnsw.embedder_id == semantic_indexer.embedder_id()
        }) {
            manifest.hnsw = None;
            manifest
                .save(data_dir)
                .map_err(|err| anyhow::anyhow!("revoking stale HNSW after delta append: {err}"))?;
        }
    }
    publish_direct_semantic_artifact(
        storage,
        data_dir,
        &index_path,
        semantic_indexer.embedder_id(),
        semantic_indexer.embedder_dimension(),
        doc_count,
        build_started_at_ms,
    )?;
    Ok(doc_count)
}

/// Perform incremental semantic embedding for messages added since the last
/// watermark. Loads the ONNX model, embeds the batch, appends to the existing
/// FSVI index via WAL, and updates the watermark.
pub(in crate::indexer) fn incremental_semantic_embed(
    embedder: &str,
    data_dir: &Path,
    storage: &Mutex<FrankenStorage>,
) -> Result<usize> {
    // 1. Read watermark
    let watermark = storage
        .lock()
        .map_err(|e| anyhow::anyhow!("lock storage for watermark read: {e}"))?
        .get_last_embedded_message_id()?
        .unwrap_or(0);

    // 2. Fetch new packet-derived canonical batches since watermark.
    let batch = {
        let storage = storage
            .lock()
            .map_err(|e| anyhow::anyhow!("lock storage for message fetch: {e}"))?;
        packet_embedding_inputs_from_storage_since(&storage, watermark)?
    };

    let Some(raw_max_id) = batch.raw_max_message_id else {
        return Ok(0);
    };

    tracing::info!(
        since_id = watermark,
        conversations = batch.conversations_in_batch,
        count = batch.inputs.len(),
        packet_driven = true,
        "incremental semantic: fetched canonical packet catch-up batch"
    );

    let embedding_inputs: Vec<EmbeddingInput> = batch
        .inputs
        .into_iter()
        .filter(|msg| !is_hard_message_noise(semantic_role_name(msg.role), &msg.content))
        .collect();

    embed_incremental_semantic_inputs(
        embedder,
        data_dir,
        storage,
        embedding_inputs,
        raw_max_id,
        "advancing incremental semantic watermark for filtered batch",
        "updating incremental semantic watermark",
    )
}
