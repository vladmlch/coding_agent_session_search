use std::collections::HashSet;
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use frankensearch::index::{
    HNSW_DEFAULT_EF_CONSTRUCTION as FS_HNSW_DEFAULT_EF_CONSTRUCTION,
    HNSW_DEFAULT_M as FS_HNSW_DEFAULT_M, HnswConfig as FsHnswConfig, HnswIndex as FsHnswIndex,
    Quantization as FsQuantization, VectorIndex as FsVectorIndex,
    VectorIndexWriter as FsVectorIndexWriter, wal_path_for as fsvi_wal_path_for,
};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

use super::batch_prep::{
    ExactEmbeddingReuse, flush_prepared_batch, length_aware_batches, prepare_window,
    prepare_window_with_memo, trace_semantic_prep_memo_window, validate_embedding_vector,
};
use super::config::{
    CancelCheck, SEMANTIC_PREP_MEMO_ALGORITHM, SEMANTIC_PREP_MEMO_VERSION,
    expected_vector_space_revision, parallel_prep_enabled, resolved_default_batch_size,
    resolved_semantic_embed_batch_char_budget, resolved_semantic_embed_batch_fail_after_ms,
    resolved_semantic_embed_batch_warn_after_ms, resolved_semantic_prep_memo_capacity,
    saturating_u64_from_millis, saturating_u64_from_usize,
};
use super::replay_cache;
#[cfg(test)]
use super::storage_paths::safe_path_component;
use super::storage_paths::{
    create_semantic_shard_generation_dir, now_ms, publish_reconciled_semantic_index,
    semantic_doc_id_for_embedded, semantic_shard_ann_index_path, semantic_shard_index_path,
    sync_parent_directory,
};
use super::types::{
    EmbeddedMessage, EmbeddingInput, SemanticShardBuildOutcome, SemanticShardBuildPlan,
};
use crate::indexer::memoization::ContentAddressedMemoCache;
use crate::indexer::semantic_progress::{
    SemanticProgressEvent, SemanticProgressFields, SemanticProgressSink,
};
use crate::search::embedder::Embedder;
use crate::search::fastembed_embedder::FastEmbedder;
use crate::search::hash_embedder::HashEmbedder;
use crate::search::policy::{CHUNKING_STRATEGY_VERSION, SEMANTIC_SCHEMA_VERSION};
#[cfg(test)]
use crate::search::semantic_manifest::TierKind;
use crate::search::semantic_manifest::{SemanticShardManifest, SemanticShardRecord};
use crate::search::vector_index::vector_index_path;

pub struct SemanticIndexer {
    pub(in crate::indexer::semantic::engine) embedder: Box<dyn Embedder>,
    pub(in crate::indexer::semantic::engine) batch_size: usize,
    // Only the concrete CASS factory admits the hash and English native
    // MiniLM producers. Multilingual retains uncached batches until qualified.
    pub(in crate::indexer::semantic::engine) exact_reuse: bool,
    pub(in crate::indexer::semantic::engine) cancelled: Option<CancelCheck>,
    pub(in crate::indexer::semantic::engine) replay_cache: replay_cache::CanonicalReplayCache,
}

impl SemanticIndexer {
    pub fn new(embedder_type: &str, data_dir: Option<&Path>) -> Result<Self> {
        Self::construct(embedder_type, data_dir, None)
    }

    /// A job owner can share its cancellation flag with the backfill loops.
    /// Only local providers exist; no external configuration is consulted.
    pub fn new_with_cancel(
        embedder_type: &str,
        data_dir: Option<&Path>,
        cancelled: CancelCheck,
    ) -> Result<Self> {
        Self::construct(embedder_type, data_dir, Some(cancelled))
    }

    fn construct(
        embedder_type: &str,
        data_dir: Option<&Path>,
        cancelled: Option<CancelCheck>,
    ) -> Result<Self> {
        let exact_reuse = embedder_type == "hash"
            || FastEmbedder::canonical_name(embedder_type) == Some("minilm");
        let embedder: Box<dyn Embedder> = match embedder_type {
            "hash" => Box::new(HashEmbedder::default()),
            other => {
                let embedder_name = FastEmbedder::canonical_name(other)
                    .ok_or_else(|| anyhow::anyhow!("unknown embedder: {other}"))?;
                let dir = data_dir
                    .ok_or_else(|| anyhow::anyhow!("data_dir required for native embedder"))?;
                Box::new(
                    FastEmbedder::load_by_name(dir, embedder_name)
                        .map_err(|e| anyhow::anyhow!("native embedder unavailable: {e}"))?,
                )
            }
        };

        Ok(Self {
            embedder,
            batch_size: resolved_default_batch_size(),
            exact_reuse,
            cancelled,
            replay_cache: replay_cache::CanonicalReplayCache::default(),
        })
    }

    pub(in crate::indexer::semantic) fn check_cancelled(&self) -> Result<()> {
        if self.cancelled.as_ref().is_some_and(|cancelled| cancelled()) {
            bail!(
                "semantic_backfill_cancelled: incomplete batch was not committed; resume the last durable checkpoint"
            );
        }
        Ok(())
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Result<Self> {
        if batch_size == 0 {
            bail!("batch_size must be > 0");
        }
        self.batch_size = batch_size;
        Ok(self)
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn embedder_id(&self) -> &str {
        self.embedder.id()
    }

    pub fn embedder_dimension(&self) -> usize {
        self.embedder.dimension()
    }

    pub(in crate::indexer::semantic::engine) fn vector_space_revision(
        &self,
    ) -> Result<&'static str> {
        expected_vector_space_revision(self.embedder_id()).ok_or_else(|| {
            anyhow::anyhow!(
                "no vector-space revision registered for embedder {}",
                self.embedder_id()
            )
        })
    }

    pub fn embed_messages(&self, messages: &[EmbeddingInput]) -> Result<Vec<EmbeddedMessage>> {
        self.embed_messages_with_sink(messages, &SemanticProgressSink::disabled())
    }

    /// Embed messages while reporting how many input rows have been fully
    /// handled. Rows rejected during canonical preparation count as handled so
    /// the caller's phase counter always reaches its declared total.
    pub(crate) fn embed_messages_with_progress<F>(
        &self,
        messages: &[EmbeddingInput],
        on_progress: F,
    ) -> Result<Vec<EmbeddedMessage>>
    where
        F: FnMut(usize, usize),
    {
        self.embed_messages_with_sink_and_progress(
            messages,
            &SemanticProgressSink::disabled(),
            on_progress,
        )
    }

    /// Variant of [`embed_messages`] that emits `embed_batch_*` events
    /// into the given JSONL sink. The sink is silent unless
    /// `CASS_SEMANTIC_PROGRESS_JSONL` is set, so this path is safe to
    /// take in production.
    pub fn embed_messages_with_sink(
        &self,
        messages: &[EmbeddingInput],
        sink: &SemanticProgressSink,
    ) -> Result<Vec<EmbeddedMessage>> {
        self.embed_messages_with_sink_and_progress(messages, sink, |_, _| {})
    }

    pub(in crate::indexer::semantic::engine) fn embed_messages_with_sink_and_progress<F>(
        &self,
        messages: &[EmbeddingInput],
        sink: &SemanticProgressSink,
        mut on_progress: F,
    ) -> Result<Vec<EmbeddedMessage>>
    where
        F: FnMut(usize, usize),
    {
        self.check_cancelled()?;
        if messages.is_empty() {
            on_progress(0, 0);
            return Ok(Vec::new());
        }

        let show_progress = std::io::stderr().is_terminal();
        let pb = ProgressBar::new(saturating_u64_from_usize(messages.len()));
        if show_progress {
            let style = ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} passages embedded")
                .unwrap_or_else(|_| ProgressStyle::default_bar());
            pb.set_style(style);
        } else {
            pb.set_draw_target(ProgressDrawTarget::hidden());
        }

        let mut embeddings = Vec::with_capacity(messages.len());
        let mut exact_reuse = self.exact_reuse.then(ExactEmbeddingReuse::default);

        // Process the corpus in windows of ~4 batches. Within each window,
        // rayon parallelizes the canonicalize + hash prep across cores; the
        // ONNX embedder is then fed serially in `batch_size` chunks so its
        // internal thread pool stays saturated without being starved by the
        // single-threaded prep loop we had before. `with_batch_size` and
        // `resolved_default_batch_size` both guarantee `batch_size >= 1`,
        // so saturating_mul(4) is always >= batch_size — no further clamp.
        let window = self.batch_size.saturating_mul(4);
        let serial_prep = !parallel_prep_enabled();
        let prep_memo_capacity = resolved_semantic_prep_memo_capacity();
        let mut prep_memo =
            serial_prep.then(|| ContentAddressedMemoCache::with_capacity(prep_memo_capacity));
        let mut batch_index: u64 = 0;
        let mut rows_processed: u64 = 0;
        let rows_total = u64::try_from(messages.len()).ok();
        let warn_after_ms = resolved_semantic_embed_batch_warn_after_ms();
        let fail_after_ms = resolved_semantic_embed_batch_fail_after_ms();
        // cass #309: bound `row_count × max_canonical_len` per embed batch so a
        // long message can't inflate a fixed-128 batch's padded tensor to
        // multiple GB. Resolved once per call (not per batch).
        let embed_char_budget = resolved_semantic_embed_batch_char_budget();
        for (window_index, window_slice) in messages.chunks(window).enumerate() {
            self.check_cancelled()?;
            let prepared_window = match prep_memo.as_mut() {
                Some(cache) => {
                    let stats_before = cache.stats().clone();
                    let prepared_window = prepare_window_with_memo(window_slice, cache);
                    trace_semantic_prep_memo_window(
                        window_index,
                        window_slice.len(),
                        prepared_window.len(),
                        prep_memo_capacity,
                        &stats_before,
                        cache.stats(),
                    );
                    prepared_window
                }
                None => prepare_window(window_slice, false),
            };
            let skipped_in_window = window_slice.len() - prepared_window.len();
            if skipped_in_window > 0 {
                pb.inc(saturating_u64_from_usize(skipped_in_window));
                rows_processed =
                    rows_processed.saturating_add(saturating_u64_from_usize(skipped_in_window));
                on_progress(
                    usize::try_from(rows_processed).unwrap_or(usize::MAX),
                    messages.len(),
                );
            }

            for batch in length_aware_batches(&prepared_window, self.batch_size, embed_char_budget)
            {
                let batch_rows = u64::try_from(batch.len()).unwrap_or(u64::MAX);
                // Sum the canonicalized byte count so an operator can
                // distinguish a stalled inference from a stalled query —
                // a tiny `bytes` value paired with a long batch wall-time
                // points at the model; a huge `bytes` paired with a short
                // wall-time points at the storage side.
                let batch_bytes: u64 = batch
                    .iter()
                    .map(|p| saturating_u64_from_usize(p.canonical.len()))
                    .sum();
                if sink.is_active() {
                    sink.emit(
                        SemanticProgressEvent::EmbedBatchStart,
                        SemanticProgressFields {
                            batch_index: Some(batch_index),
                            batch_rows: Some(batch_rows),
                            rows_processed: Some(rows_processed),
                            rows_total,
                            bytes: Some(batch_bytes),
                            ..Default::default()
                        },
                    );
                }
                let batch_started = Instant::now();
                flush_prepared_batch(
                    batch,
                    &mut embeddings,
                    &pb,
                    self.embedder.as_ref(),
                    exact_reuse.as_mut(),
                )?;
                let elapsed_ms = saturating_u64_from_millis(batch_started.elapsed().as_millis());
                rows_processed = rows_processed.saturating_add(batch_rows);
                on_progress(
                    usize::try_from(rows_processed).unwrap_or(usize::MAX),
                    messages.len(),
                );
                if warn_after_ms > 0 && elapsed_ms > warn_after_ms {
                    tracing::warn!(
                        batch_index,
                        elapsed_ms,
                        warn_after_ms,
                        batch_rows,
                        batch_bytes,
                        embedder = self.embedder_id(),
                        "semantic embed batch exceeded watchdog warning threshold"
                    );
                }
                if fail_after_ms > 0 && elapsed_ms > fail_after_ms {
                    bail!(
                        "semantic embed batch {batch_index} took {elapsed_ms}ms, exceeding \
                         CASS_SEMANTIC_EMBED_BATCH_FAIL_AFTER_MS={fail_after_ms}"
                    );
                }
                if sink.is_active() {
                    sink.emit(
                        SemanticProgressEvent::EmbedBatchDone,
                        SemanticProgressFields {
                            batch_index: Some(batch_index),
                            batch_rows: Some(batch_rows),
                            rows_processed: Some(rows_processed),
                            rows_total,
                            bytes: Some(batch_bytes),
                            ..Default::default()
                        },
                    );
                }
                batch_index = batch_index.saturating_add(1);
            }
        }

        if let Some(cache) = prep_memo.as_ref() {
            let stats = cache.stats();
            tracing::debug!(
                algorithm = SEMANTIC_PREP_MEMO_ALGORITHM,
                algorithm_version = SEMANTIC_PREP_MEMO_VERSION,
                hits = stats.hits,
                misses = stats.misses,
                inserts = stats.inserts,
                quarantined = stats.quarantined,
                live_entries = stats.live_entries,
                entry_capacity = prep_memo_capacity,
                "semantic prep memo cache summary"
            );
        }

        self.check_cancelled()?;
        pb.finish_with_message("Embedding complete");
        Ok(embeddings)
    }

    pub fn build_and_save_index<I>(
        &self,
        embedded_messages: I,
        data_dir: &Path,
    ) -> Result<FsVectorIndex>
    where
        I: IntoIterator<Item = EmbeddedMessage>,
    {
        let index_path = vector_index_path(data_dir, self.embedder_id());
        self.build_and_save_index_at_path(embedded_messages, &index_path)
    }

    /// Build the direct vector index and report each record only after its
    /// validated write succeeds.
    pub(crate) fn build_and_save_index_with_progress<I, F>(
        &self,
        embedded_messages: I,
        data_dir: &Path,
        on_progress: F,
    ) -> Result<FsVectorIndex>
    where
        I: IntoIterator<Item = EmbeddedMessage>,
        F: FnMut(usize),
    {
        let index_path = vector_index_path(data_dir, self.embedder_id());
        self.build_and_save_index_at_path_with_progress(embedded_messages, &index_path, on_progress)
    }

    pub fn build_and_save_index_shards<I>(
        &self,
        embedded_messages: I,
        data_dir: &Path,
        plan: SemanticShardBuildPlan,
    ) -> Result<SemanticShardBuildOutcome>
    where
        I: IntoIterator<Item = EmbeddedMessage>,
    {
        if plan.db_fingerprint.trim().is_empty() {
            bail!("semantic shard build requires a non-empty DB fingerprint");
        }
        if plan.max_records_per_shard == 0 {
            bail!("semantic shard build requires max_records_per_shard > 0");
        }

        // Corrupt/newer metadata is not an empty archive. Refuse before
        // consuming input, rather than erasing other tiers via load_or_default.
        SemanticShardManifest::load(data_dir).map_err(|err| {
            anyhow::anyhow!("loading semantic shard manifest before build: {err}")
        })?;
        let generation_dir = create_semantic_shard_generation_dir(
            data_dir,
            plan.tier,
            self.embedder_id(),
            &plan.db_fingerprint,
        )?;
        let mut shard_records = Vec::new();
        let mut index_paths = Vec::new();
        let mut ann_index_paths = Vec::new();
        // A limit is not a request for an archive-sized up-front allocation.
        let mut current_records = Vec::new();
        let mut shard_index = 0u32;
        let mut total_docs = 0u64;

        for embedded in embedded_messages {
            current_records.push(embedded);
            if current_records.len() >= plan.max_records_per_shard {
                let records = std::mem::take(&mut current_records);
                let (record, path, ann_path) = self.write_semantic_shard(
                    records,
                    data_dir,
                    &generation_dir,
                    &plan,
                    shard_index,
                )?;
                total_docs = total_docs.saturating_add(record.doc_count);
                shard_records.push(record);
                index_paths.push(path);
                if let Some(path) = ann_path {
                    ann_index_paths.push(path);
                }
                shard_index = shard_index
                    .checked_add(1)
                    .context("semantic shard index overflow")?;
            }
        }

        if !current_records.is_empty() {
            let records = std::mem::take(&mut current_records);
            let (record, path, ann_path) =
                self.write_semantic_shard(records, data_dir, &generation_dir, &plan, shard_index)?;
            total_docs = total_docs.saturating_add(record.doc_count);
            shard_records.push(record);
            index_paths.push(path);
            if let Some(path) = ann_path {
                ann_index_paths.push(path);
            }
        }

        let shard_count = u32::try_from(shard_records.len())
            .context("semantic shard generation exceeded u32 shard count")?;
        for record in &mut shard_records {
            record.shard_count = shard_count;
        }

        // Finish shard directory entries before their first public reference.
        sync_parent_directory(&generation_dir.join("shard-00000.fsvi"))?;
        let mut shard_manifest = SemanticShardManifest::load(data_dir)
            .map_err(|err| anyhow::anyhow!("loading semantic shard manifest for publish: {err}"))?
            .unwrap_or_default();
        shard_manifest.replace_shards_for_generation(
            plan.tier,
            self.embedder_id(),
            &plan.db_fingerprint,
            shard_records,
        );
        shard_manifest
            .save(data_dir)
            .map_err(|err| anyhow::anyhow!("saving semantic shard manifest: {err}"))?;
        let summary = shard_manifest.summary(plan.tier, self.embedder_id(), &plan.db_fingerprint);

        tracing::info!(
            tier = plan.tier.as_str(),
            embedder = self.embedder_id(),
            shard_count,
            doc_count = total_docs,
            total_conversations = plan.total_conversations,
            "published semantic shard generation sidecar"
        );

        Ok(SemanticShardBuildOutcome {
            tier: plan.tier,
            embedder_id: self.embedder_id().to_string(),
            shard_count,
            doc_count: total_docs,
            total_conversations: plan.total_conversations,
            index_paths,
            ann_index_paths,
            shard_manifest_path: SemanticShardManifest::path(data_dir),
            complete: summary.complete,
        })
    }

    fn write_semantic_shard(
        &self,
        embedded_messages: Vec<EmbeddedMessage>,
        data_dir: &Path,
        generation_dir: &Path,
        plan: &SemanticShardBuildPlan,
        shard_index: u32,
    ) -> Result<(SemanticShardRecord, PathBuf, Option<PathBuf>)> {
        let started_at_ms = now_ms();
        let shard_path = semantic_shard_index_path(generation_dir, shard_index);
        let shard_index_file = self.build_and_save_index_at_path(embedded_messages, &shard_path)?;
        let size_bytes = fs::metadata(&shard_path)
            .with_context(|| format!("stat semantic shard {}", shard_path.display()))?
            .len();
        let (ann_index_path, ann_size_bytes, ann_ready, ann_absolute_path) = if plan.build_ann {
            let ann_path = semantic_shard_ann_index_path(generation_dir, shard_index);
            let config = FsHnswConfig {
                m: FS_HNSW_DEFAULT_M,
                ef_construction: FS_HNSW_DEFAULT_EF_CONSTRUCTION,
                ..FsHnswConfig::default()
            };
            let hnsw = FsHnswIndex::build_from_vector_index(&shard_index_file, config)
                .map_err(|err| anyhow::anyhow!("build shard HNSW index failed: {err}"))?;
            hnsw.save(&ann_path)
                .map_err(|err| anyhow::anyhow!("save shard HNSW index failed: {err}"))?;
            let ann_size_bytes = fs::metadata(&ann_path)
                .with_context(|| format!("stat semantic shard ANN {}", ann_path.display()))?
                .len();
            let relative_ann_path = ann_path
                .strip_prefix(data_dir)
                .context("semantic ANN shard escaped its data directory")?
                .to_string_lossy()
                .to_string();
            (
                Some(relative_ann_path),
                ann_size_bytes,
                true,
                Some(ann_path),
            )
        } else {
            (None, 0, false, None)
        };
        let relative_index_path = shard_path
            .strip_prefix(data_dir)
            .context("semantic vector shard escaped its data directory")?
            .to_string_lossy()
            .to_string();
        let record = SemanticShardRecord {
            tier: plan.tier,
            embedder_id: self.embedder_id().to_string(),
            model_revision: plan.model_revision.clone(),
            schema_version: SEMANTIC_SCHEMA_VERSION,
            chunking_version: CHUNKING_STRATEGY_VERSION,
            dimension: self.embedder_dimension(),
            shard_index,
            shard_count: 0,
            doc_count: u64::try_from(shard_index_file.record_count()).unwrap_or(u64::MAX),
            total_conversations: plan.total_conversations,
            db_fingerprint: plan.db_fingerprint.clone(),
            index_path: relative_index_path,
            quantization: "f16".to_string(),
            mmap_ready: true,
            ann_index_path,
            ann_size_bytes,
            ann_ready,
            size_bytes,
            started_at_ms,
            completed_at_ms: now_ms(),
            ready: true,
        };
        Ok((record, shard_path, ann_absolute_path))
    }

    pub(in crate::indexer::semantic::engine) fn build_and_save_index_at_path<I>(
        &self,
        embedded_messages: I,
        index_path: &Path,
    ) -> Result<FsVectorIndex>
    where
        I: IntoIterator<Item = EmbeddedMessage>,
    {
        self.build_and_save_index_at_path_with_progress(embedded_messages, index_path, |_| {})
    }

    pub(in crate::indexer::semantic::engine) fn build_and_save_index_at_path_with_progress<I, F>(
        &self,
        embedded_messages: I,
        index_path: &Path,
        mut on_progress: F,
    ) -> Result<FsVectorIndex>
    where
        I: IntoIterator<Item = EmbeddedMessage>,
        F: FnMut(usize),
    {
        if let Some(parent) = index_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Store as f16 by default (smaller, faster I/O). Validate before the
        // writer so NaN/Inf never reaches a persisted or staged artifact.
        let mut writer: FsVectorIndexWriter = FsVectorIndex::create_with_revision(
            index_path,
            self.embedder_id(),
            self.vector_space_revision()?,
            self.embedder_dimension(),
            FsQuantization::F16,
        )
        .map_err(|err| anyhow::anyhow!("create fsvi index failed: {err}"))?;

        // The FSVI writer buffers records until finish() publishes its own
        // temporary file. A rejected row therefore leaves no partial index
        // at index_path: that path can still own the previous valid generation
        // and its acknowledged WAL. Drop only the uncommitted writer on error;
        // never unlink the destination as purported partial-output cleanup.
        let mut records_written = 0_usize;
        for embedded in embedded_messages {
            validate_embedding_vector(
                &embedded.embedding,
                self.embedder_dimension(),
                embedded.message_id,
            )?;
            let doc_id = semantic_doc_id_for_embedded(&embedded);
            writer
                .write_record(&doc_id, &embedded.embedding)
                .map_err(|err| {
                    // Preserve both the existing top-level diagnostic and the
                    // backend's typed cause for callers that inspect the chain.
                    let message = format!("write fsvi record failed: {err}");
                    anyhow::Error::new(err).context(message)
                })?;
            records_written = records_written.saturating_add(1);
            on_progress(records_written);
        }

        // Same for publication: callers distinguish a WAL-sidecar refusal
        // (InvalidConfig { field: "wal_sidecar" }) from an I/O failure.
        writer.finish().map_err(|err| {
            let message = format!("finish fsvi index failed: {err}");
            anyhow::Error::new(err).context(message)
        })?;

        FsVectorIndex::open(index_path)
            .map_err(|err| anyhow::anyhow!("open fsvi index failed: {err}"))
    }

    /// Append new embeddings to an existing FSVI index via the WAL.
    ///
    /// Used for incremental semantic indexing in watch mode. Opens the
    /// existing index, appends a batch of new embeddings, and compacts if
    /// the WAL has grown large enough.
    ///
    /// Returns the number of entries appended.
    pub fn append_to_index(
        &self,
        embedded_messages: impl IntoIterator<Item = EmbeddedMessage>,
        data_dir: &Path,
    ) -> Result<usize> {
        let index_path = vector_index_path(data_dir, self.embedder_id());
        self.append_to_index_path(embedded_messages, &index_path)
    }

    pub(in crate::indexer::semantic::engine) fn append_to_index_path(
        &self,
        embedded_messages: impl IntoIterator<Item = EmbeddedMessage>,
        index_path: &Path,
    ) -> Result<usize> {
        let mut index = FsVectorIndex::open(index_path)
            .map_err(|err| anyhow::anyhow!("open fsvi index for append: {err}"))?;

        let expected_revision = self.vector_space_revision()?;
        if index.embedder_id() != self.embedder_id()
            || index.dimension() != self.embedder_dimension()
            || index.embedder_revision() != expected_revision
        {
            bail!(
                "semantic index is incompatible with current vector space: expected embedder {} revision {} dimension {}, found embedder {} revision {} dimension {}; rebuild semantic vectors before appending",
                self.embedder_id(),
                expected_revision,
                self.embedder_dimension(),
                index.embedder_id(),
                index.embedder_revision(),
                index.dimension()
            );
        }

        let entries: Vec<(String, Vec<f32>)> = embedded_messages
            .into_iter()
            .map(|em| {
                validate_embedding_vector(&em.embedding, self.embedder_dimension(), em.message_id)?;
                let doc_id = semantic_doc_id_for_embedded(&em);
                Ok((doc_id, em.embedding))
            })
            .collect::<Result<_>>()?;

        let count = entries.len();
        if count == 0 {
            return Ok(0);
        }

        index
            .append_batch(&entries)
            .map_err(|err| anyhow::anyhow!("append_batch: {err}"))?;

        if index.needs_compaction() {
            index
                .compact()
                .map_err(|err| anyhow::anyhow!("compaction: {err}"))?;
        }

        Ok(count)
    }

    /// Rebuild a canonical semantic artifact by copying exact still-current
    /// vectors and writing only newly embedded or changed documents.
    ///
    /// The live FSVI is not replaced until the candidate contains exactly the
    /// current canonical document identities and every vector has been read
    /// back successfully. The live FSVI and WAL are copied into a private
    /// same-filesystem staging directory before compaction, so reconciliation
    /// never mutates the published artifact before the atomic candidate swap.
    ///
    /// Test-only since f6ae407b: production reconciles through
    /// `delegate::reconcile::run`. The tests here still drive the shared
    /// `reconcile_index_at_paths` path through this wrapper.
    #[cfg(test)]
    pub fn reconcile_index_with_canonical_documents(
        &self,
        embedded_messages: Vec<EmbeddedMessage>,
        data_dir: &Path,
        tier: TierKind,
        db_fingerprint: &str,
        current_doc_ids: &HashSet<String>,
    ) -> Result<FsVectorIndex> {
        let index_path = vector_index_path(data_dir, self.embedder_id());
        self.reconcile_index_at_paths(
            embedded_messages,
            &index_path,
            &index_path,
            tier,
            db_fingerprint,
            current_doc_ids,
        )
    }

    #[cfg(test)]
    pub(in crate::indexer::semantic::engine) fn reconcile_index_at_paths(
        &self,
        embedded_messages: Vec<EmbeddedMessage>,
        index_path: &Path,
        final_index_path: &Path,
        tier: TierKind,
        db_fingerprint: &str,
        current_doc_ids: &HashSet<String>,
    ) -> Result<FsVectorIndex> {
        self.reconcile_in_private_directory(
            embedded_messages,
            final_index_path,
            current_doc_ids,
            || {
                let index_parent = index_path.parent().with_context(|| {
                    format!(
                        "semantic reconciliation index path has no parent: {}",
                        index_path.display()
                    )
                })?;
                fs::create_dir_all(index_parent)?;
                let temp_prefix = format!(
                    ".reconcile-{}-{}-{:08x}-",
                    tier.as_str(),
                    safe_path_component(self.embedder_id()),
                    crc32fast::hash(db_fingerprint.as_bytes())
                );
                let directory = tempfile::Builder::new()
                    .prefix(&temp_prefix)
                    .tempdir_in(index_parent)?;
                let candidate = directory.path().join("candidate.fsvi");
                fs::copy(index_path, &candidate).with_context(|| {
                    format!("snapshot live semantic index {}", index_path.display())
                })?;
                let source_wal = fsvi_wal_path_for(index_path);
                if source_wal.exists() {
                    fs::copy(&source_wal, fsvi_wal_path_for(&candidate)).with_context(|| {
                        format!("snapshot live semantic WAL {}", source_wal.display())
                    })?;
                }
                Ok(directory)
            },
        )
    }

    /// Consume an exclusively owned scratch directory, never a published
    /// pathname disguised as scratch. Ordinary reconciliation creates its
    /// snapshot lazily; backfill transfers the snapshot it already copied and
    /// compacted while selecting coverage. No second full-file copy is needed.
    ///
    /// Keeping preparation behind FnOnce preserves validation-before-I/O for
    /// invalid replacements. The directory remains owned until publication or
    /// error cleanup, and the old-WAL replay probe still runs on its exact image.
    pub(in crate::indexer::semantic::engine) fn reconcile_in_private_directory(
        &self,
        mut embedded_messages: Vec<EmbeddedMessage>,
        final_index_path: &Path,
        current_doc_ids: &HashSet<String>,
        prepare: impl FnOnce() -> Result<tempfile::TempDir>,
    ) -> Result<FsVectorIndex> {
        let mut replacement_doc_ids = HashSet::with_capacity(embedded_messages.len());
        for embedded in &embedded_messages {
            if embedded.embedding.len() != self.embedder_dimension() {
                bail!(
                    "embedding dimension mismatch: expected {}, got {}",
                    self.embedder_dimension(),
                    embedded.embedding.len()
                );
            }
            if embedded.embedding.iter().any(|value| !value.is_finite()) {
                bail!("semantic reconciliation replacement contains a non-finite embedding");
            }
            let doc_id = semantic_doc_id_for_embedded(embedded);
            if !current_doc_ids.contains(&doc_id) {
                bail!(
                    "semantic reconciliation replacement is not present in the canonical DB: {doc_id}"
                );
            }
            if !replacement_doc_ids.insert(doc_id.clone()) {
                bail!("semantic reconciliation received duplicate replacement document: {doc_id}");
            }
        }

        let staging_dir = prepare()?;
        let staging_path = staging_dir.path().join("candidate.fsvi");
        let live_wal_path = fsvi_wal_path_for(&staging_path);
        let live_wal_snapshot_path = staging_dir.path().join("live.wal.snapshot");
        if live_wal_path.exists() {
            fs::copy(&live_wal_path, &live_wal_snapshot_path).with_context(|| {
                format!(
                    "retain pre-reconciliation WAL {} at {}",
                    live_wal_path.display(),
                    live_wal_snapshot_path.display()
                )
            })?;
        }

        let embedded_docs = embedded_messages.len();
        let mut staged = if replacement_doc_ids.eq(current_doc_ids) {
            // A complete, validated replacement needs no vectors from the old
            // input contract. Keep its snapshot/WAL private and build a fresh
            // generation; never relabel prefix vectors or truncate live data.
            fs::rename(&staging_path, staging_dir.path().join("prior.fsvi"))?;
            let staged_wal = fsvi_wal_path_for(&staging_path);
            if staged_wal.exists() {
                fs::rename(staged_wal, staging_dir.path().join("prior.wal"))?;
            }
            self.build_and_save_index_at_path(
                std::mem::take(&mut embedded_messages),
                &staging_path,
            )?
        } else {
            FsVectorIndex::open_writer(&staging_path).map_err(|err| {
                anyhow::anyhow!(
                    "open semantic reconciliation snapshot {}: {err}",
                    staging_path.display()
                )
            })?
        };
        if staged.embedder_id() != self.embedder_id()
            || staged.dimension() != self.embedder_dimension()
            || staged.embedder_revision() != self.vector_space_revision()?
        {
            bail!(
                "semantic reconciliation snapshot is incompatible with embedder {} revision {} dimension {}",
                self.embedder_id(),
                self.vector_space_revision()?,
                self.embedder_dimension()
            );
        }
        if staged.wal_record_count() > 0 {
            staged.compact().map_err(|err| {
                anyhow::anyhow!("compact semantic reconciliation snapshot: {err}")
            })?;
        }
        if staged.wal_record_count() > 0 {
            bail!("semantic reconciliation could not clear the snapshot FSVI WAL");
        }

        let initial_tombstones = staged.tombstone_count();
        let mut stale_doc_ids = HashSet::new();
        for record_index in 0..staged.record_count() {
            if staged.is_deleted(record_index) {
                continue;
            }
            let doc_id = staged.doc_id_at(record_index).map_err(|err| {
                anyhow::anyhow!("read semantic document id during reconciliation: {err}")
            })?;
            if !current_doc_ids.contains(doc_id) {
                stale_doc_ids.insert(doc_id.to_owned());
            }
        }

        let removed_stale_records = if stale_doc_ids.is_empty() {
            0
        } else {
            let stale_doc_id_refs = stale_doc_ids.iter().map(String::as_str).collect::<Vec<_>>();
            staged
                .soft_delete_batch(&stale_doc_id_refs)
                .map_err(|err| anyhow::anyhow!("remove stale semantic documents: {err}"))?
        };

        if !embedded_messages.is_empty() {
            let replacement_entries = embedded_messages
                .into_iter()
                .map(|embedded| (semantic_doc_id_for_embedded(&embedded), embedded.embedding))
                .collect::<Vec<_>>();
            staged
                .append_batch(&replacement_entries)
                .map_err(|err| anyhow::anyhow!("append replacement semantic vectors: {err}"))?;
        }
        if staged.wal_record_count() > 0 {
            staged
                .compact()
                .map_err(|err| anyhow::anyhow!("compact replacement semantic vectors: {err}"))?;
        } else if staged.tombstone_count() > 0 {
            staged
                .vacuum()
                .map_err(|err| anyhow::anyhow!("vacuum stale semantic vectors: {err}"))?;
        }
        if staged.wal_record_count() > 0 {
            bail!("reconciled semantic staging index unexpectedly contains WAL records");
        }
        if staged.tombstone_count() > 0 {
            bail!("reconciled semantic staging index unexpectedly contains tombstones");
        }
        if staged.record_count() != current_doc_ids.len() {
            bail!(
                "reconciled semantic staging count mismatch: expected {}, observed {}",
                current_doc_ids.len(),
                staged.record_count()
            );
        }
        let mut staged_doc_ids = HashSet::with_capacity(staged.record_count());
        for record_index in 0..staged.record_count() {
            let doc_id = staged.doc_id_at(record_index).map_err(|err| {
                anyhow::anyhow!("validate reconciled semantic document id: {err}")
            })?;
            if !staged_doc_ids.insert(doc_id.to_owned()) {
                bail!("reconciled semantic staging index contains duplicate document {doc_id}");
            }
            if staged
                .vector_at_f32(record_index)
                .map_err(|err| anyhow::anyhow!("validate reconciled semantic vector: {err}"))?
                .iter()
                .any(|value| !value.is_finite())
            {
                bail!("reconciled semantic staging index contains a non-finite vector");
            }
        }
        if !staged_doc_ids.eq(current_doc_ids) {
            bail!("reconciled semantic staging identities do not match the canonical DB");
        }
        drop(staged);

        if live_wal_snapshot_path.exists() {
            // Publication atomically replaces only the main FSVI. Probe the
            // exact old live WAL against the completed candidate before that
            // swap; it must be rejected as stale rather than replayed.
            fs::copy(&live_wal_snapshot_path, fsvi_wal_path_for(&staging_path)).with_context(
                || {
                    format!(
                        "attach old live WAL to completed semantic candidate {}",
                        staging_path.display()
                    )
                },
            )?;
            let generation_probe = FsVectorIndex::open(&staging_path).map_err(|err| {
                anyhow::anyhow!("validate reconciled semantic WAL generation: {err}")
            })?;
            if generation_probe.wal_record_count() > 0 {
                bail!("reconciled semantic candidate would accept the pre-publication live WAL");
            }
            drop(generation_probe);
        }

        publish_reconciled_semantic_index(&staging_path, final_index_path)?;
        let published = FsVectorIndex::open(final_index_path)
            .map_err(|err| anyhow::anyhow!("open reconciled semantic index: {err}"))?;
        if published.wal_record_count() > 0 {
            bail!("published reconciled semantic index accepted a stale live WAL");
        }
        tracing::info!(
            retained_docs = current_doc_ids.len().saturating_sub(embedded_docs),
            initial_tombstones,
            removed_stale_records,
            embedded_docs,
            published_docs = published.record_count(),
            "published reconciled semantic index"
        );
        Ok(published)
    }
}
