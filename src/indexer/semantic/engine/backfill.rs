use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use frankensearch::index::{
    HNSW_DEFAULT_EF_CONSTRUCTION as FS_HNSW_DEFAULT_EF_CONSTRUCTION,
    HNSW_DEFAULT_M as FS_HNSW_DEFAULT_M, HnswConfig as FsHnswConfig, VectorIndex as FsVectorIndex,
    wal_path_for as fsvi_wal_path_for,
};

use super::build_ops::SemanticIndexer;
use super::config::saturating_u64_from_usize;
use super::replay_cache;
use super::replay_selection::{
    SemanticCheckpointCaps, cached_semantic_total_conversations,
    fetch_canonical_embedding_batch_inner_with_caps_and_total,
    total_semantic_conversations_with_cancel,
};
use super::storage_paths::{
    hnsw_index_path, invalidate_identity_stale_semantic_shards, now_ms,
    park_semantic_destination_wal, publish_reconciled_semantic_index,
    semantic_backfill_identity_matches, semantic_staging_index_path, validated_semantic_index_ids,
};
use super::types::{
    BackfillBatchProgress, BackfillFilePair, CompletedBackfillCache, EmbeddedMessage,
    EmbeddingInput, SemanticBackfillBatchOutcome, SemanticBackfillBatchPlan,
    SemanticBackfillStoragePlan,
};
use crate::indexer::semantic_progress::{
    SemanticProgressEvent, SemanticProgressFields, SemanticProgressSink,
};
use crate::search::policy::{CHUNKING_STRATEGY_VERSION, SEMANTIC_SCHEMA_VERSION};
use crate::search::semantic_manifest::{
    ArtifactRecord, BuildCheckpoint, SemanticManifest, TierKind,
};
use crate::search::vector_index::{VECTOR_INDEX_DIR, vector_index_path};
use crate::storage::sqlite::FrankenStorage;

impl SemanticIndexer {
    fn write_backfill_staging_index(
        &self,
        embedded_messages: Vec<EmbeddedMessage>,
        staging_path: &Path,
        resume_existing: bool,
    ) -> Result<FsVectorIndex> {
        if resume_existing && staging_path.exists() {
            self.append_to_index_path(embedded_messages, staging_path)?;
            FsVectorIndex::open(staging_path)
                .map_err(|err| anyhow::anyhow!("open staged semantic index failed: {err}"))
        } else {
            self.build_and_save_index_at_path(embedded_messages, staging_path)
        }
    }

    #[cfg(test)]
    pub fn run_backfill_batch(
        &self,
        messages: &[EmbeddingInput],
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: SemanticBackfillBatchPlan,
    ) -> Result<SemanticBackfillBatchOutcome> {
        self.run_backfill_batch_with_sink(
            messages,
            data_dir,
            manifest,
            plan,
            None,
            &SemanticProgressSink::disabled(),
        )
    }

    /// Run a backfill batch while emitting semantic progress
    /// events to the given JSONL sink and persists `last_message_id`
    /// into the resumable checkpoint when supplied. The sink is silent
    /// unless `CASS_SEMANTIC_PROGRESS_JSONL` is set, so this path is
    /// safe to take in production.
    pub fn run_backfill_batch_with_sink(
        &self,
        messages: &[EmbeddingInput],
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: SemanticBackfillBatchPlan,
        last_message_id: Option<i64>,
        sink: &SemanticProgressSink,
    ) -> Result<SemanticBackfillBatchOutcome> {
        if plan.db_fingerprint.trim().is_empty() {
            bail!("semantic backfill requires a non-empty DB fingerprint");
        }
        if plan.total_conversations == 0 && plan.conversations_in_batch > 0 {
            bail!("semantic backfill batch cannot process conversations when total is zero");
        }

        let staging_path = semantic_staging_index_path(
            data_dir,
            plan.tier,
            self.embedder_id(),
            &plan.db_fingerprint,
        );
        let prior_checkpoint = manifest
            .checkpoint
            .as_ref()
            .filter(|checkpoint| {
                checkpoint.tier == plan.tier
                    && checkpoint.embedder_id == self.embedder_id()
                    && checkpoint.is_valid(&plan.db_fingerprint)
            })
            .cloned();
        let embeddings = self.embed_messages_with_sink(messages, sink)?;
        let embedded_docs = u64::try_from(embeddings.len()).unwrap_or(u64::MAX);
        if sink.is_active() {
            sink.emit(
                SemanticProgressEvent::StagingWriteStart,
                SemanticProgressFields {
                    batch_rows: Some(embedded_docs),
                    note: Some(staging_path.display().to_string()),
                    ..Default::default()
                },
            );
        }
        self.check_cancelled()?;
        let staged_index = self.write_backfill_staging_index(
            embeddings,
            &staging_path,
            prior_checkpoint.is_some(),
        )?;
        if sink.is_active() {
            sink.emit(
                SemanticProgressEvent::StagingWriteDone,
                SemanticProgressFields {
                    batch_rows: Some(embedded_docs),
                    note: Some(staging_path.display().to_string()),
                    ..Default::default()
                },
            );
        }
        self.finish_backfill_batch(
            staged_index,
            data_dir,
            manifest,
            plan,
            BackfillBatchProgress {
                prior_checkpoint,
                embedded_docs,
                last_message_id,
            },
            sink,
        )
    }

    fn finish_backfill_batch(
        &self,
        mut staged_index: FsVectorIndex,
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: SemanticBackfillBatchPlan,
        progress: BackfillBatchProgress,
        sink: &SemanticProgressSink,
    ) -> Result<SemanticBackfillBatchOutcome> {
        // Cancellation is admitted before the staging write, never halfway
        // through its durable checkpoint/publication. Once writing begins,
        // complete that boundary so a signal cannot strand a newer WAL behind
        // an older cursor. The CLI observes the signal immediately afterward.
        let BackfillBatchProgress {
            prior_checkpoint,
            embedded_docs,
            last_message_id,
        } = progress;
        let manifest_path = SemanticManifest::path(data_dir);
        let staging_path = semantic_staging_index_path(
            data_dir,
            plan.tier,
            self.embedder_id(),
            &plan.db_fingerprint,
        );
        let final_path = vector_index_path(data_dir, self.embedder_id());
        let prior_conversations = prior_checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.conversations_processed);
        let prior_docs = prior_checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.docs_embedded);
        let counted_conversations_processed =
            prior_conversations.saturating_add(plan.conversations_in_batch);
        let conversations_processed = if plan.cursor_exhausted {
            plan.total_conversations
        } else {
            counted_conversations_processed.min(plan.total_conversations)
        };
        let complete = plan.cursor_exhausted;

        manifest.refresh_backlog(plan.total_conversations, &plan.db_fingerprint);

        if complete {
            let retained_generation = self.retain_published_generation_during_replacement(
                manifest,
                plan.tier,
                &plan.db_fingerprint,
                &plan.model_revision,
            );
            let db_fingerprint = plan.db_fingerprint.clone();
            if staged_index.wal_record_count() > 0 {
                staged_index.compact().map_err(|err| {
                    anyhow::anyhow!("compact staged semantic index failed: {err}")
                })?;
            }
            let staged_doc_ids = validated_semantic_index_ids(&staged_index)?;
            drop(staged_index);
            if sink.is_active() {
                sink.emit(
                    SemanticProgressEvent::PublishStart,
                    SemanticProgressFields {
                        rows_processed: Some(conversations_processed),
                        rows_total: Some(plan.total_conversations),
                        last_conversation_id: Some(plan.last_offset),
                        last_message_id,
                        note: Some(final_path.display().to_string()),
                        ..Default::default()
                    },
                );
            }
            // Contract-replacement batches keep the old complete generation
            // published while staging. Retire it only at the actual publication
            // boundary, immediately before the fixed destination is replaced.
            if retained_generation {
                self.revoke_backfill_serving(data_dir, manifest)?;
            }
            // Keep the WAL parked even if publication fails: a failure may
            // follow the rename (for example directory fsync), so restoring it
            // could attach the old WAL to a new main file. Readiness stays
            // revoked until a later exact reconciliation succeeds.
            park_semantic_destination_wal(&final_path)?;
            publish_reconciled_semantic_index(&staging_path, &final_path)?;
            let published_index = FsVectorIndex::open(&final_path)
                .map_err(|err| anyhow::anyhow!("open published semantic index failed: {err}"))?;
            if validated_semantic_index_ids(&published_index)? != staged_doc_ids {
                bail!("published semantic identities differ from the validated candidate");
            }
            let size_bytes = fs::metadata(&final_path)
                .with_context(|| format!("stat published semantic index {}", final_path.display()))?
                .len();
            let relative_index_path = final_path
                .strip_prefix(data_dir)
                .unwrap_or(final_path.as_path())
                .to_string_lossy()
                .to_string();
            manifest.publish_artifact(ArtifactRecord {
                tier: plan.tier,
                embedder_id: self.embedder_id().to_string(),
                model_revision: plan.model_revision,
                schema_version: SEMANTIC_SCHEMA_VERSION,
                chunking_version: CHUNKING_STRATEGY_VERSION,
                dimension: self.embedder_dimension(),
                doc_count: u64::try_from(published_index.record_count()).unwrap_or(u64::MAX),
                conversation_count: conversations_processed,
                db_fingerprint: plan.db_fingerprint,
                index_path: relative_index_path,
                size_bytes,
                started_at_ms: prior_checkpoint
                    .as_ref()
                    .map_or_else(now_ms, |checkpoint| checkpoint.saved_at_ms),
                completed_at_ms: now_ms(),
                ready: true,
            });
            manifest.refresh_backlog(plan.total_conversations, &db_fingerprint);
            manifest.save(data_dir)?;
            if sink.is_active() {
                sink.emit(
                    SemanticProgressEvent::PublishDone,
                    SemanticProgressFields {
                        rows_processed: Some(conversations_processed),
                        rows_total: Some(plan.total_conversations),
                        last_conversation_id: Some(plan.last_offset),
                        last_message_id,
                        note: Some(final_path.display().to_string()),
                        ..Default::default()
                    },
                );
            }
        } else {
            let docs_embedded_on_disk =
                u64::try_from(staged_index.record_count()).unwrap_or(u64::MAX);
            let checkpoint_docs = prior_docs
                .saturating_add(embedded_docs)
                .max(docs_embedded_on_disk);
            if sink.is_active() {
                sink.emit(
                    SemanticProgressEvent::CheckpointSaveStart,
                    SemanticProgressFields {
                        rows_processed: Some(conversations_processed),
                        rows_total: Some(plan.total_conversations),
                        last_conversation_id: Some(plan.last_offset),
                        last_message_id,
                        ..Default::default()
                    },
                );
            }
            // Preserve any existing `last_message_id` cursor when the
            // caller did not supply a fresher one — see sub-fix 2 for
            // why durable message-PK resume matters.
            let prior_last_message_id = prior_checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.last_message_id);
            manifest.save_checkpoint(BuildCheckpoint {
                tier: plan.tier,
                embedder_id: self.embedder_id().to_string(),
                last_offset: plan.last_offset,
                docs_embedded: checkpoint_docs,
                conversations_processed,
                total_conversations: plan.total_conversations,
                db_fingerprint: plan.db_fingerprint,
                schema_version: SEMANTIC_SCHEMA_VERSION,
                chunking_version: CHUNKING_STRATEGY_VERSION,
                saved_at_ms: now_ms(),
                last_message_id: last_message_id.or(prior_last_message_id),
                cursor_exhausted: plan.cursor_exhausted,
            });
            manifest.save(data_dir)?;
            if sink.is_active() {
                sink.emit(
                    SemanticProgressEvent::CheckpointSaveDone,
                    SemanticProgressFields {
                        rows_processed: Some(conversations_processed),
                        rows_total: Some(plan.total_conversations),
                        last_conversation_id: Some(plan.last_offset),
                        last_message_id: last_message_id.or(prior_last_message_id),
                        ..Default::default()
                    },
                );
            }
        }

        Ok(SemanticBackfillBatchOutcome {
            tier: plan.tier,
            embedder_id: self.embedder_id().to_string(),
            embedded_docs,
            conversations_processed,
            total_conversations: plan.total_conversations,
            last_offset: plan.last_offset,
            checkpoint_saved: !complete,
            published: complete,
            unchanged: false,
            index_path: if complete { final_path } else { staging_path },
            manifest_path,
        })
    }

    #[cfg(test)]
    pub fn run_backfill_from_storage(
        &self,
        storage: &FrankenStorage,
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: SemanticBackfillStoragePlan,
    ) -> Result<SemanticBackfillBatchOutcome> {
        self.run_backfill_from_storage_with_sink(
            storage,
            data_dir,
            manifest,
            plan,
            &SemanticProgressSink::disabled(),
        )
    }

    /// Run storage backfill while emitting semantic
    /// progress events to a JSONL sink and persists `last_message_id`
    /// in the resumable checkpoint. The sink is silent unless
    /// `CASS_SEMANTIC_PROGRESS_JSONL` is set.
    pub fn run_backfill_from_storage_with_sink(
        &self,
        storage: &FrankenStorage,
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: SemanticBackfillStoragePlan,
        sink: &SemanticProgressSink,
    ) -> Result<SemanticBackfillBatchOutcome> {
        self.run_backfill_from_storage_with_caps_and_sink(
            storage,
            data_dir,
            manifest,
            plan,
            SemanticCheckpointCaps::unlimited(),
            sink,
        )
    }

    /// Variant of [`run_backfill_from_storage_with_sink`] for CLI backfill
    /// runs. It applies operator checkpoint caps from
    /// `CASS_SEMANTIC_MAX_MESSAGES_PER_CHECKPOINT` and
    /// `CASS_SEMANTIC_MAX_BYTES_PER_CHECKPOINT` while keeping each selected
    /// conversation whole, so message-cursor resume cannot strand the tail of
    /// a partially selected conversation.
    pub fn run_capped_backfill_from_storage_with_sink(
        &self,
        storage: &FrankenStorage,
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: SemanticBackfillStoragePlan,
        sink: &SemanticProgressSink,
    ) -> Result<SemanticBackfillBatchOutcome> {
        self.run_backfill_from_storage_with_caps_and_sink(
            storage,
            data_dir,
            manifest,
            plan,
            SemanticCheckpointCaps::from_env(),
            sink,
        )
    }

    pub(in crate::indexer::semantic::engine) fn reusable_backfill_candidate(
        &self,
        data_dir: &Path,
        manifest: &SemanticManifest,
        plan: &SemanticBackfillStoragePlan,
    ) -> Option<PathBuf> {
        if let Some(checkpoint) = manifest.checkpoint.as_ref().filter(|checkpoint| {
            checkpoint.tier == plan.tier
                && checkpoint.embedder_id == self.embedder_id()
                && checkpoint.schema_version == SEMANTIC_SCHEMA_VERSION
                && checkpoint.chunking_version == CHUNKING_STRATEGY_VERSION
                && semantic_backfill_identity_matches(
                    &checkpoint.db_fingerprint,
                    &plan.db_fingerprint,
                )
        }) {
            let path = semantic_staging_index_path(
                data_dir,
                plan.tier,
                self.embedder_id(),
                &checkpoint.db_fingerprint,
            );
            if path.is_file() {
                return Some(path);
            }
        }
        let artifact = match plan.tier {
            TierKind::Fast => manifest.fast_tier.as_ref(),
            TierKind::Quality => manifest.quality_tier.as_ref(),
        }?;
        let path = vector_index_path(data_dir, self.embedder_id());
        let recorded_path = data_dir.join(&artifact.index_path);
        (artifact.tier == plan.tier
            && artifact.embedder_id == self.embedder_id()
            && artifact.model_revision == plan.model_revision
            && artifact.schema_version == SEMANTIC_SCHEMA_VERSION
            && artifact.chunking_version == CHUNKING_STRATEGY_VERSION
            && artifact.dimension == self.embedder_dimension()
            && semantic_backfill_identity_matches(&artifact.db_fingerprint, &plan.db_fingerprint)
            && recorded_path == path
            && path.is_file())
        .then_some(path)
    }

    fn revoke_backfill_serving(
        &self,
        data_dir: &Path,
        manifest: &mut SemanticManifest,
    ) -> Result<()> {
        // Complete shard metadata can override an unready monolithic record.
        // Revoke both authorities before scanning or changing any candidate.
        invalidate_identity_stale_semantic_shards(data_dir, self.embedder_id())?;
        for artifact in [&mut manifest.fast_tier, &mut manifest.quality_tier]
            .into_iter()
            .flatten()
        {
            if artifact.embedder_id == self.embedder_id() {
                artifact.ready = false;
            }
        }
        if let Some(hnsw) = manifest.hnsw.as_mut()
            && hnsw.embedder_id == self.embedder_id()
        {
            hnsw.ready = false;
        }
        manifest.save(data_dir)?;
        Ok(())
    }

    /// A same-corpus contract replacement is private build work, not evidence
    /// that the last complete generation became invalid. Keep its ledger and
    /// ANN association intact until the replacement reaches the publication
    /// boundary. Query admission remains independently strict: this does not
    /// make an old vector-space revision writable or current for this binary.
    fn retain_published_generation_during_replacement(
        &self,
        manifest: &SemanticManifest,
        tier: TierKind,
        db_fingerprint: &str,
        model_revision: &str,
    ) -> bool {
        let artifact = match tier {
            TierKind::Fast => manifest.fast_tier.as_ref(),
            TierKind::Quality => manifest.quality_tier.as_ref(),
        };
        artifact.is_some_and(|artifact| {
            artifact.ready
                && artifact.embedder_id == self.embedder_id()
                && artifact.db_fingerprint == db_fingerprint
                && artifact.dimension == self.embedder_dimension()
                && (artifact.model_revision != model_revision
                    || artifact.schema_version != SEMANTIC_SCHEMA_VERSION
                    || artifact.chunking_version != CHUNKING_STRATEGY_VERSION)
        })
    }

    fn prepare_backfill_serving(
        &self,
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: &SemanticBackfillStoragePlan,
    ) -> Result<()> {
        if self.retain_published_generation_during_replacement(
            manifest,
            plan.tier,
            &plan.db_fingerprint,
            &plan.model_revision,
        ) {
            tracing::info!(
                tier = plan.tier.as_str(),
                embedder = self.embedder_id(),
                "retaining complete semantic generation while replacement contract builds"
            );
            return Ok(());
        }
        self.revoke_backfill_serving(data_dir, manifest)
    }

    /// Reuse exact canonical identities, never a count or a conversation tail
    /// as evidence that an older vector still describes the current row.
    /// Only one replayed conversation and the capped embedding delta retain
    /// message text. Covered prefixes consume no embedding batch budget.
    fn reconcile_backfill_from_storage(
        &self,
        storage: &FrankenStorage,
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: SemanticBackfillStoragePlan,
        candidate: (&Path, SemanticCheckpointCaps),
        sink: &SemanticProgressSink,
    ) -> Result<SemanticBackfillBatchOutcome> {
        self.check_cancelled()?;
        let (candidate_path, caps) = candidate;
        let snapshot_dir = tempfile::Builder::new()
            .prefix(".backfill-reuse-")
            .tempdir_in(data_dir.join(VECTOR_INDEX_DIR))?;
        let snapshot_path = snapshot_dir.path().join("candidate.fsvi");
        fs::copy(candidate_path, &snapshot_path).context("snapshot semantic backfill candidate")?;
        let candidate_wal = fsvi_wal_path_for(candidate_path);
        if candidate_wal.is_file() {
            fs::copy(candidate_wal, fsvi_wal_path_for(&snapshot_path))
                .context("snapshot semantic backfill candidate WAL")?;
        }
        let mut snapshot = FsVectorIndex::open_writer(&snapshot_path)
            .map_err(|err| anyhow::anyhow!("open semantic backfill candidate: {err}"))?;
        // Historical hash indexes used the generic "1.0" revision. Those
        // vectors cannot be appended to or served as the current space, but
        // an explicit canonical backfill is the supported way to replace
        // them. Restart coverage instead of repeatedly rejecting the only
        // command capable of producing a compatible artifact.
        if self.embedder_id() == "fnv1a-384"
            && snapshot.embedder_id() == self.embedder_id()
            && snapshot.dimension() == self.embedder_dimension()
            && snapshot.embedder_revision() == "1.0"
        {
            drop(snapshot);
            tracing::info!(
                reason = "legacy_hash_vector_space",
                "rebuilding semantic coverage from the canonical archive"
            );
            return self.build_backfill_from_storage(storage, data_dir, manifest, plan, caps, sink);
        }
        if snapshot.embedder_id() != self.embedder_id()
            || snapshot.dimension() != self.embedder_dimension()
            || snapshot.embedder_revision() != self.vector_space_revision()?
        {
            bail!("semantic backfill candidate has an incompatible vector space");
        }
        self.prepare_backfill_serving(data_dir, manifest, &plan)?;
        if snapshot.wal_record_count() > 0 {
            snapshot.compact().map_err(|err| {
                anyhow::anyhow!("compact semantic backfill candidate snapshot: {err}")
            })?;
        }
        let mut existing_ids = HashSet::new();
        for record in 0..snapshot.record_count() {
            self.check_cancelled()?;
            if !snapshot.is_deleted(record) {
                let id = snapshot.doc_id_at(record).map_err(|err| {
                    anyhow::anyhow!("read semantic backfill candidate identity: {err}")
                })?;
                if !existing_ids.insert(id.to_owned()) {
                    bail!("semantic backfill candidate contains duplicate document {id}");
                }
            }
        }
        drop(snapshot);

        let total_conversations =
            total_semantic_conversations_with_cancel(storage, self.cancelled.as_ref())?;
        sink.emit(
            SemanticProgressEvent::SelectionStart,
            SemanticProgressFields {
                rows_total: Some(total_conversations),
                note: Some("reconcile exact canonical document identities".into()),
                ..Default::default()
            },
        );
        let (selection, replay_proof) =
            self.replay_cache
                .select(self, storage, data_dir, &plan, &existing_ids, caps, sink)?;
        let replay_cache::ReconcileSelection {
            current_ids,
            selected_ids,
            inputs,
            selected_conversations,
            covered_conversations,
            last_covered_conversation,
            last_message_id,
            ..
        } = selection;
        let mut covered_ids: HashSet<String> =
            existing_ids.intersection(&current_ids).cloned().collect();
        covered_ids.extend(selected_ids);
        let complete = covered_ids == current_ids;
        sink.emit(
            SemanticProgressEvent::SelectionDone,
            SemanticProgressFields {
                conversations_in_batch: Some(saturating_u64_from_usize(selected_conversations)),
                rows_processed: Some(saturating_u64_from_usize(covered_ids.len())),
                rows_total: Some(saturating_u64_from_usize(current_ids.len())),
                ..Default::default()
            },
        );
        let embeddings = self.embed_messages_with_sink(&inputs, sink)?;
        // Every descriptor-bound selection, cached or freshly replayed, remains
        // valid only while the canonical archive is unchanged, including across
        // a potentially long embedding batch.
        replay_proof.verify(storage)?;
        let embedded_docs = saturating_u64_from_usize(embeddings.len());
        let staging_path = semantic_staging_index_path(
            data_dir,
            plan.tier,
            self.embedder_id(),
            &plan.db_fingerprint,
        );
        self.check_cancelled()?;
        let staged_index = self.reconcile_in_private_directory(
            embeddings,
            &staging_path,
            &covered_ids,
            // Move the actual TempDir owner, not a reusable path flag. The
            // existing snapshot and its compacted vectors become the candidate.
            || Ok(snapshot_dir),
        )?;
        let outcome = self.finish_backfill_batch(
            staged_index,
            data_dir,
            manifest,
            SemanticBackfillBatchPlan {
                tier: plan.tier,
                db_fingerprint: plan.db_fingerprint,
                model_revision: plan.model_revision,
                total_conversations,
                conversations_in_batch: covered_conversations,
                last_offset: last_covered_conversation,
                cursor_exhausted: complete,
            },
            BackfillBatchProgress {
                prior_checkpoint: None,
                embedded_docs,
                last_message_id,
            },
            sink,
        )?;
        sink.emit(
            SemanticProgressEvent::Complete,
            SemanticProgressFields {
                rows_processed: Some(outcome.conversations_processed),
                rows_total: Some(outcome.total_conversations),
                last_conversation_id: Some(outcome.last_offset),
                last_message_id,
                note: Some(
                    if outcome.published {
                        "published"
                    } else {
                        "checkpointed"
                    }
                    .into(),
                ),
                ..Default::default()
            },
        );
        Ok(outcome)
    }

    /// Reuse the canonical fingerprint only when the completed artifact and
    /// descriptor-bound archive/WAL observations still match. This lets the
    /// CLI avoid a full canonical count scan before an unchanged backfill.
    pub(crate) fn completed_backfill_fingerprint(
        &self,
        storage: &FrankenStorage,
        data_dir: &Path,
        manifest: &SemanticManifest,
        tier: TierKind,
        model_revision: &str,
    ) -> Result<Option<String>> {
        let artifact = match tier {
            TierKind::Fast => manifest.fast_tier.as_ref(),
            TierKind::Quality => manifest.quality_tier.as_ref(),
        };
        let Some(artifact) = artifact else {
            return Ok(None);
        };
        // An identity-rebuild fingerprint is not a canonical fingerprint.
        // That finalization may still have work to do after publication.
        if !artifact.db_fingerprint.starts_with("content-v1:") {
            return Ok(None);
        }
        let plan = SemanticBackfillStoragePlan {
            tier,
            db_fingerprint: artifact.db_fingerprint.clone(),
            model_revision: model_revision.into(),
            max_conversations: 1,
        };
        Ok(self
            .completed_backfill_cache(storage, data_dir, manifest, &plan)?
            .map(|(_, cache)| cache.artifact.db_fingerprint))
    }

    fn completed_backfill_cache(
        &self,
        storage: &FrankenStorage,
        data_dir: &Path,
        manifest: &SemanticManifest,
        plan: &SemanticBackfillStoragePlan,
    ) -> Result<Option<(PathBuf, CompletedBackfillCache)>> {
        let archive_before = BackfillFilePair::archive(storage);
        let candidate = self.reusable_backfill_candidate(data_dir, manifest, plan);
        let cache_path = data_dir.join(VECTOR_INDEX_DIR).join(format!(
            ".completed-backfill-{}-{}.json",
            plan.tier.as_str(),
            self.embedder_id()
        ));
        let artifact = match plan.tier {
            TierKind::Fast => manifest.fast_tier.as_ref(),
            TierKind::Quality => manifest.quality_tier.as_ref(),
        };
        if manifest.checkpoint.is_none()
            && let (Some(archive), Some(path), Some(artifact)) =
                (archive_before.as_ref(), candidate, artifact)
            && artifact.ready
            && artifact.db_fingerprint == plan.db_fingerprint
            && let Ok(file) = fs::File::open(&cache_path)
            && let Ok(cache) = serde_json::from_reader::<_, CompletedBackfillCache>(
                std::io::Read::take(file, 64 * 1024),
            )
            && cache.version == 1
            && cache.archive == *archive
            && cache.artifact == *artifact
            && cache.vector_space_revision == self.vector_space_revision()?
            && BackfillFilePair::capture(&path, &fsvi_wal_path_for(&path)).as_ref()
                == Some(&cache.vectors)
            && BackfillFilePair::archive(storage).as_ref() == Some(archive)
        {
            return Ok(Some((path, cache)));
        }
        Ok(None)
    }

    pub(in crate::indexer::semantic::engine) fn run_backfill_from_storage_with_caps_and_sink(
        &self,
        storage: &FrankenStorage,
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: SemanticBackfillStoragePlan,
        caps: SemanticCheckpointCaps,
        sink: &SemanticProgressSink,
    ) -> Result<SemanticBackfillBatchOutcome> {
        self.check_cancelled()?;
        let archive_before = BackfillFilePair::archive(storage);
        let candidate = self.reusable_backfill_candidate(data_dir, manifest, &plan);
        let cache_path = data_dir.join(VECTOR_INDEX_DIR).join(format!(
            ".completed-backfill-{}-{}.json",
            plan.tier.as_str(),
            self.embedder_id()
        ));
        if let Some((path, cache)) =
            self.completed_backfill_cache(storage, data_dir, manifest, &plan)?
        {
            let artifact = cache.artifact;
            sink.emit(
                SemanticProgressEvent::Complete,
                SemanticProgressFields {
                    rows_processed: Some(artifact.conversation_count),
                    rows_total: Some(artifact.conversation_count),
                    note: Some("unchanged: canonical archive and completed vectors match".into()),
                    ..Default::default()
                },
            );
            return Ok(SemanticBackfillBatchOutcome {
                tier: plan.tier,
                embedder_id: self.embedder_id().into(),
                embedded_docs: 0,
                conversations_processed: artifact.conversation_count,
                total_conversations: artifact.conversation_count,
                last_offset: cache.last_offset,
                checkpoint_saved: false,
                published: true,
                unchanged: true,
                index_path: path,
                manifest_path: SemanticManifest::path(data_dir),
            });
        }
        let tier = plan.tier;
        let outcome = if let Some(candidate) = candidate {
            self.reconcile_backfill_from_storage(
                storage,
                data_dir,
                manifest,
                plan,
                (&candidate, caps),
                sink,
            )?
        } else {
            self.build_backfill_from_storage(storage, data_dir, manifest, plan, caps, sink)?
        };
        // Publish a cache only when the entire canonical scan observed a
        // stable archive. This is a skip hint, never serving authority: every
        // manifest, producer and artifact check above must also agree.
        if outcome.published
            && let Some(archive) = archive_before
            && BackfillFilePair::archive(storage).as_ref() == Some(&archive)
            && let Some(vectors) = BackfillFilePair::capture(
                &outcome.index_path,
                &fsvi_wal_path_for(&outcome.index_path),
            )
            && let Some(artifact) = match tier {
                TierKind::Fast => manifest.fast_tier.as_ref(),
                TierKind::Quality => manifest.quality_tier.as_ref(),
            }
        {
            let cache = CompletedBackfillCache {
                version: 1,
                archive,
                vectors,
                artifact: artifact.clone(),
                vector_space_revision: self.vector_space_revision()?.into(),
                last_offset: outcome.last_offset,
            };
            let saved = (|| -> Result<()> {
                let mut staged = tempfile::NamedTempFile::new_in(data_dir.join(VECTOR_INDEX_DIR))?;
                serde_json::to_writer(staged.as_file_mut(), &cache)?;
                staged.as_file().sync_all()?;
                staged.persist(&cache_path)?;
                Ok(())
            })();
            if let Err(error) = saved {
                tracing::debug!(%error, "completed backfill cache unavailable; next run will reconcile");
            }
        }
        Ok(outcome)
    }

    fn build_backfill_from_storage(
        &self,
        storage: &FrankenStorage,
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: SemanticBackfillStoragePlan,
        caps: SemanticCheckpointCaps,
        sink: &SemanticProgressSink,
    ) -> Result<SemanticBackfillBatchOutcome> {
        // A cursor without its vectors proves no coverage. This includes a
        // crash after moving staging to live but before saving the manifest.
        if manifest.checkpoint.as_ref().is_some_and(|checkpoint| {
            checkpoint.tier == plan.tier && checkpoint.embedder_id == self.embedder_id()
        }) {
            manifest.checkpoint = None;
        }
        self.prepare_backfill_serving(data_dir, manifest, &plan)?;
        let prior_checkpoint = manifest.checkpoint.as_ref().filter(|checkpoint| {
            checkpoint.tier == plan.tier
                && checkpoint.embedder_id == self.embedder_id()
                && checkpoint.is_valid(&plan.db_fingerprint)
        });
        let after_conversation_id = prior_checkpoint.map_or(0, |checkpoint| checkpoint.last_offset);
        let prior_last_message_id =
            prior_checkpoint.and_then(|checkpoint| checkpoint.last_message_id);
        let cached_total_conversations = cached_semantic_total_conversations(
            manifest,
            plan.tier,
            self.embedder_id(),
            &plan.db_fingerprint,
        );

        if sink.is_active() {
            sink.emit(
                SemanticProgressEvent::SelectionStart,
                SemanticProgressFields {
                    last_conversation_id: Some(after_conversation_id),
                    last_message_id: prior_last_message_id,
                    rows_total: Some(saturating_u64_from_usize(plan.max_conversations)),
                    note: Some(plan.tier.as_str().to_string()),
                    ..Default::default()
                },
            );
            sink.emit(
                SemanticProgressEvent::SelectionCountStart,
                SemanticProgressFields {
                    last_conversation_id: Some(after_conversation_id),
                    last_message_id: prior_last_message_id,
                    note: Some(
                        cached_total_conversations
                            .map_or("database", |(_, source)| source)
                            .to_string(),
                    ),
                    ..Default::default()
                },
            );
        }

        let total_conversations = match cached_total_conversations {
            Some((total, _)) => total,
            None => {
                match total_semantic_conversations_with_cancel(storage, self.cancelled.as_ref()) {
                    Ok(total) => total,
                    Err(err) => {
                        if sink.is_active() {
                            sink.emit(
                                SemanticProgressEvent::Error,
                                SemanticProgressFields {
                                    error: Some(format!("selection count: {err}")),
                                    last_conversation_id: Some(after_conversation_id),
                                    last_message_id: prior_last_message_id,
                                    ..Default::default()
                                },
                            );
                        }
                        return Err(err);
                    }
                }
            }
        };

        if sink.is_active() {
            sink.emit(
                SemanticProgressEvent::SelectionCountDone,
                SemanticProgressFields {
                    rows_processed: Some(total_conversations),
                    rows_total: Some(total_conversations),
                    last_conversation_id: Some(after_conversation_id),
                    last_message_id: prior_last_message_id,
                    note: Some(
                        cached_total_conversations
                            .map_or("database", |(_, source)| source)
                            .to_string(),
                    ),
                    ..Default::default()
                },
            );
        }

        let batch = match fetch_canonical_embedding_batch_inner_with_caps_and_total(
            storage,
            after_conversation_id,
            plan.max_conversations,
            prior_last_message_id,
            caps,
            total_conversations,
            Some(sink),
        ) {
            Ok(batch) => batch,
            Err(err) => {
                if sink.is_active() {
                    sink.emit(
                        SemanticProgressEvent::Error,
                        SemanticProgressFields {
                            error: Some(format!("selection: {err}")),
                            last_conversation_id: Some(after_conversation_id),
                            last_message_id: prior_last_message_id,
                            ..Default::default()
                        },
                    );
                }
                return Err(err);
            }
        };

        if sink.is_active() {
            sink.emit(
                SemanticProgressEvent::SelectionDone,
                SemanticProgressFields {
                    last_conversation_id: Some(batch.last_conversation_id),
                    last_message_id: prior_last_message_id,
                    conversations_in_batch: Some(batch.conversations_in_batch),
                    rows_total: Some(batch.total_conversations),
                    ..Default::default()
                },
            );
            // PacketReplay {start,progress,done} bracket the
            // envelope/messages/packet build done by
            // `fetch_canonical_embedding_batch`. We can't easily plumb
            // a callback inside that helper without refactoring it, so
            // the start/done bracket here straddles the work that
            // already happened (replay always finishes before we see
            // the result). A future refactor — flagged in the
            // SQL-shape follow-up — can move the packet-replay work
            // into a streaming iterator and emit `progress` ticks
            // per conversation. For now, the bracket still gives
            // operators a clear "we got past replay" signal.
            sink.emit(
                SemanticProgressEvent::PacketReplayStart,
                SemanticProgressFields {
                    conversations_in_batch: Some(batch.conversations_in_batch),
                    ..Default::default()
                },
            );
            sink.emit(
                SemanticProgressEvent::PacketReplayProgress,
                SemanticProgressFields {
                    conversations_in_batch: Some(batch.conversations_in_batch),
                    rows_processed: Some(saturating_u64_from_usize(batch.inputs.len())),
                    bytes: Some(
                        batch
                            .inputs
                            .iter()
                            .map(|i| saturating_u64_from_usize(i.content.len()))
                            .sum(),
                    ),
                    ..Default::default()
                },
            );
            sink.emit(
                SemanticProgressEvent::PacketReplayDone,
                SemanticProgressFields {
                    conversations_in_batch: Some(batch.conversations_in_batch),
                    rows_processed: Some(saturating_u64_from_usize(batch.inputs.len())),
                    ..Default::default()
                },
            );
        }

        // Compute the freshest `last_message_id` from the inputs we are
        // about to embed. EmbeddingInput.message_id is u64 (canonical
        // message PK); we coerce to i64 since the manifest stores i64.
        let batch_last_message_id = batch
            .inputs
            .iter()
            .map(|input| i64::try_from(input.message_id).unwrap_or(i64::MAX))
            .max();
        let next_last_message_id = match (prior_last_message_id, batch_last_message_id) {
            (Some(prior), Some(batch_max)) => Some(prior.max(batch_max)),
            (Some(prior), None) => Some(prior),
            (None, Some(batch_max)) => Some(batch_max),
            (None, None) => None,
        };

        let outcome = self.run_backfill_batch_with_sink(
            &batch.inputs,
            data_dir,
            manifest,
            SemanticBackfillBatchPlan {
                tier: plan.tier,
                db_fingerprint: plan.db_fingerprint,
                model_revision: plan.model_revision,
                total_conversations: batch.total_conversations,
                conversations_in_batch: batch.conversations_in_batch,
                last_offset: batch.last_conversation_id,
                cursor_exhausted: batch.cursor_exhausted,
            },
            next_last_message_id,
            sink,
        );

        match &outcome {
            Ok(o) => {
                if sink.is_active() {
                    sink.emit(
                        SemanticProgressEvent::Complete,
                        SemanticProgressFields {
                            rows_processed: Some(o.conversations_processed),
                            rows_total: Some(o.total_conversations),
                            last_conversation_id: Some(o.last_offset),
                            last_message_id: next_last_message_id,
                            ..Default::default()
                        },
                    );
                }
            }
            Err(err) => {
                if sink.is_active() {
                    sink.emit(
                        SemanticProgressEvent::Error,
                        SemanticProgressFields {
                            error: Some(err.to_string()),
                            last_conversation_id: Some(batch.last_conversation_id),
                            last_message_id: next_last_message_id,
                            ..Default::default()
                        },
                    );
                }
            }
        }

        outcome
    }

    /// Build or verify an HNSW index for approximate nearest neighbor search.
    ///
    /// A matching persisted native graph is retained without rewriting it.
    /// Missing, invalid, or differently configured graphs are rebuilt from the
    /// existing vectors and read back before success is reported.
    ///
    /// # Arguments
    /// * `vector_index` - The VectorIndex to build HNSW from
    /// * `data_dir` - Directory to save the HNSW index
    /// * `m` - Max connections per node (default: 16)
    /// * `ef_construction` - Search width during build (default: 200)
    ///
    /// # Returns
    /// Path to the saved HNSW index file
    pub fn build_hnsw_index(
        &self,
        vector_index: &FsVectorIndex,
        data_dir: &Path,
        m: Option<usize>,
        ef_construction: Option<usize>,
    ) -> Result<PathBuf> {
        let expected_revision = self.vector_space_revision()?;
        if vector_index.embedder_id() != self.embedder_id()
            || vector_index.dimension() != self.embedder_dimension()
            || vector_index.embedder_revision() != expected_revision
        {
            bail!(
                "cannot build HNSW for an incompatible semantic vector space: expected {} revision {} dimension {}, found {} revision {} dimension {}",
                self.embedder_id(),
                expected_revision,
                self.embedder_dimension(),
                vector_index.embedder_id(),
                vector_index.embedder_revision(),
                vector_index.dimension()
            );
        }
        let config = FsHnswConfig {
            m: m.unwrap_or(FS_HNSW_DEFAULT_M),
            ef_construction: ef_construction.unwrap_or(FS_HNSW_DEFAULT_EF_CONSTRUCTION),
            ..FsHnswConfig::default()
        };
        let hnsw_path = hnsw_index_path(data_dir, self.embedder_id());
        crate::search::ann_index::maintain_hnsw_accelerator(
            &hnsw_path,
            vector_index,
            config,
            false,
        )?;
        Ok(hnsw_path)
    }
}
