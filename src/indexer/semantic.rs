//! Semantic indexing and the ownership boundary for resumable backfill artifacts.
//!
//! The engine performs embedding/publication; this facade holds the artifact
//! lease across each complete backfill call. Cleanup is deliberately outside
//! the engine's error paths: a failed save may have renamed the manifest without
//! making that rename durable, so it must not authorize retiring older staging.

mod artifacts;
mod delegate;
mod engine;
mod replay;
#[cfg(unix)]
mod unchanged;

pub use artifacts::{
    BackfillArtifactCandidate, BackfillArtifactReclaimPlan, BackfillArtifactReclaimReport,
    BackfillManifestChanged, apply_backfill_artifact_plan, plan_backfill_artifacts,
    reclaim_backfill_artifacts,
};
pub(crate) use delegate::SemanticStorageBuildPhase;
pub use engine::*;
// Explicitly select bounded message replay over the engine glob export.
#[cfg(test)]
pub(crate) use replay::packet_embedding_inputs_from_storage_with_progress;
pub(crate) use replay::{
    packet_embedding_inputs_from_storage_for_message_ids,
    packet_embedding_inputs_from_storage_since, visit_packet_embedding_inputs_from_storage,
};

use std::path::Path;

use anyhow::{Context, Result};

use crate::indexer::semantic_progress::SemanticProgressSink;
use crate::search::semantic_manifest::{SemanticManifest, TierKind};
use crate::storage::sqlite::FrankenStorage;

/// Internal parent/worker contract. A nightly worker may finish the active
/// checkpoint, but must not replace another tier's or producer's cursor.
pub(crate) const SCHEDULE_PRESERVE_CHECKPOINT: &str = "CASS_SCHEDULE_PRESERVE_CHECKPOINT";

fn scheduled_worker_preserves_checkpoint() -> bool {
    matches!(
        std::env::var(SCHEDULE_PRESERVE_CHECKPOINT).as_deref(),
        Ok("1")
    )
}

fn ensure_scheduled_checkpoint_owner(
    manifest: &SemanticManifest,
    tier: TierKind,
    embedder_id: &str,
) -> Result<()> {
    anyhow::ensure!(
        manifest.checkpoint.as_ref().is_none_or(|checkpoint| {
            checkpoint.tier == tier && checkpoint.embedder_id == embedder_id
        }),
        "semantic_checkpoint_owned: another tier or embedding space has unfinished work; resume its configured provider before switching; the scheduled worker did not replace its checkpoint"
    );
    Ok(())
}

/// Admit a scheduled command under its canonical maintenance lock, before
/// loading model weights or opening writable storage. Keep this exact ledger
/// snapshot through the later artifact-lease validation. Stored identities
/// only constrain an explicitly selected provider; they never select one.
pub(crate) fn load_scheduled_checkpoint(
    data_dir: &Path,
    tier: TierKind,
    selected: &str,
    retained_identity: Option<&str>,
) -> Result<SemanticManifest> {
    let manifest = SemanticManifest::load(data_dir)
        .context("semantic_checkpoint_unreadable: scheduled backfill retained the ledger")?
        .unwrap_or_default();
    if let Some(checkpoint) = &manifest.checkpoint {
        // Reject a foreign tier before even resolving the provider name.
        ensure_scheduled_checkpoint_owner(&manifest, tier, &checkpoint.embedder_id)?;
        let identity = if let Some(id) = retained_identity {
            id.to_owned()
        } else {
            crate::search::embedder_registry::EmbedderRegistry::new(data_dir)
                .get(selected)
                .context("semantic_checkpoint_owned: selected provider has no registered identity")?
                .id
                .to_owned()
        };
        ensure_scheduled_checkpoint_owner(&manifest, tier, &identity)?;
    }
    Ok(manifest)
}

/// Semantic indexer with lock-scoped ownership of backfill scratch artifacts.
/// Embedding and non-backfill operations retain the engine's existing API.
pub struct SemanticIndexer {
    inner: engine::SemanticIndexer,
}

impl SemanticIndexer {
    pub fn new(embedder_type: &str, data_dir: Option<&Path>) -> Result<Self> {
        engine::SemanticIndexer::new(embedder_type, data_dir).map(|inner| Self { inner })
    }

    /// Retain the existing artifact lease and checkpoint/publication path while
    /// sharing cancellation with the job owner. Only local providers exist.
    pub fn new_with_cancel(
        embedder_type: &str,
        data_dir: Option<&Path>,
        cancelled: CancelCheck,
    ) -> Result<Self> {
        engine::SemanticIndexer::new_with_cancel(embedder_type, data_dir, cancelled)
            .map(|inner| Self { inner })
    }

    pub fn with_batch_size(self, batch_size: usize) -> Result<Self> {
        let Self { inner } = self;
        inner
            .with_batch_size(batch_size)
            .map(|inner| Self { inner })
    }

    fn admit_scheduled_checkpoint(
        &self,
        manifest: &SemanticManifest,
        tier: TierKind,
    ) -> Result<()> {
        if scheduled_worker_preserves_checkpoint() {
            ensure_scheduled_checkpoint_owner(manifest, tier, self.inner.embedder_id())?;
        }
        Ok(())
    }

    fn with_backfill_artifacts<F>(
        &self,
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        run: F,
    ) -> Result<SemanticBackfillBatchOutcome>
    where
        F: FnOnce(
            &engine::SemanticIndexer,
            &mut SemanticManifest,
        ) -> Result<SemanticBackfillBatchOutcome>,
    {
        self.inner.check_cancelled()?;
        // begin validates this exact input checkpoint against the durable
        // ledger under its lease BEFORE reclamation. A new owner appearing
        // after the scheduler planned the run therefore cannot be overwritten.
        let artifacts = artifacts::BackfillArtifacts::begin(data_dir, manifest)?;
        // The ownership lease may have waited behind another writer.
        self.inner.check_cancelled()?;
        let result = run(&self.inner, manifest);
        if let Ok(outcome) = &result {
            // A writer returns after manifest.save's file/directory fsync;
            // a proved no-op retains the already durable publication. Temporary
            // snapshots/readers have closed in either case.
            // Never do this in Drop: an error or unwind is not a durable commit.
            artifacts.after_success(manifest, &outcome.index_path);
        }
        result
    }

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

    pub fn run_backfill_batch_with_sink(
        &self,
        messages: &[EmbeddingInput],
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: SemanticBackfillBatchPlan,
        last_message_id: Option<i64>,
        sink: &SemanticProgressSink,
    ) -> Result<SemanticBackfillBatchOutcome> {
        self.admit_scheduled_checkpoint(manifest, plan.tier)?;
        self.with_backfill_artifacts(data_dir, manifest, |engine, manifest| {
            engine.run_backfill_batch_with_sink(
                messages,
                data_dir,
                manifest,
                plan,
                last_message_id,
                sink,
            )
        })
    }

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

    pub fn run_backfill_from_storage_with_sink(
        &self,
        storage: &FrankenStorage,
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: SemanticBackfillStoragePlan,
        sink: &SemanticProgressSink,
    ) -> Result<SemanticBackfillBatchOutcome> {
        self.admit_scheduled_checkpoint(manifest, plan.tier)?;
        self.with_backfill_artifacts(data_dir, manifest, |engine, manifest| {
            #[cfg(unix)]
            if let Some(outcome) =
                unchanged::try_retain_completed(engine, storage, data_dir, manifest, &plan, sink)?
            {
                return Ok(outcome);
            }
            engine.run_backfill_from_storage_with_sink(storage, data_dir, manifest, plan, sink)
        })
    }

    pub fn run_capped_backfill_from_storage_with_sink(
        &self,
        storage: &FrankenStorage,
        data_dir: &Path,
        manifest: &mut SemanticManifest,
        plan: SemanticBackfillStoragePlan,
        sink: &SemanticProgressSink,
    ) -> Result<SemanticBackfillBatchOutcome> {
        self.admit_scheduled_checkpoint(manifest, plan.tier)?;
        self.with_backfill_artifacts(data_dir, manifest, |engine, manifest| {
            #[cfg(unix)]
            if let Some(outcome) =
                unchanged::try_retain_completed(engine, storage, data_dir, manifest, &plan, sink)?
            {
                return Ok(outcome);
            }
            engine
                .run_capped_backfill_from_storage_with_sink(storage, data_dir, manifest, plan, sink)
        })
    }
}

#[cfg(test)]
mod artifact_lifecycle_tests;

#[cfg(test)]
mod scheduled_admission_tests {
    use super::*;

    #[test]
    fn scheduled_admission_uses_retained_identity_and_keeps_the_admitted_snapshot() {
        use crate::search::semantic_manifest::BuildCheckpoint;
        let dir = tempfile::tempdir().unwrap();
        // Without a checkpoint, normal provider initialization owns admission.
        // Even an unrecognized provider is not resolved by this read-only step.
        assert!(
            load_scheduled_checkpoint(dir.path(), TierKind::Fast, "not-a-provider", None)
                .unwrap()
                .checkpoint
                .is_none()
        );
        let mut manifest = SemanticManifest {
            checkpoint: Some(BuildCheckpoint {
                tier: TierKind::Quality,
                embedder_id: "retained-provider".into(),
                last_offset: 1,
                docs_embedded: 1,
                conversations_processed: 1,
                total_conversations: 3,
                db_fingerprint: "old-archive-fingerprint".into(),
                schema_version: crate::search::policy::SEMANTIC_SCHEMA_VERSION,
                chunking_version: crate::search::policy::CHUNKING_STRATEGY_VERSION,
                saved_at_ms: 1,
                last_message_id: Some(1),
                cursor_exhausted: false,
            }),
            ..SemanticManifest::default()
        };
        manifest.save(dir.path()).unwrap();
        let before = std::fs::read(SemanticManifest::path(dir.path())).unwrap();
        // Use the already initialized provider; don't re-resolve its name
        // in the middle of a multi-batch command.
        let admitted = load_scheduled_checkpoint(
            dir.path(),
            TierKind::Quality,
            "any-provider",
            Some("retained-provider"),
        )
        .unwrap();
        assert_eq!(admitted.checkpoint, manifest.checkpoint);
        assert_eq!(
            std::fs::read(SemanticManifest::path(dir.path())).unwrap(),
            before
        );
        assert!(
            load_scheduled_checkpoint(
                dir.path(),
                TierKind::Quality,
                "any-provider",
                Some("different-retained-provider"),
            )
            .is_err()
        );

        // A writer after planning/admission must cause a lease refusal rather
        // than have its checkpoint silently loaded and treated as permission.
        manifest.checkpoint.as_mut().unwrap().embedder_id = "concurrent-provider".into();
        manifest.save(dir.path()).unwrap();
        let current = std::fs::read(SemanticManifest::path(dir.path())).unwrap();
        let error = match artifacts::BackfillArtifacts::begin(dir.path(), &admitted) {
            Ok(_) => panic!("stale scheduled admission must not enter reclamation"),
            Err(error) => error,
        };
        assert!(error.downcast_ref::<BackfillManifestChanged>().is_some());
        assert_eq!(
            std::fs::read(SemanticManifest::path(dir.path())).unwrap(),
            current
        );
    }

    #[test]
    fn scheduled_guard_requires_both_the_checkpoint_tier_and_producer() {
        use crate::search::semantic_manifest::BuildCheckpoint;
        let mut manifest = SemanticManifest::default();
        assert!(ensure_scheduled_checkpoint_owner(&manifest, TierKind::Fast, "fnv1a-384").is_ok());
        manifest.checkpoint = Some(BuildCheckpoint {
            tier: TierKind::Quality,
            embedder_id: "original-producer".into(),
            last_offset: 1,
            docs_embedded: 1,
            conversations_processed: 1,
            total_conversations: 3,
            db_fingerprint: "old-archive-fingerprint".into(),
            schema_version: crate::search::policy::SEMANTIC_SCHEMA_VERSION,
            chunking_version: crate::search::policy::CHUNKING_STRATEGY_VERSION,
            saved_at_ms: 1,
            last_message_id: Some(1),
            cursor_exhausted: false,
        });
        let before = serde_json::to_vec(&manifest).unwrap();
        for (tier, id) in [
            (TierKind::Fast, "original-producer"),
            (TierKind::Fast, "fnv1a-384"),
            (TierKind::Quality, "changed-producer"),
        ] {
            let error = ensure_scheduled_checkpoint_owner(&manifest, tier, id).unwrap_err();
            assert!(error.to_string().contains("semantic_checkpoint_owned"));
            assert_eq!(serde_json::to_vec(&manifest).unwrap(), before);
        }
        assert!(
            ensure_scheduled_checkpoint_owner(&manifest, TierKind::Quality, "original-producer")
                .is_ok()
        );
        assert_eq!(serde_json::to_vec(&manifest).unwrap(), before);
    }

    fn worker_checkpoint() -> crate::search::semantic_manifest::BuildCheckpoint {
        crate::search::semantic_manifest::BuildCheckpoint {
            tier: TierKind::Fast,
            embedder_id: "fnv1a-384".into(),
            last_offset: 1,
            docs_embedded: 1,
            conversations_processed: 1,
            total_conversations: 3,
            db_fingerprint: "worker-fixture".into(),
            schema_version: crate::search::policy::SEMANTIC_SCHEMA_VERSION,
            chunking_version: crate::search::policy::CHUNKING_STRATEGY_VERSION,
            saved_at_ms: 1,
            last_message_id: Some(1),
            cursor_exhausted: false,
        }
    }

    #[test]
    fn scheduled_admission_refuses_foreign_and_corrupt_ledgers() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut checkpoint = worker_checkpoint();
        checkpoint.embedder_id = "another-producer".into();
        let mut manifest = SemanticManifest {
            checkpoint: Some(checkpoint),
            ..SemanticManifest::default()
        };
        manifest.save(dir.path())?;
        let path = SemanticManifest::path(dir.path());
        let before = std::fs::read(&path)?;
        let error = match load_scheduled_checkpoint(
            dir.path(),
            TierKind::Fast,
            "hash",
            Some("fnv1a-384"),
        ) {
            Ok(_) => panic!("foreign ownership was admitted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("semantic_checkpoint_owned"));
        assert_eq!(std::fs::read(&path)?, before);

        std::fs::write(&path, b"{broken-checkpoint")?;
        let error = match load_scheduled_checkpoint(
            dir.path(),
            TierKind::Fast,
            "hash",
            Some("fnv1a-384"),
        ) {
            Ok(_) => panic!("unreadable ownership was admitted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("semantic_checkpoint_unreadable"));
        assert_eq!(std::fs::read(&path)?, b"{broken-checkpoint");
        // Ordinary foreground construction does not acquire this restriction.
        let foreground = SemanticIndexer::new("hash", Some(dir.path()))?;
        assert_eq!(foreground.embedder_id(), "fnv1a-384");
        assert_eq!(std::fs::read(&path)?, b"{broken-checkpoint");
        Ok(())
    }

    #[test]
    fn durable_batches_still_publish_and_release_the_cursor() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let worker = SemanticIndexer::new("hash", Some(dir.path()))?;
        let mut manifest = SemanticManifest::default();
        // Progress estimates are not cursor/publication authority.
        manifest.backlog.total_conversations = 2;
        let plan = |last_offset, exhausted| SemanticBackfillBatchPlan {
            tier: TierKind::Fast,
            db_fingerprint: "worker-fixture".into(),
            model_revision: "hash-fixture".into(),
            total_conversations: 2,
            conversations_in_batch: 1,
            last_offset,
            cursor_exhausted: exhausted,
        };
        let first = worker.run_backfill_batch(
            &[EmbeddingInput::new(1, "first durable worker message")],
            dir.path(),
            &mut manifest,
            plan(1, false),
        )?;
        assert!(first.checkpoint_saved && !first.published);
        let second = worker.run_backfill_batch(
            &[EmbeddingInput::new(2, "second durable worker message")],
            dir.path(),
            &mut manifest,
            plan(2, true),
        )?;
        assert!(second.published);
        assert!(manifest.checkpoint.is_none());
        let index = frankensearch::index::VectorIndex::open_read_only(&second.index_path)?;
        assert_eq!(index.record_count(), 2);
        assert_eq!(index.embedder_id(), "fnv1a-384");
        Ok(())
    }
}
