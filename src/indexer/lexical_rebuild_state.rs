use super::*;

pub(crate) use crate::storage::lexical_checkpoint_io::{
    LexicalRebuildDbState, lexical_rebuild_state_path,
};
pub use crate::storage::run_lock::IndexRunBusy;
pub(crate) use crate::storage::run_lock::{
    ArchiveAdmission, acquire_archive_write_lease, acquire_index_run_lock_with_archive_admission,
    acquire_index_run_lock_with_job_kind,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::indexer) struct PendingLexicalCommit {
    pub(in crate::indexer) next_offset: i64,
    #[serde(default)]
    pub(in crate::indexer) next_conversation_id: Option<i64>,
    pub(in crate::indexer) processed_conversations: usize,
    pub(in crate::indexer) indexed_docs: usize,
    pub(in crate::indexer) base_meta_fingerprint: Option<String>,
    /// The share of `indexed_docs` that is search-region documents. Only the
    /// terminal publication commits region documents; streaming commits carry
    /// zero. Recorded with the pending commit so a landed terminal commit
    /// becomes a checkpoint that knows its region share.
    #[serde(default)]
    pub(in crate::indexer) region_docs: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub(crate) struct LexicalRebuildPipelineRuntimeSnapshot {
    pub queue_depth: usize,
    pub inflight_message_bytes: usize,
    pub max_message_bytes_in_flight: usize,
    pub pending_batch_conversations: usize,
    pub pending_batch_message_bytes: usize,
    pub page_prep_workers: usize,
    pub active_page_prep_jobs: usize,
    pub ordered_buffered_pages: usize,
    pub budget_generation: usize,
    pub producer_budget_wait_count: usize,
    pub producer_budget_wait_ms: usize,
    #[serde(skip)]
    pub producer_budget_waiter_count: usize,
    /// Current park site of the page-production stage (#288). Transient
    /// run-time telemetry only — never persisted into checkpoints.
    #[serde(skip)]
    pub producer_state: String,
    /// Next page sequence the ordered reservation barrier will admit (#288).
    /// Transient run-time telemetry only — never persisted into checkpoints.
    #[serde(skip)]
    pub reservation_next_sequence: u64,
    pub producer_handoff_wait_count: usize,
    pub producer_handoff_wait_ms: usize,
    pub host_loadavg_1m_milli: Option<u32>,
    pub host_available_memory_bytes: Option<u64>,
    pub process_rss_bytes: Option<u64>,
    pub controller_mode: String,
    pub controller_reason: String,
    pub staged_merge_workers_max: usize,
    pub staged_merge_allowed_jobs: usize,
    pub staged_merge_active_jobs: usize,
    pub staged_merge_ready_artifacts: usize,
    pub staged_merge_ready_groups: usize,
    pub staged_merge_controller_reason: String,
    pub staged_shard_build_workers_max: usize,
    pub staged_shard_build_allowed_jobs: usize,
    pub staged_shard_build_active_jobs: usize,
    pub staged_shard_build_pending_jobs: usize,
    pub staged_shard_build_controller_reason: String,
    pub staged_shard_build_memory_reserve_bytes: usize,
    pub staged_shard_build_emergency_memory_reserve_bytes: usize,
    pub staged_shard_build_completed_jobs: usize,
    pub staged_shard_build_last_shard_index: Option<usize>,
    pub staged_shard_build_last_message_bytes: usize,
    pub staged_shard_build_last_index_size_bytes: u64,
    pub staged_shard_build_last_duration_ms: u64,
    pub staged_shard_build_last_amplification_milli: Option<u64>,
    pub staged_shard_build_observed_amplification_milli: Option<u64>,
    pub updated_at_ms: i64,
}

impl LexicalRebuildPipelineRuntimeSnapshot {
    pub(in crate::indexer) fn checkpoint_snapshot(&self) -> Self {
        let mut snapshot = self.clone();
        snapshot.producer_budget_waiter_count = 0;
        snapshot.producer_state = String::new();
        snapshot.reservation_next_sequence = 0;
        snapshot
    }

    pub(in crate::indexer) fn is_observed(&self) -> bool {
        self.updated_at_ms > 0
            || self.queue_depth > 0
            || self.inflight_message_bytes > 0
            || self.pending_batch_conversations > 0
            || self.pending_batch_message_bytes > 0
            || self.page_prep_workers > 0
            || self.active_page_prep_jobs > 0
            || self.ordered_buffered_pages > 0
            || self.budget_generation > 0
            || self.producer_budget_wait_count > 0
            || self.producer_budget_wait_ms > 0
            || self.producer_budget_waiter_count > 0
            || !self.producer_state.is_empty()
            || self.reservation_next_sequence > 0
            || self.producer_handoff_wait_count > 0
            || self.producer_handoff_wait_ms > 0
            || self.host_loadavg_1m_milli.is_some()
            || self.host_available_memory_bytes.is_some()
            || self.process_rss_bytes.is_some()
            || !self.controller_mode.is_empty()
            || !self.controller_reason.is_empty()
            || self.staged_merge_workers_max > 0
            || self.staged_merge_allowed_jobs > 0
            || self.staged_merge_active_jobs > 0
            || self.staged_merge_ready_artifacts > 0
            || self.staged_merge_ready_groups > 0
            || !self.staged_merge_controller_reason.is_empty()
            || self.staged_shard_build_workers_max > 0
            || self.staged_shard_build_allowed_jobs > 0
            || self.staged_shard_build_active_jobs > 0
            || self.staged_shard_build_pending_jobs > 0
            || !self.staged_shard_build_controller_reason.is_empty()
            || self.staged_shard_build_memory_reserve_bytes > 0
            || self.staged_shard_build_emergency_memory_reserve_bytes > 0
            || self.staged_shard_build_completed_jobs > 0
            || self.staged_shard_build_last_shard_index.is_some()
            || self.staged_shard_build_last_message_bytes > 0
            || self.staged_shard_build_last_index_size_bytes > 0
            || self.staged_shard_build_last_duration_ms > 0
            || self.staged_shard_build_last_amplification_milli.is_some()
            || self
                .staged_shard_build_observed_amplification_milli
                .is_some()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::indexer) enum LexicalRebuildExecutionMode {
    #[default]
    SharedWriter,
    /// One Quill writer building in the canonical scratch sibling. Its cursor
    /// must never be applied to the prior live generation if scratch is lost.
    StagedSingleIndex,
    /// An explicitly in-place writer. Unrelated scratch debris must not redirect
    /// pending-commit reconciliation or the next writer away from the live tree.
    LiveSingleIndex,
    StagedShardBuild,
    /// Canonical workspace metadata is changing; all existing documents need
    /// rehydration even though conversation/message counts may stay identical.
    CanonicalMetadataRepair,
}

impl LexicalRebuildExecutionMode {
    pub(in crate::indexer) fn requires_restart_from_zero_on_resume(self) -> bool {
        matches!(self, Self::StagedShardBuild | Self::CanonicalMetadataRepair)
    }

    pub(in crate::indexer) fn as_str(self) -> &'static str {
        match self {
            Self::SharedWriter => "shared_writer",
            Self::StagedSingleIndex => "staged_single_index",
            Self::LiveSingleIndex => "live_single_index",
            Self::StagedShardBuild => "staged_shard_build",
            Self::CanonicalMetadataRepair => "canonical_metadata_repair",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::indexer) struct LexicalRebuildState {
    pub(in crate::indexer) version: u8,
    pub(in crate::indexer) schema_hash: String,
    pub(in crate::indexer) db: LexicalRebuildDbState,
    pub(in crate::indexer) page_size: i64,
    pub(in crate::indexer) committed_offset: i64,
    #[serde(default)]
    pub(in crate::indexer) committed_conversation_id: Option<i64>,
    pub(in crate::indexer) processed_conversations: usize,
    pub(in crate::indexer) indexed_docs: usize,
    /// Task 5: the share of `indexed_docs` that is search-region documents
    /// (persisted `viewer_search_regions` inventory, not canonical
    /// messages). `#[serde(default)]` reads receipts written before region
    /// documents existed as zero — those archives genuinely had none.
    #[serde(default)]
    pub(in crate::indexer) indexed_region_docs: usize,
    pub(in crate::indexer) committed_meta_fingerprint: Option<String>,
    pub(in crate::indexer) pending: Option<PendingLexicalCommit>,
    pub(in crate::indexer) completed: bool,
    pub(in crate::indexer) updated_at_ms: i64,
    /// `None` means the checkpoint predates explicit execution-mode recording.
    /// New checkpoints always serialize `Some`, including `shared_writer`, so
    /// incomplete legacy state can be restarted conservatively without making
    /// completed legacy generations look staged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::indexer) execution_mode: Option<LexicalRebuildExecutionMode>,
    #[serde(default)]
    pub(in crate::indexer) runtime: LexicalRebuildPipelineRuntimeSnapshot,
}

impl LexicalRebuildState {
    pub(in crate::indexer) fn new(db: LexicalRebuildDbState, page_size: i64) -> Self {
        Self {
            version: LEXICAL_REBUILD_STATE_VERSION,
            schema_hash: crate::search::tantivy::SCHEMA_HASH.to_string(),
            db,
            page_size,
            committed_offset: 0,
            committed_conversation_id: None,
            processed_conversations: 0,
            indexed_docs: 0,
            indexed_region_docs: 0,
            committed_meta_fingerprint: None,
            pending: None,
            completed: false,
            updated_at_ms: FrankenStorage::now_millis(),
            execution_mode: Some(LexicalRebuildExecutionMode::SharedWriter),
            runtime: LexicalRebuildPipelineRuntimeSnapshot::default(),
        }
    }

    pub(in crate::indexer) fn matches_run(
        &self,
        db: &LexicalRebuildDbState,
        _page_size: i64,
    ) -> bool {
        let db_matches = if self.is_incomplete()
            && self.effective_execution_mode()
                == LexicalRebuildExecutionMode::CanonicalMetadataRepair
        {
            // This pre-mutation marker deliberately survives additional inserts
            // in the same interrupted batch. It never authorizes cursor reuse.
            lexical_rebuild_db_paths_match(&self.db.db_path, &db.db_path)
        } else if self.db.storage_fingerprint.starts_with("content-v1:")
            && db.storage_fingerprint.starts_with("content-v1:")
        {
            lexical_rebuild_db_state_matches(&self.db, db)
        } else {
            lexical_rebuild_db_state_matches_legacy(&self.db, db)
        };
        self.version == LEXICAL_REBUILD_STATE_VERSION
            && self.schema_hash == crate::search::tantivy::SCHEMA_HASH
            && db_matches
            && lexical_rebuild_page_size_is_compatible(self.page_size)
    }

    pub(in crate::indexer) fn record_pending_commit(
        &mut self,
        next_conversation_id: Option<i64>,
        processed_conversations: usize,
        indexed_docs: usize,
        base_meta_fingerprint: Option<String>,
    ) {
        self.pending = Some(PendingLexicalCommit {
            next_offset: i64::try_from(processed_conversations).unwrap_or(i64::MAX),
            next_conversation_id,
            processed_conversations,
            indexed_docs,
            base_meta_fingerprint,
            region_docs: 0,
        });
        self.completed = false;
        self.updated_at_ms = FrankenStorage::now_millis();
    }

    pub(in crate::indexer) fn finalize_commit(
        &mut self,
        committed_meta_fingerprint: Option<String>,
    ) {
        if let Some(pending) = self.pending.take() {
            self.committed_offset =
                i64::try_from(pending.processed_conversations).unwrap_or(i64::MAX);
            self.committed_conversation_id = pending.next_conversation_id;
            self.processed_conversations = pending.processed_conversations;
            self.indexed_docs = pending.indexed_docs;
            // The region share travels with the commit that wrote it: zero for
            // streaming commits, the converged region documents for the
            // terminal commit. Completion restamps the same value.
            self.indexed_region_docs = pending.region_docs;
        }
        self.committed_meta_fingerprint = committed_meta_fingerprint;
        self.completed = false;
        self.updated_at_ms = FrankenStorage::now_millis();
    }

    pub(in crate::indexer) fn clear_pending(&mut self) {
        self.pending = None;
        self.updated_at_ms = FrankenStorage::now_millis();
    }

    pub(in crate::indexer) fn set_runtime(
        &mut self,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
    ) {
        self.runtime = runtime.checkpoint_snapshot();
    }

    pub(in crate::indexer) fn clear_runtime(&mut self) {
        self.runtime = LexicalRebuildPipelineRuntimeSnapshot::default();
    }

    pub(in crate::indexer) fn set_execution_mode(
        &mut self,
        execution_mode: LexicalRebuildExecutionMode,
    ) {
        self.execution_mode = Some(execution_mode);
        self.updated_at_ms = FrankenStorage::now_millis();
    }

    pub(in crate::indexer) fn mark_completed(
        &mut self,
        committed_meta_fingerprint: Option<String>,
    ) {
        self.committed_meta_fingerprint = committed_meta_fingerprint;
        self.pending = None;
        self.clear_runtime();
        self.completed = true;
        self.updated_at_ms = FrankenStorage::now_millis();
        self.execution_mode = Some(LexicalRebuildExecutionMode::SharedWriter);
    }

    pub(in crate::indexer) fn effective_execution_mode(&self) -> LexicalRebuildExecutionMode {
        self.execution_mode.unwrap_or_default()
    }

    pub(in crate::indexer) fn requires_restart_from_zero_on_resume(&self) -> bool {
        self.execution_mode.is_none()
            || self
                .effective_execution_mode()
                .requires_restart_from_zero_on_resume()
    }

    pub(in crate::indexer) fn is_incomplete(&self) -> bool {
        !self.completed
    }

    pub(in crate::indexer) fn reported_processed_conversations(&self) -> usize {
        self.pending
            .as_ref()
            .map(|pending| pending.processed_conversations)
            .unwrap_or(self.processed_conversations)
    }

    pub(in crate::indexer) fn reported_committed_conversation_id(&self) -> Option<i64> {
        self.pending
            .as_ref()
            .and_then(|pending| pending.next_conversation_id)
            .or(self.committed_conversation_id)
    }

    pub(in crate::indexer) fn reported_indexed_docs(&self) -> usize {
        self.pending
            .as_ref()
            .map(|pending| pending.indexed_docs)
            .unwrap_or(self.indexed_docs)
    }
}

#[cfg(test)]
pub(in crate::indexer) fn acquire_index_run_lock(
    data_dir: &Path,
    db_path: &Path,
    mode: SearchMaintenanceMode,
) -> Result<IndexRunLockGuard> {
    crate::storage::run_lock::acquire_index_run_lock_with_job_kind(
        data_dir,
        db_path,
        mode,
        maintenance_job_kind_for_mode(mode),
    )
}

pub(in crate::indexer) fn lexical_rebuild_equivalence_evidence_path(index_path: &Path) -> PathBuf {
    index_path.join(".lexical-rebuild-equivalence.json")
}

pub(in crate::indexer) fn lexical_refresh_ledger_path(index_path: &Path) -> PathBuf {
    index_path.join(".lexical-refresh-ledger.json")
}

/// Companion sidecar to `.lexical-refresh-ledger.json` shipped under
/// `coding_agent_session_search-ibuuh.24` SCOPE bullet 1: a small
/// derived-evidence JSON (throughput / phase share / dominant phase /
/// aggregate items+s) that benchmark gates and operator dashboards
/// can diff across runs without re-running the math at every call
/// site. Computed by `RefreshLedger::evidence_summary()` and written
/// alongside the raw ledger so consumers always see both views in
/// the same publish.
pub(in crate::indexer) fn lexical_refresh_evidence_path(index_path: &Path) -> PathBuf {
    index_path.join(".lexical-refresh-evidence.json")
}

pub(in crate::indexer) fn persist_lexical_rebuild_equivalence_evidence(
    index_path: &Path,
    evidence: &LexicalRebuildEquivalenceEvidence,
) -> Result<()> {
    let path = lexical_rebuild_equivalence_evidence_path(index_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "creating lexical rebuild equivalence evidence parent directory {}",
                parent.display()
            )
        })?;
    }
    write_json_pretty_atomically(&path, evidence).with_context(|| {
        format!(
            "persisting lexical rebuild equivalence evidence to {}",
            path.display()
        )
    })
}

pub(in crate::indexer) fn persist_lexical_refresh_ledger(
    index_path: &Path,
    ledger: &RefreshLedger,
) -> Result<()> {
    let path = lexical_refresh_ledger_path(index_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "creating lexical refresh ledger parent directory {}",
                parent.display()
            )
        })?;
    }

    // [coding_agent_session_search-ibuuh.24] BEFORE overwriting the
    // sidecar, capture the prior persisted evidence (if any) so the
    // post-write tracing emission can compare current vs prior. Read
    // failure is non-fatal: a missing or unparseable prior sidecar
    // just means we skip the cross-run comparison this round (e.g.
    // first publish on a fresh data dir). Logged at debug because
    // first-publish is the common case.
    let prior_evidence = load_prior_refresh_evidence_for_comparison(index_path);

    write_json_pretty_atomically(&path, ledger)
        .with_context(|| format!("persisting lexical refresh ledger to {}", path.display()))?;

    // [coding_agent_session_search-ibuuh.24] Always emit the derived
    // evidence sidecar alongside the raw ledger so benchmark gates
    // and operator dashboards have the headline metrics
    // (throughput, dominant phase, phase share) without having to
    // re-derive them. Failure here MUST NOT abort the publish — the
    // raw ledger is the source of truth; the evidence sidecar is a
    // convenience derived view that can be re-computed offline by
    // calling `RefreshLedger::evidence_summary()` against the
    // persisted ledger. We log the failure at warn level so an
    // operator can investigate without losing publish progress.
    let current_evidence = ledger.evidence_summary();
    if let Err(err) = persist_lexical_refresh_evidence(index_path, &current_evidence) {
        tracing::warn!(
            target: "cass::indexer::lexical_refresh",
            error = %err,
            "failed to persist .lexical-refresh-evidence.json sidecar; raw ledger \
             persisted OK and evidence can be re-derived offline via \
             RefreshLedger::evidence_summary()"
        );
    }

    // [coding_agent_session_search-ibuuh.24] If a prior evidence
    // snapshot was readable, emit the cross-run comparison summary
    // so operators see "this rebuild was N% slower than the
    // previous publish" automatically in default-level logs without
    // running a benchmark harness. Severity is tiered to the
    // regression magnitude inside emit_tracing_summary itself
    // (warn ≥+25%, info ≤-10%, else debug).
    if let Some(prior) = prior_evidence {
        let comparison = current_evidence.compare_to(&prior);
        comparison.emit_tracing_summary();
    }

    Ok(())
}

/// Read the previously persisted `.lexical-refresh-evidence.json`
/// sidecar so the next publish can emit a cross-run comparison.
/// Returns `None` on any failure (missing file, parse error, IO
/// error) — a missing prior sidecar is the EXPECTED first-publish
/// case, so failure here is non-fatal and logged at debug.
pub(in crate::indexer) fn load_prior_refresh_evidence_for_comparison(
    index_path: &Path,
) -> Option<RefreshLedgerEvidence> {
    let path = lexical_refresh_evidence_path(index_path);
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) => {
            tracing::debug!(
                target: "cass::indexer::lexical_refresh",
                path = %path.display(),
                error = %err,
                "no prior .lexical-refresh-evidence.json sidecar to compare against \
                 (first publish on this data dir, or sidecar pruned)"
            );
            return None;
        }
    };
    match serde_json::from_str::<RefreshLedgerEvidence>(&raw) {
        Ok(prior) => Some(prior),
        Err(err) => {
            tracing::debug!(
                target: "cass::indexer::lexical_refresh",
                path = %path.display(),
                error = %err,
                "prior .lexical-refresh-evidence.json sidecar present but unparseable; \
                 skipping cross-run comparison for this publish"
            );
            None
        }
    }
}

pub(in crate::indexer) fn persist_lexical_refresh_evidence(
    index_path: &Path,
    evidence: &RefreshLedgerEvidence,
) -> Result<()> {
    let path = lexical_refresh_evidence_path(index_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "creating lexical refresh evidence parent directory {}",
                parent.display()
            )
        })?;
    }
    write_json_pretty_atomically(&path, evidence)
        .with_context(|| format!("persisting lexical refresh evidence to {}", path.display()))
}

pub(in crate::indexer) struct AuthoritativeLexicalRefreshLedgerInput<'a> {
    pub(in crate::indexer) publish_mode: &'static str,
    pub(in crate::indexer) lexical_duration: Duration,
    pub(in crate::indexer) publish_duration: Duration,
    pub(in crate::indexer) processed_conversations: usize,
    pub(in crate::indexer) total_conversations: usize,
    pub(in crate::indexer) final_observed_messages: usize,
    pub(in crate::indexer) indexed_docs: usize,
    pub(in crate::indexer) equivalence_evidence: &'a LexicalRebuildEquivalenceEvidence,
}

pub(in crate::indexer) fn build_authoritative_lexical_refresh_ledger(
    input: AuthoritativeLexicalRefreshLedgerInput<'_>,
) -> RefreshLedger {
    let AuthoritativeLexicalRefreshLedgerInput {
        publish_mode,
        lexical_duration,
        publish_duration,
        processed_conversations,
        total_conversations,
        final_observed_messages,
        indexed_docs,
        equivalence_evidence,
    } = input;
    let lexical_duration_ms = u64::try_from(lexical_duration.as_millis()).unwrap_or(u64::MAX);
    let publish_duration_ms = u64::try_from(publish_duration.as_millis()).unwrap_or(u64::MAX);
    let total_duration_ms = lexical_duration_ms.saturating_add(publish_duration_ms);
    let completed_at_ms = FrankenStorage::now_millis();
    let started_at_ms =
        completed_at_ms.saturating_sub(i64::try_from(total_duration_ms).unwrap_or(i64::MAX));
    let processed_conversations_u64 = u64::try_from(processed_conversations).unwrap_or(u64::MAX);
    let total_conversations_u64 = u64::try_from(total_conversations).unwrap_or(u64::MAX);
    let final_observed_messages_u64 = u64::try_from(final_observed_messages).unwrap_or(u64::MAX);
    let indexed_docs_u64 = u64::try_from(indexed_docs).unwrap_or(u64::MAX);
    let equivalence_probe_count =
        u64::try_from(equivalence_evidence.golden_query_hit_counts.len()).unwrap_or(u64::MAX);

    RefreshLedger {
        version: 1,
        started_at_ms,
        completed_at_ms,
        total_duration_ms,
        full_rebuild: true,
        corpus_family: "authoritative_canonical_packet_replay".to_string(),
        phases: vec![
            PhaseRecord {
                phase: RefreshPhase::LexicalRebuild,
                duration_ms: lexical_duration_ms,
                items_processed: processed_conversations_u64,
                items_skipped: total_conversations_u64.saturating_sub(processed_conversations_u64),
                errors: 0,
                counters: BTreeMap::from([
                    ("indexed_docs".to_string(), indexed_docs_u64),
                    ("observed_messages".to_string(), final_observed_messages_u64),
                    ("total_conversations".to_string(), total_conversations_u64),
                    (
                        "equivalence_probe_count".to_string(),
                        equivalence_probe_count,
                    ),
                ]),
                success: true,
                error_message: None,
            },
            PhaseRecord {
                phase: RefreshPhase::Publish,
                duration_ms: publish_duration_ms,
                items_processed: 1,
                items_skipped: 0,
                errors: 0,
                counters: BTreeMap::from([
                    ("indexed_docs".to_string(), indexed_docs_u64),
                    ("observed_messages".to_string(), final_observed_messages_u64),
                    ("published_generations".to_string(), 1),
                ]),
                success: true,
                error_message: None,
            },
        ],
        equivalence: RefreshEquivalenceArtifacts {
            conversation_count: total_conversations_u64,
            message_count: final_observed_messages_u64,
            lexical_doc_count: indexed_docs_u64,
            lexical_fingerprint: None,
            semantic_manifest_fingerprint: None,
            search_hit_digest: Some(equivalence_evidence.golden_query_digest.clone()),
            peak_rss_bytes: None,
            db_size_bytes: None,
            lexical_index_size_bytes: None,
        },
        tags: BTreeMap::from([
            ("dataflow".to_string(), "conversation_packet".to_string()),
            ("publish_mode".to_string(), publish_mode.to_string()),
            ("source".to_string(), "canonical_db".to_string()),
        ]),
    }
}

pub(in crate::indexer) fn log_lexical_refresh_ledger_published(ledger: &RefreshLedger) {
    let milestones = ledger.readiness_milestones();
    tracing::info!(
        corpus_family = ledger.corpus_family.as_str(),
        total_duration_ms = ledger.total_duration_ms,
        time_to_lexical_ready_ms = milestones.time_to_lexical_ready_ms,
        time_to_search_ready_ms = milestones.time_to_search_ready_ms,
        time_to_full_settled_ms = milestones.time_to_full_settled_ms,
        failed_phase = milestones.failed_phase.as_deref().unwrap_or(""),
        search_readiness_state = ?milestones.search_readiness_state,
        "lexical refresh ledger published"
    );
}

pub(in crate::indexer) fn build_lexical_rebuild_generation_manifest(
    source_db_fingerprint: &str,
    total_conversations: usize,
    final_observed_messages: usize,
    indexed_docs: usize,
    equivalence_evidence: &LexicalRebuildEquivalenceEvidence,
) -> lexical_generation::LexicalGenerationManifest {
    let manifest_now_ms = lexical_generation::now_ms();
    let generation_fingerprint_head = source_db_fingerprint
        .get(..16)
        .unwrap_or(source_db_fingerprint);
    let generation_id = format!("gen-{manifest_now_ms:016x}-{generation_fingerprint_head}");
    let attempt_id = format!("attempt-{manifest_now_ms:016x}");
    let mut generation_manifest = lexical_generation::LexicalGenerationManifest::new_scratch(
        generation_id,
        attempt_id,
        source_db_fingerprint.to_string(),
        manifest_now_ms,
    );
    generation_manifest.conversation_count = u64::try_from(total_conversations).unwrap_or(u64::MAX);
    generation_manifest.message_count = u64::try_from(final_observed_messages).unwrap_or(u64::MAX);
    generation_manifest.indexed_doc_count = u64::try_from(indexed_docs).unwrap_or(u64::MAX);
    generation_manifest.equivalence_manifest_fingerprint =
        Some(equivalence_evidence.manifest_fingerprint.clone());
    generation_manifest.transition_build(
        lexical_generation::LexicalGenerationBuildState::Built,
        manifest_now_ms,
    );
    generation_manifest.transition_build(
        lexical_generation::LexicalGenerationBuildState::Validated,
        manifest_now_ms,
    );
    generation_manifest.transition_publish(
        lexical_generation::LexicalGenerationPublishState::Published,
        manifest_now_ms,
    );
    generation_manifest
}

pub(in crate::indexer) fn log_lexical_generation_manifest_published(
    generation_manifest: &lexical_generation::LexicalGenerationManifest,
    equivalence_evidence: &LexicalRebuildEquivalenceEvidence,
) {
    tracing::info!(
        generation_id = generation_manifest.generation_id.as_str(),
        attempt_id = generation_manifest.attempt_id.as_str(),
        conversation_count = generation_manifest.conversation_count,
        message_count = generation_manifest.message_count,
        indexed_doc_count = generation_manifest.indexed_doc_count,
        source_db_fingerprint = generation_manifest.source_db_fingerprint.as_str(),
        equivalence_manifest_fingerprint = equivalence_evidence.manifest_fingerprint.as_str(),
        "lexical generation manifest published"
    );
}

pub(in crate::indexer) fn persist_lexical_rebuild_generation_artifacts(
    generation_dir: &Path,
    source_db_fingerprint: &str,
    processed_conversations: usize,
    total_conversations: usize,
    final_observed_messages: usize,
    indexed_docs: usize,
    equivalence_evidence: &LexicalRebuildEquivalenceEvidence,
) -> Result<lexical_generation::LexicalGenerationManifest> {
    tracing::info!(
        document_count = equivalence_evidence.document_count,
        manifest_fingerprint = equivalence_evidence.manifest_fingerprint.as_str(),
        golden_query_digest = equivalence_evidence.golden_query_digest.as_str(),
        golden_probe_count = equivalence_evidence.golden_query_hit_counts.len(),
        golden_query_hit_total = equivalence_evidence
            .golden_query_hit_counts
            .iter()
            .map(|hit| hit.hit_count)
            .sum::<u64>(),
        indexed_docs,
        total_conversations,
        processed_conversations,
        "lexical rebuild authoritative equivalence evidence"
    );
    persist_lexical_rebuild_equivalence_evidence(generation_dir, equivalence_evidence)?;

    let generation_manifest = build_lexical_rebuild_generation_manifest(
        source_db_fingerprint,
        total_conversations,
        final_observed_messages,
        indexed_docs,
        equivalence_evidence,
    );
    lexical_generation::store_manifest(generation_dir, &generation_manifest).with_context(
        || {
            format!(
                "persisting lexical generation manifest for published generation {} at {}",
                generation_manifest.generation_id,
                generation_dir.display()
            )
        },
    )?;
    Ok(generation_manifest)
}
