use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{
    LexicalGenerationBuildState, LexicalGenerationManifest, LexicalGenerationMergeDebtState,
    LexicalGenerationPublishState, LexicalShardLifecycleState, LexicalShardManifest,
};

/// `coding_agent_session_search-ibuuh.19` classification predicate:
/// returns `true` when `disposition` is one of the variants that the
/// dry-run / apply gates MUST keep on disk. The non-protected
/// variants are exactly `SupersededReclaimable` + `FailedReclaimable`
/// — the two states the policy contract says are safe to reclaim.
///
/// Lifted out of the `LexicalCleanupDryRunPlan` impl so it has a
/// single canonical home AND so the test module's exhaustiveness gate
/// can compare it against `LexicalCleanupDisposition::all_variants()`
/// directly. Mirroring impl method calls through to this function
/// keeps existing call sites unchanged.
pub(super) fn is_protected_retention_disposition(disposition: LexicalCleanupDisposition) -> bool {
    matches!(
        disposition,
        LexicalCleanupDisposition::CurrentPublished
            | LexicalCleanupDisposition::ActiveWork
            | LexicalCleanupDisposition::QuarantinedRetained
            | LexicalCleanupDisposition::SupersededRetained
            | LexicalCleanupDisposition::FailedRetained
            | LexicalCleanupDisposition::PinnedRetained
    )
}

/// Dry-run cleanup classification for one lexical artifact or generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LexicalCleanupDisposition {
    /// The artifact is part of the currently published search surface.
    CurrentPublished,
    /// The artifact is still being built, validated, resumed, or merged.
    ActiveWork,
    /// The artifact is intentionally retained for operator inspection.
    QuarantinedRetained,
    /// A superseded artifact is no longer pinned and can be reclaimed.
    SupersededReclaimable,
    /// A superseded artifact must stay on disk because policy still pins it.
    SupersededRetained,
    /// A failed or abandoned artifact can be reclaimed after dry-run approval.
    FailedReclaimable,
    /// A failed or abandoned artifact must stay on disk for inspection.
    FailedRetained,
    /// The artifact is explicitly pinned by current policy.
    PinnedRetained,
}

impl LexicalCleanupDisposition {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::CurrentPublished => "current_published",
            Self::ActiveWork => "active_work",
            Self::QuarantinedRetained => "quarantined_retained",
            Self::SupersededReclaimable => "superseded_reclaimable",
            Self::SupersededRetained => "superseded_retained",
            Self::FailedReclaimable => "failed_reclaimable",
            Self::FailedRetained => "failed_retained",
            Self::PinnedRetained => "pinned_retained",
        }
    }

    /// Every variant in declaration order. Used by the
    /// `coding_agent_session_search-ibuuh.19` golden gate to assert
    /// every variant has both an `as_str()` arm AND a serde
    /// representation, AND that the protected-vs-reclaimable
    /// classification covers every variant exhaustively. A new
    /// variant added without registering it here is a compile error
    /// (no `_ => ...` catch-all in the test).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn all_variants() -> &'static [Self] {
        &[
            Self::CurrentPublished,
            Self::ActiveWork,
            Self::QuarantinedRetained,
            Self::SupersededReclaimable,
            Self::SupersededRetained,
            Self::FailedReclaimable,
            Self::FailedRetained,
            Self::PinnedRetained,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LexicalShardCleanupInventory {
    pub shard_id: String,
    pub state: LexicalShardLifecycleState,
    pub disposition: LexicalCleanupDisposition,
    pub reason: String,
    pub artifact_bytes: u64,
    pub reclaimable_bytes: u64,
    pub retained_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LexicalGenerationCleanupInventory {
    pub generation_id: String,
    pub build_state: LexicalGenerationBuildState,
    pub publish_state: LexicalGenerationPublishState,
    pub disposition: LexicalCleanupDisposition,
    pub reason: String,
    pub retain_until_ms: Option<i64>,
    pub retention_reason: String,
    pub artifact_bytes: u64,
    pub reclaimable_bytes: u64,
    pub retained_bytes: u64,
    pub shards: Vec<LexicalShardCleanupInventory>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LexicalCleanupDryRunPlan {
    pub dry_run: bool,
    pub approval_fingerprint: String,
    pub generation_count: usize,
    pub total_artifact_bytes: u64,
    pub total_reclaimable_bytes: u64,
    pub total_retained_bytes: u64,
    #[serde(default)]
    pub reclaim_candidates: Vec<LexicalCleanupReclaimCandidate>,
    pub reclaimable_generation_ids: Vec<String>,
    pub fully_retained_generation_ids: Vec<String>,
    #[serde(default)]
    pub protected_generation_ids: Vec<String>,
    pub protected_retained_bytes: u64,
    pub quarantined_generation_ids: Vec<String>,
    pub active_generation_ids: Vec<String>,
    pub disposition_counts: BTreeMap<LexicalCleanupDisposition, usize>,
    #[serde(default)]
    pub generation_disposition_summaries:
        BTreeMap<LexicalCleanupDisposition, LexicalCleanupGenerationDispositionSummary>,
    #[serde(default)]
    pub inspection_items: Vec<LexicalCleanupInspectionItem>,
    #[serde(default)]
    pub inspection_required_generation_ids: Vec<String>,
    #[serde(default)]
    pub inspection_required_count: usize,
    #[serde(default)]
    pub inspection_required_retained_bytes: u64,
    #[serde(default)]
    pub shard_disposition_summaries:
        BTreeMap<LexicalCleanupDisposition, LexicalCleanupDispositionSummary>,
    pub inventories: Vec<LexicalGenerationCleanupInventory>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LexicalCleanupReclaimCandidate {
    pub generation_id: String,
    pub shard_id: String,
    pub disposition: LexicalCleanupDisposition,
    pub reason: String,
    pub reclaimable_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LexicalCleanupInspectionItem {
    pub generation_id: String,
    pub shard_id: Option<String>,
    pub disposition: LexicalCleanupDisposition,
    pub reason: String,
    pub retained_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LexicalCleanupDispositionSummary {
    pub shard_count: usize,
    pub artifact_bytes: u64,
    pub reclaimable_bytes: u64,
    pub retained_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LexicalCleanupGenerationDispositionSummary {
    pub generation_count: usize,
    pub artifact_bytes: u64,
    pub reclaimable_bytes: u64,
    pub retained_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LexicalCleanupApprovalFingerprintStatus {
    #[default]
    NotRequested,
    Missing,
    Matched,
    Mismatched,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LexicalCleanupApplyBlocker {
    NoReclaimableCandidates,
    OperatorApprovalRequired,
    ApprovalFingerprintMissing,
    ApprovalFingerprintMismatched,
    ActiveGenerationWork,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LexicalCleanupApplyGate {
    pub apply_allowed: bool,
    pub dry_run: bool,
    pub explicit_operator_approval: bool,
    #[serde(default)]
    pub approval_fingerprint: String,
    #[serde(default)]
    pub provided_approval_fingerprint: Option<String>,
    #[serde(default)]
    pub approval_fingerprint_status: LexicalCleanupApprovalFingerprintStatus,
    #[serde(default)]
    pub approval_fingerprint_matches: bool,
    #[serde(default)]
    pub generation_count: usize,
    #[serde(default)]
    pub total_artifact_bytes: u64,
    #[serde(default)]
    pub total_retained_bytes: u64,
    pub candidate_count: usize,
    pub reclaimable_bytes: u64,
    #[serde(default)]
    pub disposition_counts: BTreeMap<LexicalCleanupDisposition, usize>,
    #[serde(default)]
    pub generation_disposition_summaries:
        BTreeMap<LexicalCleanupDisposition, LexicalCleanupGenerationDispositionSummary>,
    #[serde(default)]
    pub shard_disposition_summaries:
        BTreeMap<LexicalCleanupDisposition, LexicalCleanupDispositionSummary>,
    #[serde(default)]
    pub candidate_previews: Vec<LexicalCleanupReclaimCandidate>,
    #[serde(default)]
    pub reclaimable_generation_ids: Vec<String>,
    #[serde(default)]
    pub fully_retained_generation_ids: Vec<String>,
    #[serde(default)]
    pub quarantined_generation_ids: Vec<String>,
    #[serde(default)]
    pub blocker_codes: Vec<LexicalCleanupApplyBlocker>,
    pub blocked_reasons: Vec<String>,
    #[serde(default)]
    pub active_generation_ids: Vec<String>,
    #[serde(default)]
    pub protected_generation_ids: Vec<String>,
    #[serde(default)]
    pub protected_retained_bytes: u64,
    #[serde(default)]
    pub inspection_previews: Vec<LexicalCleanupInspectionItem>,
    #[serde(default)]
    pub inspection_required_count: usize,
    #[serde(default)]
    pub inspection_required_retained_bytes: u64,
    #[serde(default)]
    pub inspection_required_generation_ids: Vec<String>,
}

impl LexicalCleanupDryRunPlan {
    pub fn from_manifests<'a>(
        manifests: impl IntoIterator<Item = &'a LexicalGenerationManifest>,
    ) -> Self {
        let mut plan = Self {
            dry_run: true,
            approval_fingerprint: String::new(),
            generation_count: 0,
            total_artifact_bytes: 0,
            total_reclaimable_bytes: 0,
            total_retained_bytes: 0,
            reclaim_candidates: Vec::new(),
            reclaimable_generation_ids: Vec::new(),
            fully_retained_generation_ids: Vec::new(),
            protected_generation_ids: Vec::new(),
            protected_retained_bytes: 0,
            quarantined_generation_ids: Vec::new(),
            active_generation_ids: Vec::new(),
            disposition_counts: BTreeMap::new(),
            generation_disposition_summaries: BTreeMap::new(),
            inspection_items: Vec::new(),
            inspection_required_generation_ids: Vec::new(),
            inspection_required_count: 0,
            inspection_required_retained_bytes: 0,
            shard_disposition_summaries: BTreeMap::new(),
            inventories: Vec::new(),
        };

        for manifest in manifests {
            plan.record_inventory(manifest.cleanup_inventory());
        }
        plan.approval_fingerprint = plan.compute_approval_fingerprint();

        plan
    }

    pub fn has_reclaimable_artifacts(&self) -> bool {
        self.total_reclaimable_bytes > 0
    }

    pub fn reclaim_candidates(&self) -> Vec<LexicalCleanupReclaimCandidate> {
        self.reclaim_candidates.clone()
    }

    pub fn apply_gate(&self, explicit_operator_approval: bool) -> LexicalCleanupApplyGate {
        self.apply_gate_with_fingerprint(explicit_operator_approval, None)
    }

    pub fn apply_gate_with_fingerprint(
        &self,
        explicit_operator_approval: bool,
        provided_approval_fingerprint: Option<&str>,
    ) -> LexicalCleanupApplyGate {
        let mut blocked_reasons = Vec::new();
        let mut blocker_codes = Vec::new();
        if self.reclaim_candidates.is_empty() {
            blocked_reasons.push("no reclaimable cleanup candidates".to_string());
            blocker_codes.push(LexicalCleanupApplyBlocker::NoReclaimableCandidates);
        }
        if !explicit_operator_approval {
            blocked_reasons.push(
                "destructive cleanup requires explicit operator approval after dry-run".to_string(),
            );
            blocker_codes.push(LexicalCleanupApplyBlocker::OperatorApprovalRequired);
        }
        let approval_fingerprint_status =
            match (explicit_operator_approval, provided_approval_fingerprint) {
                (false, _) => LexicalCleanupApprovalFingerprintStatus::NotRequested,
                (true, Some(fingerprint)) if fingerprint == self.approval_fingerprint => {
                    LexicalCleanupApprovalFingerprintStatus::Matched
                }
                (true, Some(_)) => LexicalCleanupApprovalFingerprintStatus::Mismatched,
                (true, None) => LexicalCleanupApprovalFingerprintStatus::Missing,
            };
        let approval_fingerprint_matches =
            approval_fingerprint_status == LexicalCleanupApprovalFingerprintStatus::Matched;
        match approval_fingerprint_status {
            LexicalCleanupApprovalFingerprintStatus::Mismatched => blocked_reasons.push(
                "provided cleanup approval fingerprint does not match dry-run plan".to_string(),
            ),
            LexicalCleanupApprovalFingerprintStatus::Missing => {
                blocked_reasons.push(format!(
                    "cleanup apply requires confirming approval fingerprint {}",
                    self.approval_fingerprint
                ));
                blocker_codes.push(LexicalCleanupApplyBlocker::ApprovalFingerprintMissing);
            }
            LexicalCleanupApprovalFingerprintStatus::NotRequested
            | LexicalCleanupApprovalFingerprintStatus::Matched => {}
        }
        if approval_fingerprint_status == LexicalCleanupApprovalFingerprintStatus::Mismatched {
            blocker_codes.push(LexicalCleanupApplyBlocker::ApprovalFingerprintMismatched);
        }
        if !self.active_generation_ids.is_empty() {
            blocked_reasons.push(format!(
                "active generation work must settle before cleanup apply: {}",
                self.active_generation_ids.join(",")
            ));
            blocker_codes.push(LexicalCleanupApplyBlocker::ActiveGenerationWork);
        }
        let inspection_required_generation_ids = self.inspection_required_generation_ids();

        LexicalCleanupApplyGate {
            apply_allowed: blocked_reasons.is_empty(),
            dry_run: self.dry_run,
            explicit_operator_approval,
            approval_fingerprint: self.approval_fingerprint.clone(),
            provided_approval_fingerprint: provided_approval_fingerprint.map(str::to_owned),
            approval_fingerprint_status,
            approval_fingerprint_matches,
            generation_count: self.generation_count,
            total_artifact_bytes: self.total_artifact_bytes,
            total_retained_bytes: self.total_retained_bytes,
            candidate_count: self.reclaim_candidates.len(),
            reclaimable_bytes: self.total_reclaimable_bytes,
            disposition_counts: self.disposition_counts.clone(),
            generation_disposition_summaries: self.generation_disposition_summaries.clone(),
            shard_disposition_summaries: self.shard_disposition_summaries.clone(),
            candidate_previews: self.reclaim_candidates.clone(),
            reclaimable_generation_ids: self.reclaimable_generation_ids.clone(),
            fully_retained_generation_ids: self.fully_retained_generation_ids.clone(),
            quarantined_generation_ids: self.quarantined_generation_ids.clone(),
            blocker_codes,
            blocked_reasons,
            active_generation_ids: self.active_generation_ids.clone(),
            protected_generation_ids: self.protected_generation_ids.clone(),
            protected_retained_bytes: self.protected_retained_bytes,
            inspection_previews: self.inspection_items.clone(),
            inspection_required_count: self.inspection_required_count,
            inspection_required_retained_bytes: self.inspection_required_retained_bytes,
            inspection_required_generation_ids,
        }
    }

    pub fn inspection_required_generation_ids(&self) -> Vec<String> {
        self.inspection_required_generation_ids.clone()
    }

    pub fn inspection_required_retained_bytes(&self) -> u64 {
        self.inspection_required_retained_bytes
    }

    fn record_inspection_item(&mut self, item: LexicalCleanupInspectionItem) {
        self.inspection_required_count = self.inspection_required_count.saturating_add(1);
        self.inspection_required_retained_bytes = self
            .inspection_required_retained_bytes
            .saturating_add(item.retained_bytes);
        if !self
            .inspection_required_generation_ids
            .contains(&item.generation_id)
        {
            self.inspection_required_generation_ids
                .push(item.generation_id.clone());
        }
        self.inspection_items.push(item);
    }

    fn record_inventory(&mut self, inventory: LexicalGenerationCleanupInventory) {
        self.generation_count = self.generation_count.saturating_add(1);
        self.total_artifact_bytes = self
            .total_artifact_bytes
            .saturating_add(inventory.artifact_bytes);
        self.total_reclaimable_bytes = self
            .total_reclaimable_bytes
            .saturating_add(inventory.reclaimable_bytes);
        self.total_retained_bytes = self
            .total_retained_bytes
            .saturating_add(inventory.retained_bytes);
        *self
            .disposition_counts
            .entry(inventory.disposition)
            .or_insert(0) += 1;
        let generation_summary = self
            .generation_disposition_summaries
            .entry(inventory.disposition)
            .or_default();
        generation_summary.generation_count = generation_summary.generation_count.saturating_add(1);
        generation_summary.artifact_bytes = generation_summary
            .artifact_bytes
            .saturating_add(inventory.artifact_bytes);
        generation_summary.reclaimable_bytes = generation_summary
            .reclaimable_bytes
            .saturating_add(inventory.reclaimable_bytes);
        generation_summary.retained_bytes = generation_summary
            .retained_bytes
            .saturating_add(inventory.retained_bytes);

        if inventory.reclaimable_bytes > 0 {
            self.reclaimable_generation_ids
                .push(inventory.generation_id.clone());
        } else {
            self.fully_retained_generation_ids
                .push(inventory.generation_id.clone());
        }
        if matches!(
            inventory.disposition,
            LexicalCleanupDisposition::QuarantinedRetained
        ) {
            self.quarantined_generation_ids
                .push(inventory.generation_id.clone());
        }
        if matches!(inventory.disposition, LexicalCleanupDisposition::ActiveWork) {
            self.active_generation_ids
                .push(inventory.generation_id.clone());
        }
        let mut has_protected_retention =
            Self::is_protected_retention(inventory.disposition) && inventory.retained_bytes > 0;
        let inventory_requires_inspection = Self::requires_inspection(inventory.disposition);
        let inventory_allows_reclaim_candidates = matches!(
            inventory.disposition,
            LexicalCleanupDisposition::SupersededReclaimable
                | LexicalCleanupDisposition::FailedReclaimable
        );
        let mut shard_inspection_items = 0usize;
        for shard in &inventory.shards {
            let summary = self
                .shard_disposition_summaries
                .entry(shard.disposition)
                .or_default();
            summary.shard_count = summary.shard_count.saturating_add(1);
            summary.artifact_bytes = summary.artifact_bytes.saturating_add(shard.artifact_bytes);
            summary.reclaimable_bytes = summary
                .reclaimable_bytes
                .saturating_add(shard.reclaimable_bytes);
            summary.retained_bytes = summary.retained_bytes.saturating_add(shard.retained_bytes);
            if Self::is_protected_retention(shard.disposition) && shard.retained_bytes > 0 {
                has_protected_retention = true;
            }

            if Self::requires_inspection(shard.disposition) {
                shard_inspection_items = shard_inspection_items.saturating_add(1);
                self.record_inspection_item(LexicalCleanupInspectionItem {
                    generation_id: inventory.generation_id.clone(),
                    shard_id: Some(shard.shard_id.clone()),
                    disposition: shard.disposition,
                    reason: shard.reason.clone(),
                    retained_bytes: shard.retained_bytes,
                });
            }

            if shard.reclaimable_bytes == 0 || !inventory_allows_reclaim_candidates {
                continue;
            }
            self.reclaim_candidates
                .push(LexicalCleanupReclaimCandidate {
                    generation_id: inventory.generation_id.clone(),
                    shard_id: shard.shard_id.clone(),
                    disposition: shard.disposition,
                    reason: shard.reason.clone(),
                    reclaimable_bytes: shard.reclaimable_bytes,
                });
        }

        if inventory_requires_inspection && shard_inspection_items == 0 {
            self.record_inspection_item(LexicalCleanupInspectionItem {
                generation_id: inventory.generation_id.clone(),
                shard_id: None,
                disposition: inventory.disposition,
                reason: inventory.reason.clone(),
                retained_bytes: inventory.retained_bytes,
            });
        }

        if has_protected_retention {
            if !self
                .protected_generation_ids
                .contains(&inventory.generation_id)
            {
                self.protected_generation_ids
                    .push(inventory.generation_id.clone());
            }
            self.protected_retained_bytes = self
                .protected_retained_bytes
                .saturating_add(inventory.retained_bytes);
        }

        // [coding_agent_session_search-ibuuh.19] Emit a structured
        // tracing event per generation classification so operators can
        // trace from logs alone WHY each artifact was reclaimed or
        // preserved (the bead's "structured logs that let a future
        // agent understand exactly why disk was reclaimed or preserved"
        // SCOPE bullet). Severity tiers match operator expectations:
        //
        // - SupersededReclaimable / FailedReclaimable -> debug
        //   (routine cleanup eligibility — high volume on big corpora)
        // - QuarantinedRetained / FailedRetained -> warn
        //   (operator inspection required — surface in default logs)
        // - ActiveWork / CurrentPublished / SupersededRetained /
        //   PinnedRetained -> info (preserved by design, but worth
        //   surfacing when the operator runs cleanup so they see WHY)
        //
        // Every event carries the disposition string + reason +
        // generation_id + reclaimable/retained byte counts so a single
        // log line answers "what got pruned?" and "why was X kept?"
        // without grepping multiple sources.
        let disposition_str = inventory.disposition.as_str();
        // [coding_agent_session_search-urscl] Pre-fix this match
        // repeated the same 8-field tracing payload across three
        // tracing::{debug,warn,info}! call sites. A field added in
        // one branch but forgotten in another would silently ship.
        // The local `emit_tier!` macro inlines the shared payload at
        // each call site (no runtime cost — same code generation),
        // so adding a field once propagates to all three tiers and
        // the per-tier difference is reduced to (macro ident,
        // message literal). The exhaustive disposition severity
        // tests (record_inventory_emits_correct_severity_for_every_disposition_variant)
        // continue to observe the per-tier level + message exactly
        // as before because each branch still expands to the same
        // tracing macro call.
        let shard_count = inventory.shards.len();
        macro_rules! emit_tier {
            ($macro:ident, $msg:literal) => {
                tracing::$macro!(
                    target: "cass::indexer::lexical_cleanup",
                    generation_id = %inventory.generation_id,
                    disposition = disposition_str,
                    reason = %inventory.reason,
                    reclaimable_bytes = inventory.reclaimable_bytes,
                    retained_bytes = inventory.retained_bytes,
                    artifact_bytes = inventory.artifact_bytes,
                    shard_count,
                    inspection_required = inventory_requires_inspection,
                    $msg
                )
            };
        }
        match inventory.disposition {
            LexicalCleanupDisposition::SupersededReclaimable
            | LexicalCleanupDisposition::FailedReclaimable => {
                emit_tier!(
                    debug,
                    "lexical cleanup classified generation as reclaimable"
                );
            }
            LexicalCleanupDisposition::QuarantinedRetained
            | LexicalCleanupDisposition::FailedRetained => {
                emit_tier!(
                    warn,
                    "lexical cleanup retained generation pending operator inspection"
                );
            }
            LexicalCleanupDisposition::ActiveWork
            | LexicalCleanupDisposition::CurrentPublished
            | LexicalCleanupDisposition::SupersededRetained
            | LexicalCleanupDisposition::PinnedRetained => {
                emit_tier!(info, "lexical cleanup retained generation by policy");
            }
        }
        // Suppress the "unused" lint for diagnostics-only locals so the
        // compiler doesn't warn even if a future variant addition
        // routes through a no-emission arm.
        let _ = (
            shard_inspection_items,
            inventory_allows_reclaim_candidates,
            has_protected_retention,
        );

        self.inventories.push(inventory);
    }

    fn requires_inspection(disposition: LexicalCleanupDisposition) -> bool {
        matches!(
            disposition,
            LexicalCleanupDisposition::QuarantinedRetained
                | LexicalCleanupDisposition::FailedRetained
        )
    }

    fn is_protected_retention(disposition: LexicalCleanupDisposition) -> bool {
        is_protected_retention_disposition(disposition)
    }

    fn compute_approval_fingerprint(&self) -> String {
        // Deterministic: hash over sorted snapshots so the fingerprint is
        // invariant under manifest/shard iteration order (filesystem scans,
        // HashMap-backed callers, etc.). BTreeMaps already iterate in order.
        let mut hasher = blake3::Hasher::new();
        hash_str(&mut hasher, "cass.lexical_cleanup_approval.v1");
        hash_usize(&mut hasher, self.generation_count);
        hash_u64(&mut hasher, self.total_artifact_bytes);
        hash_u64(&mut hasher, self.total_reclaimable_bytes);
        hash_u64(&mut hasher, self.total_retained_bytes);
        hash_u64(&mut hasher, self.protected_retained_bytes);
        hash_usize(&mut hasher, self.inspection_required_count);
        hash_u64(&mut hasher, self.inspection_required_retained_bytes);

        let mut candidates: Vec<&LexicalCleanupReclaimCandidate> =
            self.reclaim_candidates.iter().collect();
        candidates.sort_by(|a, b| {
            (
                &a.generation_id,
                &a.shard_id,
                a.disposition.as_str(),
                &a.reason,
                a.reclaimable_bytes,
            )
                .cmp(&(
                    &b.generation_id,
                    &b.shard_id,
                    b.disposition.as_str(),
                    &b.reason,
                    b.reclaimable_bytes,
                ))
        });
        for candidate in candidates {
            hash_str(&mut hasher, &candidate.generation_id);
            hash_str(&mut hasher, &candidate.shard_id);
            hash_str(&mut hasher, candidate.disposition.as_str());
            hash_str(&mut hasher, &candidate.reason);
            hash_u64(&mut hasher, candidate.reclaimable_bytes);
        }

        let mut inspections: Vec<&LexicalCleanupInspectionItem> =
            self.inspection_items.iter().collect();
        inspections.sort_by(|a, b| {
            (
                &a.generation_id,
                a.shard_id.as_deref().unwrap_or(""),
                a.disposition.as_str(),
                &a.reason,
                a.retained_bytes,
            )
                .cmp(&(
                    &b.generation_id,
                    b.shard_id.as_deref().unwrap_or(""),
                    b.disposition.as_str(),
                    &b.reason,
                    b.retained_bytes,
                ))
        });
        for item in inspections {
            hash_str(&mut hasher, &item.generation_id);
            hash_str(&mut hasher, item.shard_id.as_deref().unwrap_or(""));
            hash_str(&mut hasher, item.disposition.as_str());
            hash_str(&mut hasher, &item.reason);
            hash_u64(&mut hasher, item.retained_bytes);
        }

        let mut active: Vec<&String> = self.active_generation_ids.iter().collect();
        active.sort();
        for generation_id in active {
            hash_str(&mut hasher, generation_id);
        }
        let mut protected: Vec<&String> = self.protected_generation_ids.iter().collect();
        protected.sort();
        for generation_id in protected {
            hash_str(&mut hasher, generation_id);
        }
        for (disposition, count) in &self.disposition_counts {
            hash_str(&mut hasher, disposition.as_str());
            hash_usize(&mut hasher, *count);
        }
        for (disposition, summary) in &self.generation_disposition_summaries {
            hash_str(&mut hasher, "generation_disposition_summary");
            hash_str(&mut hasher, disposition.as_str());
            hash_usize(&mut hasher, summary.generation_count);
            hash_u64(&mut hasher, summary.artifact_bytes);
            hash_u64(&mut hasher, summary.reclaimable_bytes);
            hash_u64(&mut hasher, summary.retained_bytes);
        }
        for (disposition, summary) in &self.shard_disposition_summaries {
            hash_str(&mut hasher, "shard_disposition_summary");
            hash_str(&mut hasher, disposition.as_str());
            hash_usize(&mut hasher, summary.shard_count);
            hash_u64(&mut hasher, summary.artifact_bytes);
            hash_u64(&mut hasher, summary.reclaimable_bytes);
            hash_u64(&mut hasher, summary.retained_bytes);
        }

        format!("cleanup-v1-{}", hasher.finalize().to_hex())
    }
}

fn hash_str(hasher: &mut blake3::Hasher, value: &str) {
    hash_usize(hasher, value.len());
    hasher.update(value.as_bytes());
}

fn hash_u64(hasher: &mut blake3::Hasher, value: u64) {
    hasher.update(&value.to_le_bytes());
}

fn hash_usize(hasher: &mut blake3::Hasher, value: usize) {
    hasher.update(&u64::try_from(value).unwrap_or(u64::MAX).to_le_bytes());
}

impl LexicalGenerationManifest {
    /// Build an auditable dry-run inventory for cleanup/quarantine decisions.
    pub(crate) fn cleanup_inventory(&self) -> LexicalGenerationCleanupInventory {
        let shards: Vec<_> = self
            .shards
            .iter()
            .map(|shard| self.classify_shard_for_cleanup(shard))
            .collect();
        let shard_artifact_bytes = shards.iter().map(|shard| shard.artifact_bytes).sum::<u64>();
        let shard_reclaimable_bytes = shards
            .iter()
            .map(|shard| shard.reclaimable_bytes)
            .sum::<u64>();
        let pending_merge_bytes = if self.merge_debt.has_pending_work() {
            self.merge_debt.pending_artifact_bytes
        } else {
            0
        };
        let artifact_bytes = shard_artifact_bytes.saturating_add(pending_merge_bytes);
        let generation_reclaimable_bytes = if self.generation_cleanup_allows_reclaim() {
            shard_reclaimable_bytes
        } else {
            0
        };
        let retained_bytes = artifact_bytes.saturating_sub(generation_reclaimable_bytes);
        let (disposition, reason) =
            self.classify_generation_for_cleanup(generation_reclaimable_bytes);
        let (retain_until_ms, retention_reason) =
            self.classify_generation_retention_window(disposition);

        LexicalGenerationCleanupInventory {
            generation_id: self.generation_id.clone(),
            build_state: self.build_state,
            publish_state: self.publish_state,
            disposition,
            reason,
            retain_until_ms,
            retention_reason,
            artifact_bytes,
            reclaimable_bytes: generation_reclaimable_bytes,
            retained_bytes,
            shards,
        }
    }

    fn classify_shard_for_cleanup(
        &self,
        shard: &LexicalShardManifest,
    ) -> LexicalShardCleanupInventory {
        let (disposition, reason) =
            if matches!(self.publish_state, LexicalGenerationPublishState::Published) {
                (
                    LexicalCleanupDisposition::CurrentPublished,
                    "shard is part of the published search surface".to_string(),
                )
            } else if shard.pinned {
                (
                    LexicalCleanupDisposition::PinnedRetained,
                    "shard is pinned by current retention policy".to_string(),
                )
            } else if matches!(
                self.publish_state,
                LexicalGenerationPublishState::Quarantined
            ) || matches!(shard.state, LexicalShardLifecycleState::Quarantined)
            {
                (
                    LexicalCleanupDisposition::QuarantinedRetained,
                    shard
                        .quarantine_reason
                        .clone()
                        .or_else(|| shard.recovery_reason.clone())
                        .unwrap_or_else(|| "quarantined shard requires inspection".to_string()),
                )
            } else if self.generation_has_active_work()
                || matches!(
                    shard.state,
                    LexicalShardLifecycleState::Building
                        | LexicalShardLifecycleState::Staged
                        | LexicalShardLifecycleState::Resumable
                )
            {
                (
                    LexicalCleanupDisposition::ActiveWork,
                    "shard belongs to active or resumable maintenance work".to_string(),
                )
            } else if matches!(
                self.publish_state,
                LexicalGenerationPublishState::Superseded
            ) {
                if shard.reclaimable {
                    (
                        LexicalCleanupDisposition::SupersededReclaimable,
                        "superseded shard is unpinned and safe to reclaim after dry-run approval"
                            .to_string(),
                    )
                } else {
                    (
                        LexicalCleanupDisposition::SupersededRetained,
                        "superseded shard is retained by policy".to_string(),
                    )
                }
            } else if matches!(shard.state, LexicalShardLifecycleState::Abandoned)
                || matches!(self.build_state, LexicalGenerationBuildState::Failed)
            {
                if shard.reclaimable {
                    (
                        LexicalCleanupDisposition::FailedReclaimable,
                        shard.recovery_reason.clone().unwrap_or_else(|| {
                            "failed shard can be rebuilt from source".to_string()
                        }),
                    )
                } else {
                    (
                        LexicalCleanupDisposition::FailedRetained,
                        shard.recovery_reason.clone().unwrap_or_else(|| {
                            "failed shard is retained for inspection".to_string()
                        }),
                    )
                }
            } else {
                (
                    LexicalCleanupDisposition::ActiveWork,
                    "shard is staged until generation lifecycle reaches a terminal state"
                        .to_string(),
                )
            };

        let reclaimable_bytes = if matches!(
            disposition,
            LexicalCleanupDisposition::SupersededReclaimable
                | LexicalCleanupDisposition::FailedReclaimable
        ) && shard.reclaimable
            && !shard.pinned
        {
            shard.artifact_bytes
        } else {
            0
        };

        LexicalShardCleanupInventory {
            shard_id: shard.shard_id.clone(),
            state: shard.state,
            disposition,
            reason,
            artifact_bytes: shard.artifact_bytes,
            reclaimable_bytes,
            retained_bytes: shard.artifact_bytes.saturating_sub(reclaimable_bytes),
        }
    }

    fn classify_generation_for_cleanup(
        &self,
        reclaimable_bytes: u64,
    ) -> (LexicalCleanupDisposition, String) {
        if self.is_serveable() {
            return (
                LexicalCleanupDisposition::CurrentPublished,
                "current published lexical generation is never reclaimable".to_string(),
            );
        }
        if matches!(
            self.publish_state,
            LexicalGenerationPublishState::Quarantined
        ) || self
            .shards
            .iter()
            .any(|shard| matches!(shard.state, LexicalShardLifecycleState::Quarantined))
        {
            return (
                LexicalCleanupDisposition::QuarantinedRetained,
                "quarantined lexical generation is retained for inspection".to_string(),
            );
        }
        if self.generation_has_active_work() {
            return (
                LexicalCleanupDisposition::ActiveWork,
                "active lexical generation work is retained".to_string(),
            );
        }
        if matches!(
            self.publish_state,
            LexicalGenerationPublishState::Superseded
        ) {
            return if reclaimable_bytes > 0 {
                (
                    LexicalCleanupDisposition::SupersededReclaimable,
                    "superseded lexical generation has unpinned reclaimable artifacts".to_string(),
                )
            } else {
                (
                    LexicalCleanupDisposition::SupersededRetained,
                    "superseded lexical generation is retained by policy".to_string(),
                )
            };
        }
        if matches!(self.build_state, LexicalGenerationBuildState::Failed)
            || self
                .shards
                .iter()
                .any(|shard| matches!(shard.state, LexicalShardLifecycleState::Abandoned))
        {
            return if reclaimable_bytes > 0 {
                (
                    LexicalCleanupDisposition::FailedReclaimable,
                    "failed lexical generation can be rebuilt from canonical source".to_string(),
                )
            } else {
                (
                    LexicalCleanupDisposition::FailedRetained,
                    "failed lexical generation is retained for inspection".to_string(),
                )
            };
        }
        (
            LexicalCleanupDisposition::PinnedRetained,
            "lexical generation is retained until cleanup policy marks it reclaimable".to_string(),
        )
    }

    fn classify_generation_retention_window(
        &self,
        disposition: LexicalCleanupDisposition,
    ) -> (Option<i64>, String) {
        match disposition {
            LexicalCleanupDisposition::SupersededReclaimable => (
                Some(self.updated_at_ms),
                "superseded generation retention window has elapsed; reclaimable after dry-run approval"
                    .to_string(),
            ),
            LexicalCleanupDisposition::FailedReclaimable => (
                Some(self.updated_at_ms),
                "failed generation retention window has elapsed; canonical SQLite can rebuild it after dry-run approval"
                    .to_string(),
            ),
            LexicalCleanupDisposition::QuarantinedRetained => (
                None,
                "quarantined generation is retained indefinitely until operator inspection clears it"
                    .to_string(),
            ),
            LexicalCleanupDisposition::CurrentPublished => (
                None,
                "current published lexical generation has no retention expiry".to_string(),
            ),
            LexicalCleanupDisposition::ActiveWork => (
                None,
                "active lexical generation work has no retention expiry while locks or resumable work exist"
                    .to_string(),
            ),
            LexicalCleanupDisposition::SupersededRetained => (
                None,
                "superseded generation is retained by policy or pinned shard artifacts".to_string(),
            ),
            LexicalCleanupDisposition::FailedRetained => (
                None,
                "failed generation is retained indefinitely for inspection".to_string(),
            ),
            LexicalCleanupDisposition::PinnedRetained => (
                None,
                "pinned generation has no retention expiry until the pin is removed".to_string(),
            ),
        }
    }

    fn generation_cleanup_allows_reclaim(&self) -> bool {
        if matches!(
            self.publish_state,
            LexicalGenerationPublishState::Quarantined
        ) || self
            .shards
            .iter()
            .any(|shard| matches!(shard.state, LexicalShardLifecycleState::Quarantined))
        {
            return false;
        }
        (matches!(
            self.publish_state,
            LexicalGenerationPublishState::Superseded
        ) || matches!(self.build_state, LexicalGenerationBuildState::Failed)
            || self
                .shards
                .iter()
                .any(|shard| matches!(shard.state, LexicalShardLifecycleState::Abandoned)))
            && !self.generation_has_active_work()
    }

    fn generation_has_active_work(&self) -> bool {
        matches!(
            self.build_state,
            LexicalGenerationBuildState::Scratch
                | LexicalGenerationBuildState::Building
                | LexicalGenerationBuildState::Built
                | LexicalGenerationBuildState::Validating
        ) || matches!(
            self.merge_debt.state,
            LexicalGenerationMergeDebtState::Pending
                | LexicalGenerationMergeDebtState::Running
                | LexicalGenerationMergeDebtState::Paused
                | LexicalGenerationMergeDebtState::Blocked
        )
    }
}
