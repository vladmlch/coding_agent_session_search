//! Deterministic answer-pack evidence selection.
//!
//! This module is intentionally independent of the CLI. `cass pack` will wire it
//! to search, source health, renderers, and robot docs in later beads.

mod planner;
mod redaction;
mod render;
mod render_formats;
mod scoring;
mod types;

#[cfg(test)]
mod tests;

use std::collections::HashSet;

pub(crate) use planner::verify_pack_source_citations;
pub use planner::{
    budget_fallback_answer_pack, pack_candidate_fetch_limit, pack_planner_budget, plan_answer_pack,
};
pub use render::{
    render_answer_pack, render_answer_pack_value,
    render_answer_pack_value_without_trust_correlation,
    render_answer_pack_without_trust_correlation,
};
pub(crate) use render::{
    render_answer_pack_value_with_correlation, render_answer_pack_with_correlation,
};
pub use types::{
    OmittedPackCandidate, PackCandidate, PackEvidenceRole, PackFreshnessPolicy,
    PackLexicalReadiness, PackOmittedReason, PackPlanRequest, PackPlannerBudget,
    PackPlannerDiagnostics, PackPlannerLimitError, PackPlannerLimits, PackReadinessSnapshot,
    PackRenderError, PackRenderFormat, PackRenderRequest, PackSelectedReason, PackSelectionScore,
    PackSemanticReadiness, PackSourceReadiness, PackSourceSyncGap, PackSourceSyncGapKind,
    PlannedAnswerPack, PlannedPackEvidence,
};

#[cfg(test)]
use redaction::{
    REDACTED_ENCRYPTED_PAYLOAD_MARKER, REDACTED_REMOTE_HOST_MARKER, REDACTED_SOURCE_MARKER,
    REDACTED_VALUE_MARKER,
};
#[cfg(test)]
use render_formats::pack_toon_encode_options;
#[cfg(test)]
use scoring::{encoded_evidence_id, evidence_id, freshness_score};
#[cfg(test)]
use types::PACK_CANDIDATE_LIMIT_CAP;

use scoring::{estimated_tokens, omitted_candidate, truncate_excerpt};
use types::TOKEN_ESTIMATE_CHARS_PER_TOKEN;

impl PlannedAnswerPack {
    /// Make progress toward a measured final-output bound without changing
    /// citation identity. Rendering must be repeated after each reduction:
    /// escaping, handoff text and format overhead affect the actual savings.
    pub(crate) fn reduce_for_output_budget(
        &mut self,
        excess_tokens: usize,
        require_evidence: bool,
    ) -> bool {
        let Some(item) = self.evidence.last_mut() else {
            return false;
        };
        let chars = item.excerpt.chars().count();
        let max_chars = chars
            .saturating_sub(
                excess_tokens
                    .max(1)
                    .saturating_mul(TOKEN_ESTIMATE_CHARS_PER_TOKEN),
            )
            .max(4);
        if max_chars < chars
            && item
                .excerpt
                .chars()
                .take(max_chars - 3)
                .any(|character| !character.is_whitespace())
        {
            let (excerpt, _) = truncate_excerpt(&item.excerpt, max_chars);
            self.estimated_tokens = self.estimated_tokens.saturating_sub(item.estimated_tokens);
            item.excerpt = excerpt;
            item.excerpt_truncated = true;
            item.estimated_tokens = estimated_tokens(&item.excerpt);
            item.selection.token_cost = item.estimated_tokens;
            self.estimated_tokens += item.estimated_tokens;
            return true;
        }
        if require_evidence && self.evidence.len() == 1 {
            return false;
        }
        let Some(item) = self.evidence.pop() else {
            return false;
        };
        self.estimated_tokens = self.estimated_tokens.saturating_sub(item.estimated_tokens);
        self.omitted.push(omitted_candidate(
            &item.candidate,
            PackOmittedReason::TokenBudgetExhausted,
            item.selection,
        ));
        self.selected_evidence_count = self.evidence.len();
        self.selected_session_count = self
            .evidence
            .iter()
            .map(|item| item.candidate.session_key())
            .collect::<HashSet<_>>()
            .len();
        true
    }
}
