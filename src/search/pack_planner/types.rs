use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{Deserialize, Serialize};

use crate::robot_budget_envelope::BudgetBlock;

use super::super::query::{MatchType, SearchHit};
use super::scoring::evidence_id;

pub(super) const TOKEN_ESTIMATE_CHARS_PER_TOKEN: usize = 4;
pub(super) const DEFAULT_FRESHNESS_WINDOW_SECONDS: i64 = 30 * 24 * 60 * 60;
pub(super) const PACK_CANDIDATE_LIMIT_CAP: usize = 2_048;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackPlannerLimits {
    pub max_tokens: usize,
    pub max_sessions: usize,
    pub max_evidence: usize,
    pub context_lines: usize,
    pub max_excerpt_chars: usize,
}

impl Default for PackPlannerLimits {
    fn default() -> Self {
        Self {
            max_tokens: 12_000,
            max_sessions: 8,
            max_evidence: 24,
            context_lines: 3,
            max_excerpt_chars: 1_600,
        }
    }
}

impl PackPlannerLimits {
    pub fn validate(&self) -> Result<(), PackPlannerLimitError> {
        validate_range("max_tokens", self.max_tokens, 1_024, 200_000)?;
        validate_range("max_sessions", self.max_sessions, 1, 64)?;
        validate_range("max_evidence", self.max_evidence, 1, 256)?;
        validate_range("context_lines", self.context_lines, 0, 40)?;
        validate_range("max_excerpt_chars", self.max_excerpt_chars, 80, 8_000)?;
        Ok(())
    }
}

fn validate_range(
    field: &'static str,
    value: usize,
    min: usize,
    max: usize,
) -> Result<(), PackPlannerLimitError> {
    if (min..=max).contains(&value) {
        Ok(())
    } else {
        Err(PackPlannerLimitError {
            field,
            value,
            min,
            max,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackPlannerLimitError {
    pub field: &'static str,
    pub value: usize,
    pub min: usize,
    pub max: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PackFreshnessPolicy {
    #[default]
    PreferRecent,
    Strict,
    AllowStale,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackEvidenceRole {
    AssistantConclusion,
    ToolResult,
    UserRequirement,
    ToolCallArgument,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackSourceReadiness {
    #[default]
    Healthy,
    StaleReadable,
    IncompleteMetadata,
    Unavailable,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackLexicalReadiness {
    #[default]
    Ready,
    Stale,
    Missing,
    Rebuilding,
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackSemanticReadiness {
    #[default]
    NotReported,
    Joined,
    FallbackLexical,
    Unavailable,
    Disabled,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackSourceSyncGapKind {
    RemoteStale,
    SourcePruned,
    SyncDeferred,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackSourceSyncGap {
    pub source_id: String,
    pub origin_kind: String,
    pub kind: PackSourceSyncGapKind,
    pub lag_seconds: Option<i64>,
    pub last_synced_at_ms: Option<i64>,
    pub recommended_action: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackReadinessSnapshot {
    pub index_generation: Option<String>,
    pub lexical_readiness: PackLexicalReadiness,
    pub semantic_readiness: PackSemanticReadiness,
    pub active_rebuild: bool,
    pub lock_state: Option<String>,
    pub missing_database: bool,
    pub source_sync_gaps: Vec<PackSourceSyncGap>,
    pub recommended_action: Option<String>,
}

impl Default for PackReadinessSnapshot {
    fn default() -> Self {
        Self {
            index_generation: None,
            lexical_readiness: PackLexicalReadiness::Ready,
            semantic_readiness: PackSemanticReadiness::NotReported,
            active_rebuild: false,
            lock_state: None,
            missing_database: false,
            source_sync_gaps: Vec::new(),
            recommended_action: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PackCandidate {
    pub candidate_id: String,
    pub source_path: String,
    pub source_id: String,
    pub origin_kind: String,
    pub origin_host: Option<String>,
    pub workspace: String,
    pub workspace_original: Option<String>,
    pub agent: String,
    pub line_start: Option<usize>,
    pub line_end: Option<usize>,
    pub conversation_id: Option<i64>,
    /// One-based canonical `messages.idx + 1`, as accepted by --message-index.
    /// Physical source-file coordinates belong only in line_start/line_end.
    pub message_index: Option<usize>,
    pub citation_verified: bool,
    pub content_hash: String,
    pub span_hash: String,
    pub created_at_ms: Option<i64>,
    pub indexed_at_ms: Option<i64>,
    pub match_type: String,
    pub excerpt: String,
    pub role: PackEvidenceRole,
    pub lexical_score: Option<f64>,
    pub semantic_score: Option<f64>,
    pub hybrid_rank: Option<usize>,
    pub matched_terms: Vec<String>,
    pub matched_phrases: Vec<String>,
    pub query_term_count: usize,
    pub query_phrase_count: usize,
    pub source_readiness: PackSourceReadiness,
    pub source_explicitly_requested: bool,
}

impl PackCandidate {
    pub fn from_search_hit(
        hit: &SearchHit,
        query_term_count: usize,
        query_phrase_count: usize,
    ) -> Self {
        // Search navigation uses normalized message ordinals. Physical source
        // lines are assigned only after reading and matching the source record.
        let message_index = hit.line_number.filter(|&line| line != 0);
        let source_id = if hit.source_id.trim().is_empty() {
            "local".to_string()
        } else {
            hit.source_id.trim().to_string()
        };
        let origin_kind = if hit.origin_kind.trim().is_empty() {
            "local".to_string()
        } else {
            hit.origin_kind.trim().to_string()
        };
        let content_hash = format!("{:016x}", hit.content_hash);
        let mut candidate = Self {
            candidate_id: String::new(),
            source_path: hit.source_path.clone(),
            source_id,
            origin_kind,
            origin_host: hit.origin_host.clone(),
            workspace: hit.workspace.clone(),
            workspace_original: hit.workspace_original.clone(),
            agent: hit.agent.clone(),
            line_start: None,
            line_end: None,
            conversation_id: hit.conversation_id,
            message_index,
            citation_verified: false,
            content_hash: content_hash.clone(),
            span_hash: content_hash,
            created_at_ms: hit.created_at,
            indexed_at_ms: None,
            match_type: match_type_robot_name(hit.match_type).to_string(),
            excerpt: if hit.content.is_empty() {
                hit.snippet.clone()
            } else {
                hit.content.clone()
            },
            role: PackEvidenceRole::Unknown,
            lexical_score: Some(hit.score as f64),
            semantic_score: None,
            hybrid_rank: None,
            matched_terms: Vec::new(),
            matched_phrases: Vec::new(),
            query_term_count,
            query_phrase_count,
            source_readiness: PackSourceReadiness::Healthy,
            source_explicitly_requested: false,
        };
        candidate.candidate_id = evidence_id(&candidate);
        candidate
    }

    pub(super) fn session_key(&self) -> PackSessionKey {
        PackSessionKey {
            source_id: self.source_id.clone(),
            source_path: self.source_path.clone(),
            agent: self.agent.clone(),
            conversation_id: self.conversation_id,
        }
    }

    pub(super) fn verified_line_range(&self) -> Option<(usize, usize)> {
        if !self.citation_verified {
            return None;
        }
        let start = self.line_start?;
        let end = self.line_end.unwrap_or(start);
        (start > 0 && end >= start).then_some((start, end))
    }
}

/// Keep archive identity separate from display redaction. A provider database
/// can hold multiple conversations at one path; unknown conversation IDs must
/// not erase the provider identity we do have.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct PackSessionKey {
    pub(super) source_id: String,
    pub(super) source_path: String,
    pub(super) agent: String,
    pub(super) conversation_id: Option<i64>,
}

/// Physical-file coordinates are not scoped by archive conversation/provider.
/// Two canonical sessions can cite the same verified bytes in one local file;
/// equal paths on different sources, however, are different files.
#[derive(Debug)]
pub(super) struct VerifiedPackFileSpan {
    pub(super) source_id: String,
    pub(super) source_path: String,
    pub(super) start: usize,
    pub(super) end: usize,
}

impl VerifiedPackFileSpan {
    pub(super) fn from_candidate(candidate: &PackCandidate) -> Option<Self> {
        let (start, end) = candidate.verified_line_range()?;
        Some(Self {
            source_id: candidate.source_id.clone(),
            source_path: candidate.source_path.clone(),
            start,
            end,
        })
    }

    pub(super) fn overlaps(&self, candidate: &PackCandidate) -> bool {
        candidate.verified_line_range().is_some_and(|(start, end)| {
            self.source_id == candidate.source_id
                && self.source_path == candidate.source_path
                && self.start <= end
                && start <= self.end
        })
    }
}

fn match_type_robot_name(match_type: MatchType) -> &'static str {
    match match_type {
        MatchType::Exact => "exact",
        MatchType::Prefix => "prefix",
        MatchType::Suffix => "suffix",
        MatchType::Substring => "substring",
        MatchType::Wildcard => "wildcard",
        MatchType::ImplicitWildcard => "implicit_wildcard",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PackPlanRequest {
    pub now_ms: i64,
    pub limits: PackPlannerLimits,
    pub freshness_policy: PackFreshnessPolicy,
    pub freshness_window_seconds: i64,
    pub candidates: Vec<PackCandidate>,
    pub explain_selection: bool,
    pub include_skill_content: bool,
}

impl Default for PackPlanRequest {
    fn default() -> Self {
        Self {
            now_ms: 0,
            limits: PackPlannerLimits::default(),
            freshness_policy: PackFreshnessPolicy::PreferRecent,
            freshness_window_seconds: DEFAULT_FRESHNESS_WINDOW_SECONDS,
            candidates: Vec::new(),
            explain_selection: false,
            include_skill_content: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlannedAnswerPack {
    pub candidate_count: usize,
    pub selected_evidence_count: usize,
    pub selected_session_count: usize,
    pub estimated_tokens: usize,
    pub diagnostics: PackPlannerDiagnostics,
    pub evidence: Vec<PlannedPackEvidence>,
    pub omitted: Vec<OmittedPackCandidate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackPlannerDiagnostics {
    pub candidate_fetch_limit: usize,
    pub budget: PackPlannerBudget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackPlannerBudget {
    pub max_tokens: usize,
    pub metadata_tokens: usize,
    pub outline_tokens: usize,
    pub evidence_tokens: usize,
    pub omitted_tokens: usize,
    pub max_output_tokens_with_overflow: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlannedPackEvidence {
    pub id: String,
    pub rank: usize,
    /// Redacted and truncated output whose exact text was charged to the budget.
    pub excerpt: String,
    pub excerpt_truncated: bool,
    pub estimated_tokens: usize,
    pub candidate: PackCandidate,
    pub selection: PackSelectionScore,
    pub(super) redactions: Vec<RenderedRedaction>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OmittedPackCandidate {
    pub candidate_id: String,
    pub source_path: String,
    pub line_start: Option<usize>,
    pub agent: String,
    pub reason: PackOmittedReason,
    pub score: f64,
    pub estimated_tokens: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackOmittedReason {
    TokenBudgetExhausted,
    MaxSessionsReached,
    MaxEvidenceReached,
    DuplicateContent,
    SameSessionLowerRank,
    StaleUnderStrictPolicy,
    SourceUnavailable,
    RedactedToEmpty,
    FieldMaskExcluded,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PackSelectionScore {
    pub score: f64,
    pub relevance_score: f64,
    pub coverage_score: f64,
    pub freshness_score: f64,
    pub source_diversity_score: f64,
    pub source_authority_score: f64,
    pub role_score: f64,
    pub citation_quality_score: f64,
    pub duplicate_penalty: f64,
    pub token_cost: usize,
    pub selected_reason: PackSelectedReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackSelectedReason {
    HighRelevance,
    FreshEvidence,
    SourceDiversity,
    StrongCitation,
    BudgetFit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackRenderFormat {
    Json,
    CompactJson,
    Jsonl,
    Toon,
    Markdown,
}

impl PackRenderFormat {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::CompactJson => "compact",
            Self::Jsonl => "jsonl",
            Self::Toon => "toon",
            Self::Markdown => "markdown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackRenderRequest {
    /// What the pack's search actually ran, echoed as `_meta.effective`
    /// (2l1b0.68); `None` omits it.
    pub effective: Option<serde_json::Value>,
    pub query_text: String,
    pub normalized_query: String,
    pub generated_at_ms: i64,
    pub elapsed_ms: u64,
    pub budget: BudgetBlock,
    pub request_id: Option<String>,
    pub format: PackRenderFormat,
    pub limits: PackPlannerLimits,
    pub search_mode: String,
    pub fallback_mode: Option<String>,
    /// Why the executor fell back to `fallback_mode`, as a typed reason code
    /// (`SemanticFallbackReason::code`) it observed (GH #507). `None` means no
    /// cause was supplied, not that assets are missing; the renderer flags such
    /// a fallback instead of inventing a cause.
    pub fallback_reason: Option<String>,
    pub semantic_joined: bool,
    pub freshness_policy: PackFreshnessPolicy,
    pub freshness_window_seconds: i64,
    pub redaction_policy: String,
    pub sensitive_output: bool,
    pub explain_selection: bool,
    pub readiness: PackReadinessSnapshot,
}

impl Default for PackRenderRequest {
    fn default() -> Self {
        Self {
            effective: None,
            query_text: String::new(),
            normalized_query: String::new(),
            generated_at_ms: 0,
            elapsed_ms: 0,
            budget: BudgetBlock {
                elapsed_ms: 0,
                budget_ms: 8_000,
                timed_out: false,
                skipped_sections: Vec::new(),
                recommended_next_probe: None,
            },
            request_id: None,
            format: PackRenderFormat::Json,
            limits: PackPlannerLimits::default(),
            search_mode: "hybrid".to_string(),
            fallback_mode: None,
            fallback_reason: None,
            semantic_joined: false,
            freshness_policy: PackFreshnessPolicy::PreferRecent,
            freshness_window_seconds: DEFAULT_FRESHNESS_WINDOW_SECONDS,
            redaction_policy: "strict".to_string(),
            sensitive_output: false,
            explain_selection: false,
            readiness: PackReadinessSnapshot::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackRenderError {
    pub format: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedAnswerPack {
    pub(super) schema_version: &'static str,
    pub(super) query: RenderedQuery,
    #[serde(rename = "_meta")]
    pub(super) meta: RenderedMeta,
    pub(super) budget: BudgetBlock,
    pub(super) limits: RenderedLimits,
    pub(super) realized: RenderedRealized,
    pub(super) health: RenderedHealth,
    pub(super) freshness: RenderedFreshness,
    pub(super) pack: RenderedPack,
    pub(super) evidence: Vec<RenderedEvidence>,
    pub(super) omitted: RenderedOmitted,
    pub(super) privacy: RenderedPrivacy,
    pub(super) warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedQuery {
    pub(super) text: String,
    pub(super) normalized: String,
    pub(super) filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedMeta {
    pub(super) request_id: Option<String>,
    pub(super) generated_at_ms: i64,
    pub(super) elapsed_ms: u64,
    pub(super) partial: bool,
    pub(super) format: &'static str,
    pub(super) warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) effective: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedLimits {
    pub(super) max_tokens: usize,
    pub(super) estimated_tokens: usize,
    pub(super) max_sessions: usize,
    pub(super) max_evidence: usize,
    pub(super) context_lines: usize,
    pub(super) max_excerpt_chars: usize,
    pub(super) field_mask: &'static str,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedRealized {
    pub(super) search_mode: String,
    pub(super) fallback_mode: Option<String>,
    pub(super) fallback_reason: Option<String>,
    pub(super) semantic_joined: bool,
    pub(super) candidate_count: usize,
    pub(super) selected_evidence_count: usize,
    pub(super) selected_session_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedHealth {
    pub(super) healthy: bool,
    pub(super) recommended_action: Option<String>,
    pub(super) index_state: &'static str,
    pub(super) index_generation: Option<String>,
    pub(super) lexical_readiness: &'static str,
    pub(super) semantic_state: &'static str,
    pub(super) active_rebuild: bool,
    pub(super) lock_state: Option<String>,
    pub(super) missing_database: bool,
    pub(super) source_sync_gaps: Vec<RenderedSourceSyncGap>,
    pub(super) source_readiness: Vec<RenderedSourceReadiness>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedSourceReadiness {
    pub(super) source_id: String,
    pub(super) origin_kind: String,
    pub(super) readiness: &'static str,
    pub(super) healthy: bool,
    pub(super) evidence_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedSourceSyncGap {
    pub(super) source_id: String,
    pub(super) origin_kind: String,
    pub(super) kind: &'static str,
    pub(super) lag_seconds: Option<i64>,
    pub(super) last_synced_at_ms: Option<i64>,
    pub(super) recommended_action: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedFreshness {
    pub(super) policy: &'static str,
    pub(super) window_seconds: i64,
    pub(super) newest_evidence_at_ms: Option<i64>,
    pub(super) oldest_evidence_at_ms: Option<i64>,
    pub(super) stale_evidence_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedPack {
    pub(super) title: String,
    pub(super) answer_outline: Vec<RenderedOutlineItem>,
    pub(super) source_summary: Vec<RenderedSourceSummary>,
    pub(super) handoff: Vec<RenderedHandoffItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedOutlineItem {
    pub(super) rank: usize,
    pub(super) heading: String,
    pub(super) evidence_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedSourceSummary {
    pub(super) source_id: String,
    pub(super) origin_kind: String,
    pub(super) session_count: usize,
    pub(super) evidence_count: usize,
    pub(super) newest_evidence_at_ms: Option<i64>,
    pub(super) healthy: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedHandoffItem {
    pub(super) rank: usize,
    pub(super) kind: &'static str,
    pub(super) text: String,
    pub(super) evidence_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedEvidence {
    pub(super) id: String,
    pub(super) rank: usize,
    pub(super) excerpt: String,
    pub(super) excerpt_truncated: bool,
    pub(super) estimated_tokens: usize,
    pub(super) citation: RenderedCitation,
    /// Metadata-only trust/provenance verdict for this evidence item (bead
    /// 5u82n.3). Advisory; mirrors the per-hit `trust` block on `cass search`.
    pub(super) trust: crate::search::trust_scoring::TrustAssessment,
    pub(super) selection: RenderedSelection,
    pub(super) roles: Vec<&'static str>,
    pub(super) matched_terms: Vec<String>,
    pub(super) redactions: Vec<RenderedRedaction>,
    #[serde(skip)]
    pub(super) source_readiness: PackSourceReadiness,
    #[serde(skip)]
    pub(super) session_key: PackSessionKey,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedCitation {
    pub(super) source_path: String,
    pub(super) source_id: String,
    pub(super) origin_kind: String,
    pub(super) origin_host: Option<String>,
    pub(super) workspace: String,
    pub(super) workspace_original: Option<String>,
    pub(super) agent: String,
    pub(super) line_start: Option<usize>,
    pub(super) line_end: Option<usize>,
    pub(super) message_index: Option<usize>,
    pub(super) message_index_base: u8,
    pub(super) conversation_id: Option<i64>,
    pub(super) content_hash: String,
    pub(super) span_hash: String,
    pub(super) excerpt_sha256: String,
    pub(super) created_at_ms: Option<i64>,
    pub(super) indexed_at_ms: Option<i64>,
    pub(super) freshness_age_seconds: Option<i64>,
    pub(super) match_type: String,
    pub(super) verified: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedSelection {
    pub(super) score: f64,
    pub(super) token_cost: usize,
    pub(super) selected_reason: PackSelectedReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) relevance_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) coverage_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) freshness_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) source_diversity_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) source_authority_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) role_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) citation_quality_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) duplicate_penalty: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct RenderedRedaction {
    pub(super) kind: String,
    pub(super) start_char: usize,
    pub(super) end_char: usize,
    pub(super) replacement: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedOmitted {
    pub(super) count: usize,
    pub(super) items: Vec<OmittedPackCandidate>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct RenderedPrivacy {
    pub(super) redaction_policy: String,
    pub(super) redaction_applied: bool,
    pub(super) sensitive_output: bool,
    pub(super) skill_content_included: bool,
    pub(super) redaction_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Default)]
pub(super) struct SourceAccumulator {
    pub(super) origin_kind: String,
    pub(super) sessions: BTreeSet<PackSessionKey>,
    pub(super) evidence_count: usize,
    pub(super) newest_evidence_at_ms: Option<i64>,
    pub(super) healthy: bool,
    pub(super) worst_readiness: PackSourceReadiness,
}

#[derive(Debug, Clone)]
pub(super) struct ScoredCandidate {
    pub(super) index: usize,
    pub(super) score: PackSelectionScore,
    pub(super) excerpt: String,
    pub(super) excerpt_truncated: bool,
}

#[derive(Debug, Default)]
pub(super) struct SelectedState {
    pub(super) source_ids: HashSet<String>,
    pub(super) sessions: HashSet<PackSessionKey>,
    pub(super) span_hashes: HashSet<String>,
    pub(super) content_hashes: HashSet<String>,
    pub(super) verified_ranges: Vec<VerifiedPackFileSpan>,
}
