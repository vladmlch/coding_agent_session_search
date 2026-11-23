use std::cmp::Ordering;

use super::types::{
    OmittedPackCandidate, PackCandidate, PackEvidenceRole, PackFreshnessPolicy, PackOmittedReason,
    PackPlanRequest, PackSelectedReason, PackSelectionScore, PackSourceReadiness, ScoredCandidate,
    SelectedState, TOKEN_ESTIMATE_CHARS_PER_TOKEN,
};

pub(super) fn hard_omission_reason(
    candidate: &PackCandidate,
    request: &PackPlanRequest,
    selected_state: &SelectedState,
) -> Option<PackOmittedReason> {
    if matches!(candidate.source_readiness, PackSourceReadiness::Unavailable) {
        return Some(PackOmittedReason::SourceUnavailable);
    }
    if is_stale_under_strict_policy(candidate, request) {
        return Some(PackOmittedReason::StaleUnderStrictPolicy);
    }
    if selected_state.span_hashes.contains(&candidate.span_hash)
        || selected_state
            .content_hashes
            .contains(&candidate.content_hash)
        || selected_state
            .verified_ranges
            .iter()
            .any(|span| span.overlaps(candidate))
    {
        return Some(PackOmittedReason::DuplicateContent);
    }
    None
}

fn is_stale_under_strict_policy(candidate: &PackCandidate, request: &PackPlanRequest) -> bool {
    if !matches!(request.freshness_policy, PackFreshnessPolicy::Strict) {
        return false;
    }
    let Some(created_at_ms) = candidate.created_at_ms else {
        return true;
    };
    let max_age_ms = request.freshness_window_seconds.saturating_mul(1_000);
    request.now_ms.saturating_sub(created_at_ms) > max_age_ms
}

pub(super) fn score_candidate(
    candidate: &PackCandidate,
    request: &PackPlanRequest,
    selected_state: &SelectedState,
    lexical_range: ScoreRange,
    semantic_range: ScoreRange,
    token_cost: usize,
) -> PackSelectionScore {
    let relevance_score = relevance_score(candidate, lexical_range, semantic_range);
    let coverage_score = coverage_score(candidate);
    let freshness_score = freshness_score(candidate, request);
    let source_diversity_score = source_diversity_score(candidate, selected_state);
    let source_authority_score = source_authority_score(candidate);
    let role_score = role_score(candidate.role);
    let citation_quality_score = citation_quality_score(candidate);
    let duplicate_penalty = duplicate_penalty(candidate, selected_state);
    let score = 0.35 * relevance_score
        + 0.20 * coverage_score
        + 0.15 * freshness_score
        + 0.10 * source_diversity_score
        + 0.10 * role_score
        + 0.05 * source_authority_score
        + 0.05 * citation_quality_score
        - duplicate_penalty;

    PackSelectionScore {
        score,
        relevance_score,
        coverage_score,
        freshness_score,
        source_diversity_score,
        source_authority_score,
        role_score,
        citation_quality_score,
        duplicate_penalty,
        token_cost,
        selected_reason: selected_reason(
            relevance_score,
            freshness_score,
            source_diversity_score,
            citation_quality_score,
        ),
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ScoreRange {
    min: f64,
    max: f64,
    has_value: bool,
}

impl ScoreRange {
    pub(super) fn from_values(values: impl Iterator<Item = f64>) -> Self {
        let mut range = Self {
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            has_value: false,
        };
        for value in values {
            range.has_value = true;
            range.min = range.min.min(value);
            range.max = range.max.max(value);
        }
        range
    }

    fn normalize(self, value: Option<f64>) -> f64 {
        let Some(value) = finite_score(value) else {
            return 0.0;
        };
        if !self.has_value {
            return 0.0;
        }
        if (self.max - self.min).abs() < f64::EPSILON {
            return if value > 0.0 { 1.0 } else { 0.0 };
        }
        ((value - self.min) / (self.max - self.min)).clamp(0.0, 1.0)
    }
}

pub(super) fn finite_score(score: Option<f64>) -> Option<f64> {
    score.filter(|value| value.is_finite())
}

fn relevance_score(
    candidate: &PackCandidate,
    lexical_range: ScoreRange,
    semantic_range: ScoreRange,
) -> f64 {
    let lexical = lexical_range.normalize(candidate.lexical_score);
    let semantic = semantic_range.normalize(candidate.semantic_score);
    let hybrid = candidate
        .hybrid_rank
        .map(|rank| 1.0 / rank.max(1) as f64)
        .unwrap_or(0.0);
    lexical.max(semantic).max(hybrid).clamp(0.0, 1.0)
}

fn coverage_score(candidate: &PackCandidate) -> f64 {
    let denominator = candidate
        .query_term_count
        .saturating_add(candidate.query_phrase_count.saturating_mul(2));
    if denominator == 0 {
        return 0.0;
    }
    let numerator = candidate
        .matched_terms
        .len()
        .saturating_add(candidate.matched_phrases.len().saturating_mul(2));
    (numerator as f64 / denominator as f64).clamp(0.0, 1.0)
}

pub(super) fn freshness_score(candidate: &PackCandidate, request: &PackPlanRequest) -> f64 {
    let Some(created_at_ms) = candidate.created_at_ms else {
        return match request.freshness_policy {
            PackFreshnessPolicy::PreferRecent => 0.25,
            PackFreshnessPolicy::Strict => 0.0,
            PackFreshnessPolicy::AllowStale => 1.0,
        };
    };
    let age_ms = request.now_ms.saturating_sub(created_at_ms).max(0);
    let window_ms = request
        .freshness_window_seconds
        .max(1)
        .saturating_mul(1_000);
    if age_ms <= window_ms {
        return 1.0;
    }
    let max_decay_ms = window_ms.saturating_mul(4);
    if age_ms >= max_decay_ms {
        0.0
    } else {
        1.0 - ((age_ms - window_ms) as f64 / (max_decay_ms - window_ms) as f64)
    }
}

fn source_diversity_score(candidate: &PackCandidate, selected_state: &SelectedState) -> f64 {
    let session_key = candidate.session_key();
    if selected_state.sessions.contains(&session_key) {
        0.0
    } else if selected_state.source_ids.contains(&candidate.source_id) {
        0.5
    } else {
        1.0
    }
}

fn source_authority_score(candidate: &PackCandidate) -> f64 {
    match (
        candidate.source_explicitly_requested,
        candidate.origin_kind.as_str(),
        candidate.source_readiness,
    ) {
        (true, _, PackSourceReadiness::Healthy) => 1.0,
        (_, "local", PackSourceReadiness::Healthy) => 1.0,
        (_, _, PackSourceReadiness::Healthy) => 0.9,
        (_, _, PackSourceReadiness::StaleReadable) => 0.6,
        (_, _, PackSourceReadiness::IncompleteMetadata) => 0.4,
        (_, _, PackSourceReadiness::Unavailable) => 0.0,
    }
}

fn role_score(role: PackEvidenceRole) -> f64 {
    match role {
        PackEvidenceRole::AssistantConclusion | PackEvidenceRole::ToolResult => 1.0,
        PackEvidenceRole::UserRequirement => 0.85,
        PackEvidenceRole::ToolCallArgument => 0.65,
        PackEvidenceRole::Unknown => 0.5,
    }
}

fn citation_quality_score(candidate: &PackCandidate) -> f64 {
    let has_path = !candidate.source_path.trim().is_empty();
    let has_source = !candidate.source_id.trim().is_empty();
    let has_agent = !candidate.agent.trim().is_empty();
    let has_line_span = candidate.verified_line_range().is_some();
    if has_path && has_source && has_agent && has_line_span {
        1.0
    } else if has_path && has_source && has_agent {
        0.75
    } else if has_path && has_agent {
        0.5
    } else {
        0.0
    }
}

fn duplicate_penalty(candidate: &PackCandidate, selected_state: &SelectedState) -> f64 {
    if selected_state.span_hashes.contains(&candidate.span_hash) {
        return 1.0;
    }
    if selected_state
        .content_hashes
        .contains(&candidate.content_hash)
    {
        return 0.5;
    }
    if selected_state
        .verified_ranges
        .iter()
        .any(|span| span.overlaps(candidate))
    {
        return 0.25;
    }
    0.0
}

fn selected_reason(
    relevance_score: f64,
    freshness_score: f64,
    source_diversity_score: f64,
    citation_quality_score: f64,
) -> PackSelectedReason {
    let scores = [
        (relevance_score, PackSelectedReason::HighRelevance),
        (freshness_score, PackSelectedReason::FreshEvidence),
        (source_diversity_score, PackSelectedReason::SourceDiversity),
        (citation_quality_score, PackSelectedReason::StrongCitation),
        (0.0, PackSelectedReason::BudgetFit),
    ];
    scores
        .into_iter()
        .max_by(|(left, _), (right, _)| left.total_cmp(right))
        .map(|(_, reason)| reason)
        .unwrap_or(PackSelectedReason::BudgetFit)
}

pub(super) fn candidate_ordering(
    left: &ScoredCandidate,
    left_candidate: &PackCandidate,
    right: &ScoredCandidate,
    right_candidate: &PackCandidate,
) -> Ordering {
    right
        .score
        .score
        .total_cmp(&left.score.score)
        .then_with(|| {
            right
                .score
                .relevance_score
                .total_cmp(&left.score.relevance_score)
        })
        .then_with(|| {
            compare_newer_first(left_candidate.created_at_ms, right_candidate.created_at_ms)
        })
        .then_with(|| left_candidate.source_id.cmp(&right_candidate.source_id))
        .then_with(|| left_candidate.source_path.cmp(&right_candidate.source_path))
        .then_with(|| {
            match (
                left_candidate.conversation_id,
                right_candidate.conversation_id,
            ) {
                (Some(left), Some(right)) => left.cmp(&right),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            }
        })
        .then_with(|| left_candidate.agent.cmp(&right_candidate.agent))
        .then_with(|| {
            compare_optional_usize_low_first(
                left_candidate.message_index,
                right_candidate.message_index,
            )
        })
        .then_with(|| {
            compare_optional_usize_low_first(left_candidate.line_start, right_candidate.line_start)
        })
        .then_with(|| {
            left_candidate
                .content_hash
                .cmp(&right_candidate.content_hash)
        })
}

fn compare_newer_first(left: Option<i64>, right: Option<i64>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => right.cmp(&left),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn compare_optional_usize_low_first(left: Option<usize>, right: Option<usize>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

pub(super) fn omitted_candidate(
    candidate: &PackCandidate,
    reason: PackOmittedReason,
    score: PackSelectionScore,
) -> OmittedPackCandidate {
    OmittedPackCandidate {
        candidate_id: candidate.candidate_id.clone(),
        source_path: candidate.source_path.clone(),
        line_start: candidate.line_start,
        agent: candidate.agent.clone(),
        reason,
        score: score.score,
        estimated_tokens: score.token_cost,
    }
}

pub(super) fn truncate_excerpt(excerpt: &str, max_chars: usize) -> (String, bool) {
    if excerpt.chars().count() <= max_chars {
        return (excerpt.to_string(), false);
    }
    let keep_chars = max_chars.saturating_sub(3);
    let mut out: String = excerpt.chars().take(keep_chars).collect();
    out.push_str("...");
    (out, true)
}

pub(super) fn estimated_tokens(text: &str) -> usize {
    text.chars()
        .count()
        .div_ceil(TOKEN_ESTIMATE_CHARS_PER_TOKEN)
}

pub(super) fn evidence_id(candidate: &PackCandidate) -> String {
    // Pathnames alone are not session identities: provider databases contain
    // many conversations at one path. Length-prefix strings so embedded
    // newlines/colons cannot move data into a neighboring identity component.
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cass.pack.evidence.v2\0");
    for text in [
        &candidate.source_id,
        &candidate.source_path,
        &candidate.agent,
        &candidate.content_hash,
    ] {
        hasher.update(&(text.len() as u64).to_le_bytes());
        hasher.update(text.as_bytes());
    }
    match candidate.conversation_id {
        Some(id) => {
            hasher.update(&[1]);
            hasher.update(&id.to_le_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    for coordinate in [
        candidate.message_index,
        candidate.line_start,
        candidate.line_end,
    ] {
        match coordinate {
            Some(number) => {
                hasher.update(&[1]);
                hasher.update(&(number as u64).to_le_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
    }
    hasher.update(&(candidate.span_hash.len() as u64).to_le_bytes());
    hasher.update(candidate.span_hash.as_bytes());
    let hash = hasher.finalize();
    encoded_evidence_id(hash.as_bytes())
}

pub(super) fn encoded_evidence_id(hash: &[u8; 32]) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut id = String::with_capacity(55);
    id.push_str("ev_");
    // RFC 4648 base32, without '=' padding: the digest is always 256 bits.
    // Keep the entire hash, including its final bit in the last character.
    for chunk in hash.chunks(5) {
        let mut block = [0u8; 8];
        block[3..3 + chunk.len()].copy_from_slice(chunk);
        let word = u64::from_be_bytes(block);
        for shift in (0..chunk.len() * 8).step_by(5) {
            let digit = ((word >> (35 - shift)) & 31) as usize;
            id.push(char::from(ALPHABET[digit]));
        }
    }
    id
}
