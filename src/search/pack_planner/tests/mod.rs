mod citations_and_redaction;
mod planning;
mod rendering;

pub(super) use crate::robot_budget_envelope::BudgetBlock;
pub(super) use crate::search::query::{MatchType, SearchHit};

use super::{
    PackCandidate, PackEvidenceRole, PackFreshnessPolicy, PackPlanRequest, PackPlannerLimits,
    PackReadinessSnapshot, PackRenderFormat, PackRenderRequest, PackSourceReadiness,
};

pub(super) fn candidate(id: &str, source_id: &str, source_path: &str, score: f64) -> PackCandidate {
    PackCandidate {
        candidate_id: id.to_string(),
        source_path: source_path.to_string(),
        source_id: source_id.to_string(),
        // ubs:ignore — compares a public source-kind fixture label, not a secret.
        origin_kind: if source_id == "local" {
            "local".to_string()
        } else {
            "ssh".to_string()
        },
        origin_host: None,
        workspace: "/work".to_string(),
        workspace_original: None,
        agent: "codex".to_string(),
        line_start: Some(10),
        line_end: Some(12),
        conversation_id: None,
        message_index: None,
        citation_verified: true,
        content_hash: format!("{id}_content"),
        span_hash: format!("{id}_span"),
        created_at_ms: Some(1_000_000),
        indexed_at_ms: Some(1_000_000),
        match_type: "exact".to_string(),
        excerpt: "0123456789abcdef".to_string(),
        role: PackEvidenceRole::AssistantConclusion,
        lexical_score: Some(score),
        semantic_score: None,
        hybrid_rank: None,
        matched_terms: vec!["pack".to_string()],
        matched_phrases: Vec::new(),
        query_term_count: 1,
        query_phrase_count: 0,
        source_readiness: PackSourceReadiness::Healthy,
        source_explicitly_requested: false,
    }
}

pub(super) fn request(candidates: Vec<PackCandidate>) -> PackPlanRequest {
    PackPlanRequest {
        now_ms: 1_000_000,
        limits: PackPlannerLimits {
            max_tokens: 1_024,
            max_sessions: 8,
            max_evidence: 24,
            context_lines: 3,
            max_excerpt_chars: 80,
        },
        freshness_policy: PackFreshnessPolicy::PreferRecent,
        freshness_window_seconds: 60,
        candidates,
        explain_selection: false,
        include_skill_content: false,
    }
}

pub(super) fn render_request(format: PackRenderFormat) -> PackRenderRequest {
    PackRenderRequest {
        effective: None,
        query_text: "pack handoff".to_string(),
        normalized_query: "pack handoff".to_string(),
        generated_at_ms: 1_060_000,
        elapsed_ms: 7,
        budget: BudgetBlock {
            elapsed_ms: 7,
            budget_ms: 8_000,
            timed_out: false,
            skipped_sections: Vec::new(),
            recommended_next_probe: None,
        },
        request_id: Some("req-1".to_string()),
        format,
        limits: PackPlannerLimits {
            max_tokens: 1_024,
            max_sessions: 8,
            max_evidence: 24,
            context_lines: 3,
            max_excerpt_chars: 80,
        },
        search_mode: "hybrid".to_string(),
        fallback_mode: Some("lexical".to_string()),
        fallback_reason: Some("semantic_absent".to_string()),
        semantic_joined: false,
        freshness_policy: PackFreshnessPolicy::PreferRecent,
        freshness_window_seconds: 60,
        redaction_policy: "strict".to_string(),
        sensitive_output: false,
        explain_selection: false,
        readiness: PackReadinessSnapshot::default(),
    }
}

pub(super) fn timed_out_budget() -> BudgetBlock {
    BudgetBlock {
        elapsed_ms: 125,
        budget_ms: 100,
        timed_out: true,
        skipped_sections: vec![
            "semantic_refinement".to_string(),
            "source_health".to_string(),
        ],
        recommended_next_probe: Some("cass health --json".to_string()),
    }
}
