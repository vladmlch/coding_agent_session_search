use super::super::*;
use super::*;

#[test]
fn empty_corpus_returns_empty_plan() {
    let plan = plan_answer_pack(request(Vec::new())).unwrap();

    assert_eq!(plan.candidate_count, 0);
    assert_eq!(plan.selected_evidence_count, 0);
    assert_eq!(plan.diagnostics.candidate_fetch_limit, 192);
    assert!(plan.evidence.is_empty());
    assert!(plan.omitted.is_empty());
}

#[test]
fn budget_fallback_preserves_candidate_count_with_normal_diagnostics() {
    let limits = PackPlannerLimits {
        max_tokens: 20_000,
        max_sessions: 2,
        max_evidence: 3,
        context_lines: 5,
        max_excerpt_chars: 800,
    };
    let normal_plan = plan_answer_pack(PackPlanRequest {
        limits: limits.clone(),
        ..PackPlanRequest::default()
    })
    .unwrap();
    let fallback = budget_fallback_answer_pack(&limits, usize::MAX).unwrap();

    assert_eq!(fallback.candidate_count, usize::MAX);
    assert_eq!(fallback.selected_evidence_count, 0);
    assert_eq!(fallback.selected_session_count, 0);
    assert_eq!(fallback.estimated_tokens, 0);
    assert_eq!(fallback.diagnostics, normal_plan.diagnostics);
    assert!(fallback.evidence.is_empty());
    assert!(fallback.omitted.is_empty());
}

#[test]
fn budget_fallback_rejects_invalid_limits() {
    let limits = PackPlannerLimits {
        max_tokens: 1_023,
        ..PackPlannerLimits::default()
    };
    let error = budget_fallback_answer_pack(&limits, 17).unwrap_err();

    assert_eq!(error.field, "max_tokens");
    assert_eq!(error.value, 1_023);
    assert_eq!(error.min, 1_024);
    assert_eq!(error.max, 200_000);
}

#[test]
fn candidate_fetch_limit_matches_contract_formula() {
    let mut limits = PackPlannerLimits {
        max_tokens: 12_000,
        max_sessions: 2,
        max_evidence: 3,
        context_lines: 3,
        max_excerpt_chars: 1_600,
    };

    assert_eq!(pack_candidate_fetch_limit(&limits).unwrap(), 64);

    limits.max_sessions = 20;
    assert_eq!(pack_candidate_fetch_limit(&limits).unwrap(), 320);

    limits.max_sessions = 64;
    limits.max_evidence = 256;
    assert_eq!(
        pack_candidate_fetch_limit(&limits).unwrap(),
        PACK_CANDIDATE_LIMIT_CAP
    );
}

#[test]
fn token_budget_reserves_documented_sections() {
    let budget = pack_planner_budget(&PackPlannerLimits {
        max_tokens: 12_000,
        max_sessions: 8,
        max_evidence: 24,
        context_lines: 3,
        max_excerpt_chars: 1_600,
    })
    .unwrap();

    assert_eq!(budget.metadata_tokens, 1_800);
    assert_eq!(budget.outline_tokens, 1_800);
    assert_eq!(budget.evidence_tokens, 7_200);
    assert_eq!(budget.omitted_tokens, 1_200);
    assert_eq!(budget.max_output_tokens_with_overflow, 12_600);
    assert_eq!(
        budget.metadata_tokens
            + budget.outline_tokens
            + budget.evidence_tokens
            + budget.omitted_tokens,
        budget.max_tokens
    );
}

#[test]
fn duplicate_content_is_omitted_after_first_selection() {
    let first = candidate("a", "local", "/s/a.jsonl", 10.0);
    let mut duplicate = candidate("b", "local", "/s/b.jsonl", 9.0);
    duplicate.content_hash = first.content_hash.clone();

    let plan = plan_answer_pack(request(vec![first, duplicate])).unwrap();

    assert_eq!(plan.selected_evidence_count, 1);
    assert_eq!(plan.omitted.len(), 1);
    assert_eq!(plan.omitted[0].reason, PackOmittedReason::DuplicateContent);
}

#[test]
fn duplicate_span_and_overlapping_ranges_are_omitted_once() {
    let span_source = candidate("span-source", "local", "/s/span.jsonl", 10.0);
    let mut span_duplicate = candidate("span-dup", "remote", "/s/other.jsonl", 9.0);
    span_duplicate.span_hash = span_source.span_hash.clone();

    let range_source = candidate("range-source", "local", "/s/range.jsonl", 8.0);
    let mut range_duplicate = candidate("range-dup", "local", "/s/range.jsonl", 7.0);
    range_duplicate.line_start = Some(11);
    range_duplicate.line_end = Some(14);

    let plan = plan_answer_pack(request(vec![
        span_source,
        span_duplicate,
        range_source,
        range_duplicate,
    ]))
    .unwrap();

    let omitted_ids: Vec<_> = plan
        .omitted
        .iter()
        .map(|omitted| (omitted.candidate_id.as_str(), omitted.reason))
        .collect();
    assert_eq!(
        omitted_ids,
        vec![
            ("span-dup", PackOmittedReason::DuplicateContent),
            ("range-dup", PackOmittedReason::DuplicateContent),
        ]
    );
}

#[test]
fn unavailable_and_redacted_empty_candidates_are_omitted_once() {
    let mut unavailable = candidate("unavailable", "remote", "/s/down.jsonl", 10.0);
    unavailable.source_readiness = PackSourceReadiness::Unavailable;
    let mut redacted = candidate("redacted", "local", "/s/redacted.jsonl", 9.0);
    redacted.excerpt = " \n\t ".to_string();

    let plan = plan_answer_pack(request(vec![unavailable, redacted])).unwrap();

    assert!(plan.evidence.is_empty());
    let omitted_reasons: Vec<_> = plan
        .omitted
        .iter()
        .map(|omitted| (omitted.candidate_id.as_str(), omitted.reason))
        .collect();
    assert_eq!(
        omitted_reasons,
        vec![
            ("unavailable", PackOmittedReason::SourceUnavailable),
            ("redacted", PackOmittedReason::RedactedToEmpty),
        ]
    );
}

#[test]
fn exact_token_budget_boundary_selects_until_budget_exhausted() {
    let mut first = candidate("a", "local", "/s/a.jsonl", 10.0);
    first.excerpt = "12345678".to_string();
    let mut second = candidate("b", "remote", "/s/b.jsonl", 9.0);
    second.excerpt = "abcdefgh".to_string();

    let mut req = request(vec![first, second]);
    req.limits.max_tokens = 1_024;
    req.limits.max_excerpt_chars = 4_096;
    let evidence_budget = pack_planner_budget(&req.limits).unwrap().evidence_tokens;
    req.candidates[0].excerpt = "x".repeat(evidence_budget * TOKEN_ESTIMATE_CHARS_PER_TOKEN);
    req.candidates[1].excerpt = "y".repeat(4);

    let plan = plan_answer_pack(req).unwrap();

    assert_eq!(plan.selected_evidence_count, 1);
    assert_eq!(plan.estimated_tokens, evidence_budget);
    assert_eq!(
        plan.omitted[0].reason,
        PackOmittedReason::TokenBudgetExhausted
    );
}

#[test]
fn oversized_high_score_candidate_is_shortened_before_lower_ranked_evidence() {
    let mut oversized = candidate("oversized", "local", "/s/oversized.jsonl", 10.0);
    let mut fitting = candidate("fit", "remote", "/s/fit.jsonl", 9.0);

    let mut req = request(vec![oversized.clone(), fitting.clone()]);
    req.limits.max_excerpt_chars = 8_000;
    let evidence_budget = pack_planner_budget(&req.limits).unwrap().evidence_tokens;
    oversized.excerpt = "x".repeat((evidence_budget + 1) * TOKEN_ESTIMATE_CHARS_PER_TOKEN);
    fitting.excerpt = "y".repeat(TOKEN_ESTIMATE_CHARS_PER_TOKEN);
    req.candidates = vec![oversized.clone(), fitting];

    let plan = plan_answer_pack(req).unwrap();

    let selected = &plan.evidence[0];
    assert_eq!(selected.candidate, oversized);
    assert_eq!(selected.id, evidence_id(&oversized));
    assert_eq!(selected.estimated_tokens, evidence_budget);
    assert_eq!(selected.selection.token_cost, evidence_budget);
    assert!(selected.excerpt_truncated);
    assert_eq!(
        selected.excerpt,
        format!("{}...", "x".repeat(evidence_budget * 4 - 3))
    );
    assert_eq!(plan.omitted.len(), 1);
    assert_eq!(plan.omitted[0].candidate_id, "fit");
    assert_eq!(
        plan.omitted[0].reason,
        PackOmittedReason::TokenBudgetExhausted
    );
}

#[test]
fn final_output_reduction_preserves_citations_and_reconciles_dropped_evidence() {
    let first = candidate("first", "local", "/s/first.jsonl", 10.0);
    let mut last = candidate("last", "remote", "/s/last.jsonl", 9.0);
    last.excerpt = "界😀e\u{301}".repeat(20);
    let mut plan = plan_answer_pack(request(vec![first, last.clone()])).unwrap();
    let original_first = plan.evidence[0].clone();
    let original_last_id = plan.evidence[1].id.clone();
    assert!(plan.reduce_for_output_budget(10_000, true));
    assert_eq!(plan.evidence[0], original_first);
    assert_eq!(plan.evidence[1].candidate, last);
    assert_eq!(plan.evidence[1].id, original_last_id);
    assert_eq!(plan.evidence[1].excerpt, "界...");
    assert_eq!(plan.evidence[1].estimated_tokens, 1);
    assert_eq!(plan.evidence[1].selection.token_cost, 1);
    assert_eq!(plan.estimated_tokens, original_first.estimated_tokens + 1);
    assert!(plan.omitted.is_empty());

    assert!(plan.reduce_for_output_budget(10_000, true));
    assert_eq!(plan.evidence, vec![original_first.clone()]);
    assert_eq!(plan.selected_evidence_count, 1);
    assert_eq!(plan.selected_session_count, 1);
    assert_eq!(plan.estimated_tokens, original_first.estimated_tokens);
    assert_eq!(plan.omitted.len(), 1);
    assert_eq!(plan.omitted[0].candidate_id, last.candidate_id);
    assert_eq!(
        plan.omitted[0].reason,
        PackOmittedReason::TokenBudgetExhausted
    );
    let rendered = render_answer_pack_value_without_trust_correlation(
        &plan,
        &render_request(PackRenderFormat::Json),
    )
    .unwrap();
    assert_eq!(
        rendered["pack"]["answer_outline"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        rendered["pack"]["handoff"][0]["evidence_ids"][0],
        original_first.id
    );
    assert_eq!(
        rendered["pack"]["source_summary"].as_array().unwrap().len(),
        1
    );

    assert!(plan.reduce_for_output_budget(10_000, true));
    assert!(!plan.reduce_for_output_budget(10_000, true));
    assert_eq!(plan.evidence.len(), 1, "required evidence cannot disappear");
    assert!(plan.reduce_for_output_budget(10_000, false));
    assert!(plan.evidence.is_empty());
    assert_eq!(plan.selected_session_count, 0);
    assert_eq!(plan.selected_evidence_count, 0);
    assert_eq!(plan.estimated_tokens, 0);
    assert_eq!(plan.omitted.len(), 2);
    assert!(!plan.reduce_for_output_budget(10_000, false));
}

#[test]
fn final_output_reduction_drops_whitespace_prefix_without_fabricating_text() {
    let mut item = candidate("space", "local", "/s/space.jsonl", 10.0);
    item.excerpt = " \t\n meaningful text".to_string();
    let mut plan = plan_answer_pack(request(vec![item])).unwrap();
    let before = plan.clone();
    assert!(!plan.reduce_for_output_budget(10_000, true));
    assert_eq!(plan, before);
    assert!(plan.reduce_for_output_budget(10_000, false));
    assert!(plan.evidence.is_empty());
    assert_eq!(plan.omitted.len(), 1);
}

#[test]
fn budget_truncation_uses_remaining_tokens_and_preserves_unicode_citations() {
    for remaining_tokens in [1, 2, 20] {
        let mut first = candidate("first", "local", "/s/first.jsonl", 10.0);
        let mut second = candidate("second", "remote", "/s/second.jsonl", 9.0);
        let mut req = request(Vec::new());
        req.limits.max_excerpt_chars = 8_000;
        let budget = pack_planner_budget(&req.limits).unwrap().evidence_tokens;
        first.excerpt = "x".repeat((budget - remaining_tokens) * 4);
        second.excerpt = "界😀e\u{301}".repeat(100);
        req.candidates = vec![first, second.clone()];

        let plan = plan_answer_pack(req).unwrap();
        assert_eq!(plan.evidence.len(), 2);
        let selected = &plan.evidence[1];
        let expected: String = second
            .excerpt
            .chars()
            .take(remaining_tokens * 4 - 3)
            .collect();
        assert_eq!(selected.excerpt, format!("{expected}..."));
        assert!(selected.excerpt_truncated);
        assert_eq!(selected.candidate, second);
        assert_eq!(selected.id, evidence_id(&second));
        assert_eq!(selected.estimated_tokens, remaining_tokens);
        assert_eq!(selected.selection.token_cost, remaining_tokens);
        assert_eq!(plan.estimated_tokens, budget);
        assert!(plan.omitted.is_empty());
    }
}

#[test]
fn budget_truncation_does_not_replace_source_text_with_only_an_ellipsis() {
    let mut first = candidate("first", "local", "/s/first.jsonl", 10.0);
    let mut whitespace = candidate("whitespace", "remote", "/s/space.jsonl", 9.0);
    let mut req = request(Vec::new());
    req.limits.max_excerpt_chars = 8_000;
    let budget = pack_planner_budget(&req.limits).unwrap().evidence_tokens;
    first.excerpt = "x".repeat((budget - 1) * 4);
    whitespace.excerpt = " \t\n meaningful evidence".to_string();
    req.candidates = vec![first, whitespace];

    let plan = plan_answer_pack(req).unwrap();
    assert_eq!(plan.evidence.len(), 1);
    assert!(!plan.evidence[0].excerpt_truncated);
    assert_eq!(plan.estimated_tokens, budget - 1);
    assert_eq!(plan.omitted.len(), 1);
    assert_eq!(plan.omitted[0].candidate_id, "whitespace");
    assert_eq!(
        plan.omitted[0].reason,
        PackOmittedReason::TokenBudgetExhausted
    );
}

#[test]
fn source_diversity_changes_second_pick() {
    let first = candidate("a", "local", "/s/a.jsonl", 10.0);
    let same_source = candidate("b", "local", "/s/b.jsonl", 9.9);
    let different_source = candidate("c", "remote", "/s/c.jsonl", 9.9);

    let mut req = request(vec![first, same_source, different_source]);
    req.limits.max_evidence = 2;

    let plan = plan_answer_pack(req).unwrap();

    assert_eq!(plan.evidence[0].candidate.candidate_id, "a");
    assert_eq!(plan.evidence[1].candidate.candidate_id, "c");
}

#[test]
fn session_cap_omits_new_sessions_but_allows_existing_session_evidence() {
    let first = candidate("a", "local", "/s/a.jsonl", 10.0);
    let mut same_session = candidate("b", "local", "/s/a.jsonl", 9.0);
    same_session.line_start = Some(20);
    same_session.line_end = Some(22);
    let new_session = candidate("c", "remote", "/s/c.jsonl", 8.0);

    let mut req = request(vec![first, same_session, new_session]);
    req.limits.max_sessions = 1;
    req.limits.max_evidence = 3;

    let plan = plan_answer_pack(req).unwrap();

    let selected_ids: Vec<_> = plan
        .evidence
        .iter()
        .map(|evidence| evidence.candidate.candidate_id.as_str())
        .collect();
    assert_eq!(selected_ids, vec!["a", "b"]);
    assert_eq!(plan.omitted.len(), 1);
    assert_eq!(plan.omitted[0].candidate_id, "c");
    assert_eq!(
        plan.omitted[0].reason,
        PackOmittedReason::MaxSessionsReached
    );
}

#[test]
fn evidence_cap_omits_remaining_candidates_once() {
    let first = candidate("a", "local", "/s/a.jsonl", 10.0);
    let second = candidate("b", "remote", "/s/b.jsonl", 9.0);
    let third = candidate("c", "remote", "/s/c.jsonl", 8.0);

    let mut req = request(vec![first, second, third]);
    req.limits.max_evidence = 1;

    let plan = plan_answer_pack(req).unwrap();

    assert_eq!(plan.evidence.len(), 1);
    assert_eq!(plan.evidence[0].candidate.candidate_id, "a");
    let omitted_reasons: Vec<_> = plan
        .omitted
        .iter()
        .map(|omitted| (omitted.candidate_id.as_str(), omitted.reason))
        .collect();
    assert_eq!(
        omitted_reasons,
        vec![
            ("b", PackOmittedReason::MaxEvidenceReached),
            ("c", PackOmittedReason::MaxEvidenceReached),
        ]
    );
}

#[test]
fn strict_freshness_omits_stale_or_unknown_timestamps() {
    let mut stale = candidate("old", "local", "/s/old.jsonl", 10.0);
    stale.created_at_ms = Some(0);
    let mut unknown = candidate("unknown", "remote", "/s/unknown.jsonl", 9.0);
    unknown.created_at_ms = None;

    let mut req = request(vec![stale, unknown]);
    req.freshness_policy = PackFreshnessPolicy::Strict;
    req.freshness_window_seconds = 60;

    let plan = plan_answer_pack(req).unwrap();

    assert!(plan.evidence.is_empty());
    assert_eq!(plan.omitted.len(), 2);
    assert!(
        plan.omitted
            .iter()
            // ubs:ignore — compares a public omission enum, not secret material.
            .all(|omitted| omitted.reason == PackOmittedReason::StaleUnderStrictPolicy)
    );
}

#[test]
fn null_timestamps_sort_last_when_scores_tie() {
    let mut unknown = candidate("unknown", "a", "/a.jsonl", 1.0);
    unknown.created_at_ms = None;
    let mut timestamped = candidate("timestamped", "z", "/z.jsonl", 1.0);
    timestamped.created_at_ms = Some(1_000_000);

    let mut req = request(vec![unknown, timestamped]);
    req.freshness_policy = PackFreshnessPolicy::AllowStale;

    let plan = plan_answer_pack(req).unwrap();

    assert_eq!(plan.evidence[0].candidate.candidate_id, "timestamped");
}

#[test]
fn freshness_policy_scores_unknown_timestamps_explicitly() {
    let mut unknown = candidate("unknown", "local", "/s/unknown.jsonl", 1.0);
    unknown.created_at_ms = None;
    let mut req = request(vec![unknown.clone()]);

    req.freshness_policy = PackFreshnessPolicy::PreferRecent;
    assert_eq!(freshness_score(&unknown, &req), 0.25);

    req.freshness_policy = PackFreshnessPolicy::AllowStale;
    assert_eq!(freshness_score(&unknown, &req), 1.0);

    req.freshness_policy = PackFreshnessPolicy::Strict;
    assert_eq!(freshness_score(&unknown, &req), 0.0);
}

#[test]
fn lexical_score_drives_relevance_when_semantic_is_absent() {
    let plan = plan_answer_pack(request(vec![candidate("a", "local", "/s/a.jsonl", 7.0)])).unwrap();

    assert_eq!(plan.selected_evidence_count, 1);
    assert!(plan.evidence[0].selection.relevance_score > 0.0);
}

#[test]
fn stable_tie_breaks_do_not_depend_on_input_order() {
    let mut later_path = candidate("z", "remote", "/z.jsonl", 1.0);
    later_path.line_start = Some(50);
    let mut earlier_path = candidate("a", "local", "/a.jsonl", 1.0);
    earlier_path.line_start = Some(50);

    let left = plan_answer_pack(request(vec![later_path.clone(), earlier_path.clone()])).unwrap();
    let right = plan_answer_pack(request(vec![earlier_path, later_path])).unwrap();

    assert_eq!(left.evidence[0].candidate.source_path, "/a.jsonl");
    assert_eq!(right.evidence[0].candidate.source_path, "/a.jsonl");
}

#[test]
fn stable_ordering_keeps_cursor_like_page_boundaries() {
    let candidates = vec![
        candidate("e", "remote", "/e.jsonl", 1.0),
        candidate("b", "remote", "/b.jsonl", 1.0),
        candidate("d", "remote", "/d.jsonl", 1.0),
        candidate("a", "remote", "/a.jsonl", 1.0),
        candidate("c", "remote", "/c.jsonl", 1.0),
    ];
    let mut reversed = candidates.clone();
    reversed.reverse();

    let left = plan_answer_pack(request(candidates)).unwrap();
    let right = plan_answer_pack(request(reversed)).unwrap();
    let left_ids: Vec<_> = left
        .evidence
        .iter()
        .map(|evidence| evidence.candidate.candidate_id.as_str())
        .collect();
    let right_ids: Vec<_> = right
        .evidence
        .iter()
        .map(|evidence| evidence.candidate.candidate_id.as_str())
        .collect();

    assert_eq!(
        left_ids.chunks(2).collect::<Vec<_>>(),
        right_ids.chunks(2).collect::<Vec<_>>()
    );
}

#[test]
fn omitted_reasons_serialize_to_documented_snake_case() {
    let reasons = [
        (
            PackOmittedReason::TokenBudgetExhausted,
            "token_budget_exhausted",
        ),
        (
            PackOmittedReason::MaxSessionsReached,
            "max_sessions_reached",
        ),
        (
            PackOmittedReason::MaxEvidenceReached,
            "max_evidence_reached",
        ),
        (PackOmittedReason::DuplicateContent, "duplicate_content"),
        (
            PackOmittedReason::SameSessionLowerRank,
            "same_session_lower_rank",
        ),
        (
            PackOmittedReason::StaleUnderStrictPolicy,
            "stale_under_strict_policy",
        ),
        (PackOmittedReason::SourceUnavailable, "source_unavailable"),
        (PackOmittedReason::RedactedToEmpty, "redacted_to_empty"),
        (PackOmittedReason::FieldMaskExcluded, "field_mask_excluded"),
    ];

    for (reason, expected) in reasons {
        assert_eq!(serde_json::to_value(reason).unwrap(), expected);
    }
}
