use std::collections::BTreeMap;

use super::redaction::{
    push_full_redaction, redact_pack_output_json, redact_pack_output_text, redacted_source_label,
    rendered_origin_host,
};
use super::render_formats::{
    compact_excerpt, effective_semantic_readiness, evidence_role_label, freshness_policy_label,
    lexical_readiness_label, pack_toon_encode_options, render_answer_pack_jsonl,
    render_answer_pack_markdown, semantic_readiness_label, sha256_hex, source_readiness_label,
    source_readiness_rank, source_sync_gap_kind_label,
};
use super::types::{
    OmittedPackCandidate, PackCandidate, PackEvidenceRole, PackFreshnessPolicy,
    PackLexicalReadiness, PackOmittedReason, PackRenderError, PackRenderFormat, PackRenderRequest,
    PackSelectionScore, PackSemanticReadiness, PackSourceReadiness, PackSourceSyncGap,
    PlannedAnswerPack, PlannedPackEvidence, RenderedAnswerPack, RenderedCitation, RenderedEvidence,
    RenderedFreshness, RenderedHandoffItem, RenderedHealth, RenderedLimits, RenderedMeta,
    RenderedOmitted, RenderedOutlineItem, RenderedPack, RenderedPrivacy, RenderedQuery,
    RenderedRealized, RenderedRedaction, RenderedSelection, RenderedSourceReadiness,
    RenderedSourceSummary, RenderedSourceSyncGap, SourceAccumulator,
};

pub fn render_answer_pack(
    plan: &PlannedAnswerPack,
    request: &PackRenderRequest,
) -> Result<String, PackRenderError> {
    let envelope = rendered_answer_pack(plan, request);
    match request.format {
        PackRenderFormat::Json => {
            serde_json::to_string_pretty(&envelope).map_err(|err| render_error(request, err))
        }
        PackRenderFormat::CompactJson => {
            serde_json::to_string(&envelope).map_err(|err| render_error(request, err))
        }
        PackRenderFormat::Jsonl => render_answer_pack_jsonl(&envelope, request),
        PackRenderFormat::Toon => {
            let value =
                serde_json::to_value(&envelope).map_err(|err| render_error(request, err))?;
            Ok(toon::encode(value, Some(pack_toon_encode_options())))
        }
        PackRenderFormat::Markdown => Ok(render_answer_pack_markdown(&envelope)),
    }
}

pub fn render_answer_pack_value(
    plan: &PlannedAnswerPack,
    request: &PackRenderRequest,
) -> Result<serde_json::Value, PackRenderError> {
    serde_json::to_value(rendered_answer_pack(plan, request))
        .map_err(|err| render_error(request, err))
}

/// Render without live commit/Bead/release correlation. This is the bounded
/// fallback when advisory trust collection misses the robot deadline.
pub fn render_answer_pack_without_trust_correlation(
    plan: &PlannedAnswerPack,
    request: &PackRenderRequest,
) -> Result<String, PackRenderError> {
    let correlation = crate::search::trust_correlation::CorrelationIndex::default();
    render_answer_pack_with_correlation(plan, request, &correlation)
}

pub fn render_answer_pack_with_correlation(
    plan: &PlannedAnswerPack,
    request: &PackRenderRequest,
    correlation: &crate::search::trust_correlation::CorrelationIndex,
) -> Result<String, PackRenderError> {
    let envelope = rendered_answer_pack_with_correlation(plan, request, correlation);
    match request.format {
        PackRenderFormat::Json => {
            serde_json::to_string_pretty(&envelope).map_err(|err| render_error(request, err))
        }
        PackRenderFormat::CompactJson => {
            serde_json::to_string(&envelope).map_err(|err| render_error(request, err))
        }
        PackRenderFormat::Jsonl => render_answer_pack_jsonl(&envelope, request),
        PackRenderFormat::Toon => {
            let value =
                serde_json::to_value(&envelope).map_err(|err| render_error(request, err))?;
            Ok(toon::encode(value, Some(pack_toon_encode_options())))
        }
        PackRenderFormat::Markdown => Ok(render_answer_pack_markdown(&envelope)),
    }
}

pub fn render_answer_pack_value_without_trust_correlation(
    plan: &PlannedAnswerPack,
    request: &PackRenderRequest,
) -> Result<serde_json::Value, PackRenderError> {
    let correlation = crate::search::trust_correlation::CorrelationIndex::default();
    render_answer_pack_value_with_correlation(plan, request, &correlation)
}

pub fn render_answer_pack_value_with_correlation(
    plan: &PlannedAnswerPack,
    request: &PackRenderRequest,
    correlation: &crate::search::trust_correlation::CorrelationIndex,
) -> Result<serde_json::Value, PackRenderError> {
    serde_json::to_value(rendered_answer_pack_with_correlation(
        plan,
        request,
        correlation,
    ))
    .map_err(|err| render_error(request, err))
}

pub(super) fn render_error(error: &PackRenderRequest, err: serde_json::Error) -> PackRenderError {
    PackRenderError {
        format: error.format.label(),
        message: err.to_string(),
    }
}

fn rendered_answer_pack(
    plan: &PlannedAnswerPack,
    request: &PackRenderRequest,
) -> RenderedAnswerPack {
    // q4pau: build the commit/bead/release correlation index once per pack render
    // (fail-open empty index off-repo); its repo root anchors cwd-relative
    // workspace matching for each evidence item.
    let correlation = crate::search::trust_correlation::build_for_cwd();
    rendered_answer_pack_with_correlation(plan, request, &correlation)
}

fn rendered_answer_pack_with_correlation(
    plan: &PlannedAnswerPack,
    request: &PackRenderRequest,
    correlation: &crate::search::trust_correlation::CorrelationIndex,
) -> RenderedAnswerPack {
    let query_workspace = correlation.project_workspace();
    let evidence = plan
        .evidence
        .iter()
        .map(|item| rendered_evidence(item, request, correlation, query_workspace.as_deref()))
        .collect::<Vec<_>>();
    let mut envelope_redactions = Vec::new();
    let query_text = redact_pack_output_text(&request.query_text, &mut envelope_redactions);
    let normalized_query =
        redact_pack_output_text(&normalized_query(request), &mut envelope_redactions);
    let pack_title = redact_pack_output_text(&pack_title(request), &mut envelope_redactions);
    let source_summary = rendered_source_summary(&evidence);
    let source_readiness = rendered_source_readiness(&evidence);
    let source_sync_gaps = rendered_source_sync_gaps(
        &request.readiness.source_sync_gaps,
        &mut envelope_redactions,
    );
    let stale_evidence_count = stale_evidence_count(&evidence, request);
    let redacted_count = plan
        .omitted
        .iter()
        // ubs:ignore — compares a public omission enum, not secret material.
        .filter(|omitted| omitted.reason == PackOmittedReason::RedactedToEmpty)
        .count();
    let (omitted_items, omitted_redactions) = rendered_omitted_items(&plan.omitted);
    let semantic_readiness = effective_semantic_readiness(request);
    let health_is_healthy = health_is_healthy(request, &source_readiness);
    // GH #507: the executor's own cause for its fallback, redacted like the
    // rest of the envelope. It is never inferred from which assets exist.
    let fallback_reason = request.fallback_mode.as_ref().and_then(|_| {
        trimmed_optional_string(request.fallback_reason.as_deref())
            .map(|reason| redact_pack_output_text(&reason, &mut envelope_redactions))
    });
    let mut warnings = readiness_warnings(
        request,
        semantic_readiness,
        fallback_reason.as_deref(),
        &source_readiness,
        evidence.is_empty(),
        &mut envelope_redactions,
    );
    let recommended_action = readiness_recommended_action(
        request,
        semantic_readiness,
        fallback_reason.as_deref(),
        health_is_healthy,
    )
    .map(|action| redact_pack_output_text(&action, &mut envelope_redactions));
    let index_generation = request
        .readiness
        .index_generation
        .as_deref()
        .map(|generation| redact_pack_output_text(generation, &mut envelope_redactions));
    let lock_state = request
        .readiness
        .lock_state
        .as_deref()
        .map(|state| redact_pack_output_text(state, &mut envelope_redactions));
    // `_meta.effective` echoes the raw query, its grouping, the database path
    // and the filters, so it is redacted like the rest of the envelope.
    let effective = request
        .effective
        .as_ref()
        .map(|value| redact_pack_output_json(value, &mut envelope_redactions));
    let redaction_counts = redaction_counts(
        redacted_count,
        &evidence,
        &omitted_redactions,
        &envelope_redactions,
    );
    let redaction_applied = !redaction_counts.is_empty();
    if redaction_applied {
        warnings.push("privacy_redactions_applied".to_string());
    }

    RenderedAnswerPack {
        schema_version: "cass.pack.v2",
        query: RenderedQuery {
            text: query_text,
            normalized: normalized_query,
            filters: BTreeMap::new(),
        },
        meta: RenderedMeta {
            request_id: request.request_id.clone(),
            generated_at_ms: request.generated_at_ms,
            elapsed_ms: request.elapsed_ms,
            partial: request.budget.timed_out || !request.budget.skipped_sections.is_empty(),
            format: request.format.label(),
            warnings: warnings.clone(),
            effective,
        },
        budget: request.budget.clone(),
        limits: RenderedLimits {
            max_tokens: request.limits.max_tokens,
            estimated_tokens: plan.estimated_tokens,
            max_sessions: request.limits.max_sessions,
            max_evidence: request.limits.max_evidence,
            context_lines: request.limits.context_lines,
            max_excerpt_chars: request.limits.max_excerpt_chars,
            field_mask: "standard",
        },
        realized: RenderedRealized {
            search_mode: request.search_mode.clone(),
            fallback_mode: request.fallback_mode.clone(),
            fallback_reason,
            semantic_joined: request.semantic_joined,
            candidate_count: plan.candidate_count,
            selected_evidence_count: plan.selected_evidence_count,
            selected_session_count: plan.selected_session_count,
        },
        health: RenderedHealth {
            healthy: health_is_healthy,
            recommended_action,
            index_state: lexical_readiness_label(request.readiness.lexical_readiness),
            index_generation,
            lexical_readiness: lexical_readiness_label(request.readiness.lexical_readiness),
            semantic_state: semantic_readiness_label(semantic_readiness),
            active_rebuild: request.readiness.active_rebuild,
            lock_state,
            missing_database: request.readiness.missing_database,
            source_sync_gaps,
            source_readiness,
        },
        freshness: RenderedFreshness {
            policy: freshness_policy_label(request.freshness_policy),
            window_seconds: request.freshness_window_seconds,
            newest_evidence_at_ms: evidence
                .iter()
                .filter_map(|item| item.citation.created_at_ms)
                .max(),
            oldest_evidence_at_ms: evidence
                .iter()
                .filter_map(|item| item.citation.created_at_ms)
                .min(),
            stale_evidence_count,
        },
        pack: RenderedPack {
            title: pack_title,
            answer_outline: rendered_outline(&evidence),
            source_summary,
            handoff: rendered_handoff(&evidence),
        },
        evidence,
        omitted: RenderedOmitted {
            count: plan.omitted.len(),
            items: omitted_items,
        },
        privacy: RenderedPrivacy {
            redaction_policy: request.redaction_policy.clone(),
            redaction_applied,
            sensitive_output: request.sensitive_output,
            skill_content_included: plan.evidence.iter().any(|item| {
                crate::resource_plan::text_filters::is_skill_injection(&item.candidate.excerpt)
            }),
            redaction_counts,
        },
        warnings,
    }
}

fn health_is_healthy(
    request: &PackRenderRequest,
    source_readiness: &[RenderedSourceReadiness],
) -> bool {
    matches!(
        request.readiness.lexical_readiness,
        PackLexicalReadiness::Ready
    ) && !request.readiness.active_rebuild
        && !request.readiness.missing_database
        && request.readiness.lock_state.is_none()
        && request.readiness.source_sync_gaps.is_empty()
        && source_readiness.iter().all(|source| source.healthy)
}

fn readiness_warnings(
    request: &PackRenderRequest,
    semantic_readiness: PackSemanticReadiness,
    fallback_reason: Option<&str>,
    source_readiness: &[RenderedSourceReadiness],
    no_evidence: bool,
    redactions: &mut Vec<RenderedRedaction>,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if no_evidence {
        warnings.push("no_evidence_found".to_string());
    }
    match request.readiness.lexical_readiness {
        PackLexicalReadiness::Ready => {}
        PackLexicalReadiness::Stale => warnings.push("lexical_index_stale".to_string()),
        PackLexicalReadiness::Missing => warnings.push("lexical_index_missing".to_string()),
        PackLexicalReadiness::Rebuilding => warnings.push("lexical_index_rebuilding".to_string()),
        PackLexicalReadiness::Unknown => warnings.push("lexical_index_unknown".to_string()),
    }
    match semantic_readiness {
        PackSemanticReadiness::FallbackLexical => {
            warnings.push("semantic_fallback_lexical".to_string());
            // A fallback the executor did not explain stays visible as such.
            if fallback_reason.is_none() {
                warnings.push("semantic_fallback_reason_unreported".to_string());
            }
        }
        PackSemanticReadiness::Unavailable => {
            warnings.push("semantic_unavailable_lexical_fallback".to_string());
        }
        PackSemanticReadiness::Disabled => warnings.push("semantic_disabled".to_string()),
        PackSemanticReadiness::Joined | PackSemanticReadiness::NotReported => {}
    }
    if request.readiness.active_rebuild {
        warnings.push("active_rebuild".to_string());
    }
    if request.readiness.lock_state.is_some() {
        warnings.push("index_lock_active".to_string());
    }
    if request.readiness.missing_database {
        warnings.push("missing_database".to_string());
    }
    for gap in &request.readiness.source_sync_gaps {
        let source_id = redacted_source_label(&gap.source_id, &gap.origin_kind, redactions);
        warnings.push(format!(
            "source_sync_gap:{}:{}",
            source_id,
            source_sync_gap_kind_label(gap.kind)
        ));
    }
    for source in source_readiness.iter().filter(|source| !source.healthy) {
        warnings.push(format!(
            "source_readiness:{}:{}",
            source.source_id, source.readiness
        ));
    }
    warnings
}

fn readiness_recommended_action(
    request: &PackRenderRequest,
    semantic_readiness: PackSemanticReadiness,
    fallback_reason: Option<&str>,
    health_is_healthy: bool,
) -> Option<String> {
    if let Some(action) = trimmed_optional_string(request.readiness.recommended_action.as_deref()) {
        return Some(action);
    }
    if request.readiness.missing_database {
        return Some("run cass index --full".to_string());
    }
    match request.readiness.lexical_readiness {
        PackLexicalReadiness::Ready => {}
        PackLexicalReadiness::Stale => {
            return Some("refresh lexical index with cass index --full".to_string());
        }
        PackLexicalReadiness::Missing => {
            return Some("build lexical index with cass index --full".to_string());
        }
        PackLexicalReadiness::Rebuilding => {
            return Some("wait for active rebuild or inspect cass status --json".to_string());
        }
        PackLexicalReadiness::Unknown => {
            return Some("inspect cass health --json".to_string());
        }
    }
    if request.readiness.active_rebuild {
        return Some("wait for active rebuild or inspect cass status --json".to_string());
    }
    if request.readiness.lock_state.is_some() {
        return Some("inspect cass status --json for active locks".to_string());
    }
    if !request.readiness.source_sync_gaps.is_empty() {
        return Some("inspect cass sources doctor --json and source status".to_string());
    }
    if !health_is_healthy {
        return Some("inspect cass health --json and source status".to_string());
    }
    if matches!(
        semantic_readiness,
        PackSemanticReadiness::FallbackLexical | PackSemanticReadiness::Unavailable
    ) {
        // Pack's own lexical evidence selection is not an asset problem, so a
        // model install would not change it (GH #507).
        if fallback_reason == Some("pack_enrichment_unavailable") {
            return Some(
                "continue with lexical evidence; pack selects evidence lexically, and cass search --mode hybrid ranks with semantics".to_string(),
            );
        }
        return Some(
            "continue with lexical evidence or install semantic model explicitly".to_string(),
        );
    }
    None
}

fn trimmed_optional_string(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn normalized_query(request: &PackRenderRequest) -> String {
    if request.normalized_query.trim().is_empty() {
        request.query_text.trim().to_string()
    } else {
        request.normalized_query.trim().to_string()
    }
}

fn pack_title(request: &PackRenderRequest) -> String {
    let normalized = normalized_query(request);
    if normalized.is_empty() {
        "answer pack".to_string()
    } else {
        normalized
    }
}

/// Map a pack evidence candidate's source story to the trust source-kind. An
/// unavailable source is archive-only (source-unhealthy) for trust purposes;
/// otherwise classify by origin (live local file vs reachable remote mirror).
fn pack_trust_source_kind(
    readiness: PackSourceReadiness,
    origin_kind: &str,
) -> crate::search::trust_scoring::SourceTrustKind {
    use crate::search::trust_scoring::SourceTrustKind;
    if matches!(readiness, PackSourceReadiness::Unavailable) {
        SourceTrustKind::ArchiveOnly
    } else if origin_kind.eq_ignore_ascii_case(crate::sources::provenance::LOCAL_SOURCE_ID) {
        SourceTrustKind::LocalPresent
    } else {
        SourceTrustKind::RemoteMirror
    }
}

/// Map the pack's realized search mode (the requested mode plus any lexical
/// fallback) to the trust [`RealizedMode`](crate::search::trust_scoring::RealizedMode).
fn pack_trust_realized_mode(
    search_mode: &str,
    fallback_mode: Option<&str>,
) -> crate::search::trust_scoring::RealizedMode {
    use crate::search::trust_scoring::RealizedMode;
    if fallback_mode.is_some_and(|mode| mode.eq_ignore_ascii_case("lexical")) {
        return RealizedMode::Lexical;
    }
    match search_mode.to_ascii_lowercase().as_str() {
        "lexical" => RealizedMode::Lexical,
        "semantic" => RealizedMode::Semantic,
        _ => RealizedMode::Hybrid,
    }
}

/// Build the metadata-only trust verdict for one pack evidence item (beads
/// 5u82n.3 + q4pau). Advisory only; the same scoring core that powers the
/// per-hit `cass search` verdict. Recency/source/mode always contribute;
/// `query_workspace` drives a cwd-relative workspace match, and for on-project
/// evidence `correlation` links the excerpt to a closed bead / commit / proof /
/// release when it references a known identifier.
fn pack_trust_assessment(
    candidate: &PackCandidate,
    request: &PackRenderRequest,
    correlation: &crate::search::trust_correlation::CorrelationIndex,
    query_workspace: Option<&str>,
) -> crate::search::trust_scoring::TrustAssessment {
    use crate::search::trust_correlation::{correlate, proof_for, scan_text, workspace_matches};
    use crate::search::trust_scoring::{HitTrustContext, assess_trust, derive_trust_signals};
    let workspace = {
        let ws = candidate.workspace.trim();
        (!ws.is_empty()).then(|| ws.to_string())
    };
    let on_project = match (query_workspace, workspace.as_deref()) {
        (None, _) => None,
        (Some(q), Some(w)) => Some(workspace_matches(q, w)),
        (Some(_), None) => Some(false),
    };
    let mut ctx = HitTrustContext {
        created_at_ms: candidate.created_at_ms,
        now_ms: request.generated_at_ms,
        workspace,
        query_workspace: None,
        source_kind: if candidate.citation_verified {
            pack_trust_source_kind(candidate.source_readiness, &candidate.origin_kind)
        } else {
            crate::search::trust_scoring::SourceTrustKind::ArchiveOnly
        },
        realized_mode: pack_trust_realized_mode(
            &request.search_mode,
            request.fallback_mode.as_deref(),
        ),
        ..HitTrustContext::default()
    };
    if matches!(on_project, Some(true)) {
        let scan = scan_text(&[&candidate.excerpt]);
        let link = correlate(correlation, &scan);
        if !link.is_empty() {
            ctx.outcome = link.outcome;
            ctx.linked_commit = link.linked_commit;
            ctx.proof = proof_for(ctx.outcome, ctx.linked_commit.is_some());
        }
    }
    let mut signals = derive_trust_signals(&ctx);
    signals.workspace_match = on_project.unwrap_or(true);
    assess_trust(&signals)
}

fn rendered_evidence(
    item: &PlannedPackEvidence,
    request: &PackRenderRequest,
    correlation: &crate::search::trust_correlation::CorrelationIndex,
    query_workspace: Option<&str>,
) -> RenderedEvidence {
    let candidate = &item.candidate;
    let mut redactions = item.redactions.clone();
    // Selection already sanitized and budgeted this text. Secret patterns are
    // not idempotent: another pass can rewrite markers and count events twice.
    let excerpt = item.excerpt.clone();
    let source_id = redacted_source_label(
        &candidate.source_id,
        &candidate.origin_kind,
        &mut redactions,
    );
    let source_path = redact_pack_output_text(&candidate.source_path, &mut redactions);
    let workspace = redact_pack_output_text(&candidate.workspace, &mut redactions);
    let workspace_original = candidate
        .workspace_original
        .as_deref()
        .map(|workspace| redact_pack_output_text(workspace, &mut redactions));
    let origin_host = rendered_origin_host(candidate.origin_host.as_deref(), &mut redactions);
    let citation = RenderedCitation {
        source_path,
        source_id,
        origin_kind: candidate.origin_kind.clone(),
        origin_host,
        workspace,
        workspace_original,
        agent: redact_pack_output_text(&candidate.agent, &mut redactions),
        line_start: candidate.line_start,
        line_end: candidate.line_end,
        message_index: candidate.message_index,
        message_index_base: 1,
        conversation_id: candidate.conversation_id,
        content_hash: candidate.content_hash.clone(),
        span_hash: candidate.span_hash.clone(),
        excerpt_sha256: sha256_hex(&excerpt),
        created_at_ms: candidate.created_at_ms,
        indexed_at_ms: candidate.indexed_at_ms,
        freshness_age_seconds: candidate
            .created_at_ms
            .map(|created| request.generated_at_ms.saturating_sub(created).max(0) / 1_000),
        match_type: candidate.match_type.clone(),
        verified: candidate.citation_verified,
    };
    let trust = pack_trust_assessment(candidate, request, correlation, query_workspace);
    RenderedEvidence {
        id: item.id.clone(),
        rank: item.rank,
        excerpt,
        excerpt_truncated: item.excerpt_truncated,
        estimated_tokens: item.estimated_tokens,
        citation,
        trust,
        selection: rendered_selection(item.selection, request.explain_selection),
        roles: rendered_roles(candidate.role),
        matched_terms: candidate
            .matched_terms
            .iter()
            .map(|term| redact_pack_output_text(term, &mut redactions))
            .collect(),
        redactions,
        source_readiness: candidate.source_readiness,
        session_key: candidate.session_key(),
    }
}

fn rendered_selection(selection: PackSelectionScore, explain: bool) -> RenderedSelection {
    RenderedSelection {
        score: selection.score,
        token_cost: selection.token_cost,
        selected_reason: selection.selected_reason,
        relevance_score: explain.then_some(selection.relevance_score),
        coverage_score: explain.then_some(selection.coverage_score),
        freshness_score: explain.then_some(selection.freshness_score),
        source_diversity_score: explain.then_some(selection.source_diversity_score),
        source_authority_score: explain.then_some(selection.source_authority_score),
        role_score: explain.then_some(selection.role_score),
        citation_quality_score: explain.then_some(selection.citation_quality_score),
        duplicate_penalty: explain.then_some(selection.duplicate_penalty),
    }
}

fn rendered_roles(role: PackEvidenceRole) -> Vec<&'static str> {
    if matches!(role, PackEvidenceRole::Unknown) {
        Vec::new()
    } else {
        vec![evidence_role_label(role)]
    }
}

fn rendered_outline(evidence: &[RenderedEvidence]) -> Vec<RenderedOutlineItem> {
    evidence
        .iter()
        .map(|item| RenderedOutlineItem {
            rank: item.rank,
            heading: outline_heading(item),
            evidence_ids: vec![item.id.clone()],
        })
        .collect()
}

fn outline_heading(item: &RenderedEvidence) -> String {
    item.matched_terms
        .first()
        .map(|term| format!("Evidence for {term}"))
        .unwrap_or_else(|| {
            format!(
                "{} evidence from {}",
                item.citation.agent, item.citation.source_id
            )
        })
}

fn rendered_handoff(evidence: &[RenderedEvidence]) -> Vec<RenderedHandoffItem> {
    evidence
        .iter()
        .map(|item| RenderedHandoffItem {
            rank: item.rank,
            kind: handoff_kind(item),
            text: compact_excerpt(&item.excerpt, 220),
            evidence_ids: vec![item.id.clone()],
        })
        .collect()
}

fn handoff_kind(item: &RenderedEvidence) -> &'static str {
    match item.roles.first().copied() {
        Some("assistant_conclusion") => "decision",
        Some("tool_result") => "fact",
        Some("user_requirement") => "next_step",
        Some("tool_call_argument") => "fact",
        _ => "fact",
    }
}

fn rendered_source_summary(evidence: &[RenderedEvidence]) -> Vec<RenderedSourceSummary> {
    let mut sources: BTreeMap<(String, String), SourceAccumulator> = BTreeMap::new();
    for item in evidence {
        let key = (
            item.citation.source_id.clone(),
            item.citation.origin_kind.clone(),
        );
        let entry = sources.entry(key).or_insert_with(|| SourceAccumulator {
            origin_kind: item.citation.origin_kind.clone(),
            healthy: true,
            ..SourceAccumulator::default()
        });
        entry.sessions.insert(item.session_key.clone());
        entry.evidence_count += 1;
        entry.newest_evidence_at_ms =
            newer_timestamp(entry.newest_evidence_at_ms, item.citation.created_at_ms);
        let readiness = item.citation_source_readiness();
        entry.healthy &= matches!(readiness, PackSourceReadiness::Healthy);
        if source_readiness_rank(readiness) > source_readiness_rank(entry.worst_readiness) {
            entry.worst_readiness = readiness;
        }
    }

    sources
        .into_iter()
        .map(|((source_id, _), source)| RenderedSourceSummary {
            source_id,
            origin_kind: source.origin_kind,
            session_count: source.sessions.len(),
            evidence_count: source.evidence_count,
            newest_evidence_at_ms: source.newest_evidence_at_ms,
            healthy: source.healthy,
        })
        .collect()
}

fn rendered_source_readiness(evidence: &[RenderedEvidence]) -> Vec<RenderedSourceReadiness> {
    let mut sources: BTreeMap<(String, String), SourceAccumulator> = BTreeMap::new();
    for item in evidence {
        let key = (
            item.citation.source_id.clone(),
            item.citation.origin_kind.clone(),
        );
        let entry = sources.entry(key).or_insert_with(|| SourceAccumulator {
            origin_kind: item.citation.origin_kind.clone(),
            healthy: true,
            ..SourceAccumulator::default()
        });
        entry.evidence_count += 1;
        let readiness = item.citation_source_readiness();
        entry.healthy &= matches!(readiness, PackSourceReadiness::Healthy);
        if source_readiness_rank(readiness) > source_readiness_rank(entry.worst_readiness) {
            entry.worst_readiness = readiness;
        }
    }

    sources
        .into_iter()
        .map(|((source_id, _), source)| RenderedSourceReadiness {
            source_id,
            origin_kind: source.origin_kind,
            readiness: source_readiness_label(source.worst_readiness),
            healthy: source.healthy,
            evidence_count: source.evidence_count,
        })
        .collect()
}

fn rendered_source_sync_gaps(
    gaps: &[PackSourceSyncGap],
    redactions: &mut Vec<RenderedRedaction>,
) -> Vec<RenderedSourceSyncGap> {
    gaps.iter()
        .map(|gap| RenderedSourceSyncGap {
            source_id: redacted_source_label(&gap.source_id, &gap.origin_kind, redactions),
            origin_kind: gap.origin_kind.clone(),
            kind: source_sync_gap_kind_label(gap.kind),
            lag_seconds: gap.lag_seconds,
            last_synced_at_ms: gap.last_synced_at_ms,
            recommended_action: gap
                .recommended_action
                .as_deref()
                .map(|action| redact_pack_output_text(action, redactions)),
        })
        .collect()
}

fn stale_evidence_count(evidence: &[RenderedEvidence], request: &PackRenderRequest) -> usize {
    evidence
        .iter()
        .filter(|item| evidence_is_stale(item, request))
        .count()
}

fn evidence_is_stale(item: &RenderedEvidence, request: &PackRenderRequest) -> bool {
    let Some(created_at_ms) = item.citation.created_at_ms else {
        return !matches!(request.freshness_policy, PackFreshnessPolicy::AllowStale);
    };
    let window_ms = request
        .freshness_window_seconds
        .max(1)
        .saturating_mul(1_000);
    request.generated_at_ms.saturating_sub(created_at_ms).max(0) > window_ms
}

impl RenderedEvidence {
    fn citation_source_readiness(&self) -> PackSourceReadiness {
        if !self.citation.verified {
            PackSourceReadiness::IncompleteMetadata
        } else {
            self.source_readiness
        }
    }
}

fn newer_timestamp(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn rendered_omitted_items(
    omitted: &[OmittedPackCandidate],
) -> (Vec<OmittedPackCandidate>, Vec<RenderedRedaction>) {
    let mut redactions = Vec::new();
    let items = omitted
        .iter()
        .map(|item| {
            let mut rendered = item.clone();
            rendered.candidate_id =
                rendered_omitted_candidate_id(&rendered.candidate_id, &mut redactions);
            rendered.source_path = redact_pack_output_text(&rendered.source_path, &mut redactions);
            rendered.agent = redact_pack_output_text(&rendered.agent, &mut redactions);
            rendered
        })
        .collect();
    (items, redactions)
}

fn rendered_omitted_candidate_id(
    candidate_id: &str,
    redactions: &mut Vec<RenderedRedaction>,
) -> String {
    if candidate_id_contains_source_material(candidate_id) {
        let hash = sha256_hex(candidate_id);
        let replacement = format!("omitted_{}", &hash[..16]);
        push_full_redaction(redactions, "candidate_id", candidate_id, &replacement);
        return replacement;
    }
    redact_pack_output_text(candidate_id, redactions)
}

fn candidate_id_contains_source_material(candidate_id: &str) -> bool {
    candidate_id.contains(':')
        || candidate_id.contains('/')
        || candidate_id.contains('\\')
        || candidate_id.contains('~')
}

fn redaction_counts(
    redacted_count: usize,
    evidence: &[RenderedEvidence],
    omitted_redactions: &[RenderedRedaction],
    envelope_redactions: &[RenderedRedaction],
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    if redacted_count > 0 {
        counts.insert("redacted_to_empty".to_string(), redacted_count);
    }
    for redaction in evidence
        .iter()
        .flat_map(|item| item.redactions.iter())
        .chain(omitted_redactions.iter())
        .chain(envelope_redactions.iter())
    {
        *counts.entry(redaction.kind.clone()).or_default() += 1;
    }
    counts
}
