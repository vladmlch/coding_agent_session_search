use std::collections::BTreeMap;

use crate::indexer::redact_secrets::redact_text;

use super::redaction::redact_pack_output_text;
use super::scoring::{
    ScoreRange, candidate_ordering, estimated_tokens, evidence_id, finite_score,
    hard_omission_reason, omitted_candidate, score_candidate, truncate_excerpt,
};
use super::types::{
    PACK_CANDIDATE_LIMIT_CAP, PackOmittedReason, PackPlanRequest, PackPlannerBudget,
    PackPlannerDiagnostics, PackPlannerLimitError, PackPlannerLimits, PackSourceReadiness,
    PlannedAnswerPack, PlannedPackEvidence, ScoredCandidate, SelectedState,
    TOKEN_ESTIMATE_CHARS_PER_TOKEN, VerifiedPackFileSpan,
};

pub fn pack_candidate_fetch_limit(
    limits: &PackPlannerLimits,
) -> Result<usize, PackPlannerLimitError> {
    limits.validate()?;
    Ok(limits
        .max_evidence
        .saturating_mul(8)
        .max(limits.max_sessions.saturating_mul(16))
        .clamp(64, PACK_CANDIDATE_LIMIT_CAP))
}

pub fn pack_planner_budget(
    limits: &PackPlannerLimits,
) -> Result<PackPlannerBudget, PackPlannerLimitError> {
    limits.validate()?;
    Ok(pack_planner_budget_unchecked(limits.max_tokens))
}

/// Build the fixed-size empty plan used after a bounded pack request exhausts
/// its execution budget. This performs no candidate traversal or allocation
/// proportional to `candidate_count`.
pub fn budget_fallback_answer_pack(
    limits: &PackPlannerLimits,
    candidate_count: usize,
) -> Result<PlannedAnswerPack, PackPlannerLimitError> {
    limits.validate()?;
    let diagnostics = PackPlannerDiagnostics {
        candidate_fetch_limit: pack_candidate_fetch_limit(limits)?,
        budget: pack_planner_budget_unchecked(limits.max_tokens),
    };
    Ok(PlannedAnswerPack {
        candidate_count,
        selected_evidence_count: 0,
        selected_session_count: 0,
        estimated_tokens: 0,
        diagnostics,
        evidence: Vec::new(),
        omitted: Vec::new(),
    })
}

fn pack_planner_budget_unchecked(max_tokens: usize) -> PackPlannerBudget {
    let metadata_tokens = percent_tokens(max_tokens, 15);
    let outline_tokens = percent_tokens(max_tokens, 15);
    let evidence_tokens = percent_tokens(max_tokens, 60);
    let omitted_tokens = max_tokens
        .saturating_sub(metadata_tokens)
        .saturating_sub(outline_tokens)
        .saturating_sub(evidence_tokens);
    PackPlannerBudget {
        max_tokens,
        metadata_tokens,
        outline_tokens,
        evidence_tokens,
        omitted_tokens,
        max_output_tokens_with_overflow: max_tokens.saturating_add(max_tokens / 20),
    }
}

fn percent_tokens(max_tokens: usize, percent: usize) -> usize {
    max_tokens.saturating_mul(percent) / 100
}

pub fn plan_answer_pack(
    mut request: PackPlanRequest,
) -> Result<PlannedAnswerPack, PackPlannerLimitError> {
    request.limits.validate()?;

    // Caller-supplied ordinals or malformed spans are not verified file lines.
    // Clear them before scoring, deduplication, IDs and rendering can mistake
    // them for physical evidence. Preserve canonical message identity/content.
    for candidate in &mut request.candidates {
        if candidate.verified_line_range().is_none() {
            candidate.citation_verified = false;
            candidate.line_start = None;
            candidate.line_end = None;
        }
    }

    let candidate_count = request.candidates.len();
    let diagnostics = PackPlannerDiagnostics {
        candidate_fetch_limit: pack_candidate_fetch_limit(&request.limits)?,
        budget: pack_planner_budget_unchecked(request.limits.max_tokens),
    };
    let lexical_range = ScoreRange::from_values(
        request
            .candidates
            .iter()
            .filter_map(|candidate| finite_score(candidate.lexical_score)),
    );
    let semantic_range = ScoreRange::from_values(
        request
            .candidates
            .iter()
            .filter_map(|candidate| finite_score(candidate.semantic_score)),
    );
    // Redact before truncation: cutting a credential first can hide its shape
    // from the detector. Budget the actual output, including expanding markers,
    // and prepare it once rather than repeating regex work during selection.
    let prepared_excerpts = request
        .candidates
        .iter()
        .map(|candidate| {
            if !request.include_skill_content
                && crate::export::is_skill_injection(&candidate.excerpt)
            {
                // Reuse the export default unless the operator opted in.
                // Credential redaction still applies to included skills.
                return (String::new(), false, Vec::new());
            }
            let mut redactions = Vec::new();
            let redacted = redact_pack_output_text(&candidate.excerpt, &mut redactions);
            let (excerpt, truncated) =
                truncate_excerpt(&redacted, request.limits.max_excerpt_chars);
            (excerpt, truncated, redactions)
        })
        .collect::<Vec<_>>();

    let mut remaining: Vec<usize> = (0..request.candidates.len()).collect();
    let mut selected = Vec::new();
    let mut omitted = Vec::new();
    let mut selected_state = SelectedState::default();
    let mut used_tokens = 0usize;

    while !remaining.is_empty() && selected.len() < request.limits.max_evidence {
        let mut best: Option<ScoredCandidate> = None;
        let mut next_remaining = Vec::with_capacity(remaining.len());

        for candidate_index in remaining.iter().copied() {
            let candidate = &request.candidates[candidate_index];
            if let Some(reason) = hard_omission_reason(candidate, &request, &selected_state) {
                let score = score_candidate(
                    candidate,
                    &request,
                    &selected_state,
                    lexical_range,
                    semantic_range,
                    0,
                );
                omitted.push(omitted_candidate(candidate, reason, score));
                continue;
            }

            let (excerpt, excerpt_truncated, _) = &prepared_excerpts[candidate_index];
            if excerpt.trim().is_empty() {
                let score = score_candidate(
                    candidate,
                    &request,
                    &selected_state,
                    lexical_range,
                    semantic_range,
                    0,
                );
                omitted.push(omitted_candidate(
                    candidate,
                    PackOmittedReason::RedactedToEmpty,
                    score,
                ));
                continue;
            }

            next_remaining.push(candidate_index);
            let token_cost = estimated_tokens(excerpt);
            let score = score_candidate(
                candidate,
                &request,
                &selected_state,
                lexical_range,
                semantic_range,
                token_cost,
            );
            let scored = ScoredCandidate {
                index: candidate_index,
                score,
                excerpt: excerpt.clone(),
                excerpt_truncated: *excerpt_truncated,
            };

            if best.as_ref().is_none_or(|current| {
                candidate_ordering(
                    &scored,
                    &request.candidates[scored.index],
                    current,
                    &request.candidates[current.index],
                )
                .is_lt()
            }) {
                best = Some(scored);
            }
        }

        let Some(mut best_candidate) = best else {
            remaining = next_remaining;
            break;
        };

        next_remaining.retain(|candidate_index| *candidate_index != best_candidate.index);
        remaining = next_remaining;
        let candidate = &request.candidates[best_candidate.index];

        let remaining_tokens = diagnostics
            .budget
            .evidence_tokens
            .saturating_sub(used_tokens);
        if best_candidate.score.token_cost > remaining_tokens {
            let max_chars = remaining_tokens.saturating_mul(TOKEN_ESTIMATE_CHARS_PER_TOKEN);
            let retained_chars = max_chars.saturating_sub(3);
            // Fit the already-redacted excerpt before dropping relevant
            // evidence. Keep real source text, never just an ellipsis.
            if best_candidate
                .excerpt
                .chars()
                .take(retained_chars)
                .any(|character| !character.is_whitespace())
            {
                let (excerpt, _) = truncate_excerpt(&best_candidate.excerpt, max_chars);
                best_candidate.score.token_cost = estimated_tokens(&excerpt);
                best_candidate.excerpt = excerpt;
                best_candidate.excerpt_truncated = true;
            }
        }
        if used_tokens.saturating_add(best_candidate.score.token_cost)
            > diagnostics.budget.evidence_tokens
        {
            omitted.push(omitted_candidate(
                candidate,
                PackOmittedReason::TokenBudgetExhausted,
                best_candidate.score,
            ));
            continue;
        }

        let session_key = candidate.session_key();
        if !selected_state.sessions.contains(&session_key)
            && selected_state.sessions.len() >= request.limits.max_sessions
        {
            omitted.push(omitted_candidate(
                candidate,
                PackOmittedReason::MaxSessionsReached,
                best_candidate.score,
            ));
            continue;
        }

        used_tokens = used_tokens.saturating_add(best_candidate.score.token_cost);
        selected_state
            .source_ids
            .insert(candidate.source_id.clone());
        selected_state.sessions.insert(session_key);
        selected_state
            .span_hashes
            .insert(candidate.span_hash.clone());
        selected_state
            .content_hashes
            .insert(candidate.content_hash.clone());
        if let Some(span) = VerifiedPackFileSpan::from_candidate(candidate) {
            selected_state.verified_ranges.push(span);
        }

        selected.push(PlannedPackEvidence {
            id: evidence_id(candidate),
            rank: selected.len() + 1,
            excerpt: best_candidate.excerpt,
            excerpt_truncated: best_candidate.excerpt_truncated,
            estimated_tokens: best_candidate.score.token_cost,
            candidate: candidate.clone(),
            selection: best_candidate.score,
            redactions: prepared_excerpts[best_candidate.index].2.clone(),
        });
    }

    for candidate_index in remaining {
        let candidate = &request.candidates[candidate_index];
        let score = score_candidate(
            candidate,
            &request,
            &selected_state,
            lexical_range,
            semantic_range,
            estimated_tokens(&prepared_excerpts[candidate_index].0),
        );
        omitted.push(omitted_candidate(
            candidate,
            PackOmittedReason::MaxEvidenceReached,
            score,
        ));
    }

    Ok(PlannedAnswerPack {
        candidate_count,
        selected_evidence_count: selected.len(),
        selected_session_count: selected_state.sessions.len(),
        estimated_tokens: used_tokens,
        diagnostics,
        evidence: selected,
        omitted,
    })
}

/// Verify modern Codex citations using the same record parser as ingestion.
/// Unknown formats, remote paths, missing files and ambiguous records keep
/// their archived excerpts, with no invented source coordinates or verification.
/// Called inside the CLI's existing bounded planner worker.
pub fn verify_pack_source_citations(plan: &mut PlannedAnswerPack, deadline: std::time::Instant) {
    use std::io::Read as _;

    const MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
    let mut remaining_bytes = 32 * 1024 * 1024u64;
    let mut by_path: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, item) in plan.evidence.iter_mut().enumerate() {
        let candidate = &mut item.candidate;
        candidate.citation_verified = false;
        candidate.line_start = None;
        candidate.line_end = None;
        candidate.source_readiness = PackSourceReadiness::IncompleteMetadata;
        let path = std::path::Path::new(&candidate.source_path);
        if candidate.agent == "codex"
            && candidate.source_id == "local"
            && candidate.origin_kind == "local"
            && path.is_absolute()
            && path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
        {
            by_path
                .entry(candidate.source_path.clone())
                .or_default()
                .push(index);
        }
    }
    'source_files: for (path, indices) in by_path {
        if std::time::Instant::now() >= deadline || remaining_bytes == 0 {
            break;
        }
        let Ok(before) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !before.is_file() || before.len() > MAX_FILE_BYTES.min(remaining_bytes) {
            continue;
        }
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            // Do not block if a regular file is replaced by a FIFO, or follow a
            // replacement symlink between the metadata probe and the open.
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let Ok(file) = options.open(&path) else {
            continue;
        };
        let Ok(metadata) = file.metadata() else {
            continue;
        };
        if !metadata.is_file() || metadata.len() > MAX_FILE_BYTES.min(remaining_bytes) {
            continue;
        }
        let read_limit = metadata.len().saturating_add(1).min(remaining_bytes);
        let mut bytes = Vec::new();
        let read_result = (&file).take(read_limit).read_to_end(&mut bytes);
        remaining_bytes = remaining_bytes.saturating_sub(bytes.len() as u64);
        if read_result.is_err()
            || bytes.len() as u64 != metadata.len()
            || !file.metadata().is_ok_and(|after| {
                after.len() == metadata.len() && after.modified().ok() == metadata.modified().ok()
            })
        {
            continue;
        }
        let mut matches = vec![(0usize, 0usize, None); indices.len()];
        for (line_index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
            if std::time::Instant::now() >= deadline {
                break 'source_files;
            }
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            let Ok(raw) = serde_json::from_slice(line) else {
                continue;
            };
            let Some(message) = crate::connectors::codex::modern_codex_message(&raw) else {
                continue;
            };
            let mut redacted_content = None;
            for (slot, evidence_index) in matches.iter_mut().zip(&indices) {
                let candidate = &plan.evidence[*evidence_index].candidate;
                if candidate
                    .created_at_ms
                    .is_some_and(|created| message.created_at != Some(created))
                {
                    continue;
                }
                // The modern Codex parser trims message boundaries, while
                // archived FAD messages can retain that whitespace. Ingestion
                // can also redact credentials before storing the message.
                // Compare that exact transform, never the shortened excerpt;
                // still require a unique record and hash its original bytes.
                let excerpt = candidate.excerpt.trim();
                let same_content = if message.content == excerpt {
                    true
                } else {
                    let redacted =
                        redacted_content.get_or_insert_with(|| redact_text(&message.content));
                    redacted.as_ref() == excerpt
                };
                if same_content {
                    slot.0 += 1;
                    slot.1 = line_index + 1;
                    slot.2 = Some(blake3::hash(line));
                }
            }
        }
        for (evidence_index, (count, line, hash)) in indices.into_iter().zip(matches) {
            if count == 1 {
                let candidate = &mut plan.evidence[evidence_index].candidate;
                candidate.line_start = Some(line);
                candidate.line_end = Some(line);
                candidate.citation_verified = true;
                candidate.source_readiness = PackSourceReadiness::Healthy;
                if let Some(hash) = hash {
                    candidate.span_hash = hash.to_hex().to_string();
                }
            }
        }
    }
    // Verification can replace both physical coordinates and the span hash.
    // Bind IDs to the resulting citation, including unverified deadline exits,
    // before renderers use them in evidence, outlines and handoffs.
    for item in &mut plan.evidence {
        item.id = evidence_id(&item.candidate);
    }
}
