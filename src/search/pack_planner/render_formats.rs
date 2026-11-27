use sha2::{Digest, Sha256};
use std::fmt::Write as _;

use super::render::render_error;
use super::types::{
    PackEvidenceRole, PackFreshnessPolicy, PackLexicalReadiness, PackOmittedReason,
    PackRenderError, PackRenderRequest, PackSemanticReadiness, PackSourceReadiness,
    PackSourceSyncGapKind, RenderedAnswerPack,
};

pub(super) fn render_answer_pack_jsonl(
    envelope: &RenderedAnswerPack,
    request: &PackRenderRequest,
) -> Result<String, PackRenderError> {
    let mut lines = Vec::with_capacity(envelope.evidence.len() + 4);
    lines.push(json_line(
        serde_json::json!({
            "_meta": &envelope.meta,
            "schema_version": envelope.schema_version,
            "budget": &envelope.budget,
        }),
        request,
    )?);
    lines.push(json_line(
        serde_json::json!({ "pack": &envelope.pack }),
        request,
    )?);
    for evidence in &envelope.evidence {
        lines.push(json_line(
            serde_json::json!({ "evidence": evidence }),
            request,
        )?);
    }
    lines.push(json_line(
        serde_json::json!({ "omitted": &envelope.omitted }),
        request,
    )?);
    lines.push(json_line(
        serde_json::json!({ "privacy": &envelope.privacy }),
        request,
    )?);
    Ok(lines.join("\n"))
}

fn json_line(
    value: serde_json::Value,
    request: &PackRenderRequest,
) -> Result<String, PackRenderError> {
    serde_json::to_string(&value).map_err(|err| render_error(request, err))
}

pub(super) fn render_answer_pack_markdown(envelope: &RenderedAnswerPack) -> String {
    let mut out = String::new();
    out.push_str("# ");
    out.push_str(&markdown_literal_line(&envelope.pack.title));
    if !envelope.warnings.is_empty() {
        out.push_str("\n\n## Warnings\n");
        for warning in &envelope.warnings {
            out.push_str("- ");
            out.push_str(&markdown_literal_line(warning));
            out.push('\n');
        }
    }
    out.push_str("\n## Handoff\n");
    if envelope.pack.handoff.is_empty() {
        out.push_str("- No evidence selected.\n");
    } else {
        for item in &envelope.pack.handoff {
            out.push_str("- ");
            out.push_str(&markdown_literal_line(&item.text));
            out.push_str(" [");
            out.push_str(&item.evidence_ids.join(", "));
            out.push_str("]\n");
        }
    }

    out.push_str("\n## Evidence\n");
    if envelope.evidence.is_empty() {
        out.push_str("- No cited evidence.\n");
    } else {
        for item in &envelope.evidence {
            out.push('[');
            out.push_str(&item.id);
            out.push_str("] ");
            out.push_str(&markdown_literal_line(&item.citation.agent));
            out.push(' ');
            out.push_str(&markdown_literal_line(&item.citation.source_id));
            out.push(' ');
            out.push_str(&markdown_literal_line(&item.citation.source_path));
            if let Some(line_start) = item.citation.line_start {
                out.push(':');
                out.push_str(&line_start.to_string());
                // ubs:ignore — compares public citation line numbers, not secret material.
                if item.citation.line_end != item.citation.line_start
                    && let Some(line_end) = item.citation.line_end
                {
                    out.push('-');
                    out.push_str(&line_end.to_string());
                }
            }
            if let Some(conversation_id) = item.citation.conversation_id {
                let _ = write!(out, " conversation_id={conversation_id}");
            }
            if let Some(message_index) = item.citation.message_index {
                let _ = write!(out, " message_index={message_index} (1-based)");
            }
            out.push_str("\n\n");
            // Preserve the complete prepared excerpt, including blank lines.
            // A longer fence keeps source fences, links and HTML literal.
            let fence_length = item
                .excerpt
                .split(|character| character != '`')
                .map(str::len)
                .max()
                .unwrap_or(0)
                .saturating_add(1)
                .max(3);
            let fence = "`".repeat(fence_length);
            out.push_str(&fence);
            out.push_str("text\n");
            out.push_str(&item.excerpt);
            if !item.excerpt.ends_with('\n') {
                out.push('\n');
            }
            out.push_str(&fence);
            out.push_str("\n\n");
        }
    }

    if envelope.omitted.count > 0 {
        out.push_str("\n## Omitted\n");
        for item in &envelope.omitted.items {
            out.push_str("- ");
            out.push_str(omitted_reason_label(item.reason));
            out.push_str(": ");
            out.push_str(&markdown_literal_line(&item.source_path));
            if let Some(line_start) = item.line_start {
                out.push(':');
                out.push_str(&line_start.to_string());
            }
            out.push('\n');
        }
    }
    out
}

pub(super) fn compact_excerpt(excerpt: &str, max_chars: usize) -> String {
    let line = markdown_line(excerpt);
    if line.chars().count() <= max_chars {
        return line;
    }
    let mut compact = line
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    compact.push_str("...");
    compact
}

fn markdown_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn markdown_literal_line(text: &str) -> String {
    let mut out = String::new();
    for character in markdown_line(text).chars() {
        if matches!(
            character,
            '\\' | '`' | '*' | '_' | '[' | ']' | '<' | '>' | '!' | '|' | '~' | '&' | '#'
        ) {
            out.push('\\');
        }
        out.push(character);
    }
    out
}

pub(super) fn sha256_hex(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(super) fn pack_toon_encode_options() -> toon::EncodeOptions {
    toon::EncodeOptions {
        indent: None,
        delimiter: None,
        key_folding: Some(toon::options::KeyFoldingMode::Off),
        flatten_depth: None,
        replacer: None,
    }
}

pub(super) fn freshness_policy_label(policy: PackFreshnessPolicy) -> &'static str {
    match policy {
        PackFreshnessPolicy::PreferRecent => "prefer-recent",
        PackFreshnessPolicy::Strict => "strict",
        PackFreshnessPolicy::AllowStale => "allow-stale",
    }
}

pub(super) fn evidence_role_label(role: PackEvidenceRole) -> &'static str {
    match role {
        PackEvidenceRole::AssistantConclusion => "assistant_conclusion",
        PackEvidenceRole::ToolResult => "tool_result",
        PackEvidenceRole::UserRequirement => "user_requirement",
        PackEvidenceRole::ToolCallArgument => "tool_call_argument",
        PackEvidenceRole::Unknown => "unknown",
    }
}

fn omitted_reason_label(reason: PackOmittedReason) -> &'static str {
    match reason {
        PackOmittedReason::TokenBudgetExhausted => "token_budget_exhausted",
        PackOmittedReason::MaxSessionsReached => "max_sessions_reached",
        PackOmittedReason::MaxEvidenceReached => "max_evidence_reached",
        PackOmittedReason::DuplicateContent => "duplicate_content",
        PackOmittedReason::SameSessionLowerRank => "same_session_lower_rank",
        PackOmittedReason::StaleUnderStrictPolicy => "stale_under_strict_policy",
        PackOmittedReason::SourceUnavailable => "source_unavailable",
        PackOmittedReason::RedactedToEmpty => "redacted_to_empty",
        PackOmittedReason::FieldMaskExcluded => "field_mask_excluded",
    }
}

pub(super) fn source_readiness_rank(readiness: PackSourceReadiness) -> usize {
    match readiness {
        PackSourceReadiness::Healthy => 0,
        PackSourceReadiness::StaleReadable => 1,
        PackSourceReadiness::IncompleteMetadata => 2,
        PackSourceReadiness::Unavailable => 3,
    }
}

pub(super) fn source_readiness_label(readiness: PackSourceReadiness) -> &'static str {
    match readiness {
        PackSourceReadiness::Healthy => "healthy",
        PackSourceReadiness::StaleReadable => "stale_readable",
        PackSourceReadiness::IncompleteMetadata => "incomplete_metadata",
        PackSourceReadiness::Unavailable => "unavailable",
    }
}

pub(super) fn lexical_readiness_label(readiness: PackLexicalReadiness) -> &'static str {
    match readiness {
        PackLexicalReadiness::Ready => "ready",
        PackLexicalReadiness::Stale => "stale",
        PackLexicalReadiness::Missing => "missing",
        PackLexicalReadiness::Rebuilding => "rebuilding",
        PackLexicalReadiness::Unknown => "unknown",
    }
}

pub(super) fn semantic_readiness_label(readiness: PackSemanticReadiness) -> &'static str {
    match readiness {
        PackSemanticReadiness::NotReported => "not_reported",
        PackSemanticReadiness::Joined => "joined",
        PackSemanticReadiness::FallbackLexical => "fallback_lexical",
        PackSemanticReadiness::Unavailable => "unavailable",
        PackSemanticReadiness::Disabled => "disabled",
    }
}

pub(super) fn source_sync_gap_kind_label(kind: PackSourceSyncGapKind) -> &'static str {
    match kind {
        PackSourceSyncGapKind::RemoteStale => "remote_stale",
        PackSourceSyncGapKind::SourcePruned => "source_pruned",
        PackSourceSyncGapKind::SyncDeferred => "sync_deferred",
        PackSourceSyncGapKind::Unknown => "unknown",
    }
}

pub(super) fn effective_semantic_readiness(request: &PackRenderRequest) -> PackSemanticReadiness {
    if !matches!(
        request.readiness.semantic_readiness,
        PackSemanticReadiness::NotReported
    ) {
        return request.readiness.semantic_readiness;
    }
    if request.semantic_joined {
        return PackSemanticReadiness::Joined;
    }
    if request
        .fallback_mode
        .as_deref()
        .is_some_and(|mode| mode.eq_ignore_ascii_case("lexical"))
    {
        return PackSemanticReadiness::FallbackLexical;
    }
    PackSemanticReadiness::NotReported
}
