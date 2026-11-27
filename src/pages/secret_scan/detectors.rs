use super::types::{
    BUILTIN_PATTERNS, CUSTOM_DENYLIST_PATTERN_ID, ENTROPY_BASE64_RE, ENTROPY_HEX_RE,
    REDACTED_CONTEXT, ScanContext, SecretFinding, SecretLocation, SecretScanConfig,
    SecretScanFilters, SecretSeverity,
};
use crate::franken_sync::compat::ParamValue;
use anyhow::{Context, Result, bail};
use regex::Regex;

struct FindingCandidate<'a> {
    severity: SecretSeverity,
    kind: &'a str,
    pattern: &'a str,
    text: &'a str,
    start: usize,
    end: usize,
    location: SecretLocation,
    ctx: &'a ScanContext,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RedactionRange {
    pub(super) start: usize,
    end: usize,
}

#[derive(Debug, Clone)]
pub(super) struct FindingOccurrence {
    source_path: Option<String>,
    conversation_id: Option<i64>,
    message_id: Option<i64>,
    message_idx: Option<i64>,
    location: SecretLocation,
    start: usize,
    end: usize,
    finding_index: usize,
}

impl FindingOccurrence {
    fn overlaps(&self, candidate: &FindingCandidate<'_>) -> bool {
        self.source_path == candidate.ctx.source_path
            && self.conversation_id == candidate.ctx.conversation_id
            && self.message_id == candidate.ctx.message_id
            && self.message_idx == candidate.ctx.message_idx
            && self.location == candidate.location
            && self.start < candidate.end
            && candidate.start < self.end
    }
}

pub(super) fn scan_text(
    text: &str,
    location: SecretLocation,
    ctx: &ScanContext,
    config: &SecretScanConfig,
    findings: &mut Vec<SecretFinding>,
    seen: &mut Vec<FindingOccurrence>,
    truncated: &mut bool,
) {
    if *truncated || text.is_empty() {
        return;
    }

    // Denylist first (always critical)
    for deny in &config.denylist {
        for mat in deny.find_iter(text) {
            push_finding(
                findings,
                seen,
                FindingCandidate {
                    severity: SecretSeverity::Critical,
                    kind: "denylist",
                    pattern: CUSTOM_DENYLIST_PATTERN_ID,
                    text,
                    start: mat.start(),
                    end: mat.end(),
                    location: location.clone(),
                    ctx,
                },
                config,
                truncated,
            );
            if *truncated {
                return;
            }
        }
    }

    // Built-in patterns
    for pattern in BUILTIN_PATTERNS.iter() {
        for mat in pattern.regex.find_iter(text) {
            let matched = &text[mat.start()..mat.end()];
            if is_allowlisted(matched, config) {
                continue;
            }
            push_finding(
                findings,
                seen,
                FindingCandidate {
                    severity: pattern.severity,
                    kind: pattern.id,
                    pattern: pattern.regex.as_str(),
                    text,
                    start: mat.start(),
                    end: mat.end(),
                    location: location.clone(),
                    ctx,
                },
                config,
                truncated,
            );
            if *truncated {
                return;
            }
        }
    }

    // Entropy-based detection
    for mat in ENTROPY_BASE64_RE.find_iter(text) {
        let candidate = &text[mat.start()..mat.end()];
        // A long pure-hex run belongs to the hex classifier below; classifying
        // it here first would let the Medium base64 kind shadow the more
        // specific `high_entropy_hex` kind in overlap dedup.
        if candidate.len() >= 32 && candidate.chars().all(|ch| ch.is_ascii_hexdigit()) {
            continue;
        }
        if is_base64_entropy_secret(candidate, config, true) {
            push_finding(
                findings,
                seen,
                FindingCandidate {
                    severity: SecretSeverity::Medium,
                    kind: "high_entropy_base64",
                    pattern: "entropy",
                    text,
                    start: mat.start(),
                    end: mat.end(),
                    location: location.clone(),
                    ctx,
                },
                config,
                truncated,
            );
            if *truncated {
                return;
            }
        }
    }

    for mat in ENTROPY_HEX_RE.find_iter(text) {
        let candidate = &text[mat.start()..mat.end()];
        if is_hex_entropy_secret(candidate, config, true) {
            push_finding(
                findings,
                seen,
                FindingCandidate {
                    severity: SecretSeverity::Low,
                    kind: "high_entropy_hex",
                    pattern: "entropy",
                    text,
                    start: mat.start(),
                    end: mat.end(),
                    location: location.clone(),
                    ctx,
                },
                config,
                truncated,
            );
            if *truncated {
                return;
            }
        }
    }
}

fn push_finding(
    findings: &mut Vec<SecretFinding>,
    seen: &mut Vec<FindingOccurrence>,
    candidate: FindingCandidate<'_>,
    config: &SecretScanConfig,
    truncated: &mut bool,
) {
    if let Some(occurrence) = seen
        .iter_mut()
        .find(|occurrence| occurrence.overlaps(&candidate))
    {
        let Some(existing) = findings.get_mut(occurrence.finding_index) else {
            return;
        };
        if candidate.severity.rank() < existing.severity.rank() {
            existing.severity = candidate.severity;
            existing.kind = candidate.kind.to_string();
            existing.pattern = candidate.pattern.to_string();
            existing.match_redacted = redact_token(&candidate.text[candidate.start..candidate.end]);
            existing.context = redact_context(
                candidate.text,
                candidate.start,
                candidate.end,
                config.context_bytes,
                config,
            );
            occurrence.start = occurrence.start.min(candidate.start);
            occurrence.end = occurrence.end.max(candidate.end);
        }
        return;
    }

    if findings.len() >= config.max_findings {
        *truncated = true;
        return;
    }

    let match_text = &candidate.text[candidate.start..candidate.end];
    let match_redacted = redact_token(match_text);
    let context = redact_context(
        candidate.text,
        candidate.start,
        candidate.end,
        config.context_bytes,
        config,
    );
    let finding_index = findings.len();
    seen.push(FindingOccurrence {
        source_path: candidate.ctx.source_path.clone(),
        conversation_id: candidate.ctx.conversation_id,
        message_id: candidate.ctx.message_id,
        message_idx: candidate.ctx.message_idx,
        location: candidate.location.clone(),
        start: candidate.start,
        end: candidate.end,
        finding_index,
    });

    findings.push(SecretFinding {
        severity: candidate.severity,
        kind: candidate.kind.to_string(),
        pattern: candidate.pattern.to_string(),
        match_redacted,
        context,
        location: candidate.location,
        agent: redact_report_provenance(&candidate.ctx.agent, config),
        workspace: redact_report_provenance(&candidate.ctx.workspace, config),
        source_path: redact_report_provenance(&candidate.ctx.source_path, config),
        conversation_id: candidate.ctx.conversation_id,
        message_id: candidate.ctx.message_id,
        message_idx: candidate.ctx.message_idx,
    });
}

pub(super) fn redact_token(_token: &str) -> String {
    REDACTED_CONTEXT.to_string()
}

pub(super) fn redact_report_provenance(
    value: &Option<String>,
    config: &SecretScanConfig,
) -> Option<String> {
    value
        .as_deref()
        .map(|text| redact_report_slice(text, 0, text.len(), config, Vec::new()))
}

pub(super) fn redact_context(
    text: &str,
    start: usize,
    end: usize,
    window: usize,
    config: &SecretScanConfig,
) -> String {
    if text.is_empty() || start >= end || start >= text.len() {
        return String::new();
    }

    let safe_start = adjust_to_char_boundary(text, start.min(text.len()), false);
    let safe_end = adjust_to_char_boundary(text, end.min(text.len()), true);
    if safe_start >= safe_end {
        return String::new();
    }

    let ctx_start = safe_start.saturating_sub(window / 2);
    let ctx_end = safe_end.saturating_add(window / 2).min(text.len());
    let ctx_start = adjust_to_char_boundary(text, ctx_start, false);
    let ctx_end = adjust_to_char_boundary(text, ctx_end, true);

    if ctx_start >= ctx_end {
        return String::new();
    }

    // Context is a report surface, so it has a stronger rule than finding
    // admission: every known secret span is masked, including allowlisted
    // spans and adjacent matches that are not the focal finding. Only ranges
    // intersecting this bounded window are retained in memory.
    // The focal match must be masked even if a future finding source is not
    // represented in `collect_context_redactions` yet.
    let local_redactions = vec![RedactionRange {
        start: safe_start,
        end: safe_end,
    }];
    redact_report_slice(text, ctx_start, ctx_end, config, local_redactions)
}

fn redact_report_slice(
    text: &str,
    slice_start: usize,
    slice_end: usize,
    config: &SecretScanConfig,
    mut redactions: Vec<RedactionRange>,
) -> String {
    redactions.extend(collect_context_redactions(
        text,
        config,
        slice_start,
        slice_end,
    ));
    let redactions = merge_redaction_ranges(redactions);

    let mut redacted = String::with_capacity(slice_end.saturating_sub(slice_start));
    let mut cursor = slice_start;
    for range in redactions {
        let range_start = range.start.max(slice_start);
        let range_end = range.end.min(slice_end);
        if range_start >= range_end {
            continue;
        }
        if cursor < range_start {
            redacted.push_str(&text[cursor..range_start]);
        }
        if cursor < range_end {
            redacted.push_str(REDACTED_CONTEXT);
            cursor = range_end;
        }
    }
    if cursor < slice_end {
        redacted.push_str(&text[cursor..slice_end]);
    }

    // Defense in depth: the shared ingestion redactor is the canonical
    // credential floor. The interval pass additionally covers custom
    // denylist and entropy matches, including secrets crossing slice edges.
    crate::indexer::redact_secrets::redact_text(&redacted).into_owned()
}

pub(super) fn collect_context_redactions(
    text: &str,
    config: &SecretScanConfig,
    context_start: usize,
    context_end: usize,
) -> Vec<RedactionRange> {
    if context_start >= context_end || context_start >= text.len() {
        return Vec::new();
    }
    let mut ranges = Vec::new();

    for deny in &config.denylist {
        ranges.extend(
            deny.find_iter(text)
                .take_while(|mat| mat.start() < context_end)
                .filter(|mat| mat.start() < mat.end() && mat.end() > context_start)
                .map(|mat| RedactionRange {
                    start: mat.start(),
                    end: mat.end(),
                }),
        );
    }

    for pattern in BUILTIN_PATTERNS.iter() {
        ranges.extend(
            pattern
                .regex
                .find_iter(text)
                .take_while(|mat| mat.start() < context_end)
                .filter(|mat| mat.start() < mat.end() && mat.end() > context_start)
                .map(|mat| RedactionRange {
                    start: mat.start(),
                    end: mat.end(),
                }),
        );
    }

    ranges.extend(
        ENTROPY_BASE64_RE
            .find_iter(text)
            .take_while(|mat| mat.start() < context_end)
            .filter(|mat| mat.start() < mat.end() && mat.end() > context_start)
            .filter(|mat| is_base64_entropy_secret(mat.as_str(), config, false))
            .map(|mat| RedactionRange {
                start: mat.start(),
                end: mat.end(),
            }),
    );
    ranges.extend(
        ENTROPY_HEX_RE
            .find_iter(text)
            .take_while(|mat| mat.start() < context_end)
            .filter(|mat| mat.start() < mat.end() && mat.end() > context_start)
            .filter(|mat| is_hex_entropy_secret(mat.as_str(), config, false))
            .map(|mat| RedactionRange {
                start: mat.start(),
                end: mat.end(),
            }),
    );

    merge_redaction_ranges(ranges)
}

fn merge_redaction_ranges(mut ranges: Vec<RedactionRange>) -> Vec<RedactionRange> {
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    let mut merged: Vec<RedactionRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.start >= range.end {
            continue;
        }
        if let Some(last) = merged.last_mut()
            && range.start <= last.end
        {
            last.end = last.end.max(range.end);
        } else {
            merged.push(range);
        }
    }
    merged
}

fn is_base64_entropy_secret(
    candidate: &str,
    config: &SecretScanConfig,
    honor_allowlist: bool,
) -> bool {
    if candidate.len() < config.entropy_min_len
        || (honor_allowlist && is_allowlisted(candidate, config))
    {
        return false;
    }
    // Pure alphabetic strings are commonly code identifiers, not secrets.
    if candidate.chars().all(|ch| ch.is_ascii_alphabetic()) {
        return false;
    }
    shannon_entropy(candidate) >= config.entropy_threshold
}

fn is_hex_entropy_secret(
    candidate: &str,
    config: &SecretScanConfig,
    honor_allowlist: bool,
) -> bool {
    candidate.len() >= 32
        && !(honor_allowlist && is_allowlisted(candidate, config))
        && shannon_entropy(candidate) >= 3.0
}

pub(super) fn adjust_to_char_boundary(text: &str, idx: usize, forward: bool) -> usize {
    if idx >= text.len() {
        return text.len();
    }
    if text.is_char_boundary(idx) {
        return idx;
    }
    if forward {
        for i in idx..text.len() {
            if text.is_char_boundary(i) {
                return i;
            }
        }
        text.len()
    } else {
        for i in (0..=idx).rev() {
            if text.is_char_boundary(i) {
                return i;
            }
        }
        0
    }
}

pub(super) fn shannon_entropy(token: &str) -> f64 {
    let bytes = token.as_bytes();
    let len = bytes.len() as f64;
    if len == 0.0 {
        return 0.0;
    }
    let mut freq = [0usize; 256];
    for b in bytes {
        freq[*b as usize] += 1;
    }
    let mut entropy = 0.0;
    for count in freq.iter().copied() {
        if count == 0 {
            continue;
        }
        let p = count as f64 / len;
        entropy -= p * p.log2();
    }
    entropy
}

pub(super) fn is_allowlisted(matched: &str, config: &SecretScanConfig) -> bool {
    for allow in &config.allowlist {
        if allow.is_match(matched) {
            return true;
        }
    }
    false
}

pub(super) fn is_fully_allowlisted(value: &str, config: &SecretScanConfig) -> bool {
    config.allowlist.iter().any(|allow| {
        allow
            .find(value)
            .is_some_and(|matched| matched.start() == 0 && matched.end() == value.len())
    })
}

#[cfg(test)]
pub(super) fn build_where_clause(filters: &SecretScanFilters) -> Result<(String, Vec<ParamValue>)> {
    build_where_clause_for_columns(filters, "COALESCE(a.slug, 'unknown')", "w.path")
}

pub(super) fn build_where_clause_for_columns(
    filters: &SecretScanFilters,
    agent_expression: &str,
    workspace_expression: &str,
) -> Result<(String, Vec<ParamValue>)> {
    for expression in [agent_expression, workspace_expression] {
        if !expression.chars().all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '(' | ')' | ',' | '\'' | ' ')
        }) {
            bail!("Invalid SQLite expression while building secret-scan filters");
        }
    }

    let mut conditions: Vec<String> = Vec::new();
    let mut params: Vec<ParamValue> = Vec::new();

    if let Some(agents) = filters.agents.as_ref() {
        if agents.is_empty() {
            conditions.push("1=0".to_string());
        } else {
            let placeholders: Vec<&str> = agents.iter().map(|_| "?").collect();
            conditions.push(format!(
                "{agent_expression} IN ({})",
                placeholders.join(", ")
            ));
            for agent in agents {
                params.push(ParamValue::from(agent.as_str()));
            }
        }
    }

    if let Some(workspaces) = filters.workspaces.as_ref() {
        if workspaces.is_empty() {
            conditions.push("1=0".to_string());
        } else {
            let placeholders: Vec<&str> = workspaces.iter().map(|_| "?").collect();
            conditions.push(format!(
                "{workspace_expression} IN ({})",
                placeholders.join(", ")
            ));
            for ws in workspaces {
                params.push(ParamValue::from(ws.to_string_lossy().to_string()));
            }
        }
    }

    if let Some(since) = filters.since_ts {
        conditions.push("c.started_at >= ?".to_string());
        params.push(ParamValue::from(since));
    }

    if let Some(until) = filters.until_ts {
        conditions.push("c.started_at <= ?".to_string());
        params.push(ParamValue::from(until));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    Ok((where_clause, params))
}

pub(super) fn bounded_keyset_page_where(
    base_where: &str,
    id_column: &str,
    last_id: Option<i64>,
) -> String {
    let mut page_where = if base_where.is_empty() {
        format!(" WHERE {id_column} <= ?")
    } else {
        format!("{base_where} AND {id_column} <= ?")
    };
    if last_id.is_some() {
        page_where.push_str(&format!(" AND {id_column} > ?"));
    }
    page_where
}

pub(super) fn parse_env_regex_list(var: &str) -> Result<Vec<String>> {
    let value = match dotenvy::var(var) {
        Ok(v) => v,
        Err(_) => return Ok(Vec::new()),
    };
    let items = value
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    Ok(items)
}

pub(super) fn compile_regexes(patterns: &[String], label: &str) -> Result<Vec<Regex>> {
    let mut compiled = Vec::new();
    for pat in patterns {
        let regex = Regex::new(pat).with_context(|| format!("Invalid {} regex: {}", label, pat))?; // ubs:ignore[rust.perf.regex-in-loop] — one-time initialization compilation of caller-supplied pattern list
        compiled.push(regex);
    }
    Ok(compiled)
}
