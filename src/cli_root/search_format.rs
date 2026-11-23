#[allow(unused_imports)]
use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
use crate::indexer::IndexOptions;
#[allow(unused_imports)]
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
use crate::robot_budget_envelope::BudgetSnapshot;
#[allow(unused_imports)]
use crate::{cli_app::*, cli_root::*, lib_doctor::*, *};
#[allow(unused_imports)]
use anyhow::Result;
#[allow(unused_imports)]
use base64::prelude::*;
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[allow(unused_imports)]
use std::path::{Path, PathBuf};
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{info, warn};
#[allow(unused_imports)]
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Output search results in human-readable display format
pub(crate) fn output_display_results(
    hits: &[crate::search::query::SearchHit],
    format: DisplayFormat,
    wrap: WrapConfig,
    query: &str,
    highlight: bool,
) -> CliResult<()> {
    match format {
        DisplayFormat::Table => {
            // Aligned columns with headers
            println!("{:<6} {:<12} {:<25} SNIPPET", "SCORE", "AGENT", "WORKSPACE");
            println!("{}", "-".repeat(80));
            for hit in hits {
                let workspace = truncate_start(&hit.workspace, 24);
                let snippet = hit.snippet.replace('\n', " ");
                let snippet = if highlight {
                    highlight_matches(&snippet, query, "**", "**")
                } else {
                    snippet
                };
                let snippet_display = truncate_end(&snippet, 50);
                println!(
                    "{:<6.2} {:<12} {:<25} {}",
                    hit.score, hit.agent, workspace, snippet_display
                );
            }
            println!("\n{} results", hits.len());
        }
        DisplayFormat::Lines => {
            // One-liner per result
            for hit in hits {
                let snippet = hit.snippet.replace('\n', " ");
                let snippet = if highlight {
                    highlight_matches(&snippet, query, "**", "**")
                } else {
                    snippet
                };
                let snippet_short = truncate_end(&snippet, 60);
                println!(
                    "[{:.1}] {} | {} | {}",
                    hit.score, hit.agent, hit.source_path, snippet_short
                );
            }
        }
        DisplayFormat::Markdown => {
            // Markdown with headers and code blocks
            println!("# Search Results\n");
            println!("Found **{}** results.\n", hits.len());
            for (i, hit) in hits.iter().enumerate() {
                println!("## {}. {} (score: {:.2})\n", i + 1, hit.agent, hit.score);
                println!("- **Workspace**: `{}`", hit.workspace);
                println!("- **Path**: `{}`", hit.source_path);
                if let Some(ts) = hit.created_at {
                    let dt = chrono::DateTime::from_timestamp_millis(ts).map_or_else(
                        || "unknown".to_string(),
                        |d| d.format("%Y-%m-%d %H:%M").to_string(),
                    );
                    println!("- **Created**: {dt}");
                }
                let snippet = if highlight {
                    // Use backticks for highlighting in markdown code blocks (shows as-is)
                    // But for non-code context, we'd use **bold**
                    highlight_matches(&hit.snippet, query, ">>>", "<<<")
                } else {
                    hit.snippet.clone()
                };
                let snippet = apply_wrap(&snippet, wrap);
                println!("\n```\n{snippet}\n```\n");
            }
        }
    }
    Ok(())
}

// Named presets retain the complete canonical follow-up anchor even when
// message bodies are omitted. Custom field masks remain caller-controlled.
pub(crate) const SEARCH_MINIMAL_FIELDS: &[&str] = &[
    "source_path",
    "line_number",
    "agent",
    "source_id",
    "conversation_id",
];
pub(crate) const SEARCH_SUMMARY_FIELDS: &[&str] = &[
    "source_path",
    "line_number",
    "agent",
    "title",
    "score",
    "source_id",
    "conversation_id",
];

/// Expand field presets and return the resolved field list
pub(crate) fn expand_field_presets(fields: &Option<Vec<String>>) -> Option<Vec<String>> {
    fields.as_ref().map(|f| {
        f.iter()
            .flat_map(|field| match field.as_str() {
                "minimal" => SEARCH_MINIMAL_FIELDS
                    .iter()
                    .map(|field| (*field).to_string())
                    .collect(),
                "summary" => SEARCH_SUMMARY_FIELDS
                    .iter()
                    .map(|field| (*field).to_string())
                    .collect(),
                // Provenance preset (P3.4) - add source origin info to results
                "provenance" => vec![
                    "source_id".to_string(),
                    "origin_kind".to_string(),
                    "origin_host".to_string(),
                ],
                "*" | "all" => vec![], // Empty means include all - handled specially
                other => vec![other.to_string()],
            })
            .collect()
    })
}

pub(crate) fn resolve_field_mask(
    fields: &Option<Vec<String>>,
    max_content_length: Option<usize>,
    max_tokens: Option<usize>,
    visible_limit: usize,
    format: Option<RobotFormat>,
    display_format: Option<DisplayFormat>,
) -> crate::search::query::FieldMask {
    use crate::search::query::FieldMask;

    if matches!(format, Some(RobotFormat::Sessions)) {
        return FieldMask::new(false, false, false, false);
    }

    if format.is_none() && display_format.is_none() {
        return FieldMask::new(true, true, false, true);
    }

    if format.is_none() {
        return FieldMask::new(true, true, false, true);
    }

    let resolved_fields = expand_field_presets(fields);
    let wants_all = fields.is_none()
        || resolved_fields
            .as_ref()
            .is_some_and(|field_list| field_list.is_empty());
    let wants_snippet = wants_all
        || resolved_fields
            .as_ref()
            .is_some_and(|field_list| field_list.iter().any(|f| f == "snippet"));
    let wants_content = wants_all
        || resolved_fields
            .as_ref()
            .is_some_and(|field_list| field_list.iter().any(|f| f == "content"));
    let wants_title = wants_all
        || resolved_fields
            .as_ref()
            .is_some_and(|field_list| field_list.iter().any(|f| f == "title"));

    let needs_content_for_rendering = wants_content || wants_snippet;
    let allows_cache = needs_content_for_rendering;
    let preview_content_limit = if needs_content_for_rendering {
        preview_content_limit_for_search(max_content_length, max_tokens, visible_limit)
    } else {
        None
    };
    FieldMask::new(wants_content, wants_snippet, wants_title, allows_cache)
        .with_preview_content_limit(preview_content_limit)
}

pub(crate) const SEARCH_PREVIEW_CONTENT_LIMIT_MAX: usize = 400;
pub(crate) const TOKEN_BUDGET_DEFAULT_VISIBLE_HITS: usize = 20;
pub(crate) const TOKEN_BUDGET_SEARCH_MIN_HITS: usize = 20;
pub(crate) const TOKEN_BUDGET_SEARCH_MAX_HITS: usize = 4096;
pub(crate) const TOKEN_BUDGET_SEARCH_CHARS_PER_HIT: usize = 256;

pub(crate) fn token_budget_field_mask_visible_limit(
    max_tokens: Option<usize>,
    explicit_limit: usize,
) -> usize {
    if explicit_limit > 0 {
        explicit_limit
    } else if max_tokens.is_some() {
        TOKEN_BUDGET_DEFAULT_VISIBLE_HITS
    } else {
        explicit_limit
    }
}

pub(crate) fn token_budget_search_limit(max_tokens: Option<usize>) -> Option<usize> {
    let tokens = max_tokens?;
    let budget_chars = tokens.saturating_mul(4);
    let hits = budget_chars / TOKEN_BUDGET_SEARCH_CHARS_PER_HIT;
    Some(hits.clamp(TOKEN_BUDGET_SEARCH_MIN_HITS, TOKEN_BUDGET_SEARCH_MAX_HITS))
}

pub(crate) fn preview_content_limit_for_search(
    max_content_length: Option<usize>,
    max_tokens: Option<usize>,
    visible_limit: usize,
) -> Option<usize> {
    if let Some(max_chars) =
        max_content_length.filter(|max_chars| *max_chars <= SEARCH_PREVIEW_CONTENT_LIMIT_MAX)
    {
        return Some(max_chars);
    }

    let tokens = max_tokens?;
    let per_hit_chars = tokens.saturating_mul(4) / visible_limit.max(1);
    let content_chars = per_hit_chars.saturating_mul(35) / 100;
    let content_chars = content_chars.max(12);
    (content_chars <= SEARCH_PREVIEW_CONTENT_LIMIT_MAX).then_some(content_chars)
}

#[cfg(test)]
mod field_mask_resolution_tests {
    use super::*;

    #[test]
    fn snippet_projection_does_not_materialize_content_field() {
        let fields = Some(vec!["snippet".to_string()]);
        let mask = resolve_field_mask(&fields, None, None, 10, Some(RobotFormat::Json), None);

        assert!(mask.wants_snippet());
        assert!(!mask.needs_content());
        assert!(mask.allows_cache());
    }

    #[test]
    fn content_projection_materializes_content_field() {
        let fields = Some(vec!["content".to_string()]);
        let mask = resolve_field_mask(&fields, None, None, 10, Some(RobotFormat::Json), None);

        assert!(!mask.wants_snippet());
        assert!(mask.needs_content());
        assert!(mask.allows_cache());
    }

    #[test]
    fn small_token_budget_uses_preview_content_before_search_hydration() {
        let mask = resolve_field_mask(&None, None, Some(1024), 20, Some(RobotFormat::Json), None);

        assert!(mask.needs_content());
        assert!(mask.wants_snippet());
        assert_eq!(mask.preview_content_limit(), Some(71));
        assert!(
            !mask.allows_cache(),
            "preview-limited hits should not be cached as full-content results"
        );
    }

    #[test]
    fn large_token_budget_keeps_full_content_hydration() {
        let mask = resolve_field_mask(
            &None,
            None,
            Some(100_000),
            20,
            Some(RobotFormat::Json),
            None,
        );

        assert!(mask.needs_content());
        assert!(mask.wants_snippet());
        assert_eq!(mask.preview_content_limit(), None);
        assert!(mask.allows_cache());
    }

    #[test]
    fn no_limit_token_budget_uses_preview_limit_and_bounded_fetch() {
        let visible_limit = token_budget_field_mask_visible_limit(Some(1024), 0);
        let mask = resolve_field_mask(
            &None,
            None,
            Some(1024),
            visible_limit,
            Some(RobotFormat::Json),
            None,
        );

        assert_eq!(visible_limit, TOKEN_BUDGET_DEFAULT_VISIBLE_HITS);
        assert_eq!(mask.preview_content_limit(), Some(71));
        assert_eq!(
            token_budget_search_limit(Some(1024)),
            Some(TOKEN_BUDGET_SEARCH_MIN_HITS)
        );
        assert_eq!(
            token_budget_search_limit(Some(1_000_000)),
            Some(TOKEN_BUDGET_SEARCH_MAX_HITS)
        );
        assert_eq!(token_budget_search_limit(None), None);
    }
}

/// Filter a search hit to only include the requested fields
pub(crate) fn safe_robot_score_value(score: f32) -> serde_json::Value {
    serde_json::Value::from(if score.is_finite() { score as f64 } else { 0.0 })
}

pub(crate) fn normalized_robot_hit_source_id(hit: &crate::search::query::SearchHit) -> String {
    if let Some(source_id) = canonical_followup_source_id(Some(hit.source_id.as_str())) {
        return source_id;
    }
    if hit.origin_kind.trim().eq_ignore_ascii_case("local") {
        return crate::sources::provenance::LOCAL_SOURCE_ID.to_string();
    }
    hit.source_id.trim().to_string()
}

pub(crate) fn normalized_robot_hit_origin_kind(hit: &crate::search::query::SearchHit) -> String {
    let trimmed = hit.origin_kind.trim();
    if !trimmed.is_empty() {
        return trimmed.to_string();
    }
    if normalized_robot_hit_source_id(hit) == crate::sources::provenance::LOCAL_SOURCE_ID {
        return crate::sources::provenance::LOCAL_SOURCE_ID.to_string();
    }
    String::new()
}

pub(crate) fn normalized_robot_hit_origin_host(
    hit: &crate::search::query::SearchHit,
) -> Option<String> {
    hit.origin_host
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn normalized_robot_hit_for_output(
    hit: &crate::search::query::SearchHit,
) -> crate::search::query::SearchHit {
    let mut normalized = hit.clone();
    normalized.source_id = normalized_robot_hit_source_id(hit);
    normalized.origin_kind = normalized_robot_hit_origin_kind(hit);
    normalized.origin_host = normalized_robot_hit_origin_host(hit);
    if !normalized.score.is_finite() {
        normalized.score = 0.0;
    }
    normalized
}

pub(crate) fn projected_hit_field_value(
    hit: &crate::search::query::SearchHit,
    field: &str,
) -> Option<serde_json::Value> {
    match field {
        "score" => Some(safe_robot_score_value(hit.score)),
        "agent" => Some(serde_json::Value::String(hit.agent.clone())),
        "workspace" => Some(serde_json::Value::String(hit.workspace.clone())),
        "source_path" => Some(serde_json::Value::String(hit.source_path.clone())),
        "snippet" => Some(serde_json::Value::String(hit.snippet.clone())),
        "content" => Some(serde_json::Value::String(hit.content.clone())),
        "title" => Some(serde_json::Value::String(hit.title.clone())),
        "created_at" => serde_json::to_value(hit.created_at).ok(),
        "conversation_id" => serde_json::to_value(hit.conversation_id).ok(),
        "line_number" => serde_json::to_value(hit.line_number).ok(),
        "match_type" => serde_json::to_value(hit.match_type).ok(),
        // Provenance fields (P3.4)
        "source_id" => Some(serde_json::Value::String(normalized_robot_hit_source_id(
            hit,
        ))),
        "origin_kind" => Some(serde_json::Value::String(normalized_robot_hit_origin_kind(
            hit,
        ))),
        // Preserve SearchHit serialization semantics:
        // origin_host/workspace_original are omitted entirely when None.
        "origin_host" => normalized_robot_hit_origin_host(hit).map(serde_json::Value::String),
        "workspace_original" => hit
            .workspace_original
            .as_ref()
            .map(|value| serde_json::Value::String(value.clone())),
        _ => None,
    }
}

pub(crate) fn filter_hit_fields(
    hit: &crate::search::query::SearchHit,
    fields: &Option<Vec<String>>,
) -> serde_json::Value {
    // Sanitize NaN/Infinity score before serialization — serde_json rejects non-finite floats.
    let sanitize = |h: &crate::search::query::SearchHit| -> serde_json::Value {
        serde_json::to_value(normalized_robot_hit_for_output(h)).unwrap_or_default()
    };
    match fields {
        None => sanitize(hit),
        Some(field_list) if field_list.is_empty() => sanitize(hit),
        Some(field_list) => {
            let mut filtered = serde_json::Map::new();
            let known_fields = [
                "score",
                "agent",
                "workspace",
                "source_path",
                "snippet",
                "content",
                "title",
                "created_at",
                "conversation_id",
                "line_number",
                "match_type",
                // Provenance fields (P3.4)
                "source_id",
                "origin_kind",
                "origin_host",
                "workspace_original",
            ];

            for field in field_list {
                if let Some(value) = projected_hit_field_value(hit, field) {
                    filtered.insert(field.clone(), value);
                } else if !known_fields.contains(&field.as_str()) {
                    // Warn about unknown fields.
                    warn!(unknown_field = %field, "Unknown field in --fields, ignoring");
                }
            }
            serde_json::Value::Object(filtered)
        }
    }
}

/// Truncate a string to `max_len` characters, UTF-8 safe, with ellipsis.
/// Returns `Some(truncated)` if truncation was needed, `None` otherwise.
///
/// `coding_agent_session_search-nj5eh`: pre-fix this function always
/// walked the full string with `s.chars().count()` to decide whether
/// truncation was needed, then walked it AGAIN with `take()` if it
/// was. For 100KB content with max_len=200 that was ~100K wasted
/// iterations per check. It also always cloned the string in the
/// no-truncation case (`s.to_string()`) even though callers like
/// `apply_content_truncation` only INSERT the result when truncation
/// happened — so for the 1000-hit × 3-field robot path the clone
/// produced ~MB of allocator pressure on the no-truncation hot path.
///
/// Post-fix: short-circuit char counting at `max_len + 1` (bounded
/// by `max_len`, independent of total string length) AND signal
/// "no truncation needed" via `None` so callers skip the wasted
/// allocation entirely. The truncation path captures the cut byte
/// index during the same walk used for short-circuit counting,
/// turning two passes into one.
pub(crate) fn truncate_content(s: &str, max_len: usize) -> Option<String> {
    let truncate_at = max_len.saturating_sub(3);
    let mut chars = s.char_indices();
    // Initialize cut_byte to 0 so the `truncate_at == 0` case (max_len < 3)
    // produces an empty prefix + "..." instead of accidentally using
    // `s.len()` as the cut. The in-loop assignment below overwrites
    // this when the loop reaches the truncate_at index.
    let mut cut_byte: Option<usize> = if truncate_at == 0 { Some(0) } else { None };
    for n in 0..max_len {
        let (byte_idx, _) = chars.next()?;
        if n == truncate_at {
            cut_byte = Some(byte_idx);
        }
    }
    chars.next()?;
    let cut = cut_byte.unwrap_or(s.len());
    let mut out = String::with_capacity(cut + 3);
    out.push_str(&s[..cut]);
    out.push_str("...");
    Some(out)
}

#[cfg(test)]
mod truncate_content_tests {
    use super::*;

    /// `coding_agent_session_search-nj5eh` regression: pin behavioral
    /// equivalence with the prior `(String, bool)` implementation
    /// across the boundary cases that determine when truncation
    /// fires.
    #[test]
    fn truncate_content_returns_none_when_chars_within_limit() {
        // Empty
        assert_eq!(truncate_content("", 10), None);
        // Strictly less than max_len
        assert_eq!(truncate_content("abc", 10), None);
        // Exactly max_len — boundary: pre-fix `char_count <= max_len`
        // returned (s.to_string(), false); post-fix returns None.
        assert_eq!(truncate_content("abcdefghij", 10), None);
    }

    #[test]
    fn truncate_content_returns_some_when_chars_exceed_limit() {
        // max_len + 1 — first char count beyond limit
        assert_eq!(
            truncate_content("abcdefghijk", 10).as_deref(),
            Some("abcdefg...")
        );
        // Significantly larger than max_len
        assert_eq!(
            truncate_content("abcdefghijklmnopqrstuvwxyz", 10).as_deref(),
            Some("abcdefg...")
        );
    }

    #[test]
    fn truncate_content_handles_max_len_below_ellipsis_budget() {
        // max_len < 3 — saturating_sub(3) yields 0; truncated prefix
        // is empty so output is just "...".
        assert_eq!(truncate_content("abcdef", 0).as_deref(), Some("..."));
        assert_eq!(truncate_content("abcdef", 1).as_deref(), Some("..."));
        assert_eq!(truncate_content("abcdef", 2).as_deref(), Some("..."));
    }

    #[test]
    fn truncate_content_is_utf8_safe_on_multibyte_boundaries() {
        // 4 emojis (each 4 bytes in UTF-8) + ASCII tail; max_len=4
        // means "exactly fits" and returns None.
        let s = "😀😀😀😀x";
        assert_eq!(truncate_content(s, 5), None);
        // max_len=4: 5 chars > 4 → truncate. saturating_sub(3)=1, so
        // cut at the first multibyte boundary (byte 4) and append "..."
        let truncated = truncate_content(s, 4).expect("must truncate when chars > max_len");
        assert_eq!(truncated, "😀...");
    }

    #[test]
    fn truncate_content_short_circuits_on_huge_input() {
        // 100KB ASCII string with max_len=10 — pre-fix walked all
        // 100K chars to count, then 7 more to take. Post-fix walks
        // exactly 11 chars (max_len + 1) before deciding. This test
        // doesn't measure perf but verifies CORRECTNESS on a large
        // input that exposed the original O(N) walk.
        let s = "a".repeat(100_000);
        assert_eq!(truncate_content(&s, 10).as_deref(), Some("aaaaaaa..."));
    }

    /// Pin the post-fix contract that `apply_content_truncation` relies
    /// on: `None` ↔ caller skips clone+insert. Pre-fix, the function
    /// always returned an owned String even in the no-truncation case,
    /// causing wasted allocations on the robot-mode hot path.
    #[test]
    fn truncate_content_avoids_allocation_on_no_truncation_path() {
        // The contract is: return None when no truncation happened.
        // Callers (apply_content_truncation) MUST be able to
        // distinguish via `Option::is_none` without inspecting the
        // string contents.
        assert!(truncate_content("short", 1024).is_none());
    }
}

/// Apply content truncation to a filtered hit JSON object
#[derive(Clone, Copy)]
pub(crate) struct FieldBudgets {
    pub(crate) snippet: Option<usize>,
    pub(crate) content: Option<usize>,
    pub(crate) title: Option<usize>,
    pub(crate) fallback: Option<usize>,
}

impl FieldBudgets {
    #[inline]
    pub(crate) fn has_any_limit(self) -> bool {
        self.snippet.is_some()
            || self.content.is_some()
            || self.title.is_some()
            || self.fallback.is_some()
    }
}

pub(crate) fn apply_content_truncation(
    hit: serde_json::Value,
    budgets: FieldBudgets,
) -> serde_json::Value {
    let serde_json::Value::Object(mut obj) = hit else {
        return hit;
    };

    let fields = [
        ("snippet", budgets.snippet.or(budgets.fallback)),
        ("content", budgets.content.or(budgets.fallback)),
        ("title", budgets.title.or(budgets.fallback)),
    ];

    for (field, budget) in fields {
        if let (Some(limit), Some(serde_json::Value::String(s))) = (budget, obj.get(field))
            && let Some(truncated) = truncate_content(s, limit)
        {
            obj.insert(field.to_string(), serde_json::Value::String(truncated));
            obj.insert(format!("{field}_truncated"), serde_json::Value::Bool(true));
        }
    }

    serde_json::Value::Object(obj)
}

/// Clamp hits to an approximate token budget (4 chars ≈ 1 token). Returns (hits, `est_tokens`, clamped?)
pub(crate) fn clamp_hits_to_budget(
    hits: Vec<serde_json::Value>,
    max_tokens: Option<usize>,
    estimate_tokens: bool,
) -> (Vec<serde_json::Value>, Option<usize>, bool) {
    let input_len = hits.len();
    let Some(tokens) = max_tokens else {
        let est = if estimate_tokens {
            serde_json::to_string(&hits)
                .map(|s| s.chars().count() / 4)
                .ok()
        } else {
            None
        };
        return (hits, est, false);
    };

    let budget_chars = tokens.saturating_mul(4);
    let mut acc_chars = 0usize;
    let mut kept: Vec<serde_json::Value> = Vec::new();
    for hit in hits {
        let len = serde_json::to_string(&hit)
            .map(|s| s.chars().count())
            .unwrap_or(0);
        if !kept.is_empty() && acc_chars + len > budget_chars {
            break;
        }
        acc_chars += len;
        kept.push(hit);
        if acc_chars >= budget_chars {
            break;
        }
    }
    let est = serde_json::to_string(&kept)
        .map(|s| s.chars().count() / 4)
        .ok();
    let clamped = kept.len() < input_len || est.is_some_and(|e| e > tokens);
    (kept, est, clamped)
}

pub(crate) fn robot_format_from_env() -> Option<RobotFormat> {
    dotenvy::var("CASS_OUTPUT_FORMAT")
        .ok()
        .and_then(|val| match val.trim().to_ascii_lowercase().as_str() {
            "json" => Some(RobotFormat::Json),
            "jsonl" => Some(RobotFormat::Jsonl),
            "compact" => Some(RobotFormat::Compact),
            "sessions" => Some(RobotFormat::Sessions),
            "toon" => Some(RobotFormat::Toon),
            _ => None,
        })
        .or_else(|| {
            dotenvy::var("TOON_DEFAULT_FORMAT").ok().and_then(|val| {
                match val.trim().to_ascii_lowercase().as_str() {
                    "toon" => Some(RobotFormat::Toon),
                    "json" => Some(RobotFormat::Json),
                    _ => None,
                }
            })
        })
}

#[derive(Debug, Clone)]
pub(crate) struct SearchModeMeta {
    pub(crate) requested: crate::search::query::SearchMode,
    pub(crate) realized: crate::search::query::SearchMode,
    pub(crate) defaulted: bool,
    pub(crate) fallback_tier: Option<&'static str>,
    pub(crate) fallback_reason: Option<String>,
    pub(crate) quality_tier_refined: bool,
    pub(crate) semantic_work_completed: bool,
    /// GH #441: set when a hybrid search dropped its lexical leg (for example
    /// `query_fuel_exhausted`) and answered from the semantic leg alone.
    pub(crate) lexical_degrade_reason: Option<&'static str>,
    /// 2l1b0.68: why a sparse lexical result did not get the automatic
    /// wildcard retry (`index_over_automatic_limit`, `automatic_retry_disabled`,
    /// `long_query_term`), or kept its own hits because the retry ran out of
    /// query fuel (`query_fuel_exhausted`, 7hvlg).
    pub(crate) wildcard_fallback_skipped: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SemanticBudgetAction {
    Execute,
    StrictSemanticTimeout,
    HybridLexicalFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SemanticBudgetCheckpoint {
    BeforeSetup,
    BeforeSearch,
}

impl SemanticBudgetCheckpoint {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::BeforeSetup => "before-setup",
            Self::BeforeSearch => "before-search",
        }
    }
}

pub(crate) fn semantic_budget_diagnostic_suffix(
    snapshot: BudgetSnapshot,
    checkpoint: SemanticBudgetCheckpoint,
) -> String {
    format!(
        " [semantic_budget checkpoint={} phase={} elapsed_ms={} budget_ms={} remaining_ms={}]",
        checkpoint.as_str(),
        snapshot.phase().as_str(),
        snapshot.elapsed_ms(),
        snapshot.budget_ms(),
        snapshot.remaining_ms()
    )
}

pub(crate) fn semantic_budget_fallback_reason(
    checkpoint: SemanticBudgetCheckpoint,
) -> &'static str {
    match checkpoint {
        SemanticBudgetCheckpoint::BeforeSetup => {
            "semantic refinement skipped because the robot search budget is nearly exhausted"
        }
        SemanticBudgetCheckpoint::BeforeSearch => {
            "semantic refinement skipped after setup because the robot search budget is nearly exhausted"
        }
    }
}

pub(crate) fn strict_semantic_budget_error(
    mode_meta: &SearchModeMeta,
    snapshot: Option<BudgetSnapshot>,
    checkpoint: SemanticBudgetCheckpoint,
) -> CliError {
    if let Some(snapshot) = snapshot {
        tracing::warn!(
            requested_mode = ?mode_meta.requested,
            checkpoint = ?checkpoint,
            phase = ?snapshot.phase(),
            elapsed_ms = snapshot.elapsed_ms(),
            budget_ms = snapshot.budget_ms(),
            remaining_ms = snapshot.remaining_ms(),
            "requested semantic search rejected by robot budget admission"
        );
    }

    let message = match checkpoint {
        SemanticBudgetCheckpoint::BeforeSetup => {
            "Insufficient robot-search budget remaining to start requested semantic search"
        }
        SemanticBudgetCheckpoint::BeforeSearch => {
            "Semantic setup left insufficient robot-search budget to execute requested semantic search"
        }
    };
    let diagnostic_suffix = snapshot
        .map(|snapshot| semantic_budget_diagnostic_suffix(snapshot, checkpoint))
        .unwrap_or_default();
    CliError {
        code: 10,
        kind: CliErrorKind::Timeout.kind_str(),
        // `CliError` has no extensible details object, so the robot error
        // envelope carries the immutable checkpoint fields as stable key=value
        // tokens in `error.message`.
        message: format!("{message}{diagnostic_suffix}"),
        hint: Some(
            "Increase --timeout and retry the same semantic request; use hybrid only when lexical fail-open is acceptable"
                .to_string(),
        ),
        retryable: true,
    }
}

pub(crate) fn semantic_budget_action(
    mode_meta: &SearchModeMeta,
    snapshot: Option<BudgetSnapshot>,
) -> SemanticBudgetAction {
    use crate::robot_budget_envelope::BudgetPhase;
    use crate::search::query::SearchMode;

    if !matches!(
        mode_meta.realized,
        SearchMode::Semantic | SearchMode::Hybrid
    ) {
        return SemanticBudgetAction::Execute;
    }
    if !matches!(
        snapshot.map(BudgetSnapshot::phase),
        Some(BudgetPhase::NearLimit | BudgetPhase::Exhausted)
    ) {
        return SemanticBudgetAction::Execute;
    }
    if mode_meta.fail_open_on_semantic_unavailable() {
        return SemanticBudgetAction::HybridLexicalFallback;
    }
    if matches!(mode_meta.requested, SearchMode::Semantic) {
        return SemanticBudgetAction::StrictSemanticTimeout;
    }
    SemanticBudgetAction::Execute
}

impl SearchModeMeta {
    pub(crate) fn new(requested: crate::search::query::SearchMode, defaulted: bool) -> Self {
        Self {
            requested,
            realized: requested,
            defaulted,
            fallback_tier: None,
            fallback_reason: None,
            quality_tier_refined: true,
            semantic_work_completed: true,
            lexical_degrade_reason: None,
            wildcard_fallback_skipped: None,
        }
    }

    pub(crate) fn semantic_refinement(&self) -> bool {
        self.semantic_work_completed
            && matches!(
                self.realized,
                crate::search::query::SearchMode::Semantic
                    | crate::search::query::SearchMode::Hybrid
            )
            && !self.fell_back_to_lexical()
    }

    pub(crate) fn fail_open_on_semantic_unavailable(&self) -> bool {
        matches!(self.requested, crate::search::query::SearchMode::Hybrid)
    }

    pub(crate) fn fall_back_to_lexical(&mut self, reason: impl Into<String>) {
        self.realized = crate::search::query::SearchMode::Lexical;
        self.fallback_tier = Some("lexical");
        self.fallback_reason = Some(reason.into());
    }

    pub(crate) fn fell_back_to_lexical(&self) -> bool {
        self.fallback_tier == Some("lexical")
    }

    /// The realized refinement level for `--robot-meta` (bead .5.4): a precise
    /// `lexical_only` / `fully_hybrid_refined` instead of a bare bool. A
    /// one-shot CLI search runs to completion, so the quality tier contributes
    /// whenever semantic/hybrid did not fail open to lexical.
    pub(crate) fn realized_refinement(&self) -> crate::search::readiness::SearchRefinementLevel {
        if self.fell_back_to_lexical() {
            crate::search::readiness::SearchRefinementLevel::LexicalOnly
        } else {
            crate::search::search_mode_metadata::refinement_level(
                self.realized,
                self.quality_tier_refined,
            )
        }
    }

    /// The typed semantic fallback reason for `--robot-meta` (bead .5.4),
    /// classifying the free-form fallback string into a code agents can branch
    /// on. `None` when no fail-open happened.
    pub(crate) fn semantic_fallback_reason(
        &self,
    ) -> Option<crate::search::search_mode_metadata::SemanticFallbackReason> {
        self.fallback_reason
            .as_deref()
            .map(crate::search::search_mode_metadata::classify_fallback_reason)
    }
}

pub(crate) fn search_mode_label(mode: crate::search::query::SearchMode) -> &'static str {
    match mode {
        crate::search::query::SearchMode::Lexical => "lexical",
        crate::search::query::SearchMode::Semantic => "semantic",
        crate::search::query::SearchMode::Hybrid => "hybrid",
    }
}

pub(crate) fn ann_fallback_human_warning(
    reason: crate::search::vector_index::SemanticAnnUnavailableReason,
) -> String {
    let action = match reason {
        crate::search::vector_index::SemanticAnnUnavailableReason::SessionScopeRequiresExact => {
            "The session filter requires exact candidate selection. No index repair is needed."
        }
        crate::search::vector_index::SemanticAnnUnavailableReason::MultipleExactShards => {
            "Exact multi-shard results remain available; inspect `cass health --json` before enabling sharded ANN acceleration."
        }
        crate::search::vector_index::SemanticAnnUnavailableReason::SidecarMissing
        | crate::search::vector_index::SemanticAnnUnavailableReason::SidecarOpenFailed
        | crate::search::vector_index::SemanticAnnUnavailableReason::SidecarNotNative => {
            "Run `cass index --semantic --build-hnsw` to rebuild ANN acceleration."
        }
    };
    format!(
        "Approximate ANN unavailable ({}); CASS used exact semantic search for this request. {action}",
        reason.code()
    )
}

#[cfg(test)]
mod ann_fallback_human_warning_tests {
    use super::ann_fallback_human_warning;
    use crate::search::vector_index::SemanticAnnUnavailableReason;

    #[test]
    fn warning_uses_the_same_stable_reason_code_as_robot_metadata() {
        for reason in [
            SemanticAnnUnavailableReason::SessionScopeRequiresExact,
            SemanticAnnUnavailableReason::MultipleExactShards,
            SemanticAnnUnavailableReason::SidecarMissing,
            SemanticAnnUnavailableReason::SidecarOpenFailed,
            SemanticAnnUnavailableReason::SidecarNotNative,
        ] {
            let warning = ann_fallback_human_warning(reason);
            assert!(
                warning.contains(reason.code()),
                "human warning must expose the robot metadata code: {warning}"
            );
            assert!(
                warning.contains("exact semantic search"),
                "human warning must name the realized fallback: {warning}"
            );
        }
    }

    #[test]
    fn session_scope_fallback_preserves_filter_without_rebuild_advice() {
        let warning =
            ann_fallback_human_warning(SemanticAnnUnavailableReason::SessionScopeRequiresExact);
        assert!(warning.contains("session_scope_requires_exact"));
        assert!(warning.contains("exact semantic search"));
        assert!(warning.contains("session filter"));
        assert!(warning.contains("No index repair is needed"));
        assert!(!warning.contains("build-hnsw"));
        assert!(!warning.contains("remove"));
    }

    #[test]
    fn warning_recommends_rebuild_only_for_unsharded_sidecar_failures() {
        let missing = ann_fallback_human_warning(SemanticAnnUnavailableReason::SidecarMissing);
        assert!(missing.contains("cass index --semantic --build-hnsw"));

        let sharded = ann_fallback_human_warning(SemanticAnnUnavailableReason::MultipleExactShards);
        assert!(sharded.contains("cass health --json"));
        assert!(!sharded.contains("build-hnsw"));
    }
}

pub(crate) fn toon_encode_options_from_env() -> toon::EncodeOptions {
    let indent = match dotenvy::var("TOON_INDENT") {
        Ok(v) if !v.trim().is_empty() => match v.parse::<usize>() {
            Ok(n) => Some(n),
            Err(e) => {
                warn!("invalid TOON_INDENT={v}: {e} (ignoring)");
                None
            }
        },
        _ => None,
    };

    let key_folding = match dotenvy::var("TOON_KEY_FOLDING") {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "" | "off" | "0" | "false" => Some(toon::options::KeyFoldingMode::Off),
            "safe" => Some(toon::options::KeyFoldingMode::Safe),
            other => {
                warn!("invalid TOON_KEY_FOLDING={other} (expected off|safe); ignoring");
                None
            }
        },
        Err(_) => None,
    };

    toon::EncodeOptions {
        indent,
        delimiter: None,
        key_folding,
        flatten_depth: None,
        replacer: None,
    }
}

pub(crate) fn encode_structured_value(
    payload: serde_json::Value,
    format: RobotFormat,
) -> CliResult<String> {
    match format {
        RobotFormat::Json => serde_json::to_string_pretty(&payload),
        RobotFormat::Jsonl | RobotFormat::Compact | RobotFormat::Sessions => {
            serde_json::to_string(&payload)
        }
        RobotFormat::Toon => {
            return Ok(toon::encode(payload, Some(toon_encode_options_from_env())));
        }
    }
    .map_err(|error| CliError {
        code: 9,
        kind: CliErrorKind::EncodeJson.kind_str(),
        message: format!("failed to encode structured output: {error}"),
        hint: None,
        retryable: false,
    })
}

pub(crate) fn output_structured_value(
    payload: serde_json::Value,
    format: RobotFormat,
) -> CliResult<()> {
    let output_error = |action: &str, error: &dyn std::fmt::Display| CliError {
        code: 9,
        kind: CliErrorKind::EncodeJson.kind_str(),
        message: format!("failed to {action} structured output: {error}"),
        hint: None,
        retryable: false,
    };
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());

    match format {
        RobotFormat::Json => {
            serde_json::to_writer_pretty(&mut out, &payload)
                .map_err(|error| output_error("encode JSON", &error))?;
            writeln!(&mut out).map_err(|error| output_error("write JSON newline", &error))?;
        }
        RobotFormat::Jsonl | RobotFormat::Compact | RobotFormat::Sessions => {
            serde_json::to_writer(&mut out, &payload)
                .map_err(|error| output_error("encode compact JSON", &error))?;
            writeln!(&mut out)
                .map_err(|error| output_error("write compact JSON newline", &error))?;
        }
        RobotFormat::Toon => {
            let toon_str = toon::encode(payload, Some(toon_encode_options_from_env()));
            out.write_all(toon_str.as_bytes())
                .map_err(|error| output_error("write TOON", &error))?;
        }
    }

    out.flush().map_err(|error| output_error("flush", &error))?;
    Ok(())
}

pub(crate) fn encode_search_cursor(offset: usize, limit: usize) -> Option<String> {
    if limit == 0 {
        return None;
    }
    let payload = serde_json::json!({
        "offset": offset,
        "limit": limit,
    })
    .to_string();
    Some(BASE64_STANDARD.encode(payload))
}

pub(crate) struct SearchCursorManifestInput<'a> {
    pub(crate) requested_limit: usize,
    pub(crate) realized_limit: usize,
    pub(crate) offset: usize,
    pub(crate) returned_count: usize,
    pub(crate) search_page_count: usize,
    pub(crate) total_matches: usize,
    pub(crate) total_matches_exact: bool,
    pub(crate) has_more: bool,
    pub(crate) input_cursor_present: bool,
    pub(crate) next_cursor_present: bool,
    pub(crate) hits_clamped: bool,
    pub(crate) tokens_estimated: Option<usize>,
    pub(crate) max_tokens: Option<usize>,
    pub(crate) timed_out: bool,
    pub(crate) output_projection: &'a str,
    pub(crate) requested_fields: &'a Option<Vec<String>>,
    pub(crate) resolved_fields: &'a Option<Vec<String>>,
    pub(crate) search_mode_meta: &'a SearchModeMeta,
    pub(crate) cache_stats: &'a crate::search::query::CacheStats,
    pub(crate) index_freshness: Option<&'a serde_json::Value>,
}

pub(crate) fn search_cursor_manifest_json(
    input: SearchCursorManifestInput<'_>,
) -> serde_json::Value {
    let index_rebuilding = input
        .index_freshness
        .and_then(|freshness| freshness.get("rebuilding"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let continuation_safe = input.next_cursor_present && !input.timed_out && !index_rebuilding;
    let continuation_reason = if !input.has_more {
        "result window exhausted; no continuation cursor was emitted"
    } else if !input.next_cursor_present {
        "more results may exist, but no continuation cursor could be emitted for this response"
    } else if input.timed_out {
        "partial timeout results are best-effort; rerun before continuing for deterministic pagination"
    } else if index_rebuilding {
        "lexical index is rebuilding; cursor is best-effort until the generation is stable"
    } else if input.hits_clamped {
        "cursor offset advances by emitted hits after token-budget clamping"
    } else {
        "cursor offset advances by emitted hits; reuse the same query, filters, mode, and fields"
    };
    let count_precision = if input.total_matches_exact {
        "exact"
    } else {
        "lower_bound"
    };
    let count_reason = if input.total_matches_exact {
        "total_matches is exact; no extra recount was needed"
    } else {
        "total_matches is a lower bound from the current result window; no expensive recount was requested"
    };
    let resolved_fields = match input.resolved_fields {
        None => serde_json::json!("all"),
        Some(fields) if fields.is_empty() => serde_json::json!("all"),
        Some(fields) => serde_json::json!(fields),
    };
    let requested_fields = match input.requested_fields {
        None => serde_json::Value::Null,
        Some(fields) => serde_json::json!(fields),
    };
    let index_freshness = input
        .index_freshness
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let lexical_shard_generation = input
        .cache_stats
        .reader_generation
        .map(|generation| generation.to_string());
    let reload_ms_total = input.cache_stats.reload_ms_total.min(u128::from(u64::MAX)) as u64;

    serde_json::json!({
        "schema_version": 1,
        "has_more": input.has_more,
        "count_precision": count_precision,
        "count_reason": count_reason,
        "continuation_safe": continuation_safe,
        "continuation_reason": continuation_reason,
        "input_cursor_present": input.input_cursor_present,
        "next_cursor_present": input.next_cursor_present,
        "offset": input.offset,
        "next_offset": if input.next_cursor_present {
            serde_json::json!(input.offset.saturating_add(input.returned_count))
        } else {
            serde_json::Value::Null
        },
        "requested_limit": input.requested_limit,
        "realized_limit": input.realized_limit,
        "returned_count": input.returned_count,
        "search_page_count": input.search_page_count,
        "total_matches": input.total_matches,
        "field_mask": {
            "requested": requested_fields,
            "resolved": resolved_fields,
            "projection": input.output_projection,
        },
        "token_budget": {
            "max_tokens": input.max_tokens,
            "tokens_estimated": input.tokens_estimated,
            "hits_clamped": input.hits_clamped,
        },
        "cache_generation": {
            "reader_generation": input.cache_stats.reader_generation,
            "reloads": input.cache_stats.reloads,
            "reload_ms_total": reload_ms_total,
            "policy": input.cache_stats.eviction_policy,
            "ghost_entries": input.cache_stats.ghost_entries,
            "admission_rejects": input.cache_stats.admission_rejects,
        },
        "index_generation": {
            "lexical_shard_generation": lexical_shard_generation,
            "freshness": index_freshness,
            "stale": input.index_freshness.and_then(|f| f.get("stale")).cloned().unwrap_or(serde_json::Value::Null),
            // [coding_agent_session_search #301] surface the incomplete-index
            // signal so robot callers can tell an aborted/partial full index
            // apart from a merely stale one. `partial=true` means results
            // omit conversations indexed after a `cass index --full` abort.
            "partial": input.index_freshness.and_then(|f| f.get("partial")).cloned().unwrap_or(serde_json::Value::Null),
            "partial_reason": input.index_freshness.and_then(|f| f.get("partial_reason")).cloned().unwrap_or(serde_json::Value::Null),
            "rebuilding": index_rebuilding,
            "pending_sessions": input.index_freshness.and_then(|f| f.get("pending_sessions")).cloned().unwrap_or(serde_json::Value::Null),
        },
        "semantic_fallback": {
            "requested_search_mode": search_mode_label(input.search_mode_meta.requested),
            "realized_search_mode": search_mode_label(input.search_mode_meta.realized),
            "fallback_tier": input.search_mode_meta.fallback_tier,
            "fallback_reason": input.search_mode_meta.fallback_reason.clone(),
            "semantic_refinement": input.search_mode_meta.semantic_refinement(),
        },
    })
}

/// Map the realized [`SearchMode`] to the trust [`RealizedMode`] vocabulary.
pub(crate) fn trust_realized_mode(
    mode: crate::search::query::SearchMode,
) -> crate::search::trust_scoring::RealizedMode {
    use crate::search::query::SearchMode;
    use crate::search::trust_scoring::RealizedMode;
    match mode {
        SearchMode::Lexical => RealizedMode::Lexical,
        SearchMode::Semantic => RealizedMode::Semantic,
        SearchMode::Hybrid => RealizedMode::Hybrid,
    }
}

/// Map a hit's origin to the trust source-kind with a cheap fs/archive probe
/// (q4pau). A remote hit is backed by a reachable mirror copy. A local hit whose
/// recorded `source_path` no longer exists on disk is archive-only — the source
/// file was deleted, moved, or pruned and only the DB row survives — so it scores
/// `ArchiveOnly` (source-unhealthy) instead of overtrusting a dead path.
pub(crate) fn trust_source_kind_for_hit(
    hit: &crate::search::query::SearchHit,
) -> crate::search::trust_scoring::SourceTrustKind {
    use crate::search::trust_scoring::SourceTrustKind;
    if !normalized_robot_hit_origin_kind(hit)
        .eq_ignore_ascii_case(crate::sources::provenance::LOCAL_SOURCE_ID)
    {
        return SourceTrustKind::RemoteMirror;
    }
    let path = hit.source_path.trim();
    if !path.is_empty() && std::path::Path::new(path).exists() {
        SourceTrustKind::LocalPresent
    } else {
        SourceTrustKind::ArchiveOnly
    }
}

/// Build the metadata-only trust verdict JSON for one search hit (beads
/// 5u82n.3 + q4pau). Advisory only — it never affects result ordering.
///
/// Recency, source health (fs probe), and realized mode always contribute.
/// `query_workspace` (the project the agent is in now) drives a cwd-relative
/// workspace match. For on-project hits, `correlation` links the result to a
/// commit in the local project's git history when the hit's own indexed text
/// references a known commit id — so the verdict reaches likely/proof-debt
/// instead of only unverified/stale. Off-project hits are never correlated, so a
/// cross-project conversation cannot inherit this project's trust.
pub(crate) fn trust_value_for_hit(
    hit: &crate::search::query::SearchHit,
    now_ms: i64,
    realized_mode: crate::search::trust_scoring::RealizedMode,
    correlation: &crate::search::trust_correlation::CorrelationIndex,
    query_workspace: Option<&str>,
) -> serde_json::Value {
    use crate::search::trust_correlation::{correlate, proof_for, scan_text, workspace_matches};
    use crate::search::trust_scoring::{HitTrustContext, assess_trust, derive_trust_signals};
    let workspace = {
        let ws = hit.workspace.trim();
        (!ws.is_empty()).then(|| ws.to_string())
    };
    // cwd-relative workspace match: None (no cwd anchor) => no penalty and no
    // correlation; Some(false) => off-project; Some(true) => on-project.
    let on_project = match (query_workspace, workspace.as_deref()) {
        (None, _) => None,
        (Some(q), Some(w)) => Some(workspace_matches(q, w)),
        (Some(_), None) => Some(false),
    };
    let mut ctx = HitTrustContext {
        created_at_ms: hit.created_at,
        now_ms,
        workspace,
        // Workspace match is applied below via the cwd-relative policy, so the
        // pure derivation is left without a filter to avoid double-penalizing.
        query_workspace: None,
        source_kind: trust_source_kind_for_hit(hit),
        realized_mode,
        ..HitTrustContext::default()
    };
    if matches!(on_project, Some(true)) {
        let scan = scan_text(&[&hit.title, &hit.snippet, &hit.content]);
        let link = correlate(correlation, &scan);
        if !link.is_empty() {
            ctx.outcome = link.outcome;
            ctx.linked_commit = link.linked_commit;
            ctx.proof = proof_for(ctx.outcome, ctx.linked_commit.is_some());
        }
    }
    let mut signals = derive_trust_signals(&ctx);
    signals.workspace_match = on_project.unwrap_or(true);
    serde_json::to_value(assess_trust(&signals)).unwrap_or(serde_json::Value::Null)
}

/// GH#414: resolution of a `--sessions-from` filter against the index.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SessionsFilterStats {
    /// Distinct paths the filter file supplied.
    pub(crate) requested: usize,
    /// How many of them the index has actually seen; `None` = undetermined
    /// (DB busy/missing or resolution deadline hit), which must never be
    /// rendered as zero.
    pub(crate) matched: Option<usize>,
}

impl SessionsFilterStats {
    /// True only when the filter PROVABLY selected no indexed session — the
    /// case where an empty result says nothing about the query.
    pub(crate) fn provably_unmatched(self) -> bool {
        self.matched == Some(0) && self.requested > 0
    }
}

/// `_meta.effective` (2l1b0.68): what this search actually ran, so a caller
/// can check the interpretation instead of trusting it. It names the
/// database and what chose its path, the resolved time window with the flag
/// behind each bound, the filters as parsed, every argv auto-correction, and
/// how the lexical engine groups the query, with any parentheses it recovered
/// (the reading `--explain` shows; cass's parse matches the engine's since
/// 2l1b0.52).
/// `pack` runs the same search and echoes the same object, without the
/// search-only daemon policy (`semantic_opts: None`).
#[allow(clippy::too_many_arguments)]
pub(crate) fn search_effective_interpretation(
    command: &str,
    query: &str,
    db_path: &Path,
    db_path_source: &str,
    time_filter: &TimeFilter,
    filters: &crate::search::query::SearchFilters,
    sessions_from_paths: Option<usize>,
    interpretation: &InvocationInterpretation,
    semantic_opts: Option<&SemanticSearchOptions>,
) -> serde_json::Value {
    let mut agents: Vec<&str> = filters.agents.iter().map(String::as_str).collect();
    agents.sort_unstable();
    let mut workspaces: Vec<&str> = filters.workspaces.iter().map(String::as_str).collect();
    workspaces.sort_unstable();
    let reading = crate::search::query::read_query(query);
    let mut effective = serde_json::json!({
        "command": command,
        "query": query,
        "query_structure": reading.structure,
        "query_recoveries": reading.recoveries,
        "db_path": db_path.display().to_string(),
        "db_path_source": db_path_source,
        "time_window": {
            "since_ms": time_filter.since,
            "since_from": time_filter.since_from,
            "until_ms": time_filter.until,
            "until_from": time_filter.until_from,
        },
        "filters": {
            "agents": agents,
            "workspaces": workspaces,
            "source": filters.source_filter.to_string(),
            "sessions_from_paths": sessions_from_paths,
        },
        "auto_corrections": interpretation.corrections,
    });
    // `--daemon` asks permission to spawn the warm-model daemon. A robot
    // search is budgeted and never spawns one; it still uses a daemon that is
    // already running unless `--no-daemon` was given (2l1b0.68).
    if let Some(semantic_opts) = semantic_opts
        && let Some(object) = effective.as_object_mut()
    {
        object.insert(
            "daemon".to_string(),
            serde_json::json!({
                "use_existing": semantic_opts.use_daemon,
                "auto_spawn_requested": semantic_opts.auto_spawn_daemon,
                "auto_spawn": false,
            }),
        );
    }
    effective
}

/// Resolve user-facing agent names to the slugs stored in the archive.
/// Validate against the connector registry, not this machine's installed agents,
/// so a valid filter stays valid for remote archives and empty local datasets.
pub(crate) fn normalize_agent_filters(agents: &[String]) -> CliResult<Vec<String>> {
    if agents.is_empty() {
        return Ok(Vec::new());
    }
    let mut known = capabilities_connector_names();
    // Timeline and imported legacy data use this identity for missing agents.
    known.push("unknown".to_string());
    known.sort_unstable();
    known.dedup();

    let mut resolved = Vec::with_capacity(agents.len());
    for raw in agents {
        let key = crate::sources::config::agent_name_key(raw);
        let candidate = match key.as_str() {
            "claude" => "claude_code",
            "oh_my_pi" | "ohmypi" => "omp",
            "open_claw" => "openclaw",
            other => other,
        };
        let Some(agent) = known
            .iter()
            .find(|slug| slug.replace('-', "_") == candidate)
        else {
            let suggestion = known
                .iter()
                .map(|slug| (slug, strsim::levenshtein(candidate, slug)))
                .filter(|(_, distance)| !candidate.is_empty() && *distance <= 3)
                .min_by_key(|(_, distance)| *distance)
                .map(|(slug, _)| format!("Did you mean '{slug}'? "))
                .unwrap_or_default();
            return Err(CliError {
                code: 2,
                kind: CliErrorKind::UnknownAgent.kind_str(),
                message: format!("unknown agent filter {raw:?}"),
                hint: Some(format!(
                    "{suggestion}Known agents: {}. Run 'cass capabilities --json' to list connectors.",
                    known.join(", ")
                )),
                retryable: false,
            });
        };
        if !resolved.contains(agent) {
            resolved.push(agent.clone());
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod agent_filter_tests {
    use super::*;

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn agent_filter_aliases_resolve_to_archive_slugs() {
        let filters = names(&[
            " Claude-Code ",
            "claude",
            "Claude_Code",
            "CODEX",
            "Devin",
            "Qoder",
            "UNKNOWN",
        ]);
        assert_eq!(
            normalize_agent_filters(&filters).unwrap(),
            names(&["claude_code", "codex", "devin", "qoder", "unknown"])
        );
        assert!(normalize_agent_filters(&[]).unwrap().is_empty());
    }

    #[test]
    fn agent_filter_every_advertised_connector_round_trips() {
        for slug in capabilities_connector_names() {
            let spellings = vec![slug.clone(), slug.to_ascii_uppercase().replace('_', "-")];
            assert_eq!(normalize_agent_filters(&spellings).unwrap(), vec![slug]);
        }
    }

    #[test]
    fn agent_filter_unknown_values_fail_without_broadening_the_scope() {
        for filters in [
            names(&[""]),
            names(&["  "]),
            names(&["claud_code"]),
            names(&["claude_code", "claud_code"]),
            names(&["claud_code", "codex"]),
            names(&["未登録"]),
        ] {
            let error = normalize_agent_filters(&filters).unwrap_err();
            assert_eq!(error.code, 2);
            assert_eq!(error.kind, CliErrorKind::UnknownAgent.kind_str());
            assert!(!error.retryable);
            assert!(error.hint.as_deref().unwrap().contains("Known agents:"));
        }
        let error = normalize_agent_filters(&names(&["claud_code"])).unwrap_err();
        assert!(error.hint.as_deref().unwrap().contains("claude_code"));
    }
}

/// Which input chose the canonical database path, for `_meta.effective`.
pub(crate) fn effective_db_path_source(
    db_override: bool,
    data_dir_override: bool,
    interpretation: &InvocationInterpretation,
) -> &'static str {
    if db_override {
        if interpretation.db_from_env {
            "env:CASS_DB_PATH"
        } else {
            "--db"
        }
    } else if data_dir_override {
        "--data-dir"
    } else {
        default_data_dir_with_source().1
    }
}
