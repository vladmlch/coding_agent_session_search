//! The persisted per-session fact/metrics bag every owned lane emits
//! verbatim (Task 7).
//!
//! `viewer_session_facts` + `viewer_session_metrics` already carry the
//! measured session shape at ingest; the wire contract delivers it as ONE
//! `metrics` object on each emitted session item (list page, detail
//! summary, dashboard recent slice). The bag is `null` when no facts row
//! was ever persisted — a missing measurement stays NULL/unknown, never a
//! fabricated zero; a persisted zero is a KNOWN zero and serializes as 0.

use serde_json::{Value, json};

use super::protocol::PREVIEW_BYTES;

/// Classification coverage reports only the relationship (subagent)
/// evidence: a persisted verdict is classified no matter what unrelated
/// scopes (tools, overlays, metrics) report in the overall
/// `coverage_state`; an absent verdict stays unavailable. This matches
/// the `is_subagent` scope the extractor publishes in the coverage
/// manifest (complete iff the verdict is known). Callers handle the
/// no-facts-row case themselves (list/recent: NULL, detail: pending).
pub(crate) fn classification_coverage(is_subagent: Option<i64>) -> &'static str {
    if is_subagent.is_some() {
        "complete"
    } else {
        "unavailable"
    }
}

/// Bound a free-text preview on a UTF-8 char boundary. Returns
/// (emitted, truncated, original_bytes): a value within PREVIEW_BYTES is
/// kept whole (truncated=false, original_bytes=None - the preview IS the
/// full text); a longer one is cut at the largest char boundary <=
/// PREVIEW_BYTES (truncated=true, original_bytes=Some(source length));
/// None stays unknown. The byte fact describes the truncation event, so
/// a non-truncated row stays byte-identical across backends that serve
/// whole texts (the parity invariant).
pub(crate) fn bound_text_preview(text: Option<String>) -> (Option<String>, bool, Option<i64>) {
    let Some(text) = text else {
        return (None, false, None);
    };
    let original = text.len() as i64;
    if original <= PREVIEW_BYTES {
        return (Some(text), false, None);
    }
    let mut end = PREVIEW_BYTES as usize;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    (Some(text[..end].to_owned()), true, Some(original))
}

/// Bound a JSON-backed preview cell. JSON cannot be cut mid-document, so
/// a raw cell within PREVIEW_BYTES keeps its decoded value (an invalid
/// payload still decodes to None, unknown) with no truncation fact; a
/// longer raw cell reports the value unknown with truncated=true and the
/// source length. Returns (emitted, truncated, original_bytes) like
/// bound_text_preview.
pub(crate) fn bound_json_preview<T>(
    raw: Option<String>,
    decoded: Option<T>,
) -> (Option<T>, bool, Option<i64>) {
    let Some(raw) = raw else {
        return (None, false, None);
    };
    let original = raw.len() as i64;
    if original <= PREVIEW_BYTES {
        (decoded, false, None)
    } else {
        (None, true, Some(original))
    }
}

/// Every measured field of one session's persisted facts+metrics rows.
/// `Option` semantics are exact: `None` = unmeasured (emitted as JSON
/// null inside the bag), `Some(0)` = a measured zero.
#[derive(Clone, Debug, Default)]
pub(crate) struct SessionFactBag {
    // ---- viewer_session_facts ------------------------------------------
    pub turn_count: Option<i64>,
    pub step_count: Option<i64>,
    pub thinking_block_count: Option<i64>,
    pub artifact_count: Option<i64>,
    pub subagent_count: Option<i64>,
    pub tool_error_count: Option<i64>,
    pub tool_calls_with_exit_status: Option<i64>,
    pub file_fact_count: Option<i64>,
    pub compaction_count: Option<i64>,
    /// Provider-extracted model set (`model_set_json` decoded).
    pub model_set: Option<Vec<String>>,
    pub model_set_truncated: bool,
    pub model_set_bytes: Option<i64>,
    pub first_prompt: Option<String>,
    pub first_prompt_truncated: bool,
    pub first_prompt_bytes: Option<i64>,
    // ---- viewer_session_metrics ----------------------------------------
    pub ttfe_seconds: Option<f64>,
    pub one_shot_rate: Option<f64>,
    pub retries_count: Option<i64>,
    pub self_corrections_count: Option<i64>,
    pub user_corrections_count: Option<i64>,
    /// `tools_used_json` decoded.
    pub tools_used: Option<Vec<String>>,
    pub tools_used_truncated: bool,
    pub tools_used_bytes: Option<i64>,
    pub task_breakdown: Option<Value>,
    pub task_breakdown_truncated: bool,
    pub task_breakdown_bytes: Option<i64>,
    pub health_score: Option<i64>,
    pub health_grade: Option<String>,
    pub health_issues_count: Option<i64>,
    pub potential_waste_tokens: Option<i64>,
    pub potential_savings_usd: Option<f64>,
}

impl SessionFactBag {
    /// `null` when the facts row was never persisted; otherwise the full
    /// bag — metrics-row fields stay NULL when that row is absent or the
    /// column was unmeasured.
    pub(crate) fn emit(&self, facts_present: bool) -> Value {
        if !facts_present {
            return Value::Null;
        }
        json!({
            "turn_count": self.turn_count,
            "step_count": self.step_count,
            "thinking_block_count": self.thinking_block_count,
            "artifact_count": self.artifact_count,
            "subagent_count": self.subagent_count,
            "tool_error_count": self.tool_error_count,
            "tool_calls_with_exit_status": self.tool_calls_with_exit_status,
            "file_fact_count": self.file_fact_count,
            "compaction_count": self.compaction_count,
            "model_set": self.model_set,
            "model_set_truncated": self.model_set_truncated,
            "model_set_bytes": self.model_set_bytes,
            "first_prompt": self.first_prompt,
            "first_prompt_truncated": self.first_prompt_truncated,
            "first_prompt_bytes": self.first_prompt_bytes,
            "ttfe_seconds": self.ttfe_seconds,
            "one_shot_rate": self.one_shot_rate,
            "retries_count": self.retries_count,
            "self_corrections_count": self.self_corrections_count,
            "user_corrections_count": self.user_corrections_count,
            "tools_used": self.tools_used,
            "tools_used_truncated": self.tools_used_truncated,
            "tools_used_bytes": self.tools_used_bytes,
            "task_breakdown": self.task_breakdown,
            "task_breakdown_truncated": self.task_breakdown_truncated,
            "task_breakdown_bytes": self.task_breakdown_bytes,
            "health_score": self.health_score,
            "health_grade": self.health_grade,
            "health_issues_count": self.health_issues_count,
            "potential_waste_tokens": self.potential_waste_tokens,
            "potential_savings_usd": self.potential_savings_usd,
        })
    }

    /// EMITTED byte size of the bounded preview fields (first_prompt,
    /// model_set, tools_used, task_breakdown) - what the row byte profile
    /// counts for them. A truncated JSON preview emits NULL, so it
    /// contributes zero; a kept preview contributes its re-serialized
    /// (emitted) size.
    pub(crate) fn preview_bytes(&self) -> i64 {
        let text = self
            .first_prompt
            .as_deref()
            .map(|t| t.len() as i64)
            .unwrap_or(0);
        let encoded = |bytes: Option<Vec<u8>>| bytes.map(|b| b.len() as i64).unwrap_or(0);
        text + encoded(
            self.model_set
                .as_ref()
                .and_then(|v| serde_json::to_vec(v).ok()),
        ) + encoded(
            self.tools_used
                .as_ref()
                .and_then(|v| serde_json::to_vec(v).ok()),
        ) + encoded(
            self.task_breakdown
                .as_ref()
                .and_then(|v| serde_json::to_vec(v).ok()),
        )
    }

    /// Overlay the `viewer_session_metrics` half onto a facts-half bag —
    /// the detail lane merges its two primary-key reads through this;
    /// the list/recent lanes merge the same fields inside their join
    /// decode. Only the metrics-table fields move; facts fields are
    /// untouched.
    pub(crate) fn merge_metrics(&mut self, metrics: &SessionFactBag) {
        self.ttfe_seconds = metrics.ttfe_seconds;
        self.one_shot_rate = metrics.one_shot_rate;
        self.retries_count = metrics.retries_count;
        self.self_corrections_count = metrics.self_corrections_count;
        self.user_corrections_count = metrics.user_corrections_count;
        self.tools_used = metrics.tools_used.clone();
        self.tools_used_truncated = metrics.tools_used_truncated;
        self.tools_used_bytes = metrics.tools_used_bytes;
        self.task_breakdown = metrics.task_breakdown.clone();
        self.task_breakdown_truncated = metrics.task_breakdown_truncated;
        self.task_breakdown_bytes = metrics.task_breakdown_bytes;
        self.health_score = metrics.health_score;
        self.health_grade = metrics.health_grade.clone();
        self.health_issues_count = metrics.health_issues_count;
        self.potential_waste_tokens = metrics.potential_waste_tokens;
        self.potential_savings_usd = metrics.potential_savings_usd;
    }
}

/// Decode a persisted string-list JSON cell (`model_set_json`,
/// `tools_used_json`): invalid payloads stay unknown, never a page error.
pub(crate) fn decode_string_list(text: Option<&str>) -> Option<Vec<String>> {
    let text = text?;
    serde_json::from_str::<Vec<String>>(text).ok()
}
