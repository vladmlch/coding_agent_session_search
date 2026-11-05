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
    pub first_prompt: Option<String>,
    // ---- viewer_session_metrics ----------------------------------------
    pub ttfe_seconds: Option<f64>,
    pub one_shot_rate: Option<f64>,
    pub retries_count: Option<i64>,
    pub self_corrections_count: Option<i64>,
    pub user_corrections_count: Option<i64>,
    /// `tools_used_json` decoded.
    pub tools_used: Option<Vec<String>>,
    pub task_breakdown: Option<Value>,
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
            "first_prompt": self.first_prompt,
            "ttfe_seconds": self.ttfe_seconds,
            "one_shot_rate": self.one_shot_rate,
            "retries_count": self.retries_count,
            "self_corrections_count": self.self_corrections_count,
            "user_corrections_count": self.user_corrections_count,
            "tools_used": self.tools_used,
            "task_breakdown": self.task_breakdown,
            "health_score": self.health_score,
            "health_grade": self.health_grade,
            "health_issues_count": self.health_issues_count,
            "potential_waste_tokens": self.potential_waste_tokens,
            "potential_savings_usd": self.potential_savings_usd,
        })
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
        self.task_breakdown = metrics.task_breakdown.clone();
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
