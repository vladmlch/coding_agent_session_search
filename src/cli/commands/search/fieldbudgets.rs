use crate::cli::args_sub::RobotFormat;
use crate::cli::commands::search::safe_robot_score_value::truncate_content;
use crate::cli::error::CliError;
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
use crate::robot_budget_envelope::BudgetSnapshot;

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
