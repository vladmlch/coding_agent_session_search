pub(crate) fn sparse_threshold_for_visible_limit(
    sparse_threshold: usize,
    visible_limit: usize,
    has_aggregation: bool,
) -> usize {
    if has_aggregation || visible_limit == 0 {
        sparse_threshold
    } else {
        sparse_threshold.min(visible_limit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SearchLexicalSelfHeal {
    pub(crate) action: &'static str,
    pub(crate) reason: Option<String>,
    pub(crate) indexed_docs: Option<usize>,
}

impl SearchLexicalSelfHeal {
    pub(crate) fn skipped() -> Self {
        Self {
            action: "skipped",
            reason: None,
            indexed_docs: None,
        }
    }
}

/// Whether a search request may bypass the pre-query lexical self-heal.
///
/// Only an *explicit* `--mode semantic` qualifies: it is served from the
/// vector/HNSW assets and never fails open to lexical, so a stale lexical
/// checkpoint is irrelevant to its answer. The default (hybrid) and lexical
/// requests must keep the repair path because lexical is their required tier.
pub(crate) fn search_request_skips_lexical_self_heal(
    requested_mode: Option<crate::search::query::SearchMode>,
) -> bool {
    matches!(
        requested_mode,
        Some(crate::search::query::SearchMode::Semantic)
    )
}
