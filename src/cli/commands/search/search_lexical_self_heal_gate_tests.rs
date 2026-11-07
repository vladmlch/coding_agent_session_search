use crate::cli::commands::search::sparse_threshold_for_visible_limit::search_request_skips_lexical_self_heal;
use crate::search::query::SearchMode;

#[test]
pub(crate) fn explicit_semantic_skips_lexical_self_heal() {
    assert!(search_request_skips_lexical_self_heal(Some(
        SearchMode::Semantic
    )));
}

#[test]
pub(crate) fn lexical_hybrid_and_default_keep_lexical_self_heal() {
    assert!(!search_request_skips_lexical_self_heal(Some(
        SearchMode::Lexical
    )));
    assert!(!search_request_skips_lexical_self_heal(Some(
        SearchMode::Hybrid
    )));
    assert!(
        !search_request_skips_lexical_self_heal(None),
        "the default request resolves to hybrid and must still repair lexical"
    );
}
