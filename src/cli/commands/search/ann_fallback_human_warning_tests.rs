use crate::cli::commands::search::fieldbudgets::ann_fallback_human_warning;
use crate::search::vector_index::SemanticAnnUnavailableReason;

#[test]
pub(crate) fn warning_uses_the_same_stable_reason_code_as_robot_metadata() {
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
pub(crate) fn session_scope_fallback_preserves_filter_without_rebuild_advice() {
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
pub(crate) fn warning_recommends_rebuild_only_for_unsharded_sidecar_failures() {
    let missing = ann_fallback_human_warning(SemanticAnnUnavailableReason::SidecarMissing);
    assert!(missing.contains("cass index --semantic --build-hnsw"));

    let sharded = ann_fallback_human_warning(SemanticAnnUnavailableReason::MultipleExactShards);
    assert!(sharded.contains("cass health --json"));
    assert!(!sharded.contains("build-hnsw"));
}
