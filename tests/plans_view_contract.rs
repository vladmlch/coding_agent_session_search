//! Plans-subview contract file retained after Phase-0 TUI/analytics pruning.
//!
//! `src/ui/analytics_charts.rs` and `scripts/tests/regenerate_plans_snapshots.sh`
//! were removed in Phase 0.

mod ws_paths_1c;

#[test]
fn plans_view_pruned_in_phase_0() {
    assert!(
        !ws_paths_1c::resolve_workspace_source("src/ui/analytics_charts.rs").exists(),
        "TUI analytics_charts.rs should be removed in Phase 0"
    );
}
