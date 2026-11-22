//! Plans-subview contract file retained after Phase-0 TUI/analytics pruning.
//!
//! `src/ui/analytics_charts.rs` and `scripts/tests/regenerate_plans_snapshots.sh`
//! were removed in Phase 0.

#[test]
fn plans_view_pruned_in_phase_0() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !manifest_dir.join("src/ui/analytics_charts.rs").exists(),
        "TUI analytics_charts.rs should be removed in Phase 0"
    );
}
