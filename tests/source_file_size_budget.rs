//! File-size budget for the src/lib.rs modularization (plan stage 0).
//!
//! Every file produced from `src/lib.rs` must stay under 2000 lines
//! (working target ~1800). This test enforces the budget over the split's
//! target roots — `src/cli/`, `src/doctor/`, `src/robot_schema/` — plus
//! the root files the plan derives (`src/cli.rs`, `src/robot_schema.rs`,
//! the existing `src/doctor.rs`) and the specific files the plan names
//! (`src/indexer/stall_watchdog.rs`, `src/export/messages.rs`), so an
//! oversized file fails as soon as a stage lands it rather than at the
//! end of the program.
//!
//! Each temporary exemption was bound to the stage at which it had to be
//! gone, and all have now expired:
//!   - `src/lib.rs` was exempt through stage 6 (the thin-shim cleanup
//!     stage); stage 6 landed it and the cover is removed.
//!   - `src/doctor/run.rs` was exempt through stage 5 (the DoctorRun
//!     structural decomposition stage); stage 5 landed it and the cover
//!     is removed.
//!
//! When SPLIT_STAGE advances past an exemption's stage the file loses
//! its cover and must satisfy BUDGET like everything else; at stage 6
//! every plan-authorized cover has expired, so the exemption list is
//! empty.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

const BUDGET: usize = 2000;

/// Current progress through the split program (plan §8 stage order).
/// Stages 5 and 6 are manual; SPLIT_STAGE records how far the move-only
/// applies have landed so exemptions age out correctly.
const SPLIT_STAGE: &str = "6";

const STAGE_ORDER: &[&str] = &[
    "1a", "1b", "1c", "2a", "2b", "2c", "2d", "2e", "2f", "2g", "3a", "3b", "3c", "3d", "4", "5",
    "6",
];

/// Split target roots: every `.rs` under these dirs is budget-checked.
const ROOTS: &[&str] = &["src/cli", "src/doctor", "src/robot_schema"];

/// Individual files the plan creates or owns outside the roots above.
/// `src/cli.rs` and `src/robot_schema.rs` are the plan's root files;
/// `src/doctor.rs` already exists and stays in scope.
const FILES: &[&str] = &[
    "src/cli.rs",
    "src/doctor.rs",
    "src/robot_schema.rs",
    "src/indexer/stall_watchdog.rs",
    "src/indexer/stall_diagnostics_tests.rs",
    "src/export/messages.rs",
    "src/export/message_grouping_tests.rs",
];

/// Temporary exemptions: (path, last stage at which it may exceed the
/// budget). Only plan-authorized exceptions may appear here. Stage 6
/// landed the thin-shim cleanup, so the last cover (src/lib.rs) has
/// expired and the list is empty.
const TEMPORARY_EXCEPTIONS: &[(&str, &str)] = &[];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

fn stage_le(a: &str, b: &str) -> bool {
    let ia = STAGE_ORDER.iter().position(|s| *s == a);
    let ib = STAGE_ORDER.iter().position(|s| *s == b);
    match (ia, ib) {
        (Some(x), Some(y)) => x <= y,
        _ => false,
    }
}

/// Exemptions still in force at `current` stage.
fn active_exceptions(root: &Path, current: &str) -> HashSet<PathBuf> {
    TEMPORARY_EXCEPTIONS
        .iter()
        .filter(|(_, through)| stage_le(current, through))
        .map(|(rel, _)| root.join(rel))
        .collect()
}

fn line_count(path: &Path) -> usize {
    fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
        .lines()
        .count()
}

fn rs_files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in fs::read_dir(&d).unwrap_or_else(|e| panic!("readdir {}: {e}", d.display())) {
            let p = entry.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    out
}

/// The budget predicate, separated so tests can exercise it against a
/// synthetic tree: returns "path: N lines" for every in-scope file over
/// BUDGET that is not exempt at `current` stage.
fn oversized_in_scope(root: &Path, current: &str) -> Vec<String> {
    let exceptions = active_exceptions(root, current);
    let mut oversized = Vec::new();
    for rel in ROOTS {
        let dir = root.join(rel);
        if !dir.is_dir() {
            continue;
        }
        for file in rs_files_under(&dir) {
            if exceptions.contains(&file) {
                continue;
            }
            let n = line_count(&file);
            if n > BUDGET {
                oversized.push(format!("{}: {n} lines", file.display()));
            }
        }
    }
    for rel in FILES {
        let file = root.join(rel);
        if file.is_file() && !exceptions.contains(&file) {
            let n = line_count(&file);
            if n > BUDGET {
                oversized.push(format!("{}: {n} lines", file.display()));
            }
        }
    }
    // src/lib.rs is checked explicitly — it is a scope member whether or
    // not an exception currently covers it.
    let lib = root.join("src/lib.rs");
    if lib.is_file() && !exceptions.contains(&lib) {
        let n = line_count(&lib);
        if n > BUDGET {
            oversized.push(format!("{}: {n} lines", lib.display()));
        }
    }
    oversized
}

#[test]
fn lib_rs_split_files_respect_size_budget() {
    let oversized = oversized_in_scope(&repo_root(), SPLIT_STAGE);
    assert!(
        oversized.is_empty(),
        "lib.rs split files exceed the {BUDGET}-line budget:\n  {}",
        oversized.join("\n  ")
    );
}

#[test]
fn temporary_exceptions_are_exactly_the_plan_authorized() {
    // No exceptions remain: stage 5 removed the src/doctor/run.rs cover
    // and stage 6 removed the src/lib.rs cover when the thin-shim
    // cleanup landed. Any new entry here is unauthorized drift.
    assert_eq!(
        TEMPORARY_EXCEPTIONS,
        &[] as &[(&str, &str)],
        "exception list drifted — only plan-authorized \
         stage-bound exceptions are allowed, and none remain"
    );
    for (_, through) in TEMPORARY_EXCEPTIONS {
        assert!(
            STAGE_ORDER.contains(through),
            "exception stage {through} is not a known split stage"
        );
    }
}

#[test]
fn budget_rejects_oversized_unexempt_file() {
    // Negative proof: the guard must flag an oversized file that has no
    // exemption covering it — a scoped temp tree, not the real repo.
    // tempfile::tempdir gives a privately-owned directory (no predictable
    // world-writable path to precreate/symlink) and cleans up on drop.
    let tmp_guard = tempfile::tempdir().unwrap();
    let tmp = tmp_guard.path();
    let cli = tmp.join("src/cli");
    fs::create_dir_all(&cli).unwrap();
    let big = cli.join("oversized_probe.rs");
    let blob = "x\n".repeat(BUDGET + 1);
    fs::write(&big, &blob).unwrap();
    // Also seed an oversized lib.rs: its stage-6 cover has lapsed, so it
    // must be flagged at every stage now.
    fs::write(tmp.join("src/lib.rs"), &blob).unwrap();

    let at_4 = oversized_in_scope(tmp, "4");
    assert!(
        at_4.iter().any(|l| l.contains("oversized_probe.rs")),
        "oversized unexempt file must be flagged: {at_4:?}"
    );
    assert!(
        at_4.iter().any(|l| l.contains("src/lib.rs")),
        "src/lib.rs has no remaining exemption: {at_4:?}"
    );

    // The current stage ("6") must flag an oversized lib.rs too — the
    // thin-shim cover ended when stage 6 landed.
    let at_current = oversized_in_scope(tmp, SPLIT_STAGE);
    assert!(
        at_current.iter().any(|l| l.contains("src/lib.rs")),
        "src/lib.rs exemption expired at stage 6: {at_current:?}"
    );
    // lib-split 5 removed the doctor/run.rs exemption — an oversized
    // run.rs is flagged at every stage from 5 onward.
    let run = tmp.join("src/doctor/run.rs");
    fs::create_dir_all(run.parent().unwrap()).unwrap();
    fs::write(&run, &blob).unwrap();
    let at_5 = oversized_in_scope(tmp, "5");
    assert!(
        at_5.iter().any(|l| l.contains("run.rs")),
        "doctor/run.rs exemption removed in stage 5: {at_5:?}"
    );
    let at_6 = oversized_in_scope(tmp, "6");
    assert!(
        at_6.iter().any(|l| l.contains("run.rs")),
        "doctor/run.rs stays budget-checked past stage 5: {at_6:?}"
    );
}
