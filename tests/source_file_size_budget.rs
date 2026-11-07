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
//! Exactly one temporary exemption remains, bound to the stage at which
//! it must be gone:
//!   - `src/lib.rs` through stage 6 (the thin-shim cleanup stage).
//!   - `src/doctor/run.rs` was exempt through stage 5 (the DoctorRun
//!     structural decomposition stage); stage 5 landed it and the cover
//!     is removed.
//!
//! When SPLIT_STAGE advances past an exemption's stage the file loses
//! its cover and must satisfy BUDGET like everything else; past stage 6
//! the exemption list is effectively empty.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

const BUDGET: usize = 2000;

/// Current progress through the split program (plan §8 stage order).
/// Stages 5 and 6 are manual; SPLIT_STAGE records how far the move-only
/// applies have landed so exemptions age out correctly.
const SPLIT_STAGE: &str = "5";

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
/// budget). Only plan-authorized exceptions may appear here.
const TEMPORARY_EXCEPTIONS: &[(&str, &str)] = &[("src/lib.rs", "6")];

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
    // The only sanctioned exceptions, each bound to its plan stage.
    // lib-split 5 removed the src/doctor/run.rs cover when the DoctorRun
    // decomposition landed; src/lib.rs through stage 6 remains.
    assert_eq!(
        TEMPORARY_EXCEPTIONS,
        &[("src/lib.rs", "6")],
        "exception list drifted — only plan-authorized \
         stage-bound exceptions are allowed"
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
    let tmp = std::env::temp_dir().join(format!("cass_size_budget_neg_{}", std::process::id()));
    let cli = tmp.join("src/cli");
    fs::create_dir_all(&cli).unwrap();
    let big = cli.join("oversized_probe.rs");
    let blob = "x\n".repeat(BUDGET + 1);
    fs::write(&big, &blob).unwrap();
    // Also seed an exempt-at-stage-6 lib.rs to prove exemptions still work.
    fs::write(tmp.join("src/lib.rs"), &blob).unwrap();

    let at_4 = oversized_in_scope(&tmp, "4");
    assert!(
        at_4.iter().any(|l| l.contains("oversized_probe.rs")),
        "oversized unexempt file must be flagged: {at_4:?}"
    );
    assert!(
        !at_4.iter().any(|l| l.contains("src/lib.rs")),
        "src/lib.rs is exempt through stage 6: {at_4:?}"
    );

    // lib.rs is exempt through stage 6 inclusive (the shim stage); once
    // the program reports a stage past "6" the cover lapses and an
    // oversized lib.rs must be flagged.
    let past_end = oversized_in_scope(&tmp, "7");
    assert!(
        past_end.iter().any(|l| l.contains("src/lib.rs")),
        "src/lib.rs exemption expires after stage 6: {past_end:?}"
    );
    // lib-split 5 removed the doctor/run.rs exemption — an oversized
    // run.rs is flagged at every stage from 5 onward.
    let run = tmp.join("src/doctor/run.rs");
    fs::create_dir_all(run.parent().unwrap()).unwrap();
    fs::write(&run, &blob).unwrap();
    let at_5 = oversized_in_scope(&tmp, "5");
    assert!(
        at_5.iter().any(|l| l.contains("run.rs")),
        "doctor/run.rs exemption removed in stage 5: {at_5:?}"
    );
    let at_6 = oversized_in_scope(&tmp, "6");
    assert!(
        at_6.iter().any(|l| l.contains("run.rs")),
        "doctor/run.rs stays budget-checked past stage 5: {at_6:?}"
    );

    // cleanup is best-effort in a test; failure is not fatal.
    let _ = fs::remove_dir_all(&tmp);
}
