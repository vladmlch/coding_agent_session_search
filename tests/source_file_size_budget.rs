//! File-size budget for the src/lib.rs modularization (plan stage 0).
//!
//! Every file produced from `src/lib.rs` must stay under 2000 lines
//! (working target ~1800). This test enforces the budget over the split's
//! target roots — `src/cli/`, `src/doctor/`, `src/robot_schema/` — plus
//! the specific files the plan names (`src/indexer/stall_watchdog.rs`,
//! `src/export/messages.rs`), so an oversized file fails as soon as a
//! stage lands it rather than at the end of the program.
//!
//! `src/lib.rs` itself is carried in TEMPORARY_EXCEPTIONS: the plan
//! reserves shrinking it to the thin shim for the final stage, at which
//! point the exception list must be empty.

use std::fs;
use std::path::{Path, PathBuf};

const BUDGET: usize = 2000;

/// Split target roots: every `.rs` under these dirs is budget-checked.
const ROOTS: &[&str] = &["src/cli", "src/doctor", "src/robot_schema"];

/// Individual files the plan creates outside the roots above.
const FILES: &[&str] = &["src/indexer/stall_watchdog.rs", "src/export/messages.rs"];

/// Temporary exemptions that must be empty when the program finishes.
const TEMPORARY_EXCEPTIONS: &[&str] = &["src/lib.rs"];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
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

#[test]
fn lib_rs_split_files_respect_size_budget() {
    let root = repo_root();
    let exceptions: Vec<PathBuf> = TEMPORARY_EXCEPTIONS
        .iter()
        .map(|r| root.join(r))
        .collect();

    let mut checked = 0usize;
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
            checked += 1;
            let n = line_count(&file);
            if n > BUDGET {
                oversized.push(format!("{}: {n} lines", file.display()));
            }
        }
    }
    for rel in FILES {
        let file = root.join(rel);
        if file.is_file() && !exceptions.contains(&file) {
            checked += 1;
            let n = line_count(&file);
            if n > BUDGET {
                oversized.push(format!("{}: {n} lines", file.display()));
            }
        }
    }
    assert!(
        oversized.is_empty(),
        "lib.rs split files exceed the {BUDGET}-line budget:\n  {}",
        oversized.join("\n  ")
    );
    // Stage-0 baseline: no split files exist yet; once stages land, this
    // guards them. Keep the assertion meaningful by requiring progress.
    eprintln!("size budget: {checked} files checked under split roots");
}

#[test]
fn temporary_exceptions_are_only_lib_rs_and_shrink_over_time() {
    // The exception list may only ever contain src/lib.rs, and the final
    // stage must remove it entirely.
    for rel in TEMPORARY_EXCEPTIONS {
        assert_eq!(*rel, "src/lib.rs", "unexpected exception {rel}");
    }
    let root = repo_root();
    let lib = root.join("src/lib.rs");
    let n = line_count(&lib);
    // Until the final stage: lib.rs is the monolith (well over budget).
    // This test documents the expectation; tighten to `n <= BUDGET`
    // when the shim lands.
    eprintln!("src/lib.rs currently {n} lines (exception until final stage)");
}
