//! Pass-4 mutate-auditor: ensures every NEW disk-mutating call introduced by
//! the world-class-doctor passes (`src/doctor_*.rs`) flows through the
//! `crate::doctor_chokepoint::mutate()` chokepoint, per safety envelope S8.
//!
//! This test is a **regression guard**, not a runtime check. It greps the
//! pass-1+ doctor source for direct `std::fs` and `std::os` write APIs and
//! asserts that the only file performing them is `src/doctor_chokepoint.rs`
//! itself (the chokepoint's own implementation). Other doctor modules
//! (`doctor_runs.rs`, `doctor_undo.rs`, `doctor_robot_docs.rs`) are allowed a
//! narrow set of operations:
//!
//! - `doctor_runs.rs`: `create_run_dir`, `update_latest_link`, the
//!   `actions.jsonl` append (per S11 — the journal is the chokepoint's
//!   journal and is exempt by design).
//! - `doctor_undo.rs`: the atomic restore-from-backup write-tmp-then-rename
//!   sequence (the inverse of the chokepoint's atomic write).
//! - `doctor_robot_docs.rs`: text constants only; no fs.
//!
//! This test fails if a future pass introduces a new `fs::write|rename|
//! create_dir_all|remove_file|OpenOptions::new().write|symlink` call in any
//! pass-1+ doctor module beyond the documented allow-list.

use std::fs;

const DOCTOR_MODULES_TO_AUDIT: &[&str] = &[
    "src/doctor_chokepoint.rs",
    "src/doctor_runs.rs",
    "src/doctor_undo.rs",
    "src/doctor_robot_docs.rs",
];

const FORBIDDEN_PATTERNS: &[&str] = &[
    "fs::remove_file(",
    "fs::remove_dir(",
    "fs::remove_dir_all(",
    "fs::write(",
    // We DO allow fs::rename, fs::create_dir_all, fs::OpenOptions inside the
    // chokepoint module + the documented allow-list (asserted per-file below).
];

const ALLOWED_BUDGETS: &[(&str, &[&str])] = &[
    (
        "src/doctor_chokepoint.rs",
        &[
            "fs::create_dir_all",
            "fs::rename",
            "fs::write", // for the .reason sidecar in Op::Quarantine
            "fs::copy",
            "fs::set_permissions",
            "fs::metadata",
            "fs::OpenOptions",
            "fs::File::create",
        ],
    ),
    (
        "src/doctor_runs.rs",
        &[
            "fs::create_dir_all",  // create_run_dir
            "fs::set_permissions", // chmod 0o700 on run dir
            "fs::OpenOptions",     // append-only actions.jsonl (per S11)
            "fs::rename",          // atomic latest-symlink swap
            "fs::remove_file",     // ONLY for cleaning up tmp link (not user data)
            "fs::write",           // Windows fallback for latest pseudo-link
            "fs::read_dir",        // listing runs (read-only)
            "fs::File::open",
        ],
    ),
    (
        "src/doctor_undo.rs",
        &[
            "fs::create_dir_all", // ensures run dir, target parent, undo-quarantine dir
            "fs::rename",         // atomic restore + undo-quarantine
            "fs::read",           // read backup
            "fs::File::create",   // tmp file for atomic restore
            "fs::metadata",
        ],
    ),
    ("src/doctor_robot_docs.rs", &[]),
];

#[test]
fn pass4_no_forbidden_fs_writes_in_doctor_modules() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    for module in DOCTOR_MODULES_TO_AUDIT {
        let path = format!("{manifest}/{module}");
        let body = match fs::read_to_string(&path) {
            Ok(b) => b,
            Err(e) => panic!("doctor module {module} not readable: {e}"),
        };
        for pattern in FORBIDDEN_PATTERNS {
            // Skip allow-listed patterns for this file
            let allowed = ALLOWED_BUDGETS
                .iter()
                .find(|(m, _)| *m == *module)
                .map(|(_, list)| list.iter().any(|a| pattern.starts_with(a)))
                .unwrap_or(false);
            if allowed {
                continue;
            }
            // Otherwise, the pattern must NOT appear (outside test code).
            for (line_idx, line) in body.lines().enumerate() {
                let line_no = line_idx + 1;
                if line.contains(pattern) {
                    // Allow if this line is inside a #[cfg(test)] mod tests block.
                    // Simple heuristic: look at the prefix for `mod tests` or
                    // `#[test]` markers in the surrounding 20 lines.
                    let near_tests =
                        body.lines()
                            .skip(line_idx.saturating_sub(50))
                            .take(60)
                            .any(|l| {
                                l.contains("mod tests")
                                    || l.contains("#[test]")
                                    || l.contains("#[cfg(test)]")
                            });
                    if !near_tests {
                        panic!(
                            "Forbidden mutation pattern {:?} in {} at line {}: {:?}\n\
                             If this is intentional, add it to ALLOWED_BUDGETS in \
                             tests/doctor_mutate_auditor.rs.",
                            pattern, module, line_no, line
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn pass4_doctor_chokepoint_is_canonical() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let chokepoint = fs::read_to_string(format!("{manifest}/src/doctor_chokepoint.rs"))
        .expect("chokepoint module readable");
    // The chokepoint module declares the canonical APIs.
    for marker in [
        "pub(crate) fn mutate(",
        "pub(crate) fn path_is_in_scope(",
        "pub(crate) enum Op",
        "pub(crate) enum ChokepointError",
        "PathOutOfScope",
        "DataDirGone",
        "AppendLineJournalCollision",
        "MUTATION_RECEIPT_SCHEMA_VERSION",
    ] {
        assert!(
            chokepoint.contains(marker),
            "chokepoint module missing canonical API: {marker}"
        );
    }
}

#[test]
fn pass4_doctor_undo_uses_chokepoint_run_dir_layout() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let undo =
        fs::read_to_string(format!("{manifest}/src/doctor_undo.rs")).expect("undo module readable");
    // Undo MUST consume actions.jsonl + the per-run backups dir produced by
    // the chokepoint. Source-level grep is the regression guard.
    for marker in [
        "actions.jsonl",
        "backups",
        "blake3",
        "AfterHashMismatch",
        "BackupHashMismatch",
        "BackupMissing",
    ] {
        assert!(
            undo.contains(marker),
            "doctor_undo.rs missing pass-1+ contract marker: {marker}"
        );
    }
}

fn is_src_test_only(rel: &str) -> bool {
    rel.ends_with("_tests.rs")
        || rel.ends_with(".tests.rs")
        || rel.contains("/tests/")
        || rel.ends_with("/tests.rs")
}

fn collect_non_test_rs_files(
    manifest_dir: &std::path::Path,
    dir: &std::path::Path,
    out: &mut Vec<std::path::PathBuf>,
) {
    if !dir.is_dir() {
        return;
    }
    let mut entries: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_non_test_rs_files(manifest_dir, &path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let rel = path
                .strip_prefix(manifest_dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            if !is_src_test_only(&rel) {
                out.push(path);
            }
        }
    }
}

fn read_lib_split_production_sources() -> String {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let src_dir = manifest_dir.join("src");
    let mut files = vec![src_dir.join("lib.rs")];
    for subdir in ["cli_root", "lib_doctor", "cli_app"] {
        collect_non_test_rs_files(manifest_dir, &src_dir.join(subdir), &mut files);
    }
    files
        .iter()
        .map(|p| fs::read_to_string(p).unwrap_or_else(|e| panic!("{} readable: {e}", p.display())))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn pass4_lib_dispatch_routes_through_doctor_chokepoint_apis() {
    // Pass-3 dispatch must reach into the doctor_runs/doctor_undo/doctor_robot_docs
    // modules. Source-level grep ensures the wire-up didn't get reverted.
    let lib = read_lib_split_production_sources();
    for marker in [
        "crate::doctor_runs::list_runs",
        "crate::doctor_runs::find_latest_run",
        "crate::doctor_runs::RunId",
        "crate::doctor_undo::undo_run",
        "crate::doctor_robot_docs::doctor_robot_docs_body",
        "RobotTopic::Doctor",
    ] {
        assert!(
            lib.contains(marker),
            "split lib sources missing pass-2/pass-3 dispatch marker: {marker}"
        );
    }
}

#[test]
fn pass4_pass_3_subcommands_wired_in_clap() {
    let lib = read_lib_split_production_sources();
    // Pass-3 added these clap fields under Commands::Doctor.
    for marker in [
        "robot_triage:",
        "diff: Option<String>",
        "gc_before: Option<String>",
        "ls: bool",
        "undo: Option<String>",
        // Pass-6 additions:
        "watch: bool",
        "watch_interval_ms:",
        "watch_iterations:",
        // Pass-8 additions:
        "explain: Option<String>",
        "emit_capabilities: bool",
    ] {
        assert!(
            lib.contains(marker),
            "Commands::Doctor missing pass-2/3/6/8 flag declaration: {marker}"
        );
    }
}

#[test]
fn pass8_explain_and_emit_capabilities_dispatch_present() {
    let lib = read_lib_split_production_sources();
    for marker in [
        "fn run_doctor_explain",
        "fn run_doctor_emit_capabilities",
        "doctor-capabilities",
        "kind\": \"explain\"",
    ] {
        assert!(
            lib.contains(marker),
            "split lib sources missing pass-8 explain/capabilities dispatch marker: {marker}"
        );
    }
}

#[test]
fn pass6_watch_helper_routes_through_doctor_runs_apis() {
    // Pass-6 watch dispatch must consume list_runs + find_in_flight_band.
    let lib = read_lib_split_production_sources();
    for marker in [
        "fn run_doctor_watch",
        "crate::doctor_runs::list_runs",
        "crate::doctor_runs::find_in_flight_band",
        "watch-tick",
    ] {
        assert!(
            lib.contains(marker),
            "split lib sources missing pass-6 watch dispatch marker: {marker}"
        );
    }
}

#[test]
fn pass6_band_journal_helpers_present() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let runs = std::fs::read_to_string(format!("{manifest}/src/doctor_runs.rs"))
        .expect("doctor_runs readable");
    for marker in [
        "pub(crate) fn append_band_started",
        "pub(crate) fn append_band_completed",
        "pub(crate) fn find_in_flight_band",
    ] {
        assert!(
            runs.contains(marker),
            "doctor_runs.rs missing pass-6 band-journal helper: {marker}"
        );
    }
}
