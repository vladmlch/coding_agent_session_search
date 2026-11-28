//! INV-cass-6 — the exit codes cass emits and the exit codes it documents must
//! be the same set.
//!
//! Regression guard for two real defects:
//! - 2026-05-25: a doctor quarantine I/O path constructed
//!   `CliError { code: 73, kind: "io", .. }`, an undocumented code (fixed to 14).
//! - 2026-09-23 reality check (bead coding_agent_session_search-2l1b0.58):
//!   `cass index` aborts a stalled run with `process::exit(70)` and an
//!   interrupted `sources setup` exits 130, yet neither code was documented,
//!   because this guard only scanned `CliError` literals and compared them
//!   with a hand-mirrored list that had drifted from `capabilities`.
//!
//! Emitted set: every `CliError { code: N, kind: .. }`, every
//! `process::exit(N)` and every supervisor `terminate(N)` in the scanned
//! sources. Documented set: the real binary's `cass capabilities --json`
//! `exit_codes`, which agents read.
//! Both directions are asserted: an undocumented emitted code, and a
//! documented code nothing emits, are both failures.
//!
//! 2026-11-05 final review found the search-service supervisor exits (124
//! deadline, 125 supervisor failure, 126 resident-memory limit) emitted only
//! through `terminate(CONST)`/`code: CONST`/`process::exit(CONST)` shapes —
//! invisible to both extractors and undocumented. The sites now spell literal
//! arguments and this scan covers the search_service modules.

use std::collections::BTreeSet;
use std::error::Error;

mod ws_paths_1d;

type TestResult = Result<(), Box<dyn Error>>;

fn test_error(message: impl Into<String>) -> Box<dyn Error> {
    std::io::Error::other(message.into()).into()
}

fn ensure(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(test_error(message))
    }
}

/// The documented exit codes, read from the real binary's
/// `cass capabilities --json` (`exit_codes[].code`, where a range like
/// `"20-21"` names every code in it).
fn documented_exit_codes() -> Result<BTreeSet<i32>, Box<dyn Error>> {
    let home = tempfile::TempDir::new()?;
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cass"))
        .args(["capabilities", "--json"])
        .env("HOME", home.path())
        .env("CASS_DATA_DIR", home.path().join("cass-data"))
        .env("CASS_AUTO_REFRESH", "0")
        .output()?;
    ensure(
        output.status.success(),
        format!(
            "cass capabilities --json failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ),
    )?;
    let payload: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let entries = payload["exit_codes"]
        .as_array()
        .ok_or_else(|| test_error("capabilities has no exit_codes array"))?;
    let mut codes = BTreeSet::new();
    for entry in entries {
        let code = entry["code"]
            .as_str()
            .ok_or_else(|| test_error(format!("exit code entry without a code: {entry}")))?;
        let (first, last) = code.split_once('-').unwrap_or((code, code));
        for value in first.parse::<i32>()?..=last.parse::<i32>()? {
            codes.insert(value);
        }
    }
    Ok(codes)
}

/// Additional source files outside `src/lib.rs` and `src/{cli_root,lib_doctor,cli_app}/**`
/// that construct `CliError { code: N, kind: .. }` or exit directly.
const EXTRA_SOURCE_PATHS: &[&str] = &[
    "src/model/cli_error.rs",
    "src/doctor.rs",
    "src/doctor_undo.rs",
    "src/doctor_runs.rs",
    // `process::exit(70)` for the viewer-bridge hard deadline lives here.
    "src/viewer_bridge/deadline.rs",
    // `process::exit(70)` for the indexer checkpoint stall watchdog — the
    // cass-side "index stalled and aborted" emitter.
    "src/indexer/checkpoint_deadline.rs",
    // The read-only search service and its supervisors: `terminate(124)` for
    // the request deadline, `terminate(125)` for watchdog/monitor failure,
    // `terminate(126)` for the sampled resident-memory limit, plus the
    // CliError/`process::exit(125)` teardown paths in the session driver.
    "src/search_service.rs",
    "src/search_service/deadline.rs",
    "src/search_service/memory.rs",
    "src/search_service/once.rs",
];

fn is_src_test_only(rel: &str) -> bool {
    rel.ends_with("_tests.rs")
        || rel.ends_with(".tests.rs")
        || rel.contains("/tests/")
        || rel.ends_with("/tests.rs")
}

fn collect_non_test_rs_files(
    workspace_root: &std::path::Path,
    dir: &std::path::Path,
    out: &mut Vec<(String, String)>,
) {
    if !dir.is_dir() {
        return;
    }
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_non_test_rs_files(workspace_root, &path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let rel = path
                .strip_prefix(workspace_root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            if !is_src_test_only(&rel) {
                let src =
                    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {rel}: {e}"));
                out.push((rel, src));
            }
        }
    }
}

fn scanned_sources() -> Vec<(String, String)> {
    let root = ws_paths_1d::workspace_root();
    let lib_src = ws_paths_1d::read_workspace_source("src/lib.rs");
    let mut sources = vec![("src/lib.rs".to_string(), lib_src)];
    for src_dir in ws_paths_1d::workspace_source_dirs() {
        for subdir in ["cli_root", "lib_doctor", "cli_app"] {
            collect_non_test_rs_files(&root, &src_dir.join(subdir), &mut sources);
        }
    }
    for rel in EXTRA_SOURCE_PATHS {
        sources.push(((*rel).to_string(), ws_paths_1d::read_workspace_source(rel)));
    }
    sources
}

/// Extract the integer from every `code: N,` line that is immediately followed
/// (next non-blank line) by a `kind:` line — the `CliError` field signature.
/// This scopes the scan to `CliError` constructions and ignores unrelated
/// `code:` fields, positional constructor calls, and non-numeric `code:` values.
fn emitted_cli_error_codes(src: &str) -> BTreeSet<i32> {
    let lines: Vec<&str> = src.lines().collect();
    let mut codes = BTreeSet::new();

    for (idx, raw) in lines.iter().enumerate() {
        let line = raw.trim();
        let Some(rest) = line.strip_prefix("code: ") else {
            continue;
        };
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        if digits.is_empty() {
            continue; // e.g. `code: err.code` or `code: SOME_CONST`
        }
        // Confirm the next non-blank line is the `kind:` field — the CliError shape.
        // `skip` (not slice-index) keeps this panic-free regardless of `idx`.
        let next_non_blank = lines
            .iter()
            .skip(idx + 1)
            .map(|l| l.trim())
            .find(|l| !l.is_empty());
        if next_non_blank.is_some_and(|l| l.starts_with("kind:"))
            && let Ok(code) = digits.parse::<i32>()
        {
            codes.insert(code);
        }
    }

    codes
}

/// Every literal `process::exit(N)`: exits that bypass `CliError` entirely
/// (the index-stall aborts).
fn emitted_process_exit_codes(src: &str) -> BTreeSet<i32> {
    src.match_indices("process::exit(")
        .filter_map(|(start, needle)| {
            let rest = &src[start + needle.len()..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            if digits.is_empty() || !rest[digits.len()..].starts_with(')') {
                return None;
            }
            digits.parse::<i32>().ok()
        })
        .collect()
}

/// Every literal `terminate(N)`: the search-service supervisors' no-cleanup
/// worker exit (`_exit`/`TerminateProcess`), which deliberately is NOT
/// `process::exit` — the deadline watchdog and resident-memory monitor use it
/// for 124/125/126. File-scoped like the other extractors: inside the scanned
/// sources `terminate` names only that supervisor call (test-time
/// `terminate(code)` fixtures pass a variable and are skipped).
fn emitted_terminate_codes(src: &str) -> BTreeSet<i32> {
    src.match_indices("terminate(")
        .filter_map(|(start, needle)| {
            let rest = &src[start + needle.len()..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            if digits.is_empty() || !rest[digits.len()..].starts_with(')') {
                return None;
            }
            digits.parse::<i32>().ok()
        })
        .collect()
}

fn emitted_exit_codes() -> BTreeSet<i32> {
    scanned_sources()
        .iter()
        .flat_map(|(_, src)| {
            let mut codes = emitted_cli_error_codes(src);
            codes.extend(emitted_process_exit_codes(src));
            codes.extend(emitted_terminate_codes(src));
            codes
        })
        .collect()
}

#[test]
fn every_emitted_exit_code_is_documented() -> TestResult {
    let documented = documented_exit_codes()?;
    let undocumented: BTreeSet<i32> = emitted_exit_codes()
        .into_iter()
        .filter(|code| !documented.contains(code))
        .collect();
    eprintln!("[exit-codes] documented={documented:?} undocumented={undocumented:?}");

    ensure(
        undocumented.is_empty(),
        format!(
            "cass emits undocumented exit code(s) {undocumented:?}. Every emitted code must be in \
         `cass capabilities --json` exit_codes (and the robot-docs exit-codes, README and AGENTS \
         tables). Either document the code or fix the site to use a documented one. (Guards the \
         shipped `code: 73` and the undocumented 70/130 exits.)"
        ),
    )
}

/// The reverse direction: a documented code that nothing emits is a promise
/// agents cannot rely on (the old "8 = partial search result"). Success (0)
/// needs no emission site.
#[test]
fn every_documented_exit_code_is_emitted() -> TestResult {
    let emitted = emitted_exit_codes();
    let unemitted: BTreeSet<i32> = documented_exit_codes()?
        .into_iter()
        .filter(|code| *code != 0 && !emitted.contains(code))
        .collect();
    eprintln!("[exit-codes] emitted={emitted:?} unemitted={unemitted:?}");
    ensure(
        unemitted.is_empty(),
        format!(
            "capabilities documents exit code(s) {unemitted:?} that no scanned source emits; \
         remove them from the contract or point this scan at the emitting file"
        ),
    )
}

/// Codes in the first markdown table after `heading` (rows `| N | ...` or
/// `| N-M | ...`), stopping at the first line after the table.
fn markdown_table_codes(doc: &str, heading: &str) -> Result<BTreeSet<i32>, Box<dyn Error>> {
    let start = doc
        .find(heading)
        .ok_or_else(|| test_error(format!("heading not found: {heading}")))?;
    let mut codes = BTreeSet::new();
    let mut in_table = false;
    for line in doc[start..].lines().skip(1) {
        let trimmed = line.trim();
        if trimmed.starts_with('|') {
            in_table = true;
            let cell = trimmed
                .trim_start_matches('|')
                .split('|')
                .next()
                .unwrap_or("");
            let cell = cell.trim();
            let (first, last) = cell.split_once('-').unwrap_or((cell, cell));
            if let (Ok(first), Ok(last)) = (first.parse::<i32>(), last.parse::<i32>()) {
                codes.extend(first..=last);
            }
        } else if in_table {
            break;
        }
    }
    Ok(codes)
}

/// The human tables agents also read must list exactly the codes
/// `capabilities` documents. They drifted separately before: README and
/// AGENTS kept "4 = network" and "6 = incompatible version" and lacked 70
/// and 130 (2l1b0.58 / 2l1b0.69).
#[test]
fn readme_and_agents_exit_code_tables_match_capabilities() -> TestResult {
    let documented = documented_exit_codes()?;
    for (name, doc, heading) in [
        (
            "README.md",
            include_str!("../README.md"),
            "**Exit codes** follow a semantic convention",
        ),
        ("AGENTS.md", include_str!("../AGENTS.md"), "### Exit Codes"),
    ] {
        let table = markdown_table_codes(doc, heading)?;
        ensure(
            table == documented,
            format!("{name} exit-code table {table:?} differs from capabilities {documented:?}"),
        )?;
    }
    Ok(())
}

#[test]
fn process_exit_extractor_reads_only_literal_codes() -> TestResult {
    let sample = "std::process::exit(70);\nprocess::exit(code);\nstd::process::exit(130)\nexit(9);";
    let found = emitted_process_exit_codes(sample);
    ensure(
        found == BTreeSet::from([70, 130]),
        format!("expected the two literal process exits only, found {found:?}"),
    )
}

#[test]
fn terminate_extractor_reads_only_literal_supervisor_codes() -> TestResult {
    let sample = "terminate(124);\nterminate(code);\nsuper::terminate(125)\nfn terminate(code: i32)\nterminate(126);";
    let found = emitted_terminate_codes(sample);
    ensure(
        found == BTreeSet::from([124, 125, 126]),
        format!("expected the three literal supervisor exits only, found {found:?}"),
    )?;
    // The scanned search-service sources really do emit the documented
    // supervisor set — a drifted extractor would silently pass parity tests.
    let sources = scanned_sources();
    let supervised: BTreeSet<i32> = sources
        .iter()
        .filter(|(name, _)| name.contains("search_service"))
        .flat_map(|(_, src)| emitted_terminate_codes(src))
        .collect();
    ensure(
        supervised == BTreeSet::from([124, 125, 126]),
        format!("expected supervisor terminate codes 124/125/126, found {supervised:?}"),
    )
}

/// Sanity: the extractor must actually find the bulk of the CliError surface.
/// If this drops toward zero, the `code:`/`kind:` heuristic has drifted from
/// the real construction shape and `every_emitted_exit_code_is_documented`
/// would silently pass by finding nothing.
#[test]
fn extractor_finds_the_cli_error_surface() -> TestResult {
    let sources = scanned_sources();
    let total: usize = sources
        .iter()
        .map(|(_, src)| emitted_cli_error_codes(src).len())
        .sum::<usize>();
    // Distinct codes per file collapse to a small set; assert we at least see
    // the core spread (0-9 plus several domain codes) rather than nothing.
    let distinct: BTreeSet<i32> = sources
        .iter()
        .flat_map(|(_, src)| emitted_cli_error_codes(src))
        .collect();
    ensure(
        distinct.len() >= 10 && total >= 10,
        format!(
            "exit-code extractor found only {} distinct codes ({} file-totals); the code:/kind: \
         heuristic has likely drifted from the CliError construction shape",
            distinct.len(),
            total
        ),
    )
}

#[test]
fn model_cli_error_constructors_are_scanned() -> TestResult {
    let sources = scanned_sources();
    let source = sources
        .iter()
        .find(|(rel, _)| rel == "src/model/cli_error.rs")
        .map(|(_, source)| source)
        .ok_or_else(|| test_error("scanned sources do not include src/model/cli_error.rs"))?;
    let emitted = emitted_cli_error_codes(source);
    ensure(
        emitted == BTreeSet::from([2, 9]),
        format!("model cli_error constructors emit {emitted:?}, expected {{2, 9}}"),
    )
}
