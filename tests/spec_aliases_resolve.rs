//! INV-cass-7 — every subcommand alias resolves to a real subcommand.
//!
//! `normalize_args` carries a `SUBCOMMAND_ALIASES: &[(&str, &str)]` table that
//! rewrites agent-friendly mistakes (e.g. `find` → `search`, `answer` → `pack`)
//! to canonical subcommands. If a canonical command is ever renamed or removed
//! without updating the alias table, an alias would silently rewrite to a
//! non-existent command and surface a confusing clap error to agents.
//!
//! This test cross-checks the alias *targets* (extracted from source) against
//! the canonical command set reported by `cass introspect --json` (the live
//! binary — the authoritative source of truth, so a rename is caught rather
//! than masked by a hard-coded list).
//!
//! `introspect` is a pure-schema command: it opens no archive and scans no
//! session corpus, so this test has no data-dir or corpus dependency.

use std::collections::BTreeSet;

use assert_cmd::Command;
use serde_json::Value;

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
    let mut entries: Vec<_> = std::fs::read_dir(dir)
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

/// Extract `(alias, target)` pairs from the `SUBCOMMAND_ALIASES` table in
/// `src/lib.rs` and `src/cli_root/**/*.rs`. Scoped to the const block so
/// unrelated `("x", "y")` tuples elsewhere in the file are ignored.
fn alias_targets_from_source() -> BTreeSet<String> {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let src_dir = manifest_dir.join("src");
    let mut files = vec![src_dir.join("lib.rs")];
    collect_non_test_rs_files(manifest_dir, &src_dir.join("cli_root"), &mut files);

    let mut targets = BTreeSet::new();

    for path in files {
        let src = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let mut in_block = false;

        for line in src.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("const SUBCOMMAND_ALIASES")
                || trimmed.starts_with("pub(crate) const SUBCOMMAND_ALIASES")
                || trimmed.starts_with("pub const SUBCOMMAND_ALIASES")
            {
                in_block = true;
                continue;
            }
            if !in_block {
                continue;
            }
            if trimmed.starts_with("];") {
                break; // end of the const block
            }
            // Match `("alias", "target")` — capture the second string literal.
            if let Some(target) = parse_alias_pair_target(trimmed) {
                targets.insert(target);
            }
        }
        if !targets.is_empty() {
            break;
        }
    }

    targets
}

/// Given a line like `("find", "search"),` return `search`.
fn parse_alias_pair_target(line: &str) -> Option<String> {
    let open = line.find('(')?;
    let rest = &line[open + 1..];
    // first quoted string = alias, second = target
    let mut quotes = rest.match_indices('"');
    let (a_start, _) = quotes.next()?;
    let (a_end, _) = quotes.next()?;
    let (t_start, _) = quotes.next()?;
    let (t_end, _) = quotes.next()?;
    // sanity: alias before target
    if a_end <= a_start || t_end <= t_start || t_start <= a_end {
        return None;
    }
    Some(rest[t_start + 1..t_end].to_string())
}

/// Canonical top-level command names from the live binary.
fn canonical_commands() -> BTreeSet<String> {
    let output = Command::cargo_bin("cass")
        .expect("cass binary builds")
        .args(["introspect", "--json"])
        .output()
        .expect("run cass introspect --json");
    assert!(
        output.status.success(),
        "cass introspect --json exited non-zero: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: Value = serde_json::from_str(&String::from_utf8_lossy(&output.stdout))
        .expect("valid introspect json");
    json["commands"]
        .as_array()
        .expect("introspect.commands is an array")
        .iter()
        .filter_map(|c| c["name"].as_str().map(str::to_owned))
        .collect()
}

#[test]
fn every_subcommand_alias_resolves_to_a_real_command() {
    let targets = alias_targets_from_source();
    // ~44 alias *pairs* collapse to ~13 *distinct* canonical targets
    // (search, sessions, stats, status, index, view, diag, capabilities,
    // triage, pack, export-html, robot-docs, introspect). A floor of 10
    // distinct targets confirms the const-block scan is finding the table
    // without over-fitting to the exact current count.
    assert!(
        targets.len() >= 10,
        "expected to extract the SUBCOMMAND_ALIASES target set; found only {} distinct targets — the \
         const-block scan has likely drifted from the source shape",
        targets.len()
    );

    let commands = canonical_commands();
    let orphans: BTreeSet<&String> = targets.difference(&commands).collect();

    assert!(
        orphans.is_empty(),
        "subcommand alias(es) target non-existent command(s): {orphans:?}. Every SUBCOMMAND_ALIASES \
         target must be a real command in `cass introspect --json`. A command was likely renamed or \
         removed without updating the alias table in normalize_args."
    );
}
