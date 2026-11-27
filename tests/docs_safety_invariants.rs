//! Focused docs safety checks for invariants that must not drift.
//!
//! These tests intentionally use a small context allowlist instead of scanning
//! every generated artifact. Historical planning docs may mention superseded
//! designs, but current user/agent docs must keep the shipped contracts clear.

use std::path::{Path, PathBuf};

mod ws_paths_1b;

fn repo_root() -> PathBuf {
    ws_paths_1b::workspace_root()
}

fn read_doc(root: &Path, relative_path: &str) -> String {
    std::fs::read_to_string(root.join(relative_path))
        .unwrap_or_else(|err| panic!("read {relative_path}: {err}"))
}

fn assert_absent(surface: &str, text: &str, forbidden_phrases: &[&str]) {
    let lower = text.to_ascii_lowercase();
    for phrase in forbidden_phrases {
        assert!(
            !lower.contains(&phrase.to_ascii_lowercase()),
            "{surface} contains forbidden safety-contract phrase `{phrase}`"
        );
    }
}

fn assert_no_semantic_auto_download_claim(surface: &str, text: &str) {
    for (idx, line) in text.lines().enumerate() {
        let lower = line.to_ascii_lowercase();
        let mentions_model_surface = lower.contains("semantic")
            || lower.contains("model")
            || lower.contains("embedder")
            || lower.contains("ml model");
        let mentions_auto_download = lower.contains("auto-download")
            || lower.contains("auto download")
            || lower.contains("autodownload")
            || lower.contains("automatically download")
            || lower.contains("automatically downloads")
            || lower.contains("automatic download");

        if !(mentions_model_surface && mentions_auto_download) {
            continue;
        }

        let allowed_negative_or_schema_context = [
            "never auto-download",
            "does not auto-download",
            "did not auto-download",
            "no-auto-download",
            "auto_download_attempted",
            "auto_download_allowed",
            "skipped_auto_download_reason",
        ]
        .iter()
        .any(|marker| lower.contains(marker));

        assert!(
            allowed_negative_or_schema_context,
            "{surface}:{} implies semantic model auto-download instead of the explicit install contract: {line}",
            idx + 1
        );
    }
}

fn nearby_context(lines: &[&str], idx: usize, radius: usize) -> String {
    let start = idx.saturating_sub(radius);
    let end = (idx + radius + 1).min(lines.len());
    lines[start..end].join("\n").to_ascii_lowercase()
}

#[test]
fn current_docs_do_not_claim_semantic_models_auto_download() {
    let root = repo_root();
    let docs = [
        "AGENTS.md",
        "README.md",
        "docs/ROBOT_MODE.md",
        "docs/reference/QUICK_REFERENCE.md",
        "docs/reference/CASS_ARCHITECTURE_SUMMARY.txt",
    ];
    let forbidden = [
        "cass auto-downloads semantic model",
        "cass auto-downloads semantic models",
        "cass automatically downloads semantic model",
        "cass automatically downloads semantic models",
        "semantic models auto-download",
        "semantic model auto-downloads",
        "will auto-download semantic",
        "may auto-download semantic",
        "model auto-download",
        "auto-download ml model",
        "auto-downloaded ml model",
        "auto download semantic",
    ];

    for relative_path in docs {
        let text = read_doc(&root, relative_path);
        assert_absent(relative_path, &text, &forbidden);
        assert_no_semantic_auto_download_claim(relative_path, &text);
    }
}

#[test]
fn semantic_planning_auto_download_mentions_are_marked_historical() {
    let root = repo_root();
    let relative_path =
        "docs/planning/PLAN_TO_ADD_LIGHTWEIGHT_SEMANTIC_AND_HYBRID_SEARCH_TO_CASS.md";
    let text = read_doc(&root, relative_path);

    for required in [
        "historical design, superseded for model acquisition",
        "cass never auto-downloads models",
        "cass models install --from-file <dir>",
    ] {
        assert!(
            text.contains(required),
            "{relative_path} missing superseded-model-acquisition marker `{required}`"
        );
    }

    let lines: Vec<&str> = text.lines().collect();
    for (idx, line) in lines.iter().enumerate() {
        let lower = line.to_ascii_lowercase();
        let mentions_auto_download = lower.contains("auto-download")
            || lower.contains("auto_download")
            || lower.contains("cass_semantic_autodownload")
            || lower.contains("download-model")
            || lower.contains("tui-triggered download");
        if !mentions_auto_download {
            continue;
        }

        let context = nearby_context(&lines, idx, 2);
        let contextualized = [
            "historical",
            "superseded",
            "not current",
            "do not implement",
            "obsolete",
            "never auto-download",
            "does not auto-download",
            "current contract",
            "explicit install",
            "explicitly installed",
        ]
        .iter()
        .any(|marker| context.contains(marker));

        assert!(
            contextualized,
            "{relative_path}:{} mentions superseded auto-download behavior without historical/current-contract context: {line}",
            idx + 1
        );
    }
}

#[test]
fn cleanup_docs_do_not_make_quarantine_or_safe_to_gc_deletion_automatic() {
    let root = repo_root();
    let docs = [
        "AGENTS.md",
        "README.md",
        "docs/ROBOT_MODE.md",
        "docs/RECOVERY.md",
        "docs/planning/RECOVERY_RUNBOOK.md",
    ];
    let forbidden = [
        "safe_to_gc causes automatic deletion",
        "safe_to_gc causes deletion",
        "safe_to_gc automatically deletes",
        "safe_to_gc will delete",
        "safe_to_gc deletes",
        "quarantined evidence is automatically deleted",
        "quarantined artifacts are automatically deleted",
        "quarantine artifacts are automatically deleted",
        "hidden cleanup deletes archive",
        "hidden cleanup deletes archives",
        "cleanup automatically deletes archive",
        "cleanup automatically deletes quarantine",
        "cleanup automatically deletes quarantined",
        "automatic cleanup deletes archive",
        "automatically cleaned",
    ];

    for relative_path in docs {
        let text = read_doc(&root, relative_path);
        assert_absent(relative_path, &text, &forbidden);

        let lines: Vec<&str> = text.lines().collect();
        for (idx, line) in lines.iter().enumerate() {
            if !line.contains("safe_to_gc") {
                continue;
            }

            let context = nearby_context(&lines, idx, 1);
            assert!(
                context.contains("advisory"),
                "{relative_path}:{} mentions safe_to_gc without advisory context: {line}",
                idx + 1
            );
            assert!(
                context.contains("no automatic deletion")
                    || context.contains("not wired to any automatic deletion")
                    || context.contains("no automatic deletion path consumes it"),
                "{relative_path}:{} mentions safe_to_gc without a no-automatic-deletion contract: {line}",
                idx + 1
            );
        }
    }
}
