//! Shared workspace path resolution helpers for WS-PREP-1C integration tests.

use std::fs;
use std::path::{Path, PathBuf};

/// Return the workspace/repository root regardless of whether tests run from
/// the monolith root or a member crate under `crates/*`.
// This shared module is compiled separately into several integration targets;
// each target uses only a subset of its API, so targeted dead-code allowances
// stay with the individual helpers instead of suppressing the whole module.
#[allow(dead_code)]
pub fn workspace_root() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));

    for ancestor in manifest_dir.ancestors() {
        let cargo_toml = ancestor.join("Cargo.toml");
        if cargo_toml.is_file() {
            let has_workspace_section = fs::read_to_string(&cargo_toml)
                .map(|contents| contents.lines().any(|line| line.trim() == "[workspace]"))
                .unwrap_or(false);
            if has_workspace_section || ancestor.join("crates").is_dir() {
                return ancestor.to_path_buf();
            }
        }
    }

    for ancestor in manifest_dir.ancestors() {
        if ancestor.join("Cargo.toml").is_file() && ancestor.join("Cargo.lock").is_file() {
            return ancestor.to_path_buf();
        }
    }

    manifest_dir.to_path_buf()
}

fn sorted_crate_dirs(root: &Path) -> Vec<PathBuf> {
    let crates_dir = root.join("crates");
    if !crates_dir.is_dir() {
        return Vec::new();
    }
    let mut dirs: Vec<PathBuf> = fs::read_dir(&crates_dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.is_dir())
                .collect()
        })
        .unwrap_or_default();
    dirs.sort();
    dirs
}

/// Return all Rust source directories in the workspace (`src/` and `crates/*/src/`).
#[allow(dead_code)]
pub fn workspace_source_dirs() -> Vec<PathBuf> {
    let root = workspace_root();
    let mut dirs = Vec::new();

    let root_src = root.join("src");
    if root_src.is_dir() {
        dirs.push(root_src);
    }

    for crate_dir in sorted_crate_dirs(&root) {
        let crate_src = crate_dir.join("src");
        if crate_src.is_dir() {
            dirs.push(crate_src);
        }
    }

    dirs
}

/// Resolve a workspace-relative source path (for example `"src/model/cli_error_kind.rs"`
/// or `"src/cli_root"`) in either the monolith layout or `crates/*/`.
#[allow(dead_code)]
pub fn resolve_workspace_source(rel_path: &str) -> PathBuf {
    let root = workspace_root();
    let direct = root.join(rel_path);
    if direct.exists() {
        return direct;
    }

    let sub = rel_path.strip_prefix("src/").unwrap_or(rel_path);
    for crate_dir in sorted_crate_dirs(&root) {
        let in_src = crate_dir.join("src").join(sub);
        if in_src.exists() {
            return in_src;
        }
        let in_crate = crate_dir.join(sub);
        if in_crate.exists() {
            return in_crate;
        }
    }

    direct
}

/// Read an embedded web asset from `src/pages_assets/<name>` or the extracted
/// `crates/cass-pages/` locations.
#[allow(dead_code)]
pub fn read_pages_asset(name: &str) -> String {
    let root = workspace_root();
    let candidates = [
        root.join("src").join("pages_assets").join(name),
        root.join("crates")
            .join("cass-pages")
            .join("pages_assets")
            .join(name),
        root.join("crates")
            .join("cass-pages")
            .join("src")
            .join("pages_assets")
            .join(name),
    ];
    for candidate in &candidates {
        if candidate.is_file() {
            return fs::read_to_string(candidate).unwrap_or_else(|err| {
                panic!(
                    "failed to read pages asset {name} at {}: {err}",
                    candidate.display()
                )
            });
        }
    }
    panic!(
        "pages asset {name} not found in any expected location: {:?}",
        candidates
    );
}
