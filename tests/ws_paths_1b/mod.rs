use std::path::{Path, PathBuf};

/// Resolve the workspace root directory transparently across both the
/// monolithic layout and `crates/*` workspace members.
#[allow(dead_code)]
pub fn workspace_root() -> PathBuf {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")));
    for ancestor in manifest_dir.ancestors() {
        let cargo_toml = ancestor.join("Cargo.toml");
        if cargo_toml.is_file()
            && std::fs::read_to_string(&cargo_toml)
                .is_ok_and(|body| body.lines().any(|line| line.trim_start().starts_with("[workspace]")))
        {
            return ancestor.to_path_buf();
        }
    }
    manifest_dir
}

/// Collect candidate Rust source root directories (`src/` and `crates/*/src/`).
#[allow(dead_code)]
pub fn workspace_source_dirs(root: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let root_src = root.join("src");
    if root_src.is_dir() {
        dirs.push(root_src);
    }
    let crates_dir = root.join("crates");
    if let Ok(entries) = std::fs::read_dir(&crates_dir) {
        let mut crate_srcs: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path().join("src"))
            .filter(|src_dir| src_dir.is_dir())
            .collect();
        crate_srcs.sort();
        dirs.extend(crate_srcs);
    }
    dirs
}

/// Resolve a legacy repo-relative source path (e.g. `"src/doctor_runs.rs"`)
/// across `src/` and `crates/*/src/`.
#[allow(dead_code)]
pub fn resolve_workspace_source(root: &Path, legacy_rel: &str) -> PathBuf {
    let direct = root.join(legacy_rel);
    if direct.exists() {
        return direct;
    }
    let rel_in_src = legacy_rel.strip_prefix("src/").unwrap_or(legacy_rel);
    for src_dir in workspace_source_dirs(root) {
        let candidate = src_dir.join(rel_in_src);
        if candidate.exists() {
            return candidate;
        }
    }
    direct
}
