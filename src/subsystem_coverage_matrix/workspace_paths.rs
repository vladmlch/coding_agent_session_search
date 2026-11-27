//! Workspace-aware root and source directory resolution.
//!
//! During and after splitting the monolith into `crates/cass-*` workspace
//! members, `CARGO_MANIFEST_DIR` inside a member crate points to
//! `crates/<member>` instead of the repository root, and module source files
//! may live under either `src/` or `crates/<member>/src/`. These helpers
//! locate the workspace root and resolve repository-relative source paths
//! transparently in both layouts.

use std::fs;
use std::path::{Path, PathBuf};

fn cargo_toml_declares_workspace(cargo_toml: &Path) -> bool {
    let Ok(contents) = fs::read_to_string(cargo_toml) else {
        return false;
    };
    contents.lines().any(|line| {
        let header = line.trim_start().split('#').next().unwrap_or("").trim();
        header == "[workspace]" || (header.starts_with("[workspace.") && header.ends_with(']'))
    })
}

/// Resolve the repository/workspace root starting from a crate's
/// `CARGO_MANIFEST_DIR`.
///
/// Walks ancestors upward looking for the nearest `Cargo.toml` that declares a
/// `[workspace]` table. When none is found (the pre-workspace flat repository),
/// falls back to the nearest ancestor carrying `Cargo.toml` alongside
/// `Cargo.lock` or a `src/` directory, and finally to `manifest_dir` itself.
pub fn workspace_root_from_manifest_dir(manifest_dir: &Path) -> PathBuf {
    for ancestor in manifest_dir.ancestors() {
        let manifest = ancestor.join("Cargo.toml");
        if manifest.is_file() && cargo_toml_declares_workspace(&manifest) {
            return ancestor.to_path_buf();
        }
    }
    for ancestor in manifest_dir.ancestors() {
        let manifest = ancestor.join("Cargo.toml");
        if manifest.is_file()
            && (ancestor.join("Cargo.lock").is_file() || ancestor.join("src").is_dir())
        {
            return ancestor.to_path_buf();
        }
    }
    manifest_dir.to_path_buf()
}

/// Convenience helper returning the workspace root for the current crate's
/// `CARGO_MANIFEST_DIR`.
pub fn workspace_root() -> PathBuf {
    workspace_root_from_manifest_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
}

/// Return all Rust source directories in the workspace (`src/` and any
/// `crates/*/src/` directories), in deterministic order.
pub fn workspace_source_dirs(root: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let root_src = root.join("src");
    if root_src.is_dir() {
        dirs.push(root_src);
    }
    let crates_dir = root.join("crates");
    if let Ok(entries) = fs::read_dir(&crates_dir) {
        let mut crate_srcs = Vec::new();
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            let src_dir = entry.path().join("src");
            if src_dir.is_dir() {
                crate_srcs.push(src_dir);
            }
        }
        crate_srcs.sort();
        dirs.extend(crate_srcs);
    }
    dirs
}

/// Resolve a repository-relative path `rel` against `root`, checking
/// `crates/*/src/<rest>` when `rel` starts with `src/` and does not exist
/// directly at `root.join(rel)`.
pub fn resolve_workspace_source_file(root: &Path, rel: &str) -> PathBuf {
    let direct = root.join(rel);
    if direct.exists() {
        return direct;
    }
    if let Some(subpath) = rel
        .strip_prefix("src/")
        .or_else(|| rel.strip_prefix("src\\"))
    {
        for src_dir in workspace_source_dirs(root) {
            let candidate = src_dir.join(subpath);
            if candidate.exists() {
                return candidate;
            }
        }
    }
    direct
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TempRoot(PathBuf);

    impl TempRoot {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = loop {
                let candidate = std::env::temp_dir().join(format!(
                    "cass-workspace-paths-{}-{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ));
                match fs::create_dir(&candidate) {
                    Ok(()) => break candidate,
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(err) => panic!("create temp root: {err}"),
                }
            };
            Self(path)
        }

        fn write(&self, rel: &str, contents: &str) {
            let path = self.0.join(rel);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, contents).unwrap();
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn resolves_workspace_root_from_member_crate_dir() {
        let tmp = TempRoot::new();
        tmp.write("Cargo.toml", "[workspace]\nmembers = [\"crates/cass-model\"]\n");
        tmp.write("crates/cass-model/Cargo.toml", "[package]\nname = \"cass-model\"\n");
        let member_dir = tmp.0.join("crates/cass-model");
        assert_eq!(workspace_root_from_manifest_dir(&member_dir), tmp.0);
        assert_eq!(workspace_root_from_manifest_dir(&tmp.0), tmp.0);
    }

    #[test]
    fn resolves_flat_repo_root_and_ignores_workspace_comments() {
        let tmp = TempRoot::new();
        tmp.write(
            "Cargo.toml",
            "# [workspace]\n[package]\nname = \"coding-agent-search\"\n",
        );
        tmp.write("Cargo.lock", "version = 4\n");
        tmp.write("src/lib.rs", "");
        assert_eq!(workspace_root_from_manifest_dir(&tmp.0), tmp.0);
    }

    #[test]
    fn lists_source_dirs_and_resolves_moved_crate_files() {
        let tmp = TempRoot::new();
        tmp.write("src/lib.rs", "");
        tmp.write("crates/cass-b/src/b.rs", "");
        tmp.write("crates/cass-a/src/bookmarks.rs", "");

        let dirs = workspace_source_dirs(&tmp.0);
        assert_eq!(
            dirs,
            vec![
                tmp.0.join("src"),
                tmp.0.join("crates/cass-a/src"),
                tmp.0.join("crates/cass-b/src"),
            ]
        );

        assert_eq!(
            resolve_workspace_source_file(&tmp.0, "src/lib.rs"),
            tmp.0.join("src/lib.rs")
        );
        assert_eq!(
            resolve_workspace_source_file(&tmp.0, "src/bookmarks.rs"),
            tmp.0.join("crates/cass-a/src/bookmarks.rs")
        );
        assert_eq!(
            resolve_workspace_source_file(&tmp.0, "src/missing.rs"),
            tmp.0.join("src/missing.rs")
        );
    }
}
