use std::path::{Path, PathBuf};

/// Locate the repository / Cargo workspace root from `CARGO_MANIFEST_DIR`.
///
/// In the single-crate monolith (Wave 3.0), `CARGO_MANIFEST_DIR` is already the
/// workspace root. Once subcrates live under `crates/`, walking ancestors finds
/// the root `Cargo.toml` that declares `[workspace]`.
#[allow(dead_code)]
pub fn workspace_root() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    for ancestor in manifest_dir.ancestors() {
        let cargo_toml = ancestor.join("Cargo.toml");
        if cargo_toml.is_file()
            && let Ok(contents) = std::fs::read_to_string(&cargo_toml)
            && contents.lines().any(|line| line.trim() == "[workspace]")
        {
            return ancestor.to_path_buf();
        }
    }
    manifest_dir.to_path_buf()
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
    let crates_dir = root.join("crates");
    if crates_dir.is_dir()
        && let Ok(entries) = std::fs::read_dir(&crates_dir)
    {
        let mut crate_dirs: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect();
        crate_dirs.sort();
        for crate_dir in crate_dirs {
            let crate_src = crate_dir.join("src");
            if crate_src.is_dir() {
                dirs.push(crate_src);
            }
        }
    }
    dirs
}

/// Read a workspace source file given its legacy root-relative path (e.g.
/// `"src/doctor.rs"`), searching the root `src/` first and then `crates/*/src/`.
#[allow(dead_code)]
pub fn read_workspace_source(legacy_rel: &str) -> String {
    let root = workspace_root();
    let trimmed = legacy_rel.trim_start_matches("./");
    let direct = root.join(trimmed);
    if direct.is_file() {
        return std::fs::read_to_string(&direct)
            .unwrap_or_else(|e| panic!("read {}: {e}", direct.display()));
    }
    let rel_under_src = trimmed.strip_prefix("src/").unwrap_or(trimmed);
    for src_dir in workspace_source_dirs() {
        let candidate = src_dir.join(rel_under_src);
        if candidate.is_file() {
            return std::fs::read_to_string(&candidate)
                .unwrap_or_else(|e| panic!("read {}: {e}", candidate.display()));
        }
    }
    panic!(
        "workspace source file not found for {legacy_rel:?} under {}",
        root.display()
    );
}
