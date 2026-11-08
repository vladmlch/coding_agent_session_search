use std::path::{Path, PathBuf};

pub fn default_db_path() -> PathBuf {
    default_data_dir().join("agent_search.db")
}

/// Resolve the effective data dir, honoring an explicit `--db` override
/// (GH #403).
///
/// Historically `--db` overrode only the SQLite path while every derived
/// asset — lexical/Tantivy index, raw-mirror, checkpoints, `index-run.lock` —
/// still resolved to the live data dir. A command that read as fully
/// sandboxed (`cass --db /scratch/probe.db index --full`) therefore wrote a
/// foreign database's lexical fingerprint into the LIVE install's checkpoint
/// and contended for the live install's locks.
///
/// Rules, in order:
/// 1. An explicit data-dir override (`--data-dir` / per-command flag) always
///    wins, preserving any deliberate split layout.
/// 2. Otherwise, when `--db` points OUTSIDE the default data dir, the db
///    file's parent directory becomes the data dir — the exact inverse of
///    the `data_dir.join("agent_search.db")` fallback — so a scratch db is
///    fully isolated.
/// 3. `--db <default_data_dir>/agent_search.db`, or no `--db` at all, keeps
///    the default data dir: existing setups are untouched.
pub fn resolve_data_dir(
    data_dir_override: &Option<PathBuf>,
    db_override: Option<&PathBuf>,
) -> PathBuf {
    if let Some(dir) = data_dir_override {
        return dir.clone();
    }
    let default_dir = default_data_dir();
    if let Some(parent) = db_override
        .and_then(|db| db.parent())
        .filter(|p| !p.as_os_str().is_empty())
        && !same_directory(parent, &default_dir)
    {
        return parent.to_path_buf();
    }
    default_dir
}

/// Best-effort path identity: canonicalize both sides when possible (resolves
/// symlinks and relative segments), fall back to the literal paths otherwise
/// (e.g. the default data dir does not exist yet on a fresh install).
pub(crate) fn same_directory(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(ca), Ok(cb)) => ca == cb,
        _ => false,
    }
}

pub fn default_data_dir() -> PathBuf {
    default_data_dir_with_source().0
}

/// The default data dir and what chose it: `env:CASS_DATA_DIR`,
/// `env:XDG_DATA_HOME`, or `default` (the platform data dir).
pub(crate) fn default_data_dir_with_source() -> (PathBuf, &'static str) {
    if let Ok(dir) = dotenvy::var("CASS_DATA_DIR") {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            return (PathBuf::from(trimmed), "env:CASS_DATA_DIR");
        }
    }
    if let Ok(dir) = dotenvy::var("XDG_DATA_HOME") {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            return (
                PathBuf::from(trimmed).join("coding-agent-search"),
                "env:XDG_DATA_HOME",
            );
        }
    }
    let dir = directories::ProjectDirs::from("com", "coding-agent-search", "coding-agent-search")
        .map(|p| p.data_dir().to_path_buf())
        .or_else(|| dirs::home_dir().map(|h| h.join(".coding-agent-search")))
        .unwrap_or_else(|| PathBuf::from("./data"));
    (dir, "default")
}

#[cfg(test)]
pub(crate) mod resolve_data_dir_tests;
pub(crate) mod session_paths;
