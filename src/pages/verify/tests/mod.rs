mod path_security;
mod site_and_limits;

use super::integrity::check_integrity;
use crate::pages::bundle::{
    IntegrityEntry, IntegrityManifest, generate_integrity_manifest, write_pinned_vendor_assets,
};
use anyhow::Result;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Path to the pages_verify fixtures directory
fn fixtures_dir() -> PathBuf {
    crate::subsystem_coverage_matrix::workspace_paths::workspace_root()
        .join("tests/fixtures/pages_verify")
}

/// Copy a fixture directory to the destination.
/// `fixture_name` is the subdirectory under tests/fixtures/pages_verify/ (e.g., "valid", "unencrypted")
pub(super) fn copy_fixture(fixture_name: &str, dest: &Path) -> Result<()> {
    let src = fixtures_dir().join(fixture_name).join("site");
    let had_integrity_manifest = src.join("integrity.json").is_file();
    copy_dir_recursive(&src, dest)?;
    write_pinned_vendor_assets(dest)?;

    if had_integrity_manifest {
        let manifest = generate_integrity_manifest(dest)?;
        fs::write(
            dest.join("integrity.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )?;
    }

    Ok(())
}

/// Recursively copy a directory and its contents
fn copy_dir_recursive(src: &Path, dest: &Path) -> Result<()> {
    if !dest.exists() {
        fs::create_dir_all(dest)?;
    }
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let dest_path = dest.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &dest_path)?;
        } else {
            fs::copy(entry.path(), &dest_path)?;
        }
    }
    Ok(())
}

pub(super) fn assert_integrity_path_blocked(rel_path: &str) {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path();

    let mut files = BTreeMap::new();
    files.insert(
        rel_path.to_string(),
        IntegrityEntry {
            sha256: "deadbeef".repeat(8),
            size: 100,
        },
    );
    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2025-01-01T00:00:00Z".to_string(),
        files,
    };
    let manifest_json = serde_json::to_string(&manifest).unwrap();
    fs::write(site_dir.join("integrity.json"), manifest_json).unwrap();

    let result = check_integrity(site_dir, false);
    assert!(!result.passed, "Path should be blocked: {rel_path}");
    assert!(
        result
            .details
            .as_ref()
            .map(|d| d.contains("security violation"))
            .unwrap_or(false),
        "Should mention security violation"
    );
}
