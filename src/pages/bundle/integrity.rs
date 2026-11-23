use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::Path;

/// Integrity entry for a single file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityEntry {
    /// SHA256 hash as hex string
    pub sha256: String,
    /// File size in bytes
    pub size: u64,
}

/// Full integrity manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityManifest {
    /// Schema version for integrity format
    pub version: u8,
    /// Generated timestamp
    pub generated_at: String,
    /// Map of relative path -> integrity entry
    pub files: BTreeMap<String, IntegrityEntry>,
}

/// Generate integrity manifest for all files in a directory
pub fn generate_integrity_manifest(dir: &Path) -> Result<IntegrityManifest> {
    let mut files = BTreeMap::new();

    collect_file_hashes(dir, dir, &mut files)?;

    Ok(IntegrityManifest {
        version: 1,
        generated_at: Utc::now().to_rfc3339(),
        files,
    })
}

/// Recursively collect SHA256 hashes of all files
pub(super) fn collect_file_hashes(
    base_dir: &Path,
    current_dir: &Path,
    files: &mut BTreeMap<String, IntegrityEntry>,
) -> Result<()> {
    let canonical_base_dir = base_dir.canonicalize().with_context(|| {
        format!(
            "Failed to resolve site directory {} while generating integrity manifest",
            base_dir.display()
        )
    })?;
    collect_file_hashes_recursive(base_dir, current_dir, &canonical_base_dir, files)
}

fn collect_file_hashes_recursive(
    base_dir: &Path,
    current_dir: &Path,
    canonical_base_dir: &Path,
    files: &mut BTreeMap<String, IntegrityEntry>,
) -> Result<()> {
    for entry in fs::read_dir(current_dir)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        let file_type = metadata.file_type();
        let rel_path = path.strip_prefix(base_dir)?;
        let rel_str = rel_path.to_string_lossy().replace('\\', "/");

        // Skip integrity.json itself (chicken/egg)
        if rel_str == "integrity.json" {
            continue;
        }

        if file_type.is_dir() {
            collect_file_hashes_recursive(base_dir, &path, canonical_base_dir, files)?;
        } else if file_type.is_symlink() {
            let canonical_target = path.canonicalize().with_context(|| {
                format!(
                    "Failed to resolve symlink {} while generating integrity manifest",
                    rel_str
                )
            })?;
            if !canonical_target.starts_with(canonical_base_dir) {
                bail!(
                    "Refusing to include symlink outside site directory in integrity manifest: {}",
                    rel_str
                );
            }

            let target_meta = fs::metadata(&path).with_context(|| {
                format!(
                    "Failed to read symlink target metadata for {} while generating integrity manifest",
                    rel_str
                )
            })?;
            if !target_meta.is_file() {
                bail!(
                    "Refusing to include symlink that does not point to a regular file in integrity manifest: {}",
                    rel_str
                );
            }

            files.insert(rel_str, build_integrity_entry(&path)?);
        } else if file_type.is_file() {
            files.insert(rel_str, build_integrity_entry(&path)?);
        }
    }

    Ok(())
}

pub(super) fn build_integrity_entry(path: &Path) -> Result<IntegrityEntry> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    let size = metadata.len();

    let mut hasher = Sha256::new();
    let mut reader = BufReader::new(file);
    let mut buffer = [0u8; 8192];

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(IntegrityEntry {
        // sha2 ≥ 0.11 dropped `LowerHex` for the `Output` GenericArray;
        // route through `hex::encode` for the same lowercase-hex wire
        // format.
        sha256: hex::encode(hasher.finalize()),
        size,
    })
}

/// Compute a short fingerprint from the integrity manifest
pub fn compute_fingerprint(manifest: &IntegrityManifest) -> String {
    // Compute a fingerprint by hashing the sorted list of file hashes
    let mut hasher = Sha256::new();

    for (path, entry) in &manifest.files {
        hasher.update(path.as_bytes());
        hasher.update(entry.sha256.as_bytes());
    }

    let hash = hasher.finalize();

    // Return first 16 hex chars as fingerprint. `hex::encode` replaces the
    // pre-sha2-0.11 `format!("{:x}", hash)` path (Output no longer
    // implements `LowerHex`).
    hex::encode(hash)[..16].to_string()
}
