use super::paths::{collect_all_files, compute_file_hash, detect_encoded_path_violation};
use super::types::CheckResult;
use crate::pages::bundle::IntegrityManifest;
use anyhow::Context;
use serde_json::Value;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;

/// Integrity manifest schema understood by this verifier.
pub(super) const INTEGRITY_MANIFEST_VERSION: u8 = 1;

/// Files that indicate secret leakage
const SECRET_FILES: &[&str] = &[
    "recovery-secret.txt",
    "qr-code.png",
    "qr-code.svg",
    "master-key.json",
];

/// Directories that should not exist in site/
const SECRET_DIRS: &[&str] = &["private"];

/// JSON keys in config.json that indicate plaintext secret leakage.
const FORBIDDEN_CONFIG_KEYS: &[(&str, &str)] = &[
    ("password", "password field"),
    ("secret", "secret field"),
    ("private_key", "private_key field"),
    ("master_key", "master_key field"),
    ("recovery_secret", "recovery_secret"),
];

/// Check integrity.json hashes match file contents
pub(super) fn check_integrity(site_dir: &Path, verbose: bool) -> CheckResult {
    let integrity_path = site_dir.join("integrity.json");

    let manifest: IntegrityManifest = match File::open(&integrity_path)
        .context("Failed to open integrity.json")
        .and_then(|f| serde_json::from_reader(BufReader::new(f)).context("Failed to parse JSON"))
    {
        Ok(m) => m,
        Err(e) => return CheckResult::fail(format!("Failed to parse integrity.json: {}", e)),
    };

    if manifest.version != INTEGRITY_MANIFEST_VERSION {
        return CheckResult::fail(format!(
            "Unsupported integrity manifest version {}; expected {}",
            manifest.version, INTEGRITY_MANIFEST_VERSION
        ));
    }

    let mut errors = Vec::new();
    let mut checked_files: HashSet<&str> = HashSet::new();
    let canonical_site = match site_dir.canonicalize() {
        Ok(path) => path,
        Err(e) => {
            return CheckResult::fail(format!(
                "Failed to resolve site directory for integrity checks: {}",
                e
            ));
        }
    };

    // Verify each file in manifest
    for (rel_path, entry) in &manifest.files {
        checked_files.insert(rel_path.as_str());

        if let Some(reason) = detect_encoded_path_violation(rel_path) {
            errors.push(format!(
                "integrity.json contains {reason} (security violation): {}",
                rel_path
            ));
            continue;
        }

        // Security: Validate path doesn't escape site_dir via traversal
        let path = Path::new(rel_path);
        if path.is_absolute() {
            errors.push(format!(
                "integrity.json contains absolute path (security violation): {}",
                rel_path
            ));
            continue;
        }
        if path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            errors.push(format!(
                "integrity.json contains path traversal (security violation): {}",
                rel_path
            ));
            continue;
        }

        let file_path = site_dir.join(rel_path); // ubs:ignore[rust.security.path-traversal] — rel_path validated at lines 69-95 and canonicalized below
        let metadata = match fs::symlink_metadata(&file_path) {
            Ok(meta) => meta,
            Err(_) => {
                errors.push(format!("File in manifest but missing: {}", rel_path));
                continue;
            }
        };

        let file_type = metadata.file_type();

        if !file_type.is_file() && !file_type.is_symlink() {
            errors.push(format!(
                "integrity.json references non-file entry (security violation): {}",
                rel_path
            ));
            continue;
        }

        // Resolve symlinks and ensure final target remains within site_dir.
        let canonical_file = match file_path.canonicalize() {
            Ok(path) => path,
            Err(_) => {
                errors.push(format!("File in manifest but missing: {}", rel_path));
                continue;
            }
        };
        if !canonical_file.starts_with(&canonical_site) {
            errors.push(format!(
                "integrity.json path escapes site directory (security violation): {}",
                rel_path
            ));
            continue;
        }

        // For symlinks, only permit links to regular files within site_dir.
        if file_type.is_symlink() {
            match fs::metadata(&file_path) {
                Ok(target_meta) if target_meta.file_type().is_file() => {}
                Ok(_) => {
                    errors.push(format!(
                        "integrity.json symlink target is not a regular file (security violation): {}",
                        rel_path
                    ));
                    continue;
                }
                Err(e) => {
                    errors.push(format!(
                        "Failed to resolve symlink target for {}: {}",
                        rel_path, e
                    ));
                    continue;
                }
            }
        }

        // Fast-fail on size mismatch before the expensive SHA256 hash.
        // Use the canonical path so symlinks resolve to the actual target size.
        if let Ok(actual_meta) = fs::metadata(&canonical_file)
            && actual_meta.len() != entry.size
        {
            errors.push(format!(
                "Size mismatch for {}: expected {}, got {}",
                rel_path,
                entry.size,
                actual_meta.len()
            ));
            continue;
        }

        // Compute hash
        let computed_hash = match compute_file_hash(&file_path) {
            Ok(h) => h,
            Err(e) => {
                errors.push(format!("Failed to hash {}: {}", rel_path, e));
                continue;
            }
        };

        if computed_hash != entry.sha256 {
            errors.push(format!(
                "Hash mismatch for {}: expected {}, got {}",
                rel_path, entry.sha256, computed_hash
            ));
        } else if verbose {
            println!("    ✓ {}", rel_path);
        }
    }

    // Check for extra files not in manifest
    let actual_files = match collect_all_files(site_dir) {
        Ok(files) => files,
        Err(e) => return CheckResult::fail(format!("Failed to enumerate files: {}", e)),
    };
    for file in actual_files {
        // Skip integrity.json itself
        if file == "integrity.json" {
            continue;
        }
        if !checked_files.contains(file.as_str()) {
            errors.push(format!("File not in manifest: {}", file));
        }
    }

    if errors.is_empty() {
        CheckResult::pass()
    } else {
        CheckResult::fail(errors.join("; "))
    }
}

/// Check for secret leakage in site/
pub(super) fn check_no_secrets(site_dir: &Path) -> CheckResult {
    let mut errors = Vec::new();

    // Detect forbidden artifacts at every depth. ASCII-case-insensitive matching
    // prevents trivial case changes from bypassing this gate on case-sensitive
    // hosts while matching the behavior users see on common case-folding hosts.
    find_secrets_recursive(site_dir, site_dir, &mut errors);

    // Check config.json doesn't contain plaintext secrets.
    // Walk the parsed JSON tree instead of doing brittle raw substring checks so
    // formatting changes like `"secret" : "..."` or nested objects can't hide leakage.
    let config_path = site_dir.join("config.json");
    if config_path.exists()
        && let Ok(content) = fs::read_to_string(&config_path)
        && let Ok(config_json) = serde_json::from_str::<Value>(&content)
    {
        find_forbidden_config_keys(&config_json, "", &mut errors);
    }

    if errors.is_empty() {
        CheckResult::pass()
    } else {
        CheckResult::fail(errors.join("; "))
    }
}

fn matches_forbidden_name(name: &str, forbidden_names: &[&str]) -> bool {
    forbidden_names
        .iter()
        .any(|forbidden| name.eq_ignore_ascii_case(forbidden))
}

fn find_forbidden_config_keys(value: &Value, current_path: &str, findings: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let child_path = if current_path.is_empty() {
                    key.clone()
                } else {
                    format!("{current_path}.{key}")
                };
                if let Some((_, description)) = FORBIDDEN_CONFIG_KEYS
                    .iter()
                    .find(|(forbidden, _)| key.eq_ignore_ascii_case(forbidden))
                {
                    findings.push(format!(
                        "config.json contains forbidden field: {} at {}",
                        description, child_path
                    ));
                }
                find_forbidden_config_keys(child, &child_path, findings);
            }
        }
        Value::Array(items) => {
            for (idx, child) in items.iter().enumerate() {
                let child_path = if current_path.is_empty() {
                    format!("[{idx}]")
                } else {
                    format!("{current_path}[{idx}]")
                };
                find_forbidden_config_keys(child, &child_path, findings);
            }
        }
        _ => {}
    }
}

/// Recursively scan a directory tree for secret files and directories.
/// Finds entries whose name (not full path) matches SECRET_FILES or SECRET_DIRS
/// at any depth, catching secrets hidden in subdirectories.
fn find_secrets_recursive(base: &Path, current: &Path, findings: &mut Vec<String>) {
    let entries = match fs::read_dir(current) {
        Ok(entries) => entries,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        let name = match entry.file_name().to_str() {
            Some(n) => n.to_string(),
            None => continue,
        };
        let is_secret_file = matches_forbidden_name(&name, SECRET_FILES);
        let is_secret_dir = matches_forbidden_name(&name, SECRET_DIRS);

        let rel_path = path
            .strip_prefix(base)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");

        if is_secret_dir {
            if current == base {
                findings.push(format!("Secret directory found in site/: {rel_path}/"));
            } else {
                findings.push(format!(
                    "Secret directory found in site subdirectory: {rel_path}/"
                ));
            }
        } else if is_secret_file {
            if current == base {
                findings.push(format!("Secret file found in site/: {rel_path}"));
            } else {
                findings.push(format!(
                    "Secret file found in site subdirectory: {rel_path}"
                ));
            }
        }

        if file_type.is_dir() {
            // Only recurse into real directories. Symlinked directories are handled below
            // so a malicious or accidental loop cannot drag verification outside site/.
            find_secrets_recursive(base, &path, findings);
        }
    }
}
