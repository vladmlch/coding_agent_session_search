use super::types::CheckResult;
use crate::pages::bundle::{UnencryptedConfig, validate_pinned_vendor_assets};
use anyhow::Context;
use serde_json::{Map, Value};
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;

/// Browser runtime ceiling for an unencrypted archive payload.
pub(super) const MAX_BROWSER_ARCHIVE_PLAINTEXT_SIZE: u64 = 512 * 1024 * 1024;

/// Required files that must exist in site/
const REQUIRED_FILES: &[&str] = &[
    "index.html",
    "config.json",
    "sw.js",
    "viewer.js",
    "auth.js",
    "styles.css",
    "robots.txt",
    ".nojekyll",
];

const UNENCRYPTED_CONFIG_KEYS: &[&str] = &["encrypted", "version", "payload", "warning"];
const UNENCRYPTED_PAYLOAD_KEYS: &[&str] = &["path", "format", "size_bytes"];

/// Check that all required files exist
pub(super) fn check_required_files(site_dir: &Path) -> CheckResult {
    let mut missing = Vec::new();
    let mut invalid = Vec::new();

    for file in REQUIRED_FILES {
        let path = site_dir.join(file);
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                let file_type = metadata.file_type();
                if file_type.is_file() {
                    continue;
                }
                invalid.push(format!("{file} (must be a regular file)"));
            }
            Err(_) => missing.push(*file),
        }
    }

    // Also check payload/ directory exists
    if !site_dir.join("payload").is_dir() {
        missing.push("payload/");
    }

    if let Err(error) = validate_pinned_vendor_assets(site_dir) {
        invalid.push(format!("vendor runtime ({error:#})"));
    }

    if missing.is_empty() && invalid.is_empty() {
        CheckResult::pass()
    } else {
        let mut parts = Vec::new();
        if !missing.is_empty() {
            parts.push(format!("Missing files: {}", missing.join(", ")));
        }
        if !invalid.is_empty() {
            parts.push(format!("Invalid required files: {}", invalid.join(", ")));
        }
        CheckResult::fail(parts.join("; "))
    }
}

/// Check config.json schema validity
pub(super) fn check_config_schema(site_dir: &Path) -> CheckResult {
    let config_path = site_dir.join("config.json");

    let content = match fs::read_to_string(&config_path).context("Failed to read config.json") {
        Ok(content) => content,
        Err(e) => return CheckResult::fail(format!("Failed to read config.json: {}", e)),
    };

    let config_json: Value =
        match serde_json::from_str(&content).context("Failed to parse JSON syntax") {
            Ok(json) => json,
            Err(e) => return CheckResult::fail(format!("Failed to parse config.json: {}", e)),
        };

    let unknown_field_errors = find_unknown_config_fields(&config_json);
    if !unknown_field_errors.is_empty() {
        return CheckResult::fail(unknown_field_errors.join("; "));
    }

    let config: UnencryptedConfig = match serde_json::from_value(config_json) {
        Ok(c) => c,
        Err(e) => return CheckResult::fail(format!("Failed to parse config.json: {}", e)),
    };

    let errors = validate_unencrypted_config(&config);

    if errors.is_empty() {
        CheckResult::pass()
    } else {
        CheckResult::fail(errors.join("; "))
    }
}

fn find_unknown_config_fields(value: &Value) -> Vec<String> {
    let mut errors = Vec::new();
    let Some(root) = value.as_object() else {
        return errors;
    };

    collect_unknown_fields(root, UNENCRYPTED_CONFIG_KEYS, "", &mut errors);
    if let Some(payload) = root.get("payload").and_then(Value::as_object) {
        collect_unknown_fields(payload, UNENCRYPTED_PAYLOAD_KEYS, "payload", &mut errors);
    }

    errors
}

fn collect_unknown_fields(
    object: &Map<String, Value>,
    allowed_keys: &[&str],
    current_path: &str,
    errors: &mut Vec<String>,
) {
    for key in object.keys() {
        if !allowed_keys.contains(&key.as_str()) {
            let path = if current_path.is_empty() {
                key.clone()
            } else {
                format!("{current_path}.{key}")
            };
            errors.push(format!("config.json contains unknown field: {path}"));
        }
    }
}

fn validate_unencrypted_config(config: &UnencryptedConfig) -> Vec<String> {
    let mut errors = Vec::new();

    if config.encrypted {
        errors.push("unencrypted config must set encrypted=false".to_string());
    }

    if config.version.trim().is_empty() {
        errors.push("version cannot be empty".to_string());
    }

    if config.payload.path.trim().is_empty() {
        errors.push("payload.path cannot be empty".to_string());
    } else {
        let path = Path::new(&config.payload.path);
        validate_payload_path(&mut errors, "payload.path", path);
        validate_browser_payload_url_path(&mut errors, "payload.path", &config.payload.path);
    }

    let valid_formats = ["sqlite"];
    if !valid_formats.contains(&config.payload.format.as_str()) {
        errors.push(format!(
            "payload.format should be one of {:?}, got '{}'",
            valid_formats, config.payload.format
        ));
    }

    match config.payload.size_bytes {
        None => errors.push("payload.size_bytes is required for bounded browser loading".to_string()),
        Some(0) => errors.push("payload.size_bytes must be greater than zero".to_string()),
        Some(size) if size > MAX_BROWSER_ARCHIVE_PLAINTEXT_SIZE => errors.push(format!(
            "payload.size_bytes {size} exceeds browser runtime limit {MAX_BROWSER_ARCHIVE_PLAINTEXT_SIZE}"
        )),
        Some(_) => {}
    }

    errors
}

pub(super) fn validate_payload_path(errors: &mut Vec<String>, label: &str, path: &Path) -> bool {
    let mut ok = true;
    if path.is_absolute() {
        errors.push(format!("{label} must be relative"));
        ok = false;
    }
    if path
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        errors.push(format!("{label} must not contain '..'"));
        ok = false;
    }
    if !path.starts_with("payload") {
        errors.push(format!("{label} must reside under payload/"));
        ok = false;
    }
    ok
}

/// Mirror the browser's URL-path checks for an unencrypted payload.
///
/// `Path` validation alone is insufficient here: filesystems normalize empty
/// and `.` components, while URL fetches interpret percent escapes and reserve
/// query/fragment delimiters. A bundle must not verify successfully when the
/// shipped browser loader will reject it or request a different resource.
fn validate_browser_payload_url_path(errors: &mut Vec<String>, label: &str, raw_path: &str) {
    if raw_path.contains(['?', '#', '\\', '%']) {
        errors.push(format!(
            "{label} contains URL query, fragment, backslash, or percent-escape characters"
        ));
    }

    let segments = raw_path.split('/').collect::<Vec<_>>();
    if segments.len() < 2 {
        errors.push(format!("{label} must reference a file under payload/"));
    }

    for segment in segments {
        if segment.is_empty() || segment == "." || segment == ".." {
            errors.push(format!(
                "{label} contains an empty or traversal URL segment"
            ));
        }
    }
}

/// Check payload manifest validity
pub(super) fn check_payload_manifest(site_dir: &Path) -> CheckResult {
    let config_path = site_dir.join("config.json");
    let payload_dir = site_dir.join("payload");

    if !payload_dir.exists() {
        return CheckResult::fail("payload/ directory not found");
    }

    // Parse config for expected payload
    let config: UnencryptedConfig = match File::open(&config_path)
        .and_then(|f| Ok(serde_json::from_reader(BufReader::new(f))?))
    {
        Ok(c) => c,
        Err(_) => return CheckResult::fail("Could not parse config.json"),
    };

    let mut errors = Vec::new();
    let rel_path = Path::new(&config.payload.path);
    if validate_payload_path(&mut errors, "payload.path", rel_path) {
        let payload_path = site_dir.join(rel_path);
        match fs::symlink_metadata(&payload_path) {
            Ok(meta) => {
                let file_type = meta.file_type();
                if file_type.is_symlink() {
                    errors.push(format!("{} must not be a symlink", config.payload.path));
                } else if !file_type.is_file() {
                    errors.push(format!("{} must be a regular file", config.payload.path));
                } else if let Some(expected) = config.payload.size_bytes
                    && expected != meta.len()
                {
                    errors.push(format!(
                        "{} size does not match payload.size_bytes (actual {}, declared {})",
                        config.payload.path,
                        meta.len(),
                        expected
                    ));
                }
            }
            Err(_) => errors.push(format!("Missing payload file: {}", config.payload.path)),
        }
    }

    if errors.is_empty() {
        CheckResult::pass()
    } else {
        CheckResult::fail(errors.join("; "))
    }
}

/// Check size limits for payload files
pub(super) fn check_size_limits(site_dir: &Path) -> CheckResult {
    let mut errors = Vec::new();

    let config_path = site_dir.join("config.json");
    let config: UnencryptedConfig = match File::open(&config_path)
        .context("Failed to open config.json")
        .and_then(|f| serde_json::from_reader(BufReader::new(f)).context("Failed to parse JSON"))
    {
        Ok(c) => c,
        Err(e) => {
            return CheckResult::fail(format!("Failed to parse config.json: {}", e));
        }
    };

    let payload_path = Path::new(&config.payload.path);
    if validate_payload_path(&mut errors, "payload.path", payload_path) {
        let payload_path = site_dir.join(payload_path);
        if !payload_path.exists() {
            errors.push(format!(
                "payload file not found for size check: {}",
                config.payload.path
            ));
        } else {
            match fs::symlink_metadata(&payload_path) {
                Ok(meta) => {
                    let file_type = meta.file_type();
                    if file_type.is_symlink() {
                        errors.push(format!("{} must not be a symlink", config.payload.path));
                    } else if !file_type.is_file() {
                        errors.push(format!("{} must be a regular file", config.payload.path));
                    } else if meta.len() > MAX_BROWSER_ARCHIVE_PLAINTEXT_SIZE {
                        errors.push(format!(
                            "{} exceeds browser runtime limit ({} > {} bytes)",
                            config.payload.path,
                            meta.len(),
                            MAX_BROWSER_ARCHIVE_PLAINTEXT_SIZE
                        ));
                    }
                }
                Err(err) => errors.push(format!(
                    "failed to stat payload file {}: {}",
                    config.payload.path, err
                )),
            }
        }
    }

    if errors.is_empty() {
        CheckResult::pass()
    } else {
        CheckResult::fail(errors.join("; "))
    }
}
