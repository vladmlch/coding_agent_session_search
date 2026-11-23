#[cfg(unix)]
use super::super::checks::check_required_files;
use super::super::checks::{
    MAX_BROWSER_ARCHIVE_PLAINTEXT_SIZE, check_config_schema, check_payload_manifest,
    check_size_limits,
};
use super::super::integrity::{INTEGRITY_MANIFEST_VERSION, check_integrity, check_no_secrets};
use super::super::paths::compute_file_hash;
#[cfg(unix)]
use super::super::paths::{calculate_dir_size, collect_all_files};
use super::super::{ensure_valid_bundle, verify_bundle, with_verified_bundle_for_deployment};
use super::copy_fixture;
use crate::pages::bundle::{IntegrityEntry, IntegrityManifest};
use anyhow::Result;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::BufReader;
use tempfile::TempDir;

#[test]
#[cfg(unix)]
fn test_collect_all_files_lists_symlink_without_recursing() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();

    fs::write(temp.path().join("root.txt"), "root").unwrap();
    fs::create_dir_all(outside.path().join("nested")).unwrap();
    fs::write(outside.path().join("nested/hidden.txt"), "hidden").unwrap();
    symlink(
        outside.path().join("nested"),
        temp.path().join("linked-dir"),
    )
    .unwrap();

    let files = collect_all_files(temp.path()).unwrap();
    assert!(files.contains(&"root.txt".to_string()));
    assert!(files.contains(&"linked-dir".to_string()));
    assert!(!files.iter().any(|f| f.starts_with("linked-dir/")));
}

#[test]
#[cfg(unix)]
fn test_calculate_dir_size_skips_symlink_targets() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();

    fs::write(temp.path().join("small.txt"), vec![0u8; 8]).unwrap();
    fs::write(outside.path().join("large.bin"), vec![0u8; 8192]).unwrap();
    symlink(
        outside.path().join("large.bin"),
        temp.path().join("linked.bin"),
    )
    .unwrap();

    let size = calculate_dir_size(temp.path()).unwrap();
    assert_eq!(size, 8);
}

#[test]
#[cfg(unix)]
fn test_integrity_rejects_symlink_manifest_entry_to_directory() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let site_dir = temp.path();
    fs::create_dir_all(site_dir.join("payload/real-dir")).unwrap();
    fs::write(site_dir.join("payload/real-dir/content.txt"), b"payload").unwrap();
    symlink(
        site_dir.join("payload/real-dir"),
        site_dir.join("payload/alias-dir"),
    )
    .unwrap();

    let mut files = BTreeMap::new();
    files.insert(
        "payload/alias-dir".to_string(),
        IntegrityEntry {
            // Hash/size are irrelevant here; verification should fail before hashing.
            sha256: "deadbeef".repeat(8),
            size: 0,
        },
    );
    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2025-01-01T00:00:00Z".to_string(),
        files,
    };
    fs::write(
        site_dir.join("integrity.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();

    let result = check_integrity(site_dir, false);
    assert!(!result.passed);
    assert!(
        result
            .details
            .as_ref()
            .map(|d| d.contains("not a regular file"))
            .unwrap_or(false)
    );
}

#[test]
fn test_verify_minimal_valid_site() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");

    copy_fixture("unencrypted", &site_dir).unwrap();

    let result = verify_bundle(&site_dir, true).unwrap();

    // Debug: print which checks failed
    if !result.checks.required_files.passed {
        eprintln!(
            "FAILED: required_files - {:?}",
            result.checks.required_files.details
        );
    }
    if !result.checks.config_schema.passed {
        eprintln!(
            "FAILED: config_schema - {:?}",
            result.checks.config_schema.details
        );
    }
    if !result.checks.payload_manifest.passed {
        eprintln!(
            "FAILED: payload_manifest - {:?}",
            result.checks.payload_manifest.details
        );
    }
    if !result.checks.size_limits.passed {
        eprintln!(
            "FAILED: size_limits - {:?}",
            result.checks.size_limits.details
        );
    }
    if !result.checks.integrity.passed {
        eprintln!("FAILED: integrity - {:?}", result.checks.integrity.details);
    }
    if !result.checks.no_secrets_in_site.passed {
        eprintln!(
            "FAILED: no_secrets_in_site - {:?}",
            result.checks.no_secrets_in_site.details
        );
    }

    assert_eq!(result.status, "valid");
    assert!(result.checks.required_files.passed);
    assert!(result.checks.config_schema.passed);
}

#[test]
fn verify_rejects_same_size_vendor_runtime_tampering() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");
    copy_fixture("unencrypted", &site_dir).unwrap();

    let runtime_path = site_dir.join("vendor/sqlite3.mjs");
    if runtime_path.exists() {
        let mut runtime = fs::read(&runtime_path).unwrap();
        runtime[0] ^= 0x01;
        fs::write(runtime_path, runtime).unwrap();

        let result = verify_bundle(&site_dir, false).unwrap();
        assert_eq!(result.status, "invalid");
        assert!(!result.checks.required_files.passed);
        assert!(
            result
                .checks
                .required_files
                .details
                .as_deref()
                .is_some_and(|details| {
                    details.contains("vendor/sqlite3.mjs") && details.contains("SHA-256")
                })
        );
        assert!(!result.checks.integrity.passed);
    }
}

#[test]
fn test_verify_unencrypted_site() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");

    // Copy the unencrypted fixture to temp directory
    copy_fixture("unencrypted", &site_dir).unwrap();

    let result = verify_bundle(&site_dir, true).unwrap();
    assert!(
        result.checks.config_schema.passed,
        "config schema: {:?}",
        result.checks.config_schema
    );
    assert!(
        result.checks.payload_manifest.passed,
        "payload manifest: {:?}",
        result.checks.payload_manifest
    );
    assert_eq!(result.status, "valid", "checks: {:?}", result.checks);
}

#[test]
fn unencrypted_payload_requires_a_bounded_exact_declared_size() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");
    copy_fixture("unencrypted", &site_dir).unwrap();
    let config_path = site_dir.join("config.json");
    let original: Value = serde_json::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();

    let mut missing = original.clone();
    missing["payload"]
        .as_object_mut()
        .unwrap()
        .remove("size_bytes");
    fs::write(&config_path, serde_json::to_vec_pretty(&missing).unwrap()).unwrap();
    let schema = check_config_schema(&site_dir);
    assert!(
        !schema.passed
            && schema
                .details
                .as_deref()
                .is_some_and(|details| details.contains("size_bytes is required")),
        "missing size metadata must fail before the browser fetches the database: {:?}",
        schema.details
    );

    let mut oversized = original.clone();
    oversized["payload"]["size_bytes"] = Value::from(MAX_BROWSER_ARCHIVE_PLAINTEXT_SIZE + 1);
    fs::write(&config_path, serde_json::to_vec_pretty(&oversized).unwrap()).unwrap();
    let schema = check_config_schema(&site_dir);
    assert!(
        !schema.passed
            && schema
                .details
                .as_deref()
                .is_some_and(|details| details.contains("browser runtime limit")),
        "oversized unencrypted databases must be rejected: {:?}",
        schema.details
    );

    let mut mismatched = original;
    mismatched["payload"]["size_bytes"] = Value::from(18);
    fs::write(
        &config_path,
        serde_json::to_vec_pretty(&mismatched).unwrap(),
    )
    .unwrap();
    let manifest = check_payload_manifest(&site_dir);
    assert!(
        !manifest.passed
            && manifest
                .details
                .as_deref()
                .is_some_and(|details| details.contains("size does not match")),
        "payload bytes must exactly match their declaration: {:?}",
        manifest.details
    );
}

#[test]
fn unencrypted_payload_path_must_match_browser_url_semantics() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");
    copy_fixture("unencrypted", &site_dir).unwrap();
    let config_path = site_dir.join("config.json");
    let original: Value = serde_json::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();

    for invalid_path in [
        "payload//data.sqlite",
        "payload/./data.sqlite",
        "payload/%2e%2e/data.sqlite",
        "payload/subdir%2fdata.sqlite",
        "payload/subdir%5cdata.sqlite",
        "payload/data.sqlite?download=1",
        "payload/data.sqlite#fragment",
        "payload/%zz.sqlite",
        "payload/data%20copy.sqlite",
    ] {
        let mut config = original.clone();
        config["payload"]["path"] = Value::from(invalid_path);
        fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();

        let schema = check_config_schema(&site_dir);
        assert!(
            !schema.passed,
            "browser-incompatible payload path must fail verification: {invalid_path}"
        );
    }

    let mut ordinary_filename = original;
    ordinary_filename["payload"]["path"] = Value::from("payload/data copy.sqlite");
    fs::write(
        &config_path,
        serde_json::to_vec_pretty(&ordinary_filename).unwrap(),
    )
    .unwrap();
    let schema = check_config_schema(&site_dir);
    assert!(
        schema.passed,
        "ordinary filename characters should remain valid: {:?}",
        schema.details
    );
}

#[test]
fn verified_deployment_invokes_action_for_valid_exact_site() -> Result<()> {
    let temp = TempDir::new()?;
    let site_dir = temp.path().join("site");
    copy_fixture("unencrypted", &site_dir)?;
    let invoked = std::cell::Cell::new(false);

    let value = with_verified_bundle_for_deployment(temp.path(), false, |verified_site| {
        invoked.set(true);
        assert_eq!(verified_site, site_dir);
        Ok(17_u8)
    })?;

    assert!(invoked.get());
    assert_eq!(value, 17);
    Ok(())
}

#[test]
fn invalid_bundle_never_reaches_deployment_action() -> Result<()> {
    let temp = TempDir::new()?;
    let site_dir = temp.path().join("site");
    copy_fixture("unencrypted", &site_dir)?;
    fs::write(
        site_dir.join("recovery-secret.txt"),
        b"must never be deployed",
    )?;
    let invoked = std::cell::Cell::new(false);

    let error = with_verified_bundle_for_deployment(&site_dir, false, |_| {
        invoked.set(true);
        Ok(())
    })
    .expect_err("a private recovery artifact must block deployment");

    assert!(!invoked.get(), "invalid bundle reached deployment action");
    let message = format!("{error:#}");
    assert!(message.contains("Refusing to deploy invalid Pages bundle"));
    assert!(message.contains("no_secrets_in_site"));
    Ok(())
}

#[test]
fn completed_bundle_gate_rejects_post_build_private_artifact() -> Result<()> {
    let temp = TempDir::new()?;
    let site_dir = temp.path().join("site");
    copy_fixture("unencrypted", &site_dir)?;
    ensure_valid_bundle(&site_dir, false)?;

    fs::write(
        site_dir.join("recovery-secret.txt"),
        b"post-build private material",
    )?;
    let error = ensure_valid_bundle(&site_dir, false)
        .expect_err("post-build private artifacts must block success reporting");

    let message = format!("{error:#}");
    assert!(message.contains("failed full verification"));
    assert!(message.contains("no_secrets_in_site"));
    Ok(())
}

#[test]
fn test_verify_missing_required_files() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");

    // Copy the missing_required_no_viewer fixture (missing viewer.js)
    copy_fixture("missing_required_no_viewer", &site_dir).unwrap();

    let result = verify_bundle(&site_dir, false).unwrap();
    assert_eq!(result.status, "invalid");
    assert!(!result.checks.required_files.passed);
}

#[test]
fn test_verify_rejects_required_file_replaced_by_directory() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");
    let viewer_backup = temp.path().join("viewer.js.backup");

    copy_fixture("unencrypted", &site_dir).unwrap();
    fs::rename(site_dir.join("viewer.js"), &viewer_backup).unwrap();
    fs::create_dir(site_dir.join("viewer.js")).unwrap();

    let mut manifest: IntegrityManifest = serde_json::from_reader(BufReader::new(
        File::open(site_dir.join("integrity.json")).unwrap(),
    ))
    .unwrap();
    manifest.files.remove("viewer.js");
    fs::write(
        site_dir.join("integrity.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();

    let result = verify_bundle(&site_dir, false).unwrap();
    assert_eq!(result.status, "invalid");
    assert!(!result.checks.required_files.passed);
    assert!(
        result
            .checks
            .required_files
            .details
            .as_ref()
            .map(|details| details.contains("viewer.js (must be a regular file)"))
            .unwrap_or(false),
        "required file directories should be rejected: {:?}",
        result.checks.required_files.details
    );
}

#[test]
#[cfg(unix)]
fn test_required_files_reject_symlinked_regular_file() -> Result<()> {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new()?;
    let site_dir = temp.path().join("site");
    let outside = TempDir::new()?;
    copy_fixture("unencrypted", &site_dir)?;
    fs::rename(
        site_dir.join("viewer.js"),
        outside.path().join("viewer-original.js"),
    )?;
    fs::write(outside.path().join("viewer.js"), "outside viewer")?;
    symlink(outside.path().join("viewer.js"), site_dir.join("viewer.js"))?;

    let result = check_required_files(&site_dir);

    if result.passed {
        anyhow::bail!("symlinked required file was accepted");
    }
    match result.details.as_deref() {
        Some(details) if details.contains("viewer.js (must be a regular file)") => Ok(()),
        details => anyhow::bail!(
            "symlinked required files should be rejected; got details: {:?}",
            details
        ),
    }
}

#[test]
fn test_verify_invalid_config() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");

    copy_fixture("unencrypted", &site_dir).unwrap();

    // Write invalid config
    fs::write(
        site_dir.join("config.json"),
        r#"{"encrypted": true, "version": "1.0"}"#,
    )
    .unwrap();

    let result = verify_bundle(&site_dir, false).unwrap();
    assert!(!result.checks.config_schema.passed);
}

#[test]
fn test_verify_rejects_unknown_config_fields() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");

    copy_fixture("unencrypted", &site_dir).unwrap();
    fs::write(
        site_dir.join("config.json"),
        r#"{
            "encrypted": false,
            "version": "1.0",
            "payload": {
                "path": "payload/data.sqlite",
                "format": "sqlite"
            },
            "totally_unknown_field": 123
        }"#,
    )
    .unwrap();

    let result = verify_bundle(&site_dir, false).unwrap();
    assert!(!result.checks.config_schema.passed);
    assert!(
        result
            .checks
            .config_schema
            .details
            .as_ref()
            .map(|details| details.contains("unknown field"))
            .unwrap_or(false),
        "unknown config fields should fail schema validation: {:?}",
        result.checks.config_schema.details
    );
}

#[test]
fn test_verify_secret_leakage() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");

    // Copy the secret_leak fixture (contains recovery-secret.txt)
    copy_fixture("secret_leak", &site_dir).unwrap();

    let result = verify_bundle(&site_dir, false).unwrap();
    assert!(!result.checks.no_secrets_in_site.passed);
}

#[test]
fn test_check_no_secrets_rejects_nested_mixed_case_private_artifacts() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");
    fs::create_dir_all(site_dir.join("nested/Private")).unwrap();
    fs::write(
        site_dir.join("nested/Master-Key.json"),
        "wrapped key metadata",
    )
    .unwrap();

    let result = check_no_secrets(&site_dir);

    assert!(!result.passed);
    let details = result.details.unwrap_or_default();
    assert!(
        details.contains("nested/Private/"),
        "mixed-case private directory bypassed the scan: {details}"
    );
    assert!(
        details.contains("nested/Master-Key.json"),
        "mixed-case secret filename bypassed the scan: {details}"
    );
}

#[test]
fn test_check_no_secrets_flags_nested_config_secret_key_with_whitespace() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");
    fs::create_dir_all(&site_dir).unwrap();
    fs::write(
        site_dir.join("config.json"),
        r#"{
            "encrypted": false,
            "version": "1.0",
            "payload": { "path": "payload/data.sqlite", "format": "sqlite" },
            "metadata": { "secret" : "leaked" }
        }"#,
    )
    .unwrap();

    let result = check_no_secrets(&site_dir);
    assert!(!result.passed);
    assert!(
        result
            .details
            .as_ref()
            .map(|details| {
                details.contains(
                    "config.json contains forbidden field: secret field at metadata.secret",
                )
            })
            .unwrap_or(false),
        "nested secret key with whitespace should be detected: {:?}",
        result.details
    );
}

#[test]
fn test_check_no_secrets_flags_forbidden_config_key_inside_array() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");
    fs::create_dir_all(&site_dir).unwrap();
    fs::write(
        site_dir.join("config.json"),
        r#"{
            "encrypted": false,
            "version": "1.0",
            "payload": { "path": "payload/data.sqlite", "format": "sqlite" },
            "metadata": [{ "private_key" : "leaked" }]
        }"#,
    )
    .unwrap();

    let result = check_no_secrets(&site_dir);
    assert!(!result.passed);
    assert!(
        result
            .details
            .as_ref()
            .map(|details| {
                details.contains(
                    "config.json contains forbidden field: private_key field at metadata[0].private_key",
                )
            })
            .unwrap_or(false),
        "forbidden key inside arrays should be detected: {:?}",
        result.details
    );
}

#[test]
#[cfg(unix)]
fn test_check_no_secrets_does_not_follow_symlinked_directories() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");
    let outside_dir = temp.path().join("outside");
    fs::create_dir_all(&site_dir).unwrap();
    fs::create_dir_all(outside_dir.join("private")).unwrap();
    fs::write(outside_dir.join("private/recovery-secret.txt"), "secret").unwrap();
    symlink(&outside_dir, site_dir.join("linked-assets")).unwrap();

    let result = check_no_secrets(&site_dir);
    assert!(
        result.passed,
        "symlink targets outside site/ should not be scanned as in-tree secrets: {:?}",
        result.details
    );
}

#[test]
#[cfg(unix)]
fn test_check_no_secrets_flags_secret_named_symlink_without_recursing() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");
    let benign_dir = temp.path().join("benign");
    fs::create_dir_all(site_dir.join("nested")).unwrap();
    fs::create_dir_all(&benign_dir).unwrap();
    symlink(&benign_dir, site_dir.join("nested/private")).unwrap();

    let result = check_no_secrets(&site_dir);
    assert!(!result.passed);
    assert!(
        result
            .details
            .as_ref()
            .map(|details| {
                details.contains("Secret directory found in site subdirectory: nested/private/")
            })
            .unwrap_or(false),
        "secret-named symlink should still be reported: {:?}",
        result.details
    );
}

#[test]
#[cfg(unix)]
fn test_check_no_secrets_flags_top_level_secret_file_broken_symlink() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");
    fs::create_dir_all(&site_dir).unwrap();
    symlink(
        temp.path().join("missing-recovery-secret"),
        site_dir.join("recovery-secret.txt"),
    )
    .unwrap();

    let result = check_no_secrets(&site_dir);
    assert!(!result.passed);
    assert!(
        result
            .details
            .as_ref()
            .map(|details| details.contains("Secret file found in site/: recovery-secret.txt"))
            .unwrap_or(false),
        "top-level dangling secret symlink should still be reported: {:?}",
        result.details
    );
}

#[test]
#[cfg(unix)]
fn test_check_no_secrets_flags_top_level_secret_dir_broken_symlink() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");
    fs::create_dir_all(&site_dir).unwrap();
    symlink(
        temp.path().join("missing-private"),
        site_dir.join("private"),
    )
    .unwrap();

    let result = check_no_secrets(&site_dir);
    assert!(!result.passed);
    assert!(
        result
            .details
            .as_ref()
            .map(|details| details.contains("Secret directory found in site/: private/"))
            .unwrap_or(false),
        "top-level dangling private symlink should still be reported: {:?}",
        result.details
    );
}

#[test]
fn test_verify_with_integrity() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");

    // Copy valid fixture
    copy_fixture("valid", &site_dir).unwrap();

    // Create integrity.json over EVERY file in the site: the integrity
    // check's coverage rule flags any uncovered file, and the prepared
    // site contains more than REQUIRED_FILES (payload chunks, the
    // materialized vendor/ assets, ...). Walking the tree keeps this
    // manifest builder honest as the bundle contract grows.
    let mut files = BTreeMap::new();
    for entry in walkdir::WalkDir::new(&site_dir) {
        let entry = entry.unwrap();
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry
            .path()
            .strip_prefix(&site_dir)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "integrity.json" {
            continue;
        }
        let hash = compute_file_hash(entry.path()).unwrap();
        let size = fs::metadata(entry.path()).unwrap().len();
        files.insert(rel, IntegrityEntry { sha256: hash, size });
    }

    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2024-01-01T00:00:00Z".to_string(),
        files,
    };
    fs::write(
        site_dir.join("integrity.json"),
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();

    let result = verify_bundle(&site_dir, false).unwrap();
    assert!(
        result.checks.integrity.passed,
        "integrity check failed: {:?}",
        result.checks.integrity
    );
}

#[test]
fn test_integrity_rejects_unknown_manifest_version() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");
    copy_fixture("valid", &site_dir).unwrap();
    let integrity_path = site_dir.join("integrity.json");
    let mut manifest: IntegrityManifest =
        serde_json::from_reader(BufReader::new(File::open(&integrity_path).unwrap())).unwrap();
    manifest.version = INTEGRITY_MANIFEST_VERSION + 1;
    fs::write(
        &integrity_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();

    let result = check_integrity(&site_dir, false);

    assert!(!result.passed);
    assert!(
        result
            .details
            .as_deref()
            .is_some_and(|details| details.contains("Unsupported integrity manifest version")),
        "unknown integrity schema should fail explicitly: {:?}",
        result.details
    );
}

#[test]
fn test_verify_integrity_mismatch() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path().join("site");

    // Copy valid fixture
    copy_fixture("valid", &site_dir).unwrap();

    // Create integrity.json with wrong hash
    let mut files = BTreeMap::new();
    files.insert(
        "index.html".to_string(),
        IntegrityEntry {
            sha256: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            size: 10,
        },
    );

    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2024-01-01T00:00:00Z".to_string(),
        files,
    };
    fs::write(
        site_dir.join("integrity.json"),
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();

    let result = verify_bundle(&site_dir, false).unwrap();
    assert!(!result.checks.integrity.passed);
    let details = result.checks.integrity.details.as_ref().unwrap();
    assert!(
        details.contains("Size mismatch") || details.contains("Hash mismatch"),
        "expected size or hash mismatch, got: {details}"
    );
}

#[test]
fn test_resolve_site_dir() {
    let temp = TempDir::new().unwrap();

    // Test with site/ subdirectory
    let site_dir = temp.path().join("site");
    fs::create_dir_all(&site_dir).unwrap();

    let resolved = crate::pages::resolve_site_dir(temp.path()).unwrap();
    assert!(resolved.ends_with("site"));

    // Test with direct path
    let resolved_direct = crate::pages::resolve_site_dir(&site_dir).unwrap();
    assert_eq!(resolved_direct, site_dir);
}

#[test]
#[cfg(unix)]
fn test_resolve_site_dir_rejects_symlinked_site_directory() {
    use std::os::unix::fs::symlink;

    let bundle_root = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let outside_site = outside.path().join("site");
    fs::create_dir_all(&outside_site).unwrap();
    fs::write(outside_site.join("index.html"), "<html></html>").unwrap();
    symlink(&outside_site, bundle_root.path().join("site")).unwrap();

    let err = crate::pages::resolve_site_dir(bundle_root.path())
        .unwrap_err()
        .to_string();
    assert!(err.contains("must not be a symlink"));

    let direct_err = crate::pages::resolve_site_dir(&bundle_root.path().join("site"))
        .unwrap_err()
        .to_string();
    assert!(direct_err.contains("must not be a symlink"));
}

#[test]
fn test_unencrypted_payload_must_be_regular_file() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path();
    let payload_dir = site_dir.join("payload");
    fs::create_dir_all(&payload_dir).unwrap();
    fs::create_dir_all(payload_dir.join("data.sqlite")).unwrap();

    let config = r#"{
      "encrypted": false,
      "version": "1.0",
      "payload": {
        "path": "payload/data.sqlite",
        "format": "sqlite"
      }
    }"#;
    fs::write(site_dir.join("config.json"), config).unwrap();

    let manifest_result = check_payload_manifest(site_dir);
    assert!(!manifest_result.passed);
    assert!(
        manifest_result
            .details
            .as_ref()
            .map(|d| d.contains("payload/data.sqlite must be a regular file"))
            .unwrap_or(false)
    );

    let size_result = check_size_limits(site_dir);
    assert!(!size_result.passed);
    assert!(
        size_result
            .details
            .as_ref()
            .map(|d| d.contains("payload/data.sqlite must be a regular file"))
            .unwrap_or(false)
    );
}
