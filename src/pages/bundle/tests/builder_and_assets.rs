use anyhow::{Result, anyhow};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

use crate::pages::bundle::assets::validate_embedded_vendor_asset;
#[cfg(unix)]
use crate::pages::bundle::builder::{
    copy_payload_file, create_bundle_staging_root, prepare_bundle_root_for_publish,
};
use crate::pages::bundle::builder::{generate_public_readme, resolve_generated_doc_path};
use crate::pages::bundle::integrity::collect_file_hashes;
use crate::pages::bundle::private_artifacts::{
    replace_private_artifact_from_temp_via_backup, write_private_artifact_file,
};
use crate::pages::bundle::{
    BundleBuilder, BundleConfig, IntegrityEntry, IntegrityManifest, PAGES_VENDOR_ASSETS,
    PinnedVendorAsset, compute_fingerprint, generate_integrity_manifest,
    validate_pinned_vendor_assets, write_pinned_vendor_assets,
};
#[cfg(unix)]
use crate::pages::bundle::{UnencryptedConfig, UnencryptedPayload};
use crate::pages::docs::{DocLocation, GeneratedDoc};

use super::write_unencrypted_source;

#[test]
fn test_bundle_builder_default() {
    let builder = BundleBuilder::new();
    assert_eq!(builder.config.title, "cass Archive");
    assert!(!builder.config.hide_metadata);
}

#[test]
fn test_bundle_builder_fluent() {
    let builder = BundleBuilder::new()
        .title("My Archive")
        .description("Test description")
        .hide_metadata(true);

    assert_eq!(builder.config.title, "My Archive");
    assert_eq!(builder.config.description, "Test description");
    assert!(builder.config.hide_metadata);
}

#[test]
fn test_compute_fingerprint() {
    let mut files = BTreeMap::new();
    files.insert(
        "test.txt".to_string(),
        IntegrityEntry {
            sha256: "abc123".to_string(),
            size: 100,
        },
    );

    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2024-01-01T00:00:00Z".to_string(),
        files,
    };

    let fingerprint = compute_fingerprint(&manifest);
    assert_eq!(fingerprint.len(), 16);

    // Same manifest should produce same fingerprint
    let fingerprint2 = compute_fingerprint(&manifest);
    assert_eq!(fingerprint, fingerprint2);
}

#[test]
#[cfg(unix)]
fn private_artifacts_are_owner_only_even_under_permissive_ambient_modes() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new()?;
    let private_dir = temp.path().join("private");
    fs::create_dir_all(&private_dir)?;
    fs::set_permissions(&private_dir, fs::Permissions::from_mode(0o777))?;

    write_private_artifact_file(&private_dir, "fingerprint.txt", b"abcdef123456")?;

    let dir_mode = fs::metadata(&private_dir)?.permissions().mode();
    let file_mode = fs::metadata(private_dir.join("fingerprint.txt"))?
        .permissions()
        .mode();
    if dir_mode & 0o077 != 0 {
        return Err(anyhow!("private directory mode was {dir_mode:o}"));
    }
    if file_mode & 0o077 != 0 {
        return Err(anyhow!("private file mode was {file_mode:o}"));
    }
    Ok(())
}

#[test]
fn private_artifact_writer_replaces_an_existing_generation() -> Result<()> {
    let temp = TempDir::new()?;
    let private_dir = temp.path().join("private");

    write_private_artifact_file(&private_dir, "fingerprint.txt", b"old generation")?;
    write_private_artifact_file(&private_dir, "fingerprint.txt", b"new generation")?;

    let installed = fs::read(private_dir.join("fingerprint.txt"))?;
    if installed != b"new generation" {
        return Err(anyhow!(
            "private artifact replacement published stale bytes"
        ));
    }
    Ok(())
}

#[test]
fn private_artifact_backup_fallback_preserves_then_replaces() -> Result<()> {
    let temp = TempDir::new()?;
    let final_path = temp.path().join("master-key.json");
    let staged_path = temp.path().join("master-key.tmp.json");
    fs::write(&final_path, b"old generation")?;
    fs::write(&staged_path, b"new generation")?;
    let first_error = std::io::Error::from(std::io::ErrorKind::AlreadyExists);
    let mut retain_temp_on_error = false;

    replace_private_artifact_from_temp_via_backup(
        &staged_path,
        &final_path,
        &first_error,
        &mut retain_temp_on_error,
    )?;

    if retain_temp_on_error {
        return Err(anyhow!(
            "successful private artifact fallback requested temp retention"
        ));
    }
    if fs::read(&final_path)? != b"new generation" {
        return Err(anyhow!(
            "private artifact fallback did not publish staged bytes"
        ));
    }
    if staged_path.exists() {
        return Err(anyhow!(
            "private artifact fallback left the staged path behind"
        ));
    }
    let remaining_entries = fs::read_dir(temp.path())?.collect::<std::io::Result<Vec<_>>>()?;
    if remaining_entries.len() != 1 {
        return Err(anyhow!(
            "private artifact fallback left an unexpected backup sidecar"
        ));
    }
    Ok(())
}

#[test]
#[cfg(unix)]
fn bundle_staging_root_is_private_until_publish() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new()?;
    let staging_root = temp.path().join("bundle.staged");
    create_bundle_staging_root(&staging_root)?;

    let staged_mode = fs::metadata(&staging_root)?.permissions().mode();
    if staged_mode & 0o077 != 0 {
        return Err(anyhow!("bundle staging root mode was {staged_mode:o}"));
    }

    prepare_bundle_root_for_publish(&staging_root)?;
    let published_mode = fs::metadata(&staging_root)?.permissions().mode();
    if published_mode & 0o777 != 0o755 {
        return Err(anyhow!("published bundle root mode was {published_mode:o}"));
    }
    Ok(())
}

#[test]
fn test_generate_public_readme() {
    let unencrypted = generate_public_readme("Test Archive", "A test archive");
    assert!(unencrypted.contains("Test Archive"));
    assert!(unencrypted.contains("A test archive"));
    assert!(unencrypted.contains("NOT encrypted"));
    assert!(unencrypted.contains("no password required"));
    assert!(unencrypted.contains("Plaintext archive metadata"));
    assert!(unencrypted.contains("Unencrypted SQLite database"));
    assert!(!unencrypted.contains("Public encryption parameters"));
    assert!(!unencrypted.contains("Encrypted database chunks"));
}

#[test]
fn test_integrity_manifest_excludes_itself() {
    let temp = TempDir::new().unwrap();
    let temp_path = temp.path();

    // Create some test files
    fs::write(temp_path.join("test.txt"), "hello").unwrap();
    fs::write(temp_path.join("integrity.json"), "{}").unwrap();

    let manifest = generate_integrity_manifest(temp_path).unwrap();

    // Should include test.txt but not integrity.json
    assert!(manifest.files.contains_key("test.txt"));
    assert!(!manifest.files.contains_key("integrity.json"));
}

#[test]
fn pinned_vendor_contract_matches_every_embedded_byte() {
    for asset in PAGES_VENDOR_ASSETS {
        validate_embedded_vendor_asset(asset)
            .unwrap_or_else(|error| panic!("invalid vendor pin for {}: {error:#}", asset.path));
    }

    let manifest: serde_json::Value = serde_json::from_slice(
        PAGES_VENDOR_ASSETS
            .iter()
            .find(|asset| asset.path == "vendor/manifest.json")
            .expect("vendor manifest pin")
            .bytes,
    )
    .expect("valid vendor manifest JSON");
    let package_versions = manifest["packages"]
        .as_array()
        .expect("vendor package list")
        .iter()
        .map(|package| {
            (
                package["name"].as_str().expect("package name"),
                package["version"].as_str().expect("package version"),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        package_versions.get("@sqlite.org/sqlite-wasm"),
        Some(&"3.53.0-build1")
    );
    assert_eq!(package_versions.len(), 1);
}

#[test]
fn pinned_vendor_contract_rejects_hash_mutations() {
    let mutated = PinnedVendorAsset {
        path: "vendor/mutated.js",
        bytes: b"mutated",
        size: 7,
        sha256: "0000000000000000000000000000000000000000000000000000000000000000",
    };
    let error = validate_embedded_vendor_asset(&mutated).unwrap_err();
    assert!(error.to_string().contains("SHA-256"));
}

#[test]
fn bundle_build_validates_materialized_vendor_before_integrity_publish() {
    let source = include_str!("../builder.rs");
    let validation = source
        .find("validate_pinned_vendor_assets(&site_dir)")
        .expect("materialized vendor validation in build path");
    let integrity = source
        .find("progress(\"integrity\", \"Generating integrity manifest...\")")
        .expect("integrity generation step");
    let publish = source
        .find("replace_dir_from_temp(")
        .expect("bundle publication step");
    assert!(validation < integrity && integrity < publish);
}

#[test]
fn materialized_vendor_validation_rejects_same_size_tampering() {
    let temp = TempDir::new().unwrap();
    write_pinned_vendor_assets(temp.path()).unwrap();
    validate_pinned_vendor_assets(temp.path()).unwrap();

    let sqlite_js_path = temp.path().join("vendor/sqlite3.mjs");
    let mut mutated = fs::read(&sqlite_js_path).unwrap();
    mutated[0] ^= 0x01;
    fs::write(sqlite_js_path, mutated).unwrap();

    let error = validate_pinned_vendor_assets(temp.path()).unwrap_err();
    assert!(error.to_string().contains("SHA-256"));
}

#[test]
fn test_collect_file_hashes() {
    let temp = TempDir::new().unwrap();
    let temp_path = temp.path();

    // Create nested structure
    fs::create_dir_all(temp_path.join("subdir")).unwrap();
    fs::write(temp_path.join("root.txt"), "root").unwrap();
    fs::write(temp_path.join("subdir/nested.txt"), "nested").unwrap();

    let mut files = BTreeMap::new();
    collect_file_hashes(temp_path, temp_path, &mut files).unwrap();

    assert_eq!(files.len(), 2);
    assert!(files.contains_key("root.txt"));
    assert!(files.contains_key("subdir/nested.txt"));

    // Verify hash is SHA256 hex (64 chars)
    for entry in files.values() {
        assert_eq!(entry.sha256.len(), 64);
    }
}

#[test]
#[cfg(unix)]
fn test_collect_file_hashes_includes_symlinked_files_within_site() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let temp_path = temp.path();

    fs::write(temp_path.join("real.txt"), "real").unwrap();
    symlink("real.txt", temp_path.join("linked-file.txt")).unwrap();

    let mut files = BTreeMap::new();
    collect_file_hashes(temp_path, temp_path, &mut files).unwrap();

    assert_eq!(files.len(), 2);
    assert!(files.contains_key("real.txt"));
    assert!(files.contains_key("linked-file.txt"));
    assert_eq!(files["real.txt"].sha256, files["linked-file.txt"].sha256);
    assert_eq!(files["real.txt"].size, files["linked-file.txt"].size);
}

#[test]
#[cfg(unix)]
fn test_collect_file_hashes_rejects_symlinks_outside_site() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let temp_path = temp.path();
    let outside = TempDir::new().unwrap();

    fs::write(temp_path.join("root.txt"), "root").unwrap();
    fs::write(outside.path().join("secret.txt"), "secret").unwrap();
    fs::create_dir_all(outside.path().join("nested")).unwrap();
    fs::write(outside.path().join("nested/hidden.txt"), "hidden").unwrap();
    symlink(
        outside.path().join("secret.txt"),
        temp_path.join("linked-file.txt"),
    )
    .unwrap();
    symlink(outside.path().join("nested"), temp_path.join("linked-dir")).unwrap();

    let mut files = BTreeMap::new();
    let err = collect_file_hashes(temp_path, temp_path, &mut files).unwrap_err();
    assert!(
        err.to_string().contains("outside site directory"),
        "unexpected error: {err:#}"
    );
}

#[test]
#[cfg(unix)]
fn test_copy_unencrypted_payload_rejects_final_symlink() {
    use std::os::unix::fs::symlink;

    let source = TempDir::new().unwrap();
    let site = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();

    fs::create_dir_all(source.path().join("payload")).unwrap();
    fs::write(outside.path().join("secret.db"), "outside secret").unwrap();
    symlink(
        outside.path().join("secret.db"),
        source.path().join("payload/data.db"),
    )
    .unwrap();

    let config = UnencryptedConfig {
        encrypted: false,
        version: "1.0.0".to_string(),
        payload: UnencryptedPayload {
            path: "payload/data.db".to_string(),
            format: "sqlite".to_string(),
            size_bytes: None,
        },
        warning: None,
    };

    let err = copy_payload_file(source.path(), site.path(), &config).unwrap_err();
    assert!(
        err.to_string().contains("must not be a symlink"),
        "unexpected error: {err:#}"
    );
    assert!(!site.path().join("payload/data.db").exists());
}

#[test]
#[cfg(unix)]
fn test_copy_unencrypted_payload_rejects_symlinked_parent_escape() {
    use std::os::unix::fs::symlink;

    let source = TempDir::new().unwrap();
    let site = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();

    fs::write(outside.path().join("data.db"), "outside secret").unwrap();
    symlink(outside.path(), source.path().join("payload")).unwrap();

    let config = UnencryptedConfig {
        encrypted: false,
        version: "1.0.0".to_string(),
        payload: UnencryptedPayload {
            path: "payload/data.db".to_string(),
            format: "sqlite".to_string(),
            size_bytes: None,
        },
        warning: None,
    };

    let err = copy_payload_file(source.path(), site.path(), &config).unwrap_err();
    assert!(
        err.to_string().contains("outside bundle source directory"),
        "unexpected error: {err:#}"
    );
    assert!(!site.path().join("payload/data.db").exists());
}

#[test]
fn test_generated_docs_reject_path_traversal_filename() {
    let source = TempDir::new().unwrap();
    let output_parent = TempDir::new().unwrap();
    let output_dir = output_parent.path().join("bundle");

    write_unencrypted_source(source.path(), "data.db", "payload");

    let config = BundleConfig {
        generated_docs: vec![GeneratedDoc {
            filename: "../escaped.md".to_string(),
            content: "escaped".to_string(),
            location: DocLocation::WebRoot,
        }],
        ..BundleConfig::default()
    };

    let err = BundleBuilder::with_config(config)
        .build(source.path(), output_dir.as_path(), |_, _| {})
        .unwrap_err();
    assert!(
        err.to_string().contains("must not contain path separators"),
        "unexpected error: {err:#}"
    );
    assert!(!output_parent.path().join("escaped.md").exists());
    let leftovers = fs::read_dir(output_parent.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        leftovers.is_empty(),
        "rejected bundle staging directory leaked: {leftovers:?}"
    );
}

#[test]
fn test_generated_docs_reject_backslash_separator_filename() {
    let doc = GeneratedDoc {
        filename: r"nested\escaped.md".to_string(),
        content: "escaped".to_string(),
        location: DocLocation::WebRoot,
    };

    let err = resolve_generated_doc_path(Path::new("site"), &doc).unwrap_err();
    assert!(
        err.to_string().contains("must not contain path separators"),
        "unexpected error: {err:#}"
    );
}

#[test]
fn test_build_replaces_existing_bundle_without_stale_files() {
    let source = TempDir::new().unwrap();
    let output_parent = TempDir::new().unwrap();
    let output_dir = output_parent.path().join("bundle");

    write_unencrypted_source(source.path(), "data.db", "fresh payload");

    let builder = BundleBuilder::new();
    builder
        .build(source.path(), output_dir.as_path(), |_, _| {})
        .expect("initial build");

    fs::write(output_dir.join("site/stale.txt"), "stale").unwrap();
    fs::write(output_dir.join("private/old-secret.txt"), "secret").unwrap();
    fs::write(output_dir.join("site/payload/old.bin"), "old").unwrap();

    builder
        .build(source.path(), output_dir.as_path(), |_, _| {})
        .expect("rebuild");

    assert!(output_dir.join("site/config.json").exists());
    assert!(
        output_dir
            .join("private/integrity-fingerprint.txt")
            .exists()
    );
    assert!(!output_dir.join("site/stale.txt").exists());
    assert!(!output_dir.join("private/old-secret.txt").exists());
    assert!(!output_dir.join("site/payload/old.bin").exists());
    assert!(output_dir.join("site/payload/data.db").exists());
}

#[test]
fn test_build_failure_preserves_existing_bundle() {
    let source = TempDir::new().unwrap();
    let output_parent = TempDir::new().unwrap();
    let output_dir = output_parent.path().join("bundle");
    let broken_source = TempDir::new().unwrap();

    write_unencrypted_source(source.path(), "data.db", "fresh payload");

    let builder = BundleBuilder::new();
    builder
        .build(source.path(), output_dir.as_path(), |_, _| {})
        .expect("initial build");

    fs::write(output_dir.join("site/marker.txt"), "keep me").unwrap();

    let result = builder.build(broken_source.path(), output_dir.as_path(), |_, _| {});
    assert!(result.is_err(), "broken rebuild should fail");

    assert!(output_dir.join("site/marker.txt").exists());
    assert!(output_dir.join("site/config.json").exists());
    assert!(
        output_dir
            .join("private/integrity-fingerprint.txt")
            .exists()
    );
}

#[test]
#[cfg(unix)]
fn test_build_rejects_symlinked_output_directory() {
    use std::os::unix::fs::symlink;

    let source = TempDir::new().unwrap();
    let output_parent = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let output_dir = output_parent.path().join("bundle-link");

    write_unencrypted_source(source.path(), "data.db", "payload");
    symlink(outside.path(), &output_dir).unwrap();

    let err = BundleBuilder::new()
        .build(source.path(), output_dir.as_path(), |_, _| {})
        .unwrap_err();

    assert!(
        err.to_string().contains("must not be a symlink"),
        "unexpected error: {err:#}"
    );
    assert!(
        fs::symlink_metadata(&output_dir)
            .unwrap()
            .file_type()
            .is_symlink(),
        "rejected symlink output path must be preserved for operator inspection"
    );
    assert!(
        !outside.path().join("site").exists(),
        "build must not write through a symlinked output directory"
    );
}

#[test]
#[cfg(unix)]
fn test_build_rejects_symlinked_output_parent_before_staging() {
    use std::os::unix::fs::symlink;

    let source = TempDir::new().unwrap();
    let output_parent = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let linked_parent = output_parent.path().join("linked-parent");
    let output_dir = linked_parent.join("bundle");

    write_unencrypted_source(source.path(), "data.db", "payload");
    symlink(outside.path(), &linked_parent).unwrap();

    let err = BundleBuilder::new()
        .build(source.path(), output_dir.as_path(), |_, _| {})
        .unwrap_err();

    assert!(
        err.to_string().contains("parent must not contain symlinks"),
        "unexpected error: {err:#}"
    );
    assert!(
        fs::read_dir(outside.path()).unwrap().next().is_none(),
        "bundle builder must not stage output through a symlinked parent"
    );
}
