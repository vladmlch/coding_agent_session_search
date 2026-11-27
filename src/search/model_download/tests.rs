use super::*;
use std::error::Error as _;

/// Copy model fixtures from tests/fixtures/models/ to the target directory.
/// Copies model.onnx plus config files.
fn copy_model_fixtures(target_dir: &Path) -> std::io::Result<()> {
    let fixture_dir = crate::subsystem_coverage_matrix::workspace_paths::workspace_root()
        .join("tests/fixtures/models");
    fs::create_dir_all(target_dir)?;

    // Copy model.onnx fixture (legacy placeholder, retained for tests that
    // assert NotInstalled when only the old ONNX file is present).
    fs::copy(
        fixture_dir.join("model.onnx"),
        target_dir.join("model.onnx"),
    )?;
    // cass #308: the native backend (and the manifest's REQUIRED file set)
    // now expects `model.safetensors`. The fixture ships no real weights, so
    // write a small placeholder — check_model_installed only verifies file
    // presence + the `.verified` marker, never hashing fixture content.
    fs::write(
        target_dir.join("model.safetensors"),
        b"placeholder-safetensors",
    )?;

    // Copy config files
    for file in &[
        "tokenizer.json",
        "config.json",
        "special_tokens_map.json",
        "tokenizer_config.json",
    ] {
        fs::copy(fixture_dir.join(file), target_dir.join(file))?;
    }

    Ok(())
}

/// Build a manifest whose file checksums are computed from the given
/// contents, so local installs verify real SHA256 digests.
fn build_test_manifest(repo: &str, revision: &str, files: &[(&str, &[u8])]) -> ModelManifest {
    ModelManifest {
        id: "local-test-model".into(),
        repo: repo.into(),
        revision: revision.into(),
        files: files
            .iter()
            .map(|(name, body)| ModelFile {
                name: (*name).into(),
                sha256: hex::encode(Sha256::digest(body)),
                size: body.len() as u64,
            })
            .collect(),
        license: "Apache-2.0".into(),
    }
}

/// Write the manifest `files` into a source directory under their local
/// basenames — the layout `install_from_directory` expects.
fn write_source_dir(dir: &Path, files: &[(&str, &[u8])]) {
    fs::create_dir_all(dir).unwrap();
    for (name, body) in files {
        let local_name = Path::new(name)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        fs::write(dir.join(local_name), body).unwrap();
    }
}

#[test]
fn test_model_state_summary() {
    assert_eq!(ModelState::NotInstalled.summary(), "not installed");
    assert_eq!(ModelState::Ready.summary(), "ready");
}

#[test]
fn test_model_state_is_ready() {
    assert!(ModelState::Ready.is_ready());
    assert!(!ModelState::NotInstalled.is_ready());
}

#[test]
fn test_model_manifest_total_size() {
    let manifest = ModelManifest::minilm_v2();
    assert!(manifest.total_size() > 20_000_000); // > 20MB
}

#[test]
fn test_check_model_installed_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let model_dir = tmp.path().join("nonexistent");
    assert_eq!(
        check_model_installed(&model_dir, &ModelManifest::minilm_v2()),
        ModelState::NotInstalled
    );
}

#[test]
fn test_check_model_installed_no_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let model_dir = tmp.path().join("model");
    // Use fixture files instead of fake content - only copy model.onnx
    let fixture_dir = crate::subsystem_coverage_matrix::workspace_paths::workspace_root()
        .join("tests/fixtures/models");
    fs::create_dir_all(&model_dir).unwrap();
    fs::copy(fixture_dir.join("model.onnx"), model_dir.join("model.onnx")).unwrap();
    assert_eq!(
        check_model_installed(&model_dir, &ModelManifest::minilm_v2()),
        ModelState::NotInstalled
    );
}

#[test]
fn test_check_model_installed_ready() {
    let tmp = tempfile::tempdir().unwrap();
    let model_dir = tmp.path().join("model");
    // Use fixture files instead of fake content
    copy_model_fixtures(&model_dir).unwrap();
    fs::write(model_dir.join(".verified"), "revision=test\n").unwrap();
    assert_eq!(
        check_model_installed(&model_dir, &ModelManifest::minilm_v2()),
        ModelState::Ready
    );
}

#[test]
fn classify_cache_policy_disabled_takes_precedence_over_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let manifest = build_test_manifest("repo/model", "rev1", &[("model.onnx", b"model")]);
    let policy = ModelAcquisitionPolicy {
        install_enabled: false,
        max_model_bytes: Some(1),
        ..ModelAcquisitionPolicy::default()
    };

    let report = classify_model_cache(tmp.path(), &manifest, &policy);
    assert_eq!(report.state_code(), "disabled_by_policy");
    assert!(matches!(
        report.state,
        ModelCacheState::DisabledByPolicy { .. }
    ));
}

#[test]
fn classify_cache_reports_budget_block() {
    let tmp = tempfile::tempdir().unwrap();
    let manifest = build_test_manifest("repo/model", "rev1", &[("model.onnx", b"model")]);

    let budget = ModelAcquisitionPolicy {
        max_model_bytes: Some(1),
        ..ModelAcquisitionPolicy::default()
    };
    let report = classify_model_cache(tmp.path(), &manifest, &budget);
    assert_eq!(report.state_code(), "budget_blocked");
}

#[test]
fn classify_cache_reports_missing_files() {
    let tmp = tempfile::tempdir().unwrap();
    let model_dir = tmp.path().join("model");
    fs::create_dir_all(&model_dir).unwrap();
    fs::write(model_dir.join("model.onnx"), b"model").unwrap();
    let manifest = build_test_manifest(
        "repo/model",
        "rev1",
        &[("model.onnx", b"model"), ("tokenizer.json", b"tok")],
    );

    let report = classify_model_cache(&model_dir, &manifest, &ModelAcquisitionPolicy::default());
    assert_eq!(report.state_code(), "not_acquired");
    assert!(matches!(
        &report.state,
        ModelCacheState::NotAcquired { missing_files } if missing_files == &["tokenizer.json"]
    ));
    assert_eq!(
        report.state.next_step("minilm").as_deref(),
        Some("cass models install --model minilm --from-file <DIR>")
    );
}

#[test]
fn classify_cache_accepts_preseeded_local_manifest_files() {
    let tmp = tempfile::tempdir().unwrap();
    let model_dir = tmp.path().join("model");
    fs::create_dir_all(model_dir.join("onnx")).unwrap();
    fs::write(model_dir.join("onnx/model.onnx"), b"model").unwrap();
    fs::write(model_dir.join("tokenizer.json"), b"tok").unwrap();
    let manifest = build_test_manifest(
        "repo/model",
        "rev1",
        &[("onnx/model.onnx", b"model"), ("tokenizer.json", b"tok")],
    );

    let report = classify_model_cache(&model_dir, &manifest, &ModelAcquisitionPolicy::default());
    assert_eq!(report.state_code(), "preseeded_local");
    assert!(report.is_usable());
}

#[test]
fn classify_cache_detects_checksum_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let model_dir = tmp.path().join("model");
    fs::create_dir_all(&model_dir).unwrap();
    fs::write(model_dir.join("model.onnx"), b"wrong").unwrap();
    let manifest = build_test_manifest("repo/model", "rev1", &[("model.onnx", b"model")]);

    let report = classify_model_cache(&model_dir, &manifest, &ModelAcquisitionPolicy::default());
    assert_eq!(report.state_code(), "checksum_mismatch");
    assert!(matches!(
        report.state,
        ModelCacheState::ChecksumMismatch { .. }
    ));
}

#[test]
fn classify_cache_metadata_trusts_verified_marker_without_hashing_payload() {
    let tmp = tempfile::tempdir().unwrap();
    let model_dir = tmp.path().join("model");
    fs::create_dir_all(&model_dir).unwrap();
    fs::write(model_dir.join("model.onnx"), b"m0del").unwrap();
    fs::write(
        model_dir.join(".verified"),
        "revision=rev1\nsource=from-file\n",
    )
    .unwrap();
    let manifest = build_test_manifest("repo/model", "rev1", &[("model.onnx", b"model")]);

    let metadata_report =
        classify_model_cache_metadata(&model_dir, &manifest, &ModelAcquisitionPolicy::default());
    assert_eq!(metadata_report.state_code(), "acquired");
    assert!(metadata_report.is_usable());

    let full_report =
        classify_model_cache(&model_dir, &manifest, &ModelAcquisitionPolicy::default());
    assert_eq!(full_report.state_code(), "checksum_mismatch");
}

#[test]
fn classify_cache_detects_incompatible_revision() {
    let tmp = tempfile::tempdir().unwrap();
    let model_dir = tmp.path().join("model");
    fs::create_dir_all(&model_dir).unwrap();
    fs::write(model_dir.join("model.onnx"), b"model").unwrap();
    fs::write(model_dir.join(".verified"), "revision=old\n").unwrap();
    let manifest = build_test_manifest("repo/model", "rev1", &[("model.onnx", b"model")]);

    let report = classify_model_cache(&model_dir, &manifest, &ModelAcquisitionPolicy::default());
    assert_eq!(report.state_code(), "incompatible_version");
    assert!(matches!(
        report.state,
        ModelCacheState::IncompatibleVersion {
            current_revision,
            expected_revision
        } if current_revision == "old" && expected_revision == "rev1"
    ));
}

#[test]
fn classify_cache_treats_legacy_mirror_marker_as_acquired() {
    // Pre-local-only installs may carry `source=mirror:<url>` markers.
    // There is no mirror path anymore, but a verified install stays usable.
    let tmp = tempfile::tempdir().unwrap();
    let model_dir = tmp.path().join("model");
    fs::create_dir_all(&model_dir).unwrap();
    fs::write(model_dir.join("model.onnx"), b"model").unwrap();
    fs::write(
        model_dir.join(".verified"),
        "revision=rev1\nsource=mirror:https://mirror.example/cache\n",
    )
    .unwrap();
    let manifest = build_test_manifest("repo/model", "rev1", &[("model.onnx", b"model")]);

    let report = classify_model_cache(&model_dir, &manifest, &ModelAcquisitionPolicy::default());
    assert_eq!(report.state_code(), "acquired");
    assert!(report.is_usable());
}

#[test]
fn classify_cache_reports_quarantine_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let model_dir = tmp.path().join("model");
    fs::create_dir_all(&model_dir).unwrap();
    fs::write(model_dir.join(".quarantined"), "bad checksum\n").unwrap();
    let manifest = build_test_manifest("repo/model", "rev1", &[("model.onnx", b"model")]);

    let report = classify_model_cache(&model_dir, &manifest, &ModelAcquisitionPolicy::default());
    assert_eq!(report.state_code(), "quarantined_corrupt");
    assert!(matches!(
        report.state,
        ModelCacheState::QuarantinedCorrupt { reason, .. } if reason == "bad checksum"
    ));
}

#[test]
fn test_compute_sha256() {
    let tmp = tempfile::tempdir().unwrap();
    let file_path = tmp.path().join("test.txt");
    fs::write(&file_path, b"hello world").unwrap();
    let hash = compute_sha256(&file_path).unwrap();
    // SHA256 of "hello world"
    assert_eq!(
        hash,
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    );
}

#[test]
fn test_model_file_local_name() {
    // Test that local_name() extracts basename from path with subdirectories
    let file = ModelFile {
        name: "onnx/model.onnx".into(),
        sha256: "abc123".into(),
        size: 1000,
    };
    assert_eq!(file.local_name(), "model.onnx");

    // Test that local_name() works for files without subdirectory
    let file2 = ModelFile {
        name: "tokenizer.json".into(),
        sha256: "def456".into(),
        size: 500,
    };
    assert_eq!(file2.local_name(), "tokenizer.json");

    // Test nested paths
    let file3 = ModelFile {
        name: "path/to/deep/model.bin".into(),
        sha256: "ghi789".into(),
        size: 2000,
    };
    assert_eq!(file3.local_name(), "model.bin");
}

#[test]
fn test_atomic_install_preserves_preexisting_legacy_backup_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let target_dir = tmp.path().join("model");
    copy_model_fixtures(&target_dir).unwrap();
    fs::write(target_dir.join(".verified"), "revision=old\n").unwrap();

    let legacy_backup_dir = tmp.path().join("model.bak");
    fs::create_dir_all(&legacy_backup_dir).unwrap();
    fs::write(legacy_backup_dir.join("sentinel.txt"), "keep me").unwrap();

    let installer = ModelInstaller::new(target_dir.clone());
    copy_model_fixtures(&installer.temp_dir).unwrap();
    fs::write(installer.temp_dir.join(".verified"), "revision=new\n").unwrap();

    installer.atomic_install().unwrap();

    assert_eq!(
        fs::read_to_string(legacy_backup_dir.join("sentinel.txt")).unwrap(),
        "keep me"
    );
    assert_eq!(
        fs::read_to_string(target_dir.join(".verified")).unwrap(),
        "revision=new\n"
    );
}

#[test]
fn test_atomic_install_rejects_file_target() {
    let tmp = tempfile::tempdir().unwrap();
    let target_dir = tmp.path().join("model");
    fs::write(&target_dir, "not a directory").unwrap();

    let installer = ModelInstaller::new(target_dir.clone());
    copy_model_fixtures(&installer.temp_dir).unwrap();

    let err = installer.atomic_install().unwrap_err();

    assert!(
        err.to_string().contains("not a directory"),
        "unexpected error: {err}"
    );
    assert!(installer.temp_dir.exists());
    assert_eq!(fs::read_to_string(&target_dir).unwrap(), "not a directory");
}

#[test]
#[cfg(unix)]
fn test_atomic_install_rejects_dangling_symlink_target() {
    use std::os::unix::fs::symlink;

    let tmp = tempfile::tempdir().unwrap();
    let target_dir = tmp.path().join("model");
    let missing_target = tmp.path().join("missing-model");
    symlink(&missing_target, &target_dir).unwrap();

    let installer = ModelInstaller::new(target_dir.clone());
    copy_model_fixtures(&installer.temp_dir).unwrap();

    let err = installer.atomic_install().unwrap_err();

    assert!(
        err.to_string().contains("through symlink"),
        "unexpected error: {err}"
    );
    assert!(installer.temp_dir.exists());
    assert!(
        fs::symlink_metadata(&target_dir)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(!missing_target.exists());
}

#[test]
fn test_write_verified_marker_records_from_file_provenance() {
    let tmp = tempfile::tempdir().unwrap();
    let target_dir = tmp.path().join("model");
    let source_dir = tmp.path().join("source");
    fs::create_dir_all(&source_dir).unwrap();

    let installer = ModelInstaller::new(target_dir);
    fs::create_dir_all(&installer.temp_dir).unwrap();
    let manifest = build_test_manifest("repo/model", "rev123", &[("model.onnx", b"m")]);
    installer
        .write_verified_marker(&source_dir, &manifest)
        .unwrap();

    let marker = fs::read_to_string(installer.temp_dir.join(".verified")).unwrap();
    assert!(marker.starts_with("revision=rev123\n"));
    assert!(marker.contains("verified_at="));
    assert!(marker.contains("source=from-file"));
    assert!(marker.contains("source_path="));
    assert!(marker.contains("fingerprint="));
}

#[test]
fn test_install_error_display() {
    let display_cases = [
        (
            InstallError::VerificationFailed {
                file: "test.onnx".into(),
                expected: "abc".into(),
                actual: "def".into(),
            },
            "verification failed for test.onnx: expected abc, got def",
        ),
        (
            InstallError::MissingSourceFile {
                file: "model.onnx".into(),
                source_dir: "/models/src".into(),
            },
            "required file 'model.onnx' not found in /models/src",
        ),
        (
            InstallError::SourceIsTarget("/models/minilm".into()),
            "source directory /models/minilm is the model install target itself",
        ),
        (
            InstallError::ManifestNotVerified {
                model_id: "test-model".into(),
                unverified_files: vec!["model.onnx".into(), "config.json".into()],
                revision_unpinned: true,
            },
            "model 'test-model' is not production-ready: 2 file(s) have placeholder checksums and revision is not pinned",
        ),
        (
            InstallError::ManifestNotVerified {
                model_id: "test-model".into(),
                unverified_files: vec!["model.onnx".into()],
                revision_unpinned: false,
            },
            "model 'test-model' is not production-ready: 1 file(s) have placeholder checksums",
        ),
    ];

    for (err, expected) in display_cases {
        assert_eq!(err.to_string(), expected);
    }

    let err: InstallError = std::io::Error::other("disk full").into();

    assert_eq!(err.to_string(), "I/O error: disk full");
    let source = err.source().expect("I/O errors expose their source");
    assert_eq!(source.to_string(), "disk full");

    assert!(
        InstallError::MissingSourceFile {
            file: "x".into(),
            source_dir: "y".into(),
        }
        .source()
        .is_none(),
        "non-source variants must not gain an error source"
    );
}

#[test]
fn test_manifest_production_ready_minilm() {
    // MiniLM should be production-ready (verified checksums + pinned revision)
    let manifest = ModelManifest::minilm_v2();
    assert!(manifest.has_verified_checksums());
    assert!(manifest.has_pinned_revision());
    assert!(manifest.is_production_ready());
}

#[test]
fn multilingual_manifest_matches_the_pinned_frankensearch_contract() {
    let manifest = ModelManifest::multilingual_minilm_l12_v2();
    assert!(manifest.is_production_ready());
    assert_eq!(manifest.id, "paraphrase-multilingual-minilm-l12-v2");
    assert_eq!(
        manifest.revision,
        "e8f8c211226b894fcb81acc59f3b34ba3efd5f42"
    );
    assert_eq!(manifest.total_size(), 479_724_528);
    assert!(
        manifest.total_size()
            <= SemanticPolicy::compiled_defaults()
                .max_model_size_mb
                .saturating_mul(1_048_576),
        "the default acquisition policy must admit the explicitly supported multilingual model"
    );
    assert_eq!(manifest.files.len(), 5);
    assert_eq!(
        manifest.files[0].sha256,
        "eaa086f0ffee582aeb45b36e34cdd1fe2d6de2bef61f8a559a1bbc9bd955917b"
    );
}

#[test]
fn test_prepare_temp_dir_prunes_stale_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let installer = ModelInstaller::new(tmp.path().join("model"));
    fs::create_dir_all(&installer.temp_dir).unwrap();
    // model.safetensors is in the manifest → kept in staging for the copy.
    fs::write(installer.temp_dir.join("model.safetensors"), b"partial").unwrap();
    // model.onnx is no longer in the manifest (cass #308) → must be pruned.
    fs::write(installer.temp_dir.join("model.onnx"), b"stale-onnx").unwrap();
    fs::write(installer.temp_dir.join("stale.bin"), b"stale").unwrap();
    fs::create_dir_all(installer.temp_dir.join("nested")).unwrap();
    fs::write(
        installer.temp_dir.join("nested").join("should-remove.txt"),
        b"stale",
    )
    .unwrap();

    installer
        .prepare_temp_dir(&ModelManifest::minilm_v2())
        .unwrap();

    assert!(installer.temp_dir.join("model.safetensors").exists());
    assert!(!installer.temp_dir.join("model.onnx").exists());
    assert!(!installer.temp_dir.join("stale.bin").exists());
    assert!(!installer.temp_dir.join("nested").exists());
}

#[test]
fn test_prepare_temp_dir_removes_legacy_downloading_sibling() {
    // A `<target>.downloading` dir is debris from the pre-local-only
    // downloader; the local installer must clean it up before staging.
    let tmp = tempfile::tempdir().unwrap();
    let target_dir = tmp.path().join("model");
    let installer = ModelInstaller::new(target_dir.clone());
    let legacy_dir = model_staging_dir(&target_dir, "downloading");
    fs::create_dir_all(&legacy_dir).unwrap();
    fs::write(legacy_dir.join("partial.onnx"), b"stale").unwrap();

    installer
        .prepare_temp_dir(&ModelManifest::minilm_v2())
        .unwrap();

    assert!(
        !legacy_dir.exists(),
        "legacy .downloading staging dir must be removed"
    );
}

#[test]
#[cfg(unix)]
fn test_prepare_temp_dir_removes_symlink_entries() {
    use std::os::unix::fs::symlink;

    let tmp = tempfile::tempdir().unwrap();
    let installer = ModelInstaller::new(tmp.path().join("model"));
    fs::create_dir_all(&installer.temp_dir).unwrap();
    let outside = tmp.path().join("outside.bin");
    fs::write(&outside, b"outside").unwrap();
    symlink(&outside, installer.temp_dir.join("model.onnx")).unwrap();

    installer
        .prepare_temp_dir(&ModelManifest::minilm_v2())
        .unwrap();

    let metadata = fs::symlink_metadata(installer.temp_dir.join("model.onnx"));
    assert!(
        metadata.is_err(),
        "symlink should be removed before staging"
    );
    assert!(
        outside.exists(),
        "cleanup must not touch the symlink target"
    );
}

#[test]
#[cfg(unix)]
fn test_prepare_temp_dir_rejects_symlinked_temp_dir_without_pruning_target() {
    use std::os::unix::fs::symlink;

    let tmp = tempfile::tempdir().unwrap();
    let installer = ModelInstaller::new(tmp.path().join("model"));
    let outside = tmp.path().join("outside-install-cache");
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("stale.bin"), b"must remain").unwrap();
    symlink(&outside, &installer.temp_dir).unwrap();

    let err = installer
        .prepare_temp_dir(&ModelManifest::minilm_v2())
        .expect_err("symlinked temp dir must be rejected before pruning");

    assert!(
        err.to_string().contains("temp dir through symlink"),
        "unexpected symlink-temp-dir error: {err}"
    );
    assert_eq!(fs::read(outside.join("stale.bin")).unwrap(), b"must remain");
    assert!(
        fs::symlink_metadata(&installer.temp_dir)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[test]
#[cfg(unix)]
fn test_cleanup_temp_skips_symlinked_temp_dir() {
    use std::os::unix::fs::symlink;

    let tmp = tempfile::tempdir().unwrap();
    let installer = ModelInstaller::new(tmp.path().join("model"));
    let outside = tmp.path().join("outside-install-cache");
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("sentinel.bin"), b"must remain").unwrap();
    symlink(&outside, &installer.temp_dir).unwrap();

    installer.cleanup_temp();

    assert_eq!(
        fs::read(outside.join("sentinel.bin")).unwrap(),
        b"must remain"
    );
    assert!(
        fs::symlink_metadata(&installer.temp_dir)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[test]
fn test_install_from_directory_installs_verified_model() {
    let files = [
        ("onnx/model.onnx", b"local-model".as_slice()),
        ("tokenizer.json", br#"{"tokenizer":"ok"}"#.as_slice()),
    ];
    let manifest = build_test_manifest("local/test-model", "rev123", &files);
    let tmp = tempfile::tempdir().unwrap();
    let source_dir = tmp.path().join("source");
    write_source_dir(&source_dir, &files);
    let target_dir = tmp.path().join("model");
    let installer = ModelInstaller::new(target_dir.clone());

    installer
        .install_from_directory(&source_dir, &manifest)
        .unwrap();

    // Files land under their local basenames with exact contents.
    assert_eq!(
        fs::read(target_dir.join("model.onnx")).unwrap(),
        b"local-model"
    );
    assert_eq!(
        fs::read(target_dir.join("tokenizer.json")).unwrap(),
        br#"{"tokenizer":"ok"}"#
    );

    let marker = fs::read_to_string(target_dir.join(".verified")).unwrap();
    assert!(
        marker.contains("revision=rev123"),
        "verified marker should preserve manifest identity: {marker}"
    );
    assert!(
        marker.contains("source=from-file"),
        "verified marker should record local-file source: {marker}"
    );

    // The installed model classifies as acquired/usable.
    let report = classify_model_cache(&target_dir, &manifest, &ModelAcquisitionPolicy::default());
    assert_eq!(report.state_code(), "acquired");
    assert!(report.is_usable());

    // No staging debris remains.
    assert!(!installer.temp_dir.exists());
}

#[test]
fn test_install_from_directory_reports_missing_source_file() {
    let files = [
        ("model.onnx", b"local-model".as_slice()),
        ("tokenizer.json", b"tok".as_slice()),
    ];
    let manifest = build_test_manifest("local/test-model", "rev123", &files);
    let tmp = tempfile::tempdir().unwrap();
    let source_dir = tmp.path().join("source");
    fs::create_dir_all(&source_dir).unwrap();
    fs::write(source_dir.join("model.onnx"), b"local-model").unwrap();
    // tokenizer.json is absent.
    let target_dir = tmp.path().join("model");
    let installer = ModelInstaller::new(target_dir.clone());

    let err = installer
        .install_from_directory(&source_dir, &manifest)
        .unwrap_err();

    assert!(
        matches!(
            err,
            InstallError::MissingSourceFile { ref file, .. } if file == "tokenizer.json"
        ),
        "unexpected error: {err}"
    );
    assert!(
        !target_dir.exists(),
        "a failed install must not create the target directory"
    );
    assert!(
        !installer.temp_dir.exists(),
        "staging must be discarded after a missing-source-file failure"
    );
}

#[test]
fn test_install_from_directory_checksum_failure_preserves_existing_install() {
    let good_files = [("model.onnx", b"good-model".as_slice())];
    let good_manifest = build_test_manifest("local/test-model", "rev1", &good_files);
    let tmp = tempfile::tempdir().unwrap();
    let source_v1 = tmp.path().join("source-v1");
    write_source_dir(&source_v1, &good_files);
    let target_dir = tmp.path().join("model");
    let installer = ModelInstaller::new(target_dir.clone());
    installer
        .install_from_directory(&source_v1, &good_manifest)
        .unwrap();
    let marker_before = fs::read_to_string(target_dir.join(".verified")).unwrap();

    // Second manifest expects different content but the source ships
    // corrupt bytes: verification must fail before publication.
    let bad_manifest = build_test_manifest(
        "local/test-model",
        "rev2",
        &[("model.onnx", b"expected-other")],
    );
    let source_v2 = tmp.path().join("source-v2");
    fs::create_dir_all(&source_v2).unwrap();
    fs::write(source_v2.join("model.onnx"), b"corrupt-bytes").unwrap();

    let err = installer
        .install_from_directory(&source_v2, &bad_manifest)
        .unwrap_err();

    assert!(
        matches!(err, InstallError::VerificationFailed { .. }),
        "corrupt local payloads must fail checksum verification, got: {err}"
    );
    assert!(
        !installer.temp_dir.exists(),
        "verification failures should discard the staging directory"
    );
    assert_eq!(
        fs::read(target_dir.join("model.onnx")).unwrap(),
        b"good-model",
        "the prior installation must remain intact"
    );
    assert_eq!(
        fs::read_to_string(target_dir.join(".verified")).unwrap(),
        marker_before,
        "the prior .verified marker must remain intact"
    );
}

#[test]
fn test_install_from_directory_rejects_source_equal_to_target() {
    let files = [("model.onnx", b"local-model".as_slice())];
    let manifest = build_test_manifest("local/test-model", "rev1", &files);
    let tmp = tempfile::tempdir().unwrap();
    let target_dir = tmp.path().join("model");
    write_source_dir(&target_dir, &files);
    let installer = ModelInstaller::new(target_dir.clone());

    let err = installer
        .install_from_directory(&target_dir, &manifest)
        .unwrap_err();

    assert!(
        matches!(err, InstallError::SourceIsTarget(_)),
        "installing the target onto itself must be refused, got: {err}"
    );
    assert_eq!(
        fs::read(target_dir.join("model.onnx")).unwrap(),
        b"local-model",
        "the in-place directory must not be touched"
    );
}

#[test]
fn test_install_from_directory_rejects_placeholder_checksum_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let manifest = ModelManifest {
        id: "bake-off-candidate".into(),
        repo: "repo/model".into(),
        revision: "rev1".into(),
        files: vec![ModelFile {
            name: "model.onnx".into(),
            sha256: PLACEHOLDER_CHECKSUM.into(),
            size: 5,
        }],
        license: "Apache-2.0".into(),
    };
    let source_dir = tmp.path().join("source");
    fs::create_dir_all(&source_dir).unwrap();
    fs::write(source_dir.join("model.onnx"), b"model").unwrap();
    let target_dir = tmp.path().join("model");
    let installer = ModelInstaller::new(target_dir.clone());

    let err = installer
        .install_from_directory(&source_dir, &manifest)
        .unwrap_err();

    assert!(
        matches!(err, InstallError::ManifestNotVerified { .. }),
        "placeholder manifests must be rejected before any copy, got: {err}"
    );
    assert!(!target_dir.exists());
}

#[test]
fn test_install_from_directory_replaces_existing_install_atomically() {
    let files_v1 = [("model.onnx", b"model-v1".as_slice())];
    let manifest_v1 = build_test_manifest("local/test-model", "rev1", &files_v1);
    let tmp = tempfile::tempdir().unwrap();
    let source_v1 = tmp.path().join("source-v1");
    write_source_dir(&source_v1, &files_v1);
    let target_dir = tmp.path().join("model");
    let installer = ModelInstaller::new(target_dir.clone());
    installer
        .install_from_directory(&source_v1, &manifest_v1)
        .unwrap();
    // Extra junk in the old install must not survive replacement.
    fs::write(target_dir.join("junk.bin"), b"junk").unwrap();

    let files_v2 = [("model.onnx", b"model-v2".as_slice())];
    let manifest_v2 = build_test_manifest("local/test-model", "rev2", &files_v2);
    let source_v2 = tmp.path().join("source-v2");
    write_source_dir(&source_v2, &files_v2);
    installer
        .install_from_directory(&source_v2, &manifest_v2)
        .unwrap();

    assert_eq!(
        fs::read(target_dir.join("model.onnx")).unwrap(),
        b"model-v2"
    );
    let marker = fs::read_to_string(target_dir.join(".verified")).unwrap();
    assert!(marker.contains("revision=rev2"), "marker: {marker}");
    assert!(
        !target_dir.join("junk.bin").exists(),
        "the previous install must be fully replaced, not merged"
    );
    assert!(!installer.temp_dir.exists());
}

#[test]
fn test_install_from_directory_rejects_non_directory_source() {
    let files = [("model.onnx", b"local-model".as_slice())];
    let manifest = build_test_manifest("local/test-model", "rev1", &files);
    let tmp = tempfile::tempdir().unwrap();
    let source_file = tmp.path().join("source.bin");
    fs::write(&source_file, b"not a dir").unwrap();
    let target_dir = tmp.path().join("model");
    let installer = ModelInstaller::new(target_dir.clone());

    let err = installer
        .install_from_directory(&source_file, &manifest)
        .unwrap_err();

    assert!(
        err.to_string().contains("not a directory"),
        "unexpected error: {err}"
    );
    assert!(!target_dir.exists());
}
