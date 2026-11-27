use super::super::*;

#[test]
fn prune_fails_closed_on_hostile_manifest_inventory() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("source.jsonl");
    fs::write(
        &source_path,
        b"{\"type\":\"message\",\"text\":\"hostile\"}\n",
    )
    .expect("write source");

    let captured = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect("capture source");
    let root = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR);
    let manifest_path = root.join(&captured.manifest_relative_path);
    let blob_path = root.join(&captured.blob_relative_path);
    let mut manifest = read_raw_mirror_manifest(&manifest_path).expect("read manifest");
    manifest.blob_relative_path = "../outside.raw".to_string();
    manifest.manifest_blake3 = Some(raw_mirror_manifest_blake3(&manifest));
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("serialize manifest"),
    )
    .expect("tamper manifest");

    let err = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            max_size_bytes: None,
            keep_tags: Vec::new(),
            safety_hold_down_ms: 0,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect_err("hostile inventory should fail closed");

    let detail = format!("{err:#}");
    assert!(
        detail.contains("blob path does not match its digest"),
        "error should explain the unsafe manifest inventory: {detail}"
    );
    assert!(manifest_path.exists());
    assert!(blob_path.exists());
    assert!(!root.join("pruned.jsonl").exists());
}

#[test]
fn prune_fails_closed_on_manifest_checksum_drift_before_removing_evidence() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("source.jsonl");
    fs::write(&source_path, b"{\"type\":\"message\",\"text\":\"old\"}\n").expect("write source");
    let captured = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect("capture source");
    let root = raw_mirror_root(&data_dir);
    let manifest_path = root.join(&captured.manifest_relative_path);
    let blob_path = root.join(&captured.blob_relative_path);
    let mut manifest = read_raw_mirror_manifest(&manifest_path).expect("read manifest");
    manifest.captured_at_ms = 0;
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("serialize drifted manifest"),
    )
    .expect("plant manifest checksum drift");

    let err = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            safety_hold_down_ms: 0,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect_err("manifest checksum drift must not authorize pruning");
    let detail = format!("{err:#}");
    assert!(
        detail.contains("valid identity and checksum"),
        "unexpected checksum-drift error: {detail}"
    );
    assert!(manifest_path.exists());
    assert!(blob_path.exists());
    assert!(!root.join("pruned.jsonl").exists());
}

#[test]
fn prune_partial_apply_preserves_completed_results_and_retry_does_not_repeat_them() -> Result<()> {
    let temp = tempfile::TempDir::new()?;
    let data_dir = temp.path().join("cass-data");
    let root = ensure_raw_mirror_root(&data_dir)?;
    for bytes in [b"first blob", b"other blob"] {
        let digest = blake3::hash(bytes).to_hex().to_string();
        let relative = raw_mirror_blob_relative_path(&digest).context("valid digest")?;
        let path = root.join(relative);
        ensure_private_dir_descendant(&root, path.parent().context("blob parent")?)?;
        fs::write(path, bytes)?;
    }
    let mut report = prune(
        &data_dir,
        RawMirrorPruneOptions {
            max_size_bytes: Some(0),
            ..RawMirrorPruneOptions::default()
        },
    )?;
    assert_eq!(report.entries.len(), 2);
    let first = root.join(&report.entries[0].path);
    let second = root.join(&report.entries[1].path);
    let original = fs::read(&second)?;
    let mut changed = original.clone();
    changed[0] ^= 1;
    // Exercise the production apply loop with actual post-plan byte drift,
    // without replacing the filesystem or deletion/audit implementation.
    fs::write(&second, &changed)?;
    let (audit_path, mut audit_file) = open_prune_audit_log(&root)?;
    let error = apply_prune_entries(&root, &mut report, &audit_path, &mut audit_file)
        .expect_err("the second blob changed after planning");
    assert!(format!("{error:#}").contains("revalidating raw mirror blob"));
    assert!(!first.exists());
    assert_eq!(fs::read(&second)?, changed);
    assert_eq!(report.applied_blob_count, 1);
    let partial_audit = fs::read_to_string(&audit_path)?;
    let records = partial_audit
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(
        records.len(),
        3,
        "two intents and the completed first result"
    );
    assert!(
        records[..2]
            .iter()
            .all(|row| { row["phase"] == "intent" && row["applied"] == false })
    );
    assert_eq!(records[2]["phase"], "result");
    assert_eq!(records[2]["path"], report.entries[0].path);
    assert_eq!(records[2]["applied"], true);
    drop(audit_file);

    fs::write(&second, original)?;
    let retry = prune(
        &data_dir,
        RawMirrorPruneOptions {
            max_size_bytes: Some(0),
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )?;
    assert_eq!(retry.applied_blob_count, 1);
    assert!(!second.exists());
    let completed_audit = fs::read_to_string(&audit_path)?;
    let results = completed_audit
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 5);
    for entry in &report.entries {
        assert_eq!(
            results
                .iter()
                .filter(|row| row["phase"] == "result" && row["path"] == entry.path)
                .count(),
            1,
            "retry must not repeat a completed removal"
        );
    }
    let empty_retry = prune(
        &data_dir,
        RawMirrorPruneOptions {
            max_size_bytes: Some(0),
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )?;
    assert_eq!(empty_retry.applied_blob_count, 0);
    assert_eq!(fs::read_to_string(audit_path)?, completed_audit);
    Ok(())
}

#[test]
fn prune_inventories_and_reclaims_crash_orphaned_content_blobs() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let root = ensure_raw_mirror_root(&data_dir).expect("raw mirror root");
    let orphan_bytes = b"published chunk whose manifest never committed";
    let orphan_blake3 = blake3::hash(orphan_bytes).to_hex().to_string();
    let orphan_relative = raw_mirror_blob_relative_path(&orphan_blake3).expect("orphan blob path");
    let orphan_path = root.join(&orphan_relative);
    ensure_private_dir_descendant(&root, orphan_path.parent().expect("orphan parent"))
        .expect("orphan parent directory");
    fs::write(&orphan_path, orphan_bytes).expect("plant crash-orphaned blob");

    let summary = storage_summary(&data_dir);
    assert_eq!(summary.unique_blob_count, 0);
    assert_eq!(summary.total_blob_bytes, 0);
    assert_eq!(summary.orphan_blob_count, 1);
    assert_eq!(summary.orphan_blob_bytes, orphan_bytes.len() as u64);

    let dry_run = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            safety_hold_down_ms: 0,
            apply: false,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("plan orphan reclaim");
    assert_eq!(dry_run.manifest_count, 0);
    assert_eq!(dry_run.unique_blob_count, 1);
    assert_eq!(dry_run.current_blob_bytes, orphan_bytes.len() as u64);
    assert_eq!(dry_run.orphan_blob_count, 1);
    assert_eq!(dry_run.orphan_blob_bytes, orphan_bytes.len() as u64);
    assert_eq!(dry_run.planned_manifest_count, 0);
    assert_eq!(dry_run.planned_blob_count, 1);
    assert!(dry_run.entries.iter().any(|entry| {
        entry.kind == "blob"
            && entry.path == orphan_relative
            && entry.reason.contains("unreferenced blob")
    }));
    assert!(orphan_path.exists(), "dry-run must retain orphan evidence");

    let applied = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            safety_hold_down_ms: 0,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("apply orphan reclaim");
    assert_eq!(applied.applied_manifest_count, 0);
    assert_eq!(applied.applied_blob_count, 1);
    assert_eq!(applied.applied_reclaim_bytes, orphan_bytes.len() as u64);
    assert!(!orphan_path.exists());
}

#[test]
fn prune_hold_down_keeps_recent_crash_orphaned_blob() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let root = ensure_raw_mirror_root(&data_dir).expect("raw mirror root");
    let orphan_bytes = b"recent crash orphan";
    let orphan_blake3 = blake3::hash(orphan_bytes).to_hex().to_string();
    let orphan_relative = raw_mirror_blob_relative_path(&orphan_blake3).expect("orphan blob path");
    let orphan_path = root.join(&orphan_relative);
    ensure_private_dir_descendant(&root, orphan_path.parent().expect("orphan parent"))
        .expect("orphan parent directory");
    fs::write(&orphan_path, orphan_bytes).expect("plant recent orphan");

    let report = prune(
        &data_dir,
        RawMirrorPruneOptions {
            max_size_bytes: Some(0),
            safety_hold_down_ms: 7 * 24 * 60 * 60 * 1_000,
            apply: false,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("plan held-down orphan");
    assert_eq!(report.orphan_blob_count, 1);
    assert_eq!(report.pinned_blob_count, 1);
    assert_eq!(report.planned_blob_count, 0);
    assert!(orphan_path.exists());
}

#[test]
fn prune_apply_hashes_orphan_before_deleting_any_evidence() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let root = ensure_raw_mirror_root(&data_dir).expect("raw mirror root");
    let expected_bytes = b"expected orphan bytes";
    let orphan_blake3 = blake3::hash(expected_bytes).to_hex().to_string();
    let orphan_relative = raw_mirror_blob_relative_path(&orphan_blake3).expect("orphan blob path");
    let orphan_path = root.join(&orphan_relative);
    ensure_private_dir_descendant(&root, orphan_path.parent().expect("orphan parent"))
        .expect("orphan parent directory");
    fs::write(&orphan_path, b"corrupted orphan byte").expect("plant corrupt orphan");
    assert_eq!(
        fs::metadata(&orphan_path).expect("orphan metadata").len(),
        expected_bytes.len() as u64,
        "fixture must preserve size so only the checksum catches corruption"
    );

    let error = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            safety_hold_down_ms: 0,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect_err("checksum-drifted orphan must not be deleted");
    assert!(
        format!("{error:#}").contains("checksum mismatch"),
        "unexpected corrupt-orphan error: {error:#}"
    );
    assert!(orphan_path.exists());
    assert!(!root.join("pruned.jsonl").exists());
}

#[test]
fn prune_delete_revalidates_exact_blob_after_plan_preflight() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let root = ensure_raw_mirror_root(&data_dir).expect("raw mirror root");
    let expected_bytes = b"preflighted raw mirror evidence";
    let expected_blake3 = blake3::hash(expected_bytes).to_hex().to_string();
    let relative_path = raw_mirror_blob_relative_path(&expected_blake3).expect("blob path");
    let blob_path = root.join(&relative_path);
    ensure_private_dir_descendant(&root, blob_path.parent().expect("blob parent directory"))
        .expect("create blob parent directory");
    fs::write(&blob_path, expected_bytes).expect("plant preflighted blob");
    let entry = RawMirrorPruneEntry {
        kind: "blob".to_string(),
        path: relative_path,
        blob_blake3: Some(expected_blake3),
        manifest_blake3: None,
        size_bytes: expected_bytes.len() as u64,
        reason: "test target".to_string(),
        applied: false,
    };

    let mut replacement = expected_bytes.to_vec();
    replacement[0] ^= 1;
    fs::write(&blob_path, replacement).expect("replace blob after plan preflight");

    let error = remove_prune_entry_file(&root, &entry)
        .expect_err("same-size replacement must not be deleted");
    assert!(
        format!("{error:#}").contains("checksum mismatch"),
        "unexpected changed-target error: {error:#}"
    );
    assert!(
        blob_path.exists(),
        "a target whose content changed after preflight must remain on disk"
    );
}

#[test]
fn prune_delete_revalidates_exact_manifest_after_plan_preflight() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("session.jsonl");
    fs::write(&source_path, b"{\"type\":\"message\",\"text\":\"safe\"}\n").expect("write source");
    let captured = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect("capture source");
    let root = raw_mirror_root(&data_dir);
    let manifest_path = root.join(&captured.manifest_relative_path);
    let mut manifest = read_raw_mirror_manifest(&manifest_path).expect("read manifest");
    let expected_manifest_blake3 = manifest
        .manifest_blake3
        .clone()
        .expect("captured manifest checksum");
    let original_size = fs::metadata(&manifest_path)
        .expect("manifest metadata")
        .len();
    let entry = RawMirrorPruneEntry {
        kind: "manifest".to_string(),
        path: captured.manifest_relative_path,
        blob_blake3: Some(captured.blob_blake3),
        manifest_blake3: Some(expected_manifest_blake3),
        size_bytes: original_size,
        reason: "test target".to_string(),
        applied: false,
    };

    manifest.captured_at_ms = manifest.captured_at_ms.saturating_add(1);
    manifest.manifest_blake3 = Some(raw_mirror_manifest_blake3(&manifest));
    let replacement = serde_json::to_vec_pretty(&manifest).expect("serialize replacement");
    assert_eq!(
        replacement.len() as u64,
        original_size,
        "fixture must preserve manifest size so the exact checksum is causal"
    );
    fs::write(&manifest_path, replacement).expect("replace manifest after preflight");

    let error = remove_prune_entry_file(&root, &entry)
        .expect_err("same-size valid replacement manifest must not be deleted");
    assert!(
        format!("{error:#}").contains("manifest checksum changed after preflight"),
        "unexpected changed-manifest error: {error:#}"
    );
    assert!(
        manifest_path.exists(),
        "a manifest whose descriptor changed after preflight must remain on disk"
    );
}

#[test]
fn prune_apply_hashes_selected_manifest_blobs_before_deleting_manifest() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("selected.jsonl");
    let source_bytes = b"{\"type\":\"message\",\"text\":\"safe\"}\n";
    fs::write(&source_path, source_bytes).expect("write source");
    let captured = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect("capture source");
    let root = raw_mirror_root(&data_dir);
    let manifest_path = root.join(&captured.manifest_relative_path);
    let blob_path = root.join(&captured.blob_relative_path);
    let mut manifest = read_raw_mirror_manifest(&manifest_path).expect("read manifest");
    manifest.captured_at_ms = 0;
    manifest.manifest_blake3 = Some(raw_mirror_manifest_blake3(&manifest));
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("serialize aged manifest"),
    )
    .expect("age manifest fixture");
    let mut corrupted = source_bytes.to_vec();
    corrupted[0] ^= 1;
    fs::write(&blob_path, &corrupted).expect("plant same-size blob corruption");

    let error = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            safety_hold_down_ms: 0,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect_err("corrupt selected evidence must stop prune before deletion");
    assert!(
        format!("{error:#}").contains("checksum mismatch"),
        "unexpected selected-blob checksum error: {error:#}"
    );
    assert!(manifest_path.exists());
    assert!(blob_path.exists());
    assert!(!root.join("pruned.jsonl").exists());
}

#[test]
fn prune_apply_refuses_a_held_index_run_lock_without_removing_evidence() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("source.jsonl");
    fs::write(&source_path, b"{\"type\":\"message\",\"text\":\"old\"}\n").expect("write source");
    let captured = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect("capture source");
    let root = raw_mirror_root(&data_dir);
    let index_lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(data_dir.join("index-run.lock"))
        .expect("open index-run lock");
    fs2::FileExt::try_lock_exclusive(&index_lock).expect("hold index-run lock");

    let error = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            safety_hold_down_ms: 0,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect_err("held index-run lock must block applied prune");

    assert!(
        error.to_string().contains("index run acquired"),
        "{error:#}"
    );
    assert!(root.join(captured.manifest_relative_path).exists());
    assert!(root.join(captured.blob_relative_path).exists());
    assert!(!root.join("pruned.jsonl").exists());
}

#[test]
fn prune_dry_run_preserves_mirror_and_existing_audit_bytes() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("source.jsonl");
    fs::write(&source_path, b"{\"type\":\"message\",\"text\":\"old\"}\n").expect("write source");
    let captured = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect("capture source");

    let root = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR);
    let audit_path = root.join("pruned.jsonl");
    let manifest_path = root.join(&captured.manifest_relative_path);
    let blob_path = root.join(&captured.blob_relative_path);
    let manifest_before = fs::read(&manifest_path).expect("manifest bytes");
    let blob_before = fs::read(&blob_path).expect("blob bytes");
    let historical_audit = b"{\"mode\":\"apply\",\"applied\":true}\n";

    for preview in 0..4 {
        if preview == 1 {
            fs::write(&audit_path, historical_audit).expect("seed existing audit");
        }
        let report = prune(
            &data_dir,
            RawMirrorPruneOptions {
                older_than_ms: Some(0),
                safety_hold_down_ms: 0,
                apply: false,
                ..RawMirrorPruneOptions::default()
            },
        )
        .expect("dry-run prune");

        assert!(report.initialized);
        assert_eq!(report.mode, "dry-run");
        assert_eq!(report.planned_manifest_count, 1);
        assert_eq!(report.planned_blob_count, 1);
        assert!(report.planned_reclaim_bytes > 0);
        assert_eq!(report.applied_reclaim_bytes, 0);
        assert!(report.audit_log_path.is_none());
        assert_eq!(fs::read(&manifest_path).expect("manifest"), manifest_before);
        assert_eq!(fs::read(&blob_path).expect("blob"), blob_before);
        if preview == 0 {
            assert!(!audit_path.exists(), "preview created a persistent audit");
        } else {
            assert_eq!(fs::read(&audit_path).expect("audit"), historical_audit);
        }
    }
}

#[test]
#[cfg(unix)]
fn prune_refuses_symlinked_audit_log_without_writing_target() -> Result<()> {
    use std::os::unix::fs::symlink;

    let temp = tempfile::TempDir::new()?;
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("source.jsonl");
    let protected_audit_target = temp.path().join("protected-prune-audit.jsonl");
    fs::write(&source_path, b"{\"type\":\"message\",\"text\":\"old\"}\n")?;
    fs::write(&protected_audit_target, b"protected\n")?;

    let captured = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })?;
    let root = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR);
    let audit_path = root.join("pruned.jsonl");
    symlink(&protected_audit_target, &audit_path)?;

    let err = match prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            max_size_bytes: None,
            keep_tags: Vec::new(),
            safety_hold_down_ms: 0,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    ) {
        Ok(_) => anyhow::bail!("symlinked prune audit log was accepted before deletion"),
        Err(err) => err,
    };

    if !err.to_string().contains("prune audit through symlink") {
        anyhow::bail!("unexpected audit symlink error: {err:#}");
    }
    if !fs::read(&protected_audit_target)?
        .as_slice()
        .eq(b"protected\n")
    {
        anyhow::bail!("protected audit target was modified");
    }
    if !fs::read_link(&audit_path)?
        .as_os_str()
        .eq(protected_audit_target.as_os_str())
    {
        anyhow::bail!("audit path did not remain a symlink to the protected target");
    }
    if !root.join(&captured.manifest_relative_path).exists() {
        anyhow::bail!("failed audit append removed the captured manifest");
    }
    if !root.join(&captured.blob_relative_path).exists() {
        anyhow::bail!("failed audit append removed the captured blob");
    }
    Ok(())
}

#[test]
#[cfg(unix)]
fn prune_preflight_rejects_symlinked_blob_ancestor_before_removing_manifest() -> Result<()> {
    use std::os::unix::fs::symlink;

    let temp = tempfile::TempDir::new()?;
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("source.jsonl");
    fs::write(&source_path, b"{\"type\":\"message\",\"text\":\"old\"}\n")?;
    let captured = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })?;
    let root = raw_mirror_root(&data_dir);
    let manifest_path = root.join(&captured.manifest_relative_path);
    let blob_path = root.join(&captured.blob_relative_path);
    let blob_bucket = blob_path.parent().context("blob bucket")?;
    let outside_bucket = temp.path().join("outside-blob-bucket");
    fs::rename(blob_bucket, &outside_bucket)?;
    symlink(&outside_bucket, blob_bucket)?;

    let err = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            safety_hold_down_ms: 0,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect_err("a symlinked blob ancestor must abort prune before any removal");
    let detail = format!("{err:#}");
    // bet45: the refusal wording moved from "symlinked ancestor" to the
    // blob-path phrasing; pin the semantic (a symlink refusal) rather
    // than the exact sentence.
    assert!(
        detail.contains("symlink"),
        "unexpected prune preflight error: {detail}"
    );
    assert!(
        manifest_path.exists(),
        "preflight failure removed the manifest"
    );
    assert!(blob_path.exists(), "preflight failure removed the blob");
    assert!(
        outside_bucket
            .join(blob_path.file_name().context("blob file name")?)
            .exists(),
        "preflight failure removed the external blob target"
    );
    assert!(!root.join("pruned.jsonl").exists());
    Ok(())
}

#[test]
fn prune_apply_removes_selected_manifest_and_unreferenced_blob() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("source.jsonl");
    fs::write(&source_path, b"{\"type\":\"message\",\"text\":\"apply\"}\n").expect("write source");
    let captured = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect("capture source");
    let root = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR);
    let manifest_path = root.join(&captured.manifest_relative_path);
    let blob_path = root.join(&captured.blob_relative_path);

    let report = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            max_size_bytes: None,
            keep_tags: Vec::new(),
            safety_hold_down_ms: 0,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("apply prune");

    assert_eq!(report.applied_manifest_count, 1);
    assert_eq!(report.applied_blob_count, 1);
    assert!(!manifest_path.exists());
    assert!(!blob_path.exists());
    let audit = fs::read_to_string(root.join("pruned.jsonl")).expect("read audit");
    assert!(audit.contains("\"mode\":\"apply\""));
    assert!(audit.contains("\"phase\":\"intent\""));
    assert!(audit.contains("\"phase\":\"result\""));
    assert!(audit.contains("\"applied\":true"));
}

#[test]
fn prune_apply_keeps_blob_referenced_by_retained_manifest() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let first_source = temp.path().join("first.jsonl");
    let second_source = temp.path().join("second.jsonl");
    let bytes = b"{\"type\":\"message\",\"text\":\"shared-retained\"}\n";
    fs::write(&first_source, bytes).expect("write first");
    fs::write(&second_source, bytes).expect("write second");
    let first = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &first_source,
        db_links: &[],
    })
    .expect("capture first");
    let second = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &second_source,
        db_links: &[],
    })
    .expect("capture second");
    let root = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR);
    let first_manifest_path = root.join(&first.manifest_relative_path);
    let second_manifest_path = root.join(&second.manifest_relative_path);
    let mut first_manifest =
        read_raw_mirror_manifest(&first_manifest_path).expect("first manifest");
    first_manifest.captured_at_ms = now_ms().saturating_sub(2 * 86_400_000);
    first_manifest.manifest_blake3 = Some(raw_mirror_manifest_blake3(&first_manifest));
    fs::write(
        &first_manifest_path,
        serde_json::to_vec_pretty(&first_manifest).expect("serialize first manifest"),
    )
    .expect("rewrite first manifest");

    let report = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(86_400_000),
            max_size_bytes: None,
            keep_tags: Vec::new(),
            safety_hold_down_ms: 0,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("apply one-manifest prune");

    assert_eq!(report.applied_manifest_count, 1);
    assert_eq!(report.applied_blob_count, 0);
    assert!(!first_manifest_path.exists());
    assert!(second_manifest_path.exists());
    assert!(
        root.join(&first.blob_relative_path).exists(),
        "shared blob must stay while a retained manifest still references it"
    );
}

#[test]
fn prune_apply_keep_tag_pins_linked_manifest_and_blob() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let source_path = temp.path().join("tagged.jsonl");
    fs::write(
        &source_path,
        b"{\"type\":\"message\",\"text\":\"tagged\"}\n",
    )
    .expect("write source");
    let db_link = RawMirrorDbLink {
        conversation_id: Some(7),
        message_count: Some(1),
        source_path: Some(source_path.display().to_string()),
        started_at_ms: Some(1_733_000_000_000),
    };
    let captured = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: std::slice::from_ref(&db_link),
    })
    .expect("capture source");
    let db_path = data_dir.join("agent_search.db");
    let conn = crate::franken_sync::Connection::open(db_path.to_string_lossy())
        .expect("open keep-tag db");
    conn.execute_batch(
        "PRAGMA application_id = 1128354643;
         PRAGMA user_version = 1;",
    )
    .expect("stamp schema v1");
    {
        let mut tx = conn
            .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)
            .expect("begin immediate keep-tag setup");
        tx.execute("CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)")
            .expect("create tags");
        tx.execute(
            "CREATE TABLE conversation_tags (conversation_id INTEGER NOT NULL, tag_id INTEGER NOT NULL, PRIMARY KEY (conversation_id, tag_id))",
        )
        .expect("create conversation_tags");
        tx.execute("INSERT INTO tags (id, name) VALUES (1, 'keep')")
            .expect("insert tag");
        tx.execute("INSERT INTO conversation_tags (conversation_id, tag_id) VALUES (7, 1)")
            .expect("insert conversation tag");
        tx.commit().expect("commit keep-tag setup");
    }
    conn.close().expect("close keep-tag db");

    let report = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            max_size_bytes: Some(0),
            keep_tags: vec!["keep".to_string()],
            safety_hold_down_ms: 0,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("keep-tag prune");

    let root = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR);
    assert_eq!(report.pinned_manifest_count, 1);
    assert_eq!(report.pinned_blob_count, 1);
    assert_eq!(report.planned_manifest_count, 0);
    assert_eq!(report.planned_blob_count, 0);
    assert!(root.join(&captured.manifest_relative_path).exists());
    assert!(root.join(&captured.blob_relative_path).exists());
}

#[test]
fn prune_apply_safety_hold_down_pins_recent_manifest_during_size_prune() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("recent.jsonl");
    fs::write(
        &source_path,
        b"{\"type\":\"message\",\"text\":\"recent\"}\n",
    )
    .expect("write source");
    let captured = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect("capture source");

    let report = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: None,
            max_size_bytes: Some(0),
            keep_tags: Vec::new(),
            safety_hold_down_ms: 7 * 86_400_000,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("hold-down prune");

    let root = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR);
    assert_eq!(report.pinned_manifest_count, 1);
    assert_eq!(report.pinned_blob_count, 1);
    assert_eq!(report.planned_manifest_count, 0);
    assert_eq!(report.planned_blob_count, 0);
    assert!(root.join(&captured.manifest_relative_path).exists());
    assert!(root.join(&captured.blob_relative_path).exists());
}

#[test]
fn prune_keep_latest_retires_superseded_captures_but_never_a_sources_only_copy() {
    const DAY_MS: i64 = 86_400_000;
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let root = raw_mirror_root(&data_dir);
    // 64-byte chunks make successive captures of an appended transcript
    // share their leading chunks, as 256 KiB chunks do on real logs.
    let capture = |source_id: &str, source_path: &Path, age_ms: i64| {
        let record = capture_source_file_with_chunk_policy(
            RawMirrorCaptureInput {
                data_dir: &data_dir,
                provider: "codex",
                source_id,
                origin_kind: "local",
                origin_host: None,
                source_path,
                db_links: &[],
            },
            1,
            64,
        )
        .expect("capture source");
        let manifest_path = root.join(&record.manifest_relative_path);
        let mut manifest = read_raw_mirror_manifest(&manifest_path).expect("manifest");
        manifest.captured_at_ms = now_ms().saturating_sub(age_ms);
        manifest.manifest_blake3 = Some(raw_mirror_manifest_blake3(&manifest));
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).expect("serialize manifest"),
        )
        .expect("backdate manifest");
        record
    };

    // A transcript appended to between three index runs.
    let active = temp.path().join("active.jsonl");
    let mut content = Vec::new();
    let mut runs = Vec::new();
    for run in 0..3_u8 {
        let text = blake3::hash(&[run]).to_hex();
        content.extend(format!("{{\"run\":{run},\"text\":\"{text}\"}}\n").into_bytes());
        fs::write(&active, &content).expect("append to transcript");
        runs.push(capture("local", &active, (3 - i64::from(run)) * DAY_MS));
    }
    // A session captured once, before anything else: the only copy of its
    // source, and the first capture a size rule reaches.
    let lone = temp.path().join("lone.jsonl");
    fs::write(&lone, b"{\"text\":\"only copy\"}\n").expect("write lone session");
    let only_copy = capture("local", &lone, 5 * DAY_MS);
    // The same path captured from another machine is a different source,
    // even though it is older than every local capture of that path.
    let laptop = capture("laptop", &active, 4 * DAY_MS);

    let size_only = prune(
        &data_dir,
        RawMirrorPruneOptions {
            max_size_bytes: Some(0),
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("size-only preview");
    assert_eq!(
        size_only.planned_manifest_count, 5,
        "without keep-latest a size rule retires every capture, the only copy included"
    );

    let preview = prune(
        &data_dir,
        RawMirrorPruneOptions {
            keep_latest: Some(1),
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("keep-latest preview");
    let mut planned = preview
        .entries
        .iter()
        .filter(|entry| entry.kind == "manifest")
        .map(|entry| (entry.path.as_str(), entry.reason.as_str()))
        .collect::<Vec<_>>();
    planned.sort_unstable();
    let mut superseded = vec![
        (
            runs[0].manifest_relative_path.as_str(),
            "superseded by the newest 1 captures of its source file",
        ),
        (
            runs[1].manifest_relative_path.as_str(),
            "superseded by the newest 1 captures of its source file",
        ),
    ];
    superseded.sort_unstable();
    assert_eq!(planned, superseded);
    assert_eq!(preview.keep_latest, Some(1));

    // Combined with a size rule that wants everything gone, the newest
    // capture of each source still survives.
    let report = prune(
        &data_dir,
        RawMirrorPruneOptions {
            max_size_bytes: Some(0),
            keep_latest: Some(1),
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("keep-latest apply");
    assert_eq!(report.applied_manifest_count, 2);
    assert!(!root.join(&runs[0].manifest_relative_path).exists());
    assert!(!root.join(&runs[1].manifest_relative_path).exists());
    assert_eq!(
        read_source_bytes(&data_dir, &runs[2].manifest_id).expect("newest capture"),
        content
    );
    assert_eq!(
        read_source_bytes(&data_dir, &laptop.manifest_id).expect("laptop capture"),
        content
    );
    assert_eq!(
        read_source_bytes(&data_dir, &only_copy.manifest_id).expect("only copy"),
        b"{\"text\":\"only copy\"}\n"
    );
    let summary = storage_summary(&data_dir);
    assert_eq!(summary.manifest_count, 3);
    assert_eq!(summary.orphan_blob_count, 0);
    assert_eq!(summary.missing_blob_count, 0);
    assert!(
        report.applied_blob_count > 0,
        "the superseded captures' unshared tail chunks and descriptors are reclaimed"
    );

    let again = prune(
        &data_dir,
        RawMirrorPruneOptions {
            keep_latest: Some(1),
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("second keep-latest apply");
    assert_eq!(again.planned_manifest_count, 0);

    let zero = prune(
        &data_dir,
        RawMirrorPruneOptions {
            keep_latest: Some(0),
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect_err("keep-latest 0 would retire every capture");
    assert!(zero.to_string().contains("at least one capture per source"));
}

#[cfg(unix)]
#[test]
fn prune_reclaims_abandoned_capture_staging_only_unscoped_and_after_an_hour() {
    use std::os::unix::fs::symlink;
    use std::time::{Duration, SystemTime};

    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let root = raw_mirror_root(&data_dir);
    let source = temp.path().join("session.jsonl");
    fs::write(&source, b"{\"text\":\"kept\"}\n").expect("write source");
    let captured = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source,
        db_links: &[],
    })
    .expect("capture source");
    let tmp = root.join("tmp");
    let two_hours_ago = SystemTime::now() - Duration::from_secs(2 * 60 * 60);
    let staging = |dir: PathBuf, bytes: &[u8], modified: Option<SystemTime>| {
        fs::create_dir_all(&dir).expect("staging dir");
        let file = dir.join(".blob.4242.1.0.tmp");
        fs::write(&file, bytes).expect("staging file");
        if let Some(modified) = modified {
            for path in [&file, &dir] {
                std::fs::File::open(path)
                    .expect("open staging entry")
                    .set_times(fs::FileTimes::new().set_modified(modified))
                    .expect("backdate staging entry");
            }
        }
        dir
    };
    // What a capture killed mid-copy leaves behind.
    let abandoned = staging(
        tmp.join("capture.4242.1791900000000000000.7"),
        b"0123456789",
        Some(two_hours_ago),
    );
    let recent = staging(
        tmp.join("capture.4243.1791900000000000000.8"),
        b"young",
        None,
    );
    let unrelated = staging(tmp.join("not-a-capture"), b"other", Some(two_hours_ago));
    let outside = staging(temp.path().join("outside"), b"outside", Some(two_hours_ago));
    let link = tmp.join("capture.4244.1791900000000000000.9");
    symlink(&outside, &link).expect("staging-named symlink");

    let preview = prune(
        &data_dir,
        RawMirrorPruneOptions {
            keep_latest: Some(1),
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("preview");
    let planned = preview
        .entries
        .iter()
        .filter(|entry| entry.kind == "staging")
        .map(|entry| (entry.path.as_str(), entry.size_bytes))
        .collect::<Vec<_>>();
    assert_eq!(
        planned,
        vec![("tmp/capture.4242.1791900000000000000.7", 10)]
    );
    assert_eq!(preview.planned_staging_count, 1);
    assert_eq!(preview.planned_reclaim_bytes, 10);
    assert!(abandoned.exists(), "a preview removes nothing");

    // Staging cannot be attributed to a provider, so a scoped prune leaves it.
    let scoped = prune(
        &data_dir,
        RawMirrorPruneOptions {
            keep_latest: Some(1),
            providers: vec!["codex".to_string()],
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("scoped apply");
    assert_eq!(scoped.planned_staging_count, 0);
    assert!(abandoned.exists());

    let applied = prune(
        &data_dir,
        RawMirrorPruneOptions {
            keep_latest: Some(1),
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("apply");
    assert_eq!(applied.applied_staging_count, 1);
    assert_eq!(applied.applied_reclaim_bytes, 10);
    assert!(!abandoned.exists());
    assert!(recent.exists(), "staging written within the hour stays");
    assert!(
        unrelated.exists(),
        "only capture.<pid>.<nanos>.<nonce> names are staging"
    );
    assert!(
        fs::symlink_metadata(&link).is_ok() && outside.join(".blob.4242.1.0.tmp").exists(),
        "a staging-named symlink is neither followed nor removed"
    );
    assert_eq!(
        read_source_bytes(&data_dir, &captured.manifest_id).expect("published capture"),
        b"{\"text\":\"kept\"}\n"
    );
    let audit = fs::read_to_string(root.join("pruned.jsonl")).expect("prune audit");
    assert!(audit.contains("\"kind\":\"staging\""));
}
