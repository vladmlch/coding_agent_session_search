use super::super::*;

#[test]
fn capture_source_file_merges_db_links_into_existing_manifest() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("preparse-then-parsed.jsonl");
    let source_bytes = b"{\"type\":\"message\",\"text\":\"hello\"}\n";
    fs::write(&source_path, source_bytes).expect("write source");

    let preparse = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect("preparse capture");

    let parsed_link = RawMirrorDbLink {
        conversation_id: None,
        message_count: Some(1),
        source_path: Some(source_path.display().to_string()),
        started_at_ms: Some(1_733_000_000_000),
    };
    let parsed = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: std::slice::from_ref(&parsed_link),
    })
    .expect("parsed capture");

    assert_eq!(preparse.manifest_id, parsed.manifest_id);
    assert_eq!(preparse.blob_blake3, parsed.blob_blake3);
    assert!(parsed.already_present);

    let manifest_path = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR)
        .join(&parsed.manifest_relative_path);
    let manifest = read_raw_mirror_manifest(&manifest_path).expect("merged manifest");
    assert_eq!(
        manifest.db_links,
        vec![parsed_link],
        "second capture must enrich the pre-parse manifest with DB-link evidence"
    );
    let expected_manifest_blake3 = raw_mirror_manifest_blake3(&manifest);
    assert_eq!(
        manifest.manifest_blake3.as_deref(),
        Some(expected_manifest_blake3.as_str()),
        "manifest checksum must be recomputed after DB-link merge"
    );
    assert_eq!(fs::read(&source_path).expect("source bytes"), source_bytes);
}

#[test]
fn merge_manifest_db_links_rejects_hostile_relative_paths() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let db_link = RawMirrorDbLink {
        conversation_id: Some(42),
        message_count: Some(1),
        source_path: Some("source.jsonl".to_string()),
        started_at_ms: Some(1_733_000_000_000),
    };

    for relative in [
        "../escape.json",
        "/tmp/escape.json",
        "manifests/../escape.json",
        "blobs/blake3/ab/not-a-manifest.raw",
        "manifests/not-json.txt",
    ] {
        let err = merge_manifest_db_links(&data_dir, relative, std::slice::from_ref(&db_link))
            .expect_err("hostile manifest path should be rejected");
        assert!(
            err.to_string().contains("raw mirror manifest path"),
            "unexpected error for {relative}: {err}"
        );
    }
}

#[test]
fn merge_manifest_db_links_refuses_to_publish_an_unreadable_oversized_manifest() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("source.jsonl");
    fs::write(
        &source_path,
        b"{\"type\":\"message\",\"text\":\"bounded\"}\n",
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
    let manifest_path = raw_mirror_root(&data_dir).join(&captured.manifest_relative_path);
    let original_manifest_bytes = fs::read(&manifest_path).expect("read original manifest");
    let oversized_link = RawMirrorDbLink {
        conversation_id: Some(42),
        message_count: Some(1),
        source_path: Some("x".repeat(RAW_MIRROR_MANIFEST_MAX_BYTES as usize)),
        started_at_ms: Some(1_733_000_000_000),
    };

    let err = merge_manifest_db_links(
        &data_dir,
        &captured.manifest_relative_path,
        std::slice::from_ref(&oversized_link),
    )
    .expect_err("oversized merged manifest must be refused");

    assert!(
        format!("{err:#}").contains("validation limit is 16777216 bytes"),
        "unexpected oversized-merge error: {err:#}"
    );
    assert_eq!(
        fs::read(&manifest_path).expect("re-read manifest"),
        original_manifest_bytes,
        "a refused merge must leave the last readable manifest intact"
    );
    read_raw_mirror_manifest(&manifest_path).expect("original manifest remains readable");
}

#[test]
fn recapture_and_db_link_merge_refuse_to_launder_manifest_checksum_drift() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("source.jsonl");
    fs::write(
        &source_path,
        b"{\"type\":\"message\",\"text\":\"integrity\"}\n",
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
    let manifest_path = raw_mirror_root(&data_dir).join(&captured.manifest_relative_path);
    let mut manifest = read_raw_mirror_manifest(&manifest_path).expect("read manifest");
    manifest.redacted_original_path = "[tampered]/source.jsonl".to_string();
    let tampered_bytes = serde_json::to_vec_pretty(&manifest).expect("serialize drift");
    fs::write(&manifest_path, &tampered_bytes).expect("plant checksum drift");

    let summary = storage_summary(&data_dir);
    assert_eq!(summary.manifest_count, 1);
    assert_eq!(summary.invalid_manifest_count, 1);
    assert_eq!(summary.unique_blob_count, 0);
    assert_eq!(summary.total_blob_bytes, 0);

    let recapture_error = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect_err("idempotent recapture must reject manifest checksum drift");
    assert!(
        recapture_error
            .to_string()
            .contains("identity or checksum is invalid"),
        "unexpected recapture error: {recapture_error:#}"
    );

    let merge_error = merge_manifest_db_links(
        &data_dir,
        &captured.manifest_relative_path,
        &[RawMirrorDbLink {
            conversation_id: Some(42),
            message_count: Some(1),
            source_path: Some(source_path.display().to_string()),
            started_at_ms: Some(1_733_000_000_000),
        }],
    )
    .expect_err("DB-link merge must reject manifest checksum drift");
    assert!(
        merge_error
            .to_string()
            .contains("identity or checksum is invalid"),
        "unexpected merge error: {merge_error:#}"
    );
    assert_eq!(
        fs::read(&manifest_path).expect("read refused manifest"),
        tampered_bytes,
        "refusal must not rewrite a drifted manifest with a fresh checksum"
    );
}

#[cfg(unix)]
#[test]
fn merge_manifest_db_links_rejects_symlink_manifest_path() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let manifest_dir = data_dir.join("raw-mirror/v1/manifests");
    fs::create_dir_all(&manifest_dir).expect("manifest dir");
    let outside = temp.path().join("outside.json");
    fs::write(&outside, "{}").expect("outside manifest");
    std::os::unix::fs::symlink(&outside, manifest_dir.join("link.json")).expect("symlink manifest");
    let db_link = RawMirrorDbLink {
        conversation_id: Some(42),
        message_count: Some(1),
        source_path: Some("source.jsonl".to_string()),
        started_at_ms: Some(1_733_000_000_000),
    };

    let err = merge_manifest_db_links(
        &data_dir,
        "manifests/link.json",
        std::slice::from_ref(&db_link),
    )
    .expect_err("symlink manifest should be rejected");
    assert_eq!(
        err.to_string(),
        "raw mirror manifest path contains a symlink"
    );
    assert_eq!(
        fs::read(&outside).expect("unchanged outside manifest"),
        b"{}"
    );
    assert!(
        fs::symlink_metadata(manifest_dir.join("link.json"))
            .expect("preserved manifest symlink")
            .file_type()
            .is_symlink()
    );
}

#[test]
fn capture_source_file_deduplicates_blob_for_distinct_source_paths() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let first_source = temp.path().join("first.jsonl");
    let second_source = temp.path().join("second.jsonl");
    let source_bytes = b"{\"type\":\"message\",\"text\":\"shared\"}\n";
    fs::write(&first_source, source_bytes).expect("write first source");
    fs::write(&second_source, source_bytes).expect("write second source");

    let first = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &first_source,
        db_links: &[],
    })
    .expect("first capture");
    let second = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &second_source,
        db_links: &[],
    })
    .expect("second capture");

    assert_eq!(first.blob_blake3, second.blob_blake3);
    assert_eq!(first.blob_relative_path, second.blob_relative_path);
    assert_ne!(first.manifest_id, second.manifest_id);
    assert!(
        !second.already_present,
        "a duplicate blob with a new source manifest is not a full capture replay"
    );

    let manifest_root = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR)
        .join("manifests");
    let manifests = fs::read_dir(manifest_root)
        .expect("manifest dir")
        .collect::<std::io::Result<Vec<_>>>()
        .expect("manifest entries");
    assert_eq!(manifests.len(), 2);

    let summary = storage_summary(&data_dir);
    assert!(summary.initialized);
    assert_eq!(summary.manifest_count, 2);
    assert_eq!(summary.unique_blob_count, 1);
    assert_eq!(summary.total_blob_bytes, source_bytes.len() as u64);
    assert_eq!(summary.largest_blob_bytes, source_bytes.len() as u64);
    assert_eq!(summary.missing_blob_count, 0);
    assert_eq!(summary.invalid_manifest_count, 0);
    assert!(summary.total_storage_bytes >= source_bytes.len() as u64);
}

#[cfg(unix)]
#[test]
fn storage_summary_rejects_blob_below_symlinked_directory() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("source.jsonl");
    let source_bytes = b"{\"type\":\"message\",\"text\":\"outside\"}\n";
    fs::write(&source_path, source_bytes).expect("source bytes");
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
    let blob_path = root.join(&captured.blob_relative_path);
    let blob_parent = blob_path.parent().expect("blob parent");
    let relocated_parent = temp.path().join("relocated-blob-prefix");
    fs::rename(blob_parent, &relocated_parent).expect("relocate blob prefix");
    std::os::unix::fs::symlink(&relocated_parent, blob_parent).expect("symlink blob prefix");

    let summary = storage_summary(&data_dir);
    assert_eq!(summary.manifest_count, 1);
    assert_eq!(summary.unique_blob_count, 0);
    assert_eq!(summary.total_blob_bytes, 0);
    assert_eq!(summary.missing_blob_count, 1);
    assert_eq!(
        fs::read(&blob_path).expect("outside blob bytes"),
        source_bytes
    );
}

#[test]
fn storage_summary_rejects_hostile_blob_relative_path() {
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
    let manifest_path = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR)
        .join(&captured.manifest_relative_path);
    let mut manifest = read_raw_mirror_manifest(&manifest_path).expect("read manifest");
    manifest.blob_relative_path = "../outside.raw".to_string();
    manifest.manifest_blake3 = Some(raw_mirror_manifest_blake3(&manifest));
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("serialize manifest"),
    )
    .expect("tamper manifest");

    let summary = storage_summary(&data_dir);
    assert_eq!(summary.manifest_count, 1);
    assert_eq!(summary.invalid_manifest_count, 1);
    assert_eq!(summary.unique_blob_count, 0);
    assert_eq!(summary.total_blob_bytes, 0);

    let recapture_error = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect_err("recapture must reject checksum-valid but structurally invalid metadata");
    assert!(
        format!("{recapture_error:#}").contains("storage metadata is invalid"),
        "unexpected structurally-invalid recapture error: {recapture_error:#}"
    );
}

#[test]
fn manifest_read_stays_bounded_when_file_grows_after_open() -> Result<()> {
    let temp = tempfile::TempDir::new()?;
    let path = temp.path().join("growing-manifest.json");
    fs::write(&path, b"{}")?;
    let observed = fs::symlink_metadata(&path)?;
    let mut opened = open_stable_source_file(&path, &observed)?;
    assert_eq!(observed.len(), 2);
    OpenOptions::new()
        .write(true)
        .open(&path)?
        .set_len(RAW_MIRROR_MANIFEST_MAX_BYTES * 4)?;

    let error = read_raw_mirror_manifest_from_open_file(&path, &mut opened)
        .expect_err("growth after stat/open must not bypass the byte limit");
    assert!(error.to_string().contains("larger than 16777216 bytes"));
    assert_eq!(
        std::io::Seek::stream_position(&mut opened)?,
        RAW_MIRROR_MANIFEST_MAX_BYTES + 1,
        "reject after the sentinel byte without reading the full grown file"
    );
    assert_eq!(
        fs::metadata(&path)?.len(),
        RAW_MIRROR_MANIFEST_MAX_BYTES * 4
    );
    Ok(())
}

#[test]
fn oversized_manifest_is_rejected_before_json_allocation_or_prune_planning() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("source.jsonl");
    fs::write(
        &source_path,
        b"{\"type\":\"message\",\"text\":\"bounded\"}\n",
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
    let root = raw_mirror_root(&data_dir);
    let manifest_path = root.join(&captured.manifest_relative_path);
    OpenOptions::new()
        .write(true)
        .open(&manifest_path)
        .expect("open manifest fixture")
        .set_len(RAW_MIRROR_MANIFEST_MAX_BYTES + 1)
        .expect("plant oversized sparse manifest");

    let summary = storage_summary(&data_dir);
    assert_eq!(summary.manifest_count, 0);
    assert_eq!(summary.invalid_manifest_count, 1);
    let err = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            apply: false,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect_err("oversized manifest must not enter prune planning");
    assert!(
        format!("{err:#}").contains("larger than 16777216 bytes"),
        "unexpected oversized-manifest error: {err:#}"
    );
    assert!(manifest_path.exists());
    assert!(root.join(&captured.blob_relative_path).exists());
}
