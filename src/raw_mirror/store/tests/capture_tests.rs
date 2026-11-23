use super::super::*;
use super::RawMirrorTraceBuffer;

#[test]
fn gh538_capture_and_link_publication_trace_real_operation_paths() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let data = temp.path().join("data");
    let source = temp.path().join("session.jsonl");
    let original = b"{\"message\":\"diagnose publication without losing recovery evidence\"}\n";
    fs::write(&source, original)?;
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let writer = RawMirrorTraceBuffer(Arc::clone(&bytes));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || writer.clone())
        .finish();
    let completed = Arc::new(AtomicU64::new(0));
    let progress = Arc::clone(&completed);
    let record = tracing::subscriber::with_default(subscriber, || -> Result<_> {
        let _observer = observe_raw_mirror_io(
            Arc::new(move || {
                progress.fetch_add(1, Ordering::Relaxed);
            }),
            Arc::new(|| false),
        );
        let record = policy::capture_default(
            RawMirrorCaptureInput {
                data_dir: &data,
                provider: "codex",
                source_id: "local",
                origin_kind: "local",
                origin_host: None,
                source_path: &source,
                db_links: &[],
            },
            policy::RawMirrorRetentionPolicy {
                max_bytes: 1024 * 1024,
                min_free_bytes: 0,
            },
        )?;
        merge_manifest_db_links(
            &data,
            &record.manifest_relative_path,
            &[RawMirrorDbLink {
                conversation_id: Some(538),
                message_count: Some(1),
                source_path: Some(source.display().to_string()),
                started_at_ms: None,
            }],
        )?;
        // Exercise the actual directory flush without changing process-wide
        // CASS_RAW_MIRROR_FSYNC or requiring any test-only filesystem path.
        sync_parent_unconditionally(&raw_mirror_root(&data).join(&record.manifest_relative_path))?;
        Ok(record)
    })?;
    let log = String::from_utf8(bytes.lock().unwrap().clone())?;
    let events = log
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let observed = |operation: &str, path: &Path| {
        events.iter().any(|event| {
            event["fields"]["operation"] == operation
                && event["fields"]["path"] == path.display().to_string()
        })
    };
    let root = raw_mirror_root(&data);
    let manifest = root.join(&record.manifest_relative_path);
    assert!(observed("fstat_source", &source));
    assert!(observed(
        "fstat_mutation_lock",
        &root.join(RAW_MIRROR_MUTATION_LOCK_FILE)
    ));
    assert!(observed(
        "publish_blob",
        &root.join(&record.blob_relative_path)
    ));
    assert!(observed("publish_manifest", &manifest));
    assert!(observed("replace_manifest", &manifest));
    assert!(observed(
        "replace_manifest",
        &root.join(".usage-generation")
    ));
    for operation in [
        "create_file",
        "write_manifest",
        "remove_temp_file",
        "remove_temp_dir",
    ] {
        assert!(
            events.iter().any(|event| {
                event["fields"]["operation"] == operation
                    && event["fields"]["path"]
                        .as_str()
                        .is_some_and(|path| Path::new(path).starts_with(root.join("tmp")))
            }),
            "missing staging trace for {operation}"
        );
    }
    #[cfg(not(windows))]
    {
        assert!(observed("open_parent_for_sync", &root.join("manifests")));
        assert!(observed("sync_parent", &root.join("manifests")));
    }
    assert!(completed.load(Ordering::Relaxed) > 0);
    assert!(
        raw_mirror_io_snapshot()
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| {
                !entry["path"]
                    .as_str()
                    .is_some_and(|path| Path::new(path).starts_with(&root))
            })
    );
    assert_eq!(read_source_bytes(&data, &record.manifest_id)?, original);
    assert_eq!(fs::read_dir(root.join("tmp"))?.count(), 0);
    Ok(())
}

#[test]
fn gh538_cancellation_does_not_skip_publication_cleanup_or_durability() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let staging = temp.path().join("staging");
    fs::create_dir(&staging)?;
    let path = staging.join("staged.raw");
    fs::write(&path, b"a completed write still needs cleanup")?;
    let completed = Arc::new(AtomicU64::new(0));
    let progress = Arc::clone(&completed);
    let _observer = observe_raw_mirror_io(
        Arc::new(move || {
            progress.fetch_add(1, Ordering::Relaxed);
        }),
        Arc::new(|| true),
    );
    assert_eq!(
        raw_mirror_symlink_metadata(&path).unwrap_err().kind(),
        std::io::ErrorKind::Interrupted
    );
    assert_eq!(completed.load(Ordering::Relaxed), 0);
    sync_parent_unconditionally(&path)?;
    #[cfg(not(windows))]
    assert_eq!(
        completed.load(Ordering::Relaxed),
        2,
        "the directory open and sync must still complete"
    );
    let before_cleanup = completed.load(Ordering::Relaxed);
    remove_temp_best_effort(&path);
    remove_empty_temp_dir_best_effort(&staging);
    assert_eq!(completed.load(Ordering::Relaxed), before_cleanup + 2);
    assert!(!path.try_exists()?);
    assert!(!staging.try_exists()?);
    Ok(())
}

#[test]
#[cfg(unix)]
fn gh538_dangling_manifest_symlink_is_refused_before_staging() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = ensure_raw_mirror_root(temp.path())?;
    let manifests = root.join("manifests");
    ensure_private_dir_descendant(&root, &manifests)?;
    let manifest = manifests.join("pending.json");
    let absent = temp.path().join("absent.json");
    std::os::unix::fs::symlink(&absent, &manifest)?;
    let error = publish_manifest_bytes_create_new(&root, &manifest, b"{}", &"a".repeat(64))
        .expect_err("a dangling symlink is an existing unsafe manifest, not an absent path");
    assert!(error.to_string().contains("symlink"), "{error:#}");
    assert!(fs::symlink_metadata(&manifest)?.file_type().is_symlink());
    assert!(!absent.try_exists()?);
    let staging = root.join("tmp");
    assert!(
        !staging.try_exists()? || fs::read_dir(staging)?.count() == 0,
        "refusing a symlink must not leak an unowned manifest staging file"
    );
    Ok(())
}

#[test]
fn gh538_blocked_io_names_its_path_without_fabricating_progress() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("blocked-manifest.json");
    let completed = Arc::new(AtomicU64::new(0));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker_path = path.clone();
    let worker_completed = Arc::clone(&completed);
    let worker = std::thread::spawn(move || {
        let _observer = observe_raw_mirror_io(
            Arc::new(move || {
                worker_completed.fetch_add(1, Ordering::Relaxed);
            }),
            Arc::new(|| false),
        );
        raw_mirror_io("symlink_metadata", &worker_path, || {
            started_tx.send(()).map_err(std::io::Error::other)?;
            release_rx
                .recv_timeout(Duration::from_secs(10))
                .map_err(std::io::Error::other)?;
            Ok(())
        })
    });
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let snapshot = raw_mirror_io_snapshot();
    let active = snapshot
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["path"] == json!(path))
        .expect("the blocked operation must remain attributable");
    assert_eq!(active["operation"], "symlink_metadata");
    assert!(active["elapsed_ms"].is_u64());
    assert_eq!(completed.load(Ordering::Relaxed), 0);
    release_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    assert_eq!(completed.load(Ordering::Relaxed), 1);
    assert!(
        raw_mirror_io_snapshot()
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["path"] != json!(path))
    );
}

#[test]
fn gh538_blob_verification_observes_cancellation_between_real_reads() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("blob.raw");
    fs::write(&path, vec![b'x'; 256 * 1024]).unwrap();
    let completed = Arc::new(AtomicU64::new(0));
    let tick_completed = Arc::clone(&completed);
    let cancel_completed = Arc::clone(&completed);
    let _observer = observe_raw_mirror_io(
        Arc::new(move || {
            tick_completed.fetch_add(1, Ordering::Relaxed);
        }),
        Arc::new(move || cancel_completed.load(Ordering::Relaxed) >= 5),
    );
    // Metadata, open, fstat, identity restat and one 64 KiB read complete; the
    // next read is cancelled, so a large verification is interruptible.
    let error = file_blake3(&path).unwrap_err();
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::Interrupted
    );
    assert_eq!(completed.load(Ordering::Relaxed), 5);
    assert_eq!(fs::metadata(&path).unwrap().len(), 256 * 1024);
}

#[test]
fn gh538_repeated_db_links_do_not_mutate_the_mirror() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("archive");
    let source = temp.path().join("session.jsonl");
    fs::write(&source, b"{\"message\":\"retained recovery evidence\"}\n").unwrap();
    // This checks repeated link updates, independently of the machine's
    // production retention settings. Keep real admission and a nonzero
    // reserve while budgeting for this tiny fixture.
    let record = policy::capture(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source,
            db_links: &[],
        },
        RAW_MIRROR_CHUNK_THRESHOLD_BYTES,
        RAW_MIRROR_CHUNK_SIZE_BYTES,
        policy::RawMirrorRetentionPolicy {
            max_bytes: 1024 * 1024,
            min_free_bytes: 64 * 1024 * 1024,
        },
    )
    .unwrap();
    let links = [RawMirrorDbLink {
        conversation_id: Some(42),
        message_count: Some(1),
        source_path: Some(source.display().to_string()),
        started_at_ms: Some(123),
    }];
    merge_manifest_db_links(&data_dir, &record.manifest_relative_path, &links).unwrap();
    let root = raw_mirror_root(&data_dir);
    let manifest_path = root.join(&record.manifest_relative_path);
    let generation_path = root.join(".usage-generation");
    let before = fs::read(&manifest_path).unwrap();
    let generation = fs::read(&generation_path).unwrap();
    let manifest_mtime = fs::metadata(&manifest_path).unwrap().modified().unwrap();
    let tmp_mtime = fs::metadata(root.join("tmp")).unwrap().modified().unwrap();
    for _ in 0..128 {
        merge_manifest_db_links(&data_dir, &record.manifest_relative_path, &links).unwrap();
    }
    assert_eq!(fs::read(&manifest_path).unwrap(), before);
    assert_eq!(fs::read(&generation_path).unwrap(), generation);
    assert_eq!(
        fs::metadata(&manifest_path).unwrap().modified().unwrap(),
        manifest_mtime
    );
    assert_eq!(
        fs::metadata(root.join("tmp")).unwrap().modified().unwrap(),
        tmp_mtime
    );
    assert_eq!(
        read_validated_raw_mirror_manifest(&root, &record.manifest_id)
            .unwrap()
            .db_links,
        links
    );
}

#[test]
fn gh447_sensitive_containers_refuse_capture_before_reading_or_creating_mirrors() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("archive");
    let source = temp.path().join("replica.blob");
    let bytes = br#"{"schemaVersion":1,"value":{"entries":[{"kind":"secret-request","content":"excluded-secret"}]}}"#;
    fs::write(&source, bytes).unwrap();
    let modified = fs::metadata(&source).unwrap().modified().unwrap();
    for provider in ["grok_bot", "shelley", "pi_durable"] {
        for source_path in [&source, &temp.path().join("missing.blob")] {
            let result = capture_source_file(RawMirrorCaptureInput {
                data_dir: &data_dir,
                provider,
                source_id: "local",
                origin_kind: "local",
                origin_host: None,
                source_path,
                db_links: &[],
            });
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .starts_with("disabled_sensitive_container:")
            );
            assert!(!data_dir.exists(), "denial cannot initialize a raw mirror");
        }
    }
    assert_eq!(fs::read(&source).unwrap(), bytes);
    assert_eq!(fs::metadata(&source).unwrap().modified().unwrap(), modified);
    let control = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source,
        db_links: &[],
    })
    .unwrap();
    assert!(
        !control.already_present,
        "ordinary providers remain capturable"
    );
}

#[test]
fn capture_source_file_writes_doctor_compatible_manifest_idempotently() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("rollout-fixture.jsonl");
    let source_bytes = b"{\"type\":\"message\",\"text\":\"hello\"}\n";
    fs::write(&source_path, source_bytes).expect("write source");
    let db_link = RawMirrorDbLink {
        conversation_id: Some(42),
        message_count: Some(1),
        source_path: Some(source_path.display().to_string()),
        started_at_ms: Some(1_733_000_000_000),
    };

    let first = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: std::slice::from_ref(&db_link),
    })
    .expect("first capture");
    let second = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: std::slice::from_ref(&db_link),
    })
    .expect("second capture");

    assert_eq!(first.manifest_id, second.manifest_id);
    assert_eq!(first.blob_blake3, second.blob_blake3);
    assert_eq!(first.captured_at_ms, second.captured_at_ms);
    assert_eq!(first.source_mtime_ms, second.source_mtime_ms);
    assert!(!first.already_present);
    assert!(second.already_present);
    assert_eq!(fs::read(&source_path).expect("source bytes"), source_bytes);

    let blob_path = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR)
        .join(&first.blob_relative_path);
    let manifest_path = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR)
        .join(&first.manifest_relative_path);
    assert_eq!(fs::read(blob_path).expect("blob bytes"), source_bytes);

    let manifest: Value =
        serde_json::from_slice(&fs::read(&manifest_path).expect("manifest bytes"))
            .expect("manifest json");
    assert_eq!(
        manifest["manifest_kind"].as_str(),
        Some(RAW_MIRROR_MANIFEST_KIND)
    );
    assert_eq!(manifest["provider"].as_str(), Some("codex"));
    assert_eq!(
        manifest["blob_blake3"].as_str(),
        Some(first.blob_blake3.as_str())
    );
    assert_eq!(
        manifest["redacted_original_path"].as_str(),
        Some("[codex]/rollout-fixture.jsonl")
    );
    assert_eq!(
        manifest["db_links"][0]["conversation_id"].as_i64(),
        Some(42)
    );
    assert_eq!(manifest["db_links"][0]["message_count"].as_u64(), Some(1));
    assert!(
        manifest["manifest_blake3"]
            .as_str()
            .is_some_and(|value| value.starts_with("doctor-raw-mirror-manifest-v1-"))
    );
    let tmp_root = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR)
        .join("tmp");
    assert_eq!(
        fs::read_dir(&tmp_root)
            .expect("raw mirror tmp root")
            .collect::<Vec<_>>()
            .len(),
        0,
        "successful captures must not leave doctor-visible interrupted temp artifacts"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let root = data_dir
            .join(RAW_MIRROR_ROOT_DIR)
            .join(RAW_MIRROR_VERSION_DIR);
        assert_eq!(
            fs::metadata(&root)
                .expect("raw mirror root metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&manifest_path)
                .expect("manifest metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(root.join(RAW_MIRROR_MUTATION_LOCK_FILE))
                .expect("mutation lock metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[test]
fn growing_sources_reuse_fixed_chunks_and_reconstruct_every_snapshot() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("growing-rollout.jsonl");
    let first_bytes = b"0123456789abcdefABCDEFGHIJKLMNOPqrstuvwxyz!@#$%^";
    assert_eq!(first_bytes.len(), 48);
    fs::write(&source_path, first_bytes).expect("write first source version");

    let first = capture_source_file_with_chunk_policy(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        16,
    )
    .expect("capture first chunked version");

    let mut second_bytes = first_bytes.to_vec();
    second_bytes.extend_from_slice(b"tail!");
    fs::write(&source_path, &second_bytes).expect("append next source version");
    let second = capture_source_file_with_chunk_policy(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        16,
    )
    .expect("capture appended chunked version");

    assert_eq!(first.storage_kind, RAW_MIRROR_FIXED_CHUNKS_KIND);
    assert_eq!(first.chunk_count, 3);
    assert_eq!(second.storage_kind, RAW_MIRROR_FIXED_CHUNKS_KIND);
    assert_eq!(second.chunk_count, 4);
    assert_eq!(
        read_source_bytes(&data_dir, &first.manifest_id).expect("reconstruct first version"),
        first_bytes
    );
    assert_eq!(
        read_source_bytes(&data_dir, &second.manifest_id).expect("reconstruct second version"),
        second_bytes
    );

    let root = raw_mirror_root(&data_dir);
    let first_manifest = read_raw_mirror_manifest(&root.join(&first.manifest_relative_path))
        .expect("first manifest");
    let second_manifest = read_raw_mirror_manifest(&root.join(&second.manifest_relative_path))
        .expect("second manifest");
    let first_storage = first_manifest.content_storage.expect("first chunk storage");
    let second_storage = second_manifest
        .content_storage
        .expect("second chunk storage");
    assert_eq!(
        first_storage.chunks,
        second_storage.chunks[..3],
        "an append must reuse every complete prior chunk instead of copying the full file"
    );
    assert_eq!(second_storage.chunks[3].blob_size_bytes, 5);

    let mut third_bytes = second_bytes.clone();
    third_bytes[20] ^= 0x20;
    fs::write(&source_path, &third_bytes).expect("write in-place-mutated source version");
    let third = capture_source_file_with_chunk_policy(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "cursor",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        16,
    )
    .expect("capture in-place-mutated chunked version");
    assert_eq!(
        read_source_bytes(&data_dir, &third.manifest_id).expect("reconstruct third version"),
        third_bytes
    );
    let third_manifest = read_raw_mirror_manifest(&root.join(&third.manifest_relative_path))
        .expect("third manifest");
    let third_storage = third_manifest.content_storage.expect("third chunk storage");
    assert_eq!(third_storage.chunks.len(), 4);
    assert_eq!(third_storage.chunks[0], second_storage.chunks[0]);
    assert_ne!(third_storage.chunks[1], second_storage.chunks[1]);
    assert_eq!(third_storage.chunks[2], second_storage.chunks[2]);
    assert_eq!(third_storage.chunks[3], second_storage.chunks[3]);

    let summary = storage_summary(&data_dir);
    assert_eq!(summary.manifest_count, 3);
    assert_eq!(summary.unique_blob_count, 8);
    assert_eq!(
        summary.total_blob_bytes,
        69 + first.blob_size_bytes + second.blob_size_bytes + third.blob_size_bytes,
        "physical inventory must count shared content chunks once plus one descriptor per snapshot"
    );

    let first_manifest_path = root.join(&first.manifest_relative_path);
    let mut old_first_manifest =
        read_raw_mirror_manifest(&first_manifest_path).expect("re-read first manifest");
    old_first_manifest.captured_at_ms = 0;
    old_first_manifest.manifest_blake3 = Some(raw_mirror_manifest_blake3(&old_first_manifest));
    fs::write(
        &first_manifest_path,
        serde_json::to_vec_pretty(&old_first_manifest).expect("serialize old manifest"),
    )
    .expect("age first manifest fixture");
    let prune_report = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(now_ms() / 2),
            safety_hold_down_ms: 0,
            apply: false,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("plan chunk-aware prune");
    assert_eq!(prune_report.planned_manifest_count, 1);
    assert_eq!(prune_report.planned_blob_count, 1);
    let planned_manifest = prune_report
        .entries
        .iter()
        .find(|entry| entry.kind == "manifest" && entry.path == first.manifest_relative_path)
        .expect("retired chunked manifest prune entry");
    assert_eq!(
        planned_manifest.blob_blake3.as_deref(),
        Some(first.blob_blake3.as_str()),
        "a chunked manifest entry must pin its canonical descriptor identity instead of depending on reference ordering"
    );
    assert_eq!(
        planned_manifest.manifest_blake3.as_deref(),
        old_first_manifest.manifest_blake3.as_deref(),
        "a manifest prune plan must pin the exact descriptor checksum"
    );
    assert!(
        prune_report
            .entries
            .iter()
            .any(|entry| { entry.kind == "blob" && entry.path == first.blob_relative_path }),
        "the retired snapshot descriptor should be reclaimable"
    );
    for shared_chunk in &first_storage.chunks {
        assert!(
            prune_report.entries.iter().all(|entry| {
                entry.kind != "blob" || entry.path != shared_chunk.blob_relative_path
            }),
            "a chunk retained by a newer snapshot must not be reclaimed: {shared_chunk:?}"
        );
    }

    let size_report = prune(
        &data_dir,
        RawMirrorPruneOptions {
            max_size_bytes: Some(
                summary
                    .total_blob_bytes
                    .saturating_sub(first.blob_size_bytes),
            ),
            safety_hold_down_ms: 0,
            apply: false,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("plan chunk-aware max-size prune");
    assert_eq!(
        size_report.planned_manifest_count, 1,
        "a shared base chunk must not cause max-size pruning to retire every snapshot"
    );
    assert_eq!(size_report.planned_blob_count, 1);
    assert!(
        size_report.entries.iter().any(|entry| {
            entry.kind == "manifest" && entry.path == first.manifest_relative_path
        })
    );

    let shared_chunk_path = root.join(&first_storage.chunks[0].blob_relative_path);
    fs::write(&shared_chunk_path, b"tampered-16-byte").expect("plant same-size corruption");
    assert_eq!(
        fs::metadata(&shared_chunk_path)
            .expect("chunk metadata")
            .len(),
        16
    );
    let error = verify_source_capture(&data_dir, &third.manifest_id)
        .expect_err("same-size chunk corruption must invalidate reconstruction authority");
    assert!(
        error.to_string().contains("failed content verification"),
        "unexpected chunk verification error: {error}"
    );
    let recapture_error = capture_source_file_with_chunk_policy(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "cursor",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        16,
    )
    .expect_err("a cache hit must not conceal same-size chunk corruption");
    // bet45: same-size corruption is now surfaced as an explicit blake3
    // "checksum mismatch" instead of the old "existing raw mirror blob"
    // phrasing — strictly better diagnostics; pin the semantic.
    assert!(
        recapture_error.to_string().contains("checksum mismatch"),
        "unexpected recapture error: {recapture_error}"
    );
    assert_eq!(
        fs::read(&source_path).expect("source remains untouched"),
        third_bytes
    );
}

#[test]
fn growing_source_with_partial_tail_reuses_only_complete_prefix_chunks() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("partial-tail.jsonl");
    let first_bytes = b"0123456789abcdefABCDEFGHIJKLMNOPqrstuvwxyz!@#$%^xyz";
    assert_eq!(first_bytes.len(), 51);
    fs::write(&source_path, first_bytes).expect("write first partial-tail version");

    let first = capture_source_file_with_chunk_policy(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        16,
    )
    .expect("capture first partial-tail version");

    let mut second_bytes = first_bytes.to_vec();
    second_bytes.extend_from_slice(b"tail!");
    fs::write(&source_path, &second_bytes).expect("append partial tail");
    let second = capture_source_file_with_chunk_policy(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        16,
    )
    .expect("capture extended partial-tail version");

    let root = raw_mirror_root(&data_dir);
    let first_storage = read_raw_mirror_manifest(&root.join(&first.manifest_relative_path))
        .expect("first manifest")
        .content_storage
        .expect("first chunk storage");
    let second_storage = read_raw_mirror_manifest(&root.join(&second.manifest_relative_path))
        .expect("second manifest")
        .content_storage
        .expect("second chunk storage");
    assert_eq!(first_storage.chunks.len(), 4);
    assert_eq!(first_storage.chunks[3].blob_size_bytes, 3);
    assert_eq!(second_storage.chunks.len(), 4);
    assert_eq!(second_storage.chunks[3].blob_size_bytes, 8);
    assert_eq!(first_storage.chunks[..3], second_storage.chunks[..3]);
    assert_ne!(first_storage.chunks[3], second_storage.chunks[3]);
    assert_eq!(
        read_source_bytes(&data_dir, &first.manifest_id).expect("reconstruct first snapshot"),
        first_bytes
    );
    assert_eq!(
        read_source_bytes(&data_dir, &second.manifest_id).expect("reconstruct second snapshot"),
        second_bytes
    );
    assert_eq!(
        storage_summary(&data_dir).unique_blob_count,
        7,
        "an append into a partial tail should add only one replacement tail and one descriptor"
    );
}

#[test]
fn prune_apply_accepts_exact_chunked_manifest_identity() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("chunked-prune.jsonl");
    fs::write(&source_path, b"0123456789abcdefABCDEFGHIJKLMNOP").expect("write chunked source");
    let captured = capture_source_file_with_chunk_policy(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        16,
    )
    .expect("capture chunked source");
    let root = raw_mirror_root(&data_dir);
    let manifest_path = root.join(&captured.manifest_relative_path);
    let mut manifest = read_raw_mirror_manifest(&manifest_path).expect("read manifest");
    manifest.captured_at_ms = 0;
    manifest.manifest_blake3 = Some(raw_mirror_manifest_blake3(&manifest));
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("serialize aged manifest"),
    )
    .expect("age manifest");

    let report = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(now_ms() / 2),
            safety_hold_down_ms: 0,
            apply: true,
            ..RawMirrorPruneOptions::default()
        },
    )
    .expect("apply chunk-aware prune");

    assert_eq!(report.applied_manifest_count, 1);
    assert!(report.applied_blob_count >= 3);
    assert!(!manifest_path.exists());
}

#[test]
fn sparse_page_store_mutation_reuses_unchanged_middle_chunks() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("page-store.sqlite");
    let first_bytes = b"0123456789abcdefABCDEFGHIJKLMNOPqrstuvwxyz!@#$%^&*()-_=+[]{}<>?!";
    assert_eq!(first_bytes.len(), 64);
    fs::write(&source_path, first_bytes).expect("write first page-store snapshot");

    let first = capture_source_file_with_chunk_policy(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "cursor",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        16,
    )
    .expect("capture first page-store snapshot");

    let mut second_bytes = first_bytes.to_vec();
    second_bytes[2] ^= 0x20;
    second_bytes[50] ^= 0x20;
    fs::write(&source_path, &second_bytes).expect("write sparse page-store mutation");
    let second = capture_source_file_with_chunk_policy(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "cursor",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        16,
    )
    .expect("capture sparse page-store mutation");

    let root = raw_mirror_root(&data_dir);
    let first_storage = read_raw_mirror_manifest(&root.join(&first.manifest_relative_path))
        .expect("first page-store manifest")
        .content_storage
        .expect("first page-store chunks");
    let second_storage = read_raw_mirror_manifest(&root.join(&second.manifest_relative_path))
        .expect("second page-store manifest")
        .content_storage
        .expect("second page-store chunks");
    assert_eq!(first_storage.chunks.len(), 4);
    assert_eq!(second_storage.chunks.len(), 4);
    assert_ne!(first_storage.chunks[0], second_storage.chunks[0]);
    assert_eq!(first_storage.chunks[1], second_storage.chunks[1]);
    assert_eq!(first_storage.chunks[2], second_storage.chunks[2]);
    assert_ne!(first_storage.chunks[3], second_storage.chunks[3]);
    assert_eq!(
        read_source_bytes(&data_dir, &second.manifest_id)
            .expect("reconstruct sparse page-store mutation"),
        second_bytes
    );
}

#[test]
fn blob_cache_does_not_cross_chunk_policy_boundaries() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("policy-sensitive.jsonl");
    let source_bytes = b"0123456789abcdef0123456789abcdef";
    fs::write(&source_path, source_bytes).expect("write source");

    let chunked = capture_source_file_with_chunk_policy(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        16,
    )
    .expect("capture with chunking");
    let whole = capture_source_file_with_chunk_policy(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        u64::MAX,
        16,
    )
    .expect("capture without chunking");

    assert_eq!(chunked.storage_kind, RAW_MIRROR_FIXED_CHUNKS_KIND);
    assert_eq!(whole.storage_kind, "whole_blob_v1");
    assert_ne!(chunked.manifest_id, whole.manifest_id);
    assert_eq!(
        read_source_bytes(&data_dir, &whole.manifest_id).expect("read whole snapshot"),
        source_bytes
    );
}

#[test]
fn fixed_chunk_manifest_rejects_reconstructed_size_overflow() {
    let content_blake3 = "11".repeat(32);
    let first_chunk_blake3 = "22".repeat(32);
    let second_chunk_blake3 = "33".repeat(32);
    let storage = RawMirrorContentStorage {
        kind: RAW_MIRROR_FIXED_CHUNKS_KIND.to_string(),
        content_hash_algorithm: RAW_MIRROR_HASH_ALGORITHM.to_string(),
        content_blake3: content_blake3.clone(),
        content_size_bytes: u64::MAX,
        chunk_size_bytes: u64::MAX,
        chunks: vec![
            RawMirrorChunkRef {
                blob_relative_path: raw_mirror_blob_relative_path(&first_chunk_blake3)
                    .expect("first chunk path"),
                blob_blake3: first_chunk_blake3,
                blob_size_bytes: u64::MAX,
            },
            RawMirrorChunkRef {
                blob_relative_path: raw_mirror_blob_relative_path(&second_chunk_blake3)
                    .expect("second chunk path"),
                blob_blake3: second_chunk_blake3,
                blob_size_bytes: 1,
            },
        ],
    };
    let descriptor_bytes = serde_json::to_vec(&storage).expect("serialize descriptor");
    let descriptor_blake3 = blake3::hash(&descriptor_bytes).to_hex().to_string();
    let manifest = RawMirrorManifestFile {
        schema_version: RAW_MIRROR_SCHEMA_VERSION,
        manifest_kind: RAW_MIRROR_MANIFEST_KIND.to_string(),
        manifest_id: "overflow-fixture".to_string(),
        blob_hash_algorithm: RAW_MIRROR_HASH_ALGORITHM.to_string(),
        blob_relative_path: raw_mirror_blob_relative_path(&descriptor_blake3)
            .expect("descriptor path"),
        blob_blake3: descriptor_blake3,
        blob_size_bytes: descriptor_bytes.len() as u64,
        provider: "codex".to_string(),
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
        original_path: "/fixture/overflow.jsonl".to_string(),
        redacted_original_path: "[codex]/overflow.jsonl".to_string(),
        original_path_blake3: "unused-by-storage-validator".to_string(),
        captured_at_ms: 0,
        source_mtime_ms: None,
        source_size_bytes: u64::MAX,
        content_storage: Some(storage),
        compression: RawMirrorCompressionEnvelope {
            state: "none".to_string(),
            algorithm: None,
            uncompressed_size_bytes: Some(u64::MAX),
        },
        encryption: RawMirrorEncryptionEnvelope {
            state: "none".to_string(),
            algorithm: None,
            key_id: None,
            envelope_version: None,
        },
        db_links: Vec::new(),
        verification: RawMirrorVerificationRecord {
            status: "captured".to_string(),
            verifier: "cass_indexer".to_string(),
            content_blake3: Some(content_blake3),
            verified_at_ms: Some(0),
        },
        manifest_blake3: None,
    };

    let error = raw_mirror_manifest_blob_references(&manifest)
        .expect_err("overflowing reconstructed size must be rejected");
    assert!(error.to_string().contains("sizes overflow"), "{error:#}");
}

#[test]
fn appended_chunk_preparation_writes_only_the_new_tail_and_descriptor() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("growing-rollout.jsonl");
    let first_bytes = b"0123456789abcdefABCDEFGHIJKLMNOPqrstuvwxyz!@#$%^";
    fs::write(&source_path, first_bytes).expect("write first source version");
    capture_source_file_with_chunk_policy(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        16,
    )
    .expect("capture first chunked version");

    let mut appended_bytes = first_bytes.to_vec();
    appended_bytes.extend_from_slice(b"tail!");
    fs::write(&source_path, &appended_bytes).expect("append source version");
    let root = raw_mirror_root(&data_dir);
    let temp_dir = root.join("tmp/manual-prepare");
    ensure_private_dir_descendant(&root, &temp_dir).expect("create preparation temp dir");
    let source_metadata = fs::symlink_metadata(&source_path).expect("source metadata");

    let prepared = prepare_source_content(&root, &source_path, &temp_dir, &source_metadata, 1, 16)
        .expect("prepare appended source");

    assert_eq!(
        prepared.record.source_size_bytes,
        appended_bytes.len() as u64
    );
    assert_eq!(prepared.files.len(), 2);
    assert!(
        prepared.files.iter().any(|file| file.bytes_copied == 5),
        "the appended tail must be the only new source-content temp"
    );
    assert!(
        prepared.files.iter().all(|file| file.bytes_copied != 16),
        "unchanged complete chunks must be verified and reused without temp rewrites"
    );
}

#[test]
fn chunk_preparation_materializes_repeated_content_once() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("repeated-chunks.jsonl");
    fs::write(&source_path, b"abcdefghijklmnopabcdefghijklmnop").expect("write repeated chunks");
    let root = ensure_raw_mirror_root(&data_dir).expect("create raw mirror root");
    let temp_dir = root.join("tmp/manual-repeated-prepare");
    ensure_private_dir_descendant(&root, &temp_dir).expect("create preparation temp dir");
    let source_metadata = fs::symlink_metadata(&source_path).expect("source metadata");

    let prepared = prepare_source_content(&root, &source_path, &temp_dir, &source_metadata, 1, 16)
        .expect("prepare repeated chunks");

    assert_eq!(prepared.record.source_size_bytes, 32);
    assert_eq!(
        prepared
            .record
            .content_storage
            .as_ref()
            .expect("chunk storage")
            .chunks
            .len(),
        2
    );
    assert_eq!(
        prepared
            .files
            .iter()
            .filter(|file| file.bytes_copied == 16)
            .count(),
        1,
        "one repeated content hash must create one pending chunk temp"
    );
    assert_eq!(prepared.files.len(), 2, "one chunk plus one descriptor");
}
