use super::super::*;

#[test]
fn capture_source_file_revalidates_cached_blob_contents() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("cached-source.jsonl");
    let source_bytes = b"{\"type\":\"message\",\"text\":\"cache me\"}\n";
    fs::write(&source_path, source_bytes).expect("write source");

    let first = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect("first capture");

    let blob_path = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR)
        .join(&first.blob_relative_path);
    fs::write(&blob_path, b"corrupted cached blob").expect("corrupt cached blob");

    let err = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect_err("corrupted content-addressed blob must be rejected");
    // bet45: the refusal wording moved (size/safety classification now
    // says "raw mirror blob ... is missing, unsafe, or has the wrong
    // size"); pin the shared "raw mirror blob" refusal identity.
    assert!(
        err.to_string().contains("raw mirror blob"),
        "unexpected cached-blob error: {err:#}"
    );
    assert_eq!(fs::read(&source_path).expect("source bytes"), source_bytes);
}

#[test]
fn gh461_prune_selectors_preserve_outside_shared_and_orphan_blobs() {
    let temp = tempfile::tempdir().expect("fixture");
    let data_dir = temp.path().join("data");
    let capture = |provider, name, bytes: &[u8]| {
        let source_path = temp.path().join(name);
        fs::write(&source_path, bytes).expect("source");
        capture_source_file(RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider,
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        })
        .expect("capture")
    };
    let selected = capture("opencode", "target.db", b"shared bytes");
    let sole_copy = capture("claude_code", "sole.jsonl", b"shared bytes");
    let other_path = capture("opencode", "other.db", b"different source");
    let root = raw_mirror_root(&data_dir);
    let orphan_bytes = b"orphan without provider provenance";
    let orphan_digest = blake3::hash(orphan_bytes).to_hex().to_string();
    let orphan_path = root.join(raw_mirror_blob_relative_path(&orphan_digest).expect("digest"));
    ensure_private_dir_descendant(&root, orphan_path.parent().expect("parent"))
        .expect("orphan directory");
    fs::write(&orphan_path, orphan_bytes).expect("orphan");

    let options = RawMirrorPruneOptions {
        providers: vec!["opencode".to_string()],
        source_path: Some("*target.db".to_string()),
        safety_hold_down_ms: 0,
        ..RawMirrorPruneOptions::default()
    };
    for invalid in ["", "[invalid"] {
        assert!(
            prune(
                &data_dir,
                RawMirrorPruneOptions {
                    source_path: Some(invalid.to_string()),
                    ..options.clone()
                }
            )
            .is_err()
        );
    }
    assert!(!root.join("pruned.jsonl").exists());

    let within_budget = prune(
        &data_dir,
        RawMirrorPruneOptions {
            max_size_bytes: Some(selected.source_size_bytes),
            ..options.clone()
        },
    )
    .expect("scope budget");
    assert_eq!(
        within_budget.scope_blob_bytes,
        Some(selected.source_size_bytes)
    );
    assert!(within_budget.current_blob_bytes > selected.source_size_bytes);
    assert_eq!(within_budget.planned_manifest_count, 0);

    let report = prune(
        &data_dir,
        RawMirrorPruneOptions {
            older_than_ms: Some(0),
            max_size_bytes: Some(0),
            apply: true,
            ..options
        },
    )
    .expect("scoped apply");
    assert_eq!(report.applied_manifest_count, 1);
    assert_eq!(report.applied_blob_count, 0);
    assert_eq!(report.providers, ["opencode"]);
    assert!(!root.join(&selected.manifest_relative_path).exists());
    assert!(root.join(&sole_copy.manifest_relative_path).exists());
    assert!(root.join(&other_path.manifest_relative_path).exists());
    assert_eq!(
        fs::read(root.join(&selected.blob_relative_path)).expect("shared"),
        b"shared bytes"
    );
    assert_eq!(
        fs::read(&orphan_path).expect("orphan remains"),
        orphan_bytes
    );
}

#[test]
fn blob_cache_keeps_one_bounded_entry_per_source_path() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let source_path = temp.path().join("source.jsonl");
    let first_key = RawMirrorBlobCacheKey {
        data_dir: temp.path().join("cass-data"),
        source_path: source_path.clone(),
        source_identity: Some("first".to_string()),
        source_size_bytes: 1,
        source_mtime_ns: Some(1),
        source_change_time_ns: Some(1),
        source_sidecars_fingerprint: Some("no sidecars".to_string()),
        chunk_threshold_bytes: 1,
        chunk_size_bytes: 16,
    };
    let second_key = RawMirrorBlobCacheKey {
        source_identity: Some("second".to_string()),
        source_size_bytes: 2,
        source_mtime_ns: Some(2),
        source_change_time_ns: Some(2),
        ..first_key.clone()
    };
    let first_record = RawMirrorBlobRecord {
        blob_blake3: "00".repeat(32),
        blob_size_bytes: 1,
        source_content_blake3: "00".repeat(32),
        source_size_bytes: 1,
        content_storage: None,
    };
    let second_record = RawMirrorBlobRecord {
        blob_blake3: "11".repeat(32),
        blob_size_bytes: 2,
        source_content_blake3: "11".repeat(32),
        source_size_bytes: 2,
        content_storage: None,
    };
    let root = ensure_raw_mirror_root(&first_key.data_dir).expect("create mirror root");
    for (record, bytes) in [(&first_record, &b"a"[..]), (&second_record, &b"bb"[..])] {
        let relative =
            raw_mirror_blob_relative_path(&record.blob_blake3).expect("fixture blob relative path");
        let path = root.join(relative);
        ensure_private_dir_descendant(&root, path.parent().expect("blob parent"))
            .expect("create fixture blob parent");
        fs::write(path, bytes).expect("write fixture blob");
    }

    cache_raw_mirror_blob_record(first_key, first_record);
    cache_raw_mirror_blob_record(second_key.clone(), second_record.clone());

    let source_key = raw_mirror_blob_source_key(&second_key);
    if cfg!(not(unix)) {
        // Stored blobs have no stable file identity or change time on this
        // platform, so the capture cache must stay off rather than trust
        // mtime alone; this test used to assume unix (2l1b0.62).
        let cached = BLOB_CAPTURE_CACHE
            .get()
            .and_then(|cache| cache.lock().ok()?.get(&source_key).cloned());
        assert_eq!(cached, None);
        return;
    }
    let cache = BLOB_CAPTURE_CACHE.get().expect("cache initialized");
    let guard = cache.lock().expect("cache lock");
    let cached = guard.get(&source_key).expect("latest source cache entry");
    assert_eq!(cached.cache_key, second_key);
    assert_eq!(cached.record, second_record);
    assert!(guard.len() <= RAW_MIRROR_BLOB_CACHE_MAX_ENTRIES);
}

#[cfg(unix)]
#[test]
fn gh461_persisted_cache_rejects_changed_source_sidecars_and_bad_entries() {
    let fixture = tempfile::tempdir().expect("fixture");
    let data_dir = fixture.path().join("data");
    let source_path = fixture.path().join("source.jsonl");
    fs::write(&source_path, b"original source\n").expect("source");
    let input = || RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    };
    let current_key = || {
        raw_mirror_blob_cache_key(
            &input(),
            &fs::metadata(&source_path).expect("metadata"),
            RAW_MIRROR_CHUNK_THRESHOLD_BYTES,
            RAW_MIRROR_CHUNK_SIZE_BYTES,
        )
    };
    let first = capture_source_file(input()).expect("capture");
    let root = raw_mirror_root(&data_dir);
    let initial = current_key();
    let cache_path = raw_mirror_blob_cache_path(&root, &initial).expect("cache path");
    let clear_memo = || {
        BLOB_CAPTURE_CACHE
            .get()
            .expect("memo")
            .lock()
            .expect("memo lock")
            .remove(&raw_mirror_blob_source_key(&initial));
    };
    clear_memo();
    assert!(cached_raw_mirror_blob_record(&initial, &root).is_some());
    clear_memo();
    fs::write(fixture.path().join("source.jsonl-wal"), b"new WAL state").expect("WAL");
    let with_wal = current_key();
    assert_ne!(with_wal, initial);
    assert!(cached_raw_mirror_blob_record(&initial, &root).is_none());
    assert!(cached_raw_mirror_blob_record(&with_wal, &root).is_none());
    capture_source_file(input()).expect("recapture after sidecar change");
    clear_memo();
    assert!(cached_raw_mirror_blob_record(&with_wal, &root).is_some());

    clear_memo();
    let original_mtime = fs::metadata(&source_path)
        .expect("mtime")
        .modified()
        .expect("mtime");
    fs::write(&source_path, b"modified source\n").expect("same-length change");
    File::options()
        .write(true)
        .open(&source_path)
        .expect("source file")
        .set_times(fs::FileTimes::new().set_modified(original_mtime))
        .expect("preserve mtime");
    let modified = current_key();
    assert_eq!(modified.source_size_bytes, initial.source_size_bytes);
    assert_eq!(modified.source_mtime_ns, initial.source_mtime_ns);
    assert!(cached_raw_mirror_blob_record(&with_wal, &root).is_none());
    assert!(cached_raw_mirror_blob_record(&modified, &root).is_none());
    let changed = capture_source_file(input()).expect("capture changed source");
    assert_ne!(changed.blob_blake3, first.blob_blake3);

    clear_memo();
    fs::write(&cache_path, b"{truncated").expect("corrupt advisory cache");
    assert!(cached_raw_mirror_blob_record(&modified, &root).is_none());
    capture_source_file(input()).expect("bad cache falls back to capture");
    clear_memo();
    let mut envelope: PersistedRawMirrorBlobCache =
        serde_json::from_slice(&fs::read(&cache_path).expect("cache")).expect("cache JSON");
    envelope.entry.record.source_content_blake3 = "0".repeat(64);
    fs::write(
        &cache_path,
        serde_json::to_vec(&envelope).expect("altered cache"),
    )
    .expect("cache write");
    assert!(
        cached_raw_mirror_blob_record(&modified, &root).is_none(),
        "checksum mismatch cannot authorize reuse"
    );
}

#[test]
fn blob_cache_refuses_keys_without_file_identity_and_change_time() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let weak_key = RawMirrorBlobCacheKey {
        data_dir: temp.path().join("cass-data"),
        source_path: temp.path().join("weak-key-source.jsonl"),
        source_identity: None,
        source_size_bytes: 1,
        source_mtime_ns: Some(1),
        source_change_time_ns: None,
        source_sidecars_fingerprint: None,
        chunk_threshold_bytes: 1,
        chunk_size_bytes: 16,
    };
    let record = RawMirrorBlobRecord {
        blob_blake3: "22".repeat(32),
        blob_size_bytes: 1,
        source_content_blake3: "22".repeat(32),
        source_size_bytes: 1,
        content_storage: None,
    };

    assert!(!raw_mirror_blob_cache_key_is_strong(&weak_key));
    cache_raw_mirror_blob_record(weak_key.clone(), record);

    let source_key = raw_mirror_blob_source_key(&weak_key);
    // bet45: the global cache is a OnceLock that a refused weak-key
    // insert never initializes — under filtered test runs no other test
    // may have initialized it either, in which case the property holds
    // vacuously (nothing was cached at all).
    if let Some(cache) = BLOB_CAPTURE_CACHE.get() {
        assert!(
            !cache.lock().expect("cache lock").contains_key(&source_key),
            "a weak non-Unix-style metadata key must never authorize cache reuse"
        );
    }
}

#[cfg(unix)]
#[test]
fn final_source_path_check_detects_same_size_atomic_replacement() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let source_path = temp.path().join("rotating-source.jsonl");
    let replacement_path = temp.path().join("replacement.jsonl");
    fs::write(&source_path, b"original\n").expect("write original source");
    fs::write(&replacement_path, b"replaced\n").expect("write same-size replacement");
    let initial = fs::symlink_metadata(&source_path).expect("initial source metadata");

    assert!(!source_path_changed_identity_during_capture(
        &source_path,
        &initial
    ));
    fs::rename(&replacement_path, &source_path).expect("atomically replace source path");
    assert!(
        source_path_changed_identity_during_capture(&source_path, &initial),
        "capture must reject bytes from an inode no longer reachable through the recorded path"
    );
}

#[cfg(unix)]
#[test]
fn capture_source_file_does_not_reuse_cache_after_same_size_mtime_preserving_rewrite() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("same-size-rewrite.jsonl");
    let first_bytes = b"same length payload A\n";
    let second_bytes = b"same length payload B\n";
    fs::write(&source_path, first_bytes).expect("write first source");

    let first_modified = fs::metadata(&source_path)
        .expect("first metadata")
        .modified()
        .expect("first modified time");
    let first = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect("first capture");

    std::thread::sleep(std::time::Duration::from_millis(5));
    fs::write(&source_path, second_bytes).expect("rewrite source");
    let source = OpenOptions::new()
        .write(true)
        .open(&source_path)
        .expect("open rewritten source");
    source
        .set_times(std::fs::FileTimes::new().set_modified(first_modified))
        .expect("restore original mtime");

    let second = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect("second capture");

    assert_ne!(first.blob_blake3, second.blob_blake3);
    assert_eq!(
        second.blob_blake3,
        blake3::hash(second_bytes).to_hex().to_string()
    );
    assert_eq!(
        fs::read(&source_path).expect("source bytes after rewrite"),
        second_bytes
    );
}

#[cfg(unix)]
#[test]
fn capture_source_file_rejects_symlinked_existing_blob_path() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("cached-source.jsonl");
    let source_bytes = b"{\"type\":\"message\",\"text\":\"cache me\"}\n";
    fs::write(&source_path, source_bytes).expect("write source");

    let blob_blake3 = blake3::hash(source_bytes).to_hex().to_string();
    let blob_relative_path =
        raw_mirror_blob_relative_path(&blob_blake3).expect("blob relative path");
    let blob_path = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR)
        .join(&blob_relative_path);
    fs::create_dir_all(blob_path.parent().expect("blob parent")).expect("blob parent dir");
    let outside = temp.path().join("outside.raw");
    fs::write(&outside, source_bytes).expect("outside blob bytes");
    std::os::unix::fs::symlink(&outside, &blob_path).expect("symlink blob");

    let err = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect_err("symlinked content-addressed blob path must be rejected");
    assert!(
        err.to_string().contains("contains a symlink"),
        "unexpected symlink-blob error: {err:#}"
    );

    let manifest_root = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR)
        .join("manifests");
    assert!(
        !manifest_root.exists(),
        "failed blob publish must not write a manifest pointing at a symlinked blob"
    );
    assert_eq!(fs::read(&source_path).expect("source bytes"), source_bytes);
    assert_eq!(fs::read(&outside).expect("outside bytes"), source_bytes);
}

#[cfg(unix)]
#[test]
fn capture_source_file_rejects_symlinked_mutation_lock() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let root = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR);
    let source_path = temp.path().join("source.jsonl");
    let protected_target = temp.path().join("protected-lock-target");
    let source_bytes = b"{\"type\":\"message\",\"text\":\"hello\"}\n";
    fs::create_dir_all(&root).expect("raw mirror root");
    fs::write(&source_path, source_bytes).expect("source bytes");
    fs::write(&protected_target, b"protected\n").expect("protected target");
    std::os::unix::fs::symlink(&protected_target, root.join(RAW_MIRROR_MUTATION_LOCK_FILE))
        .expect("symlink mutation lock");

    let error = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect_err("symlinked mutation lock must be rejected");

    assert!(
        error.to_string().contains("mutation lock path"),
        "{error:#}"
    );
    assert_eq!(
        fs::read(&protected_target).expect("protected bytes"),
        b"protected\n"
    );
    assert!(!root.join("manifests").exists());
}

#[cfg(unix)]
#[test]
fn capture_source_file_rejects_symlinked_raw_mirror_root_dir() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("source.jsonl");
    let outside_mirror = temp.path().join("outside-mirror");
    let source_bytes = b"{\"type\":\"message\",\"text\":\"do not redirect archive\"}\n";

    fs::create_dir_all(&data_dir).expect("data dir");
    fs::create_dir_all(&outside_mirror).expect("outside mirror dir");
    fs::write(&source_path, source_bytes).expect("write source");
    std::os::unix::fs::symlink(&outside_mirror, data_dir.join(RAW_MIRROR_ROOT_DIR))
        .expect("symlink raw mirror root");

    let err = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect_err("symlinked raw-mirror root must be rejected");

    assert!(
        err.to_string().contains("symlink raw mirror dir"),
        "unexpected symlink-root error: {err:#}"
    );
    assert!(
        !outside_mirror.join(RAW_MIRROR_VERSION_DIR).exists(),
        "raw mirror capture must not create redirected archive state outside data_dir"
    );
    assert_eq!(fs::read(&source_path).expect("source bytes"), source_bytes);
}

#[cfg(unix)]
#[test]
fn capture_source_file_rejects_symlinked_blob_directory_component() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let root = data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR);
    let source_path = temp.path().join("source.jsonl");
    let outside_blobs = temp.path().join("outside-blobs");
    let source_bytes = b"{\"type\":\"message\",\"text\":\"do not redirect blobs\"}\n";

    fs::create_dir_all(&root).expect("raw mirror root");
    fs::create_dir_all(&outside_blobs).expect("outside blobs dir");
    fs::write(&source_path, source_bytes).expect("write source");
    std::os::unix::fs::symlink(&outside_blobs, root.join("blobs")).expect("symlink blobs dir");

    let err = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect_err("symlinked blob directory must be rejected");

    assert!(
        err.to_string().contains("symlink raw mirror dir"),
        "unexpected symlink-blob-dir error: {err:#}"
    );
    assert!(
        !outside_blobs.join(RAW_MIRROR_HASH_ALGORITHM).exists(),
        "raw mirror capture must not create redirected blob state outside data_dir"
    );
    assert!(
        !root.join("manifests").exists(),
        "failed blob publish must not write a manifest"
    );
    assert_eq!(fs::read(&source_path).expect("source bytes"), source_bytes);
}

#[test]
fn capture_source_file_rejects_non_file_sources() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_dir = temp.path().join("source-dir");
    fs::create_dir(&source_dir).expect("source dir");

    let err = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_dir,
        db_links: &[],
    })
    .expect_err("directory source should be rejected");
    assert!(
        err.to_string().contains("non-file source"),
        "unexpected non-file-source error: {err}"
    );
    assert!(
        !data_dir.join(RAW_MIRROR_ROOT_DIR).exists(),
        "rejected non-file sources must not initialize raw mirror storage"
    );
}

#[cfg(unix)]
#[test]
fn capture_source_file_rejects_unreadable_sources_without_manifest() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("unreadable.jsonl");
    fs::write(&source_path, b"private session bytes\n").expect("source");
    fs::set_permissions(&source_path, fs::Permissions::from_mode(0o000))
        .expect("make source unreadable");

    // root (or CAP_DAC_OVERRIDE) bypasses POSIX permission checks, so the
    // rejection under test cannot trigger. Probe instead of asserting a
    // uid so the skip covers capability-granted environments too.
    if fs::File::open(&source_path).is_ok() {
        eprintln!(
            "skipping capture_source_file_rejects_unreadable_sources_without_manifest: \
             process bypasses POSIX permission checks (root/CAP_DAC_OVERRIDE)"
        );
        let _ = fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600));
        return;
    }

    let err = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    })
    .expect_err("unreadable source should be rejected");
    fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600))
        .expect("restore source perms");
    assert!(
        err.to_string().contains("open raw mirror source"),
        "unexpected unreadable-source error: {err}"
    );
    assert!(
        !data_dir.join("raw-mirror/v1/manifests").exists(),
        "failed unreadable-source captures must not publish manifests"
    );
}

#[cfg(unix)]
#[test]
fn capture_source_file_rejects_symlink_sources() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let real_source = temp.path().join("real.jsonl");
    let symlink_source = temp.path().join("link.jsonl");
    fs::write(&real_source, b"secret session").expect("write source");
    symlink(&real_source, &symlink_source).expect("symlink");

    let err = capture_source_file(RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &symlink_source,
        db_links: &[],
    })
    .expect_err("symlink source should be rejected");
    assert!(
        err.to_string().contains("symlink source"),
        "unexpected error: {err:#}"
    );
}

#[test]
fn capture_source_file_routes_through_policy_and_pauses_when_budget_exceeded() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("oversized.jsonl");
    let payload = vec![b'x'; 2048];
    fs::write(&source_path, &payload).expect("write source");

    let err = policy::capture(
        RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        RAW_MIRROR_CHUNK_THRESHOLD_BYTES,
        RAW_MIRROR_CHUNK_SIZE_BYTES,
        policy::RawMirrorRetentionPolicy {
            max_bytes: 32,
            min_free_bytes: 0,
        },
    )
    .expect_err("capture exceeding retention budget should pause");

    let paused = err
        .downcast_ref::<policy::RawMirrorCapturePaused>()
        .expect("error should downcast to RawMirrorCapturePaused");
    assert_eq!(paused.max_bytes, 32);
    assert!(paused.requested_bytes >= 2048);
    assert!(
        !data_dir.join("raw-mirror/v1/manifests").exists(),
        "paused capture must not publish manifests"
    );
}
