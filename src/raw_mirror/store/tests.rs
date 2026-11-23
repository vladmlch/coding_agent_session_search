use super::*;

#[path = "tests/cache_security_tests.rs"]
mod cache_security_tests;
#[path = "tests/capture_tests.rs"]
mod capture_tests;
#[path = "tests/manifest_tests.rs"]
mod manifest_tests;
#[path = "tests/prune_tests.rs"]
mod prune_tests;

#[derive(Clone)]
struct RawMirrorTraceBuffer(Arc<Mutex<Vec<u8>>>);

impl Write for RawMirrorTraceBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(unix)]
#[test]
fn gh461_capture_cache_survives_fresh_process() {
    let child_root = dotenvy::var("CASS_TEST_GH461_CACHE_ROOT").ok();
    let temporary = child_root
        .is_none()
        .then(|| tempfile::tempdir().expect("fixture"));
    let fixture = child_root.as_ref().map(PathBuf::from).unwrap_or_else(|| {
        temporary
            .as_ref()
            .expect("parent fixture")
            .path()
            .to_path_buf()
    });
    let data_dir = fixture.join("data");
    let source_path = fixture.join("source.jsonl");
    if child_root.is_none() {
        fs::write(&source_path, b"unchanged source bytes\n").expect("source");
    } else {
        assert!(
            BLOB_CAPTURE_CACHE.get().is_none(),
            "child has no inherited memo"
        );
    }
    let input = RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source_path,
        db_links: &[],
    };
    let key = raw_mirror_blob_cache_key(
        &input,
        &fs::metadata(&source_path).expect("source metadata"),
        RAW_MIRROR_CHUNK_THRESHOLD_BYTES,
        RAW_MIRROR_CHUNK_SIZE_BYTES,
    );
    let cache_path =
        raw_mirror_blob_cache_path(&raw_mirror_root(&data_dir), &key).expect("cache path");
    let prior = fs::metadata(&cache_path).ok();
    let capture = capture_source_file(input).expect("capture");
    assert_eq!(
        read_source_bytes(&data_dir, &capture.manifest_id).expect("mirror bytes"),
        b"unchanged source bytes\n"
    );
    if child_root.is_some() {
        let prior = prior.expect("parent persisted the cache");
        let after = fs::metadata(&cache_path).expect("cache remains");
        assert!(
            same_source_identity(&prior, &after),
            "recapture would replace the cache file"
        );
        assert!(!source_file_changed_during_capture(&prior, &after));
        assert!(capture.already_present);
        return;
    }

    let mut child = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "raw_mirror::tests::gh461_capture_cache_survives_fresh_process",
            "--nocapture",
        ])
        .env("CASS_TEST_GH461_CACHE_ROOT", &fixture)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("fresh process");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if child.try_wait().expect("child status").is_some() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("fresh-process capture exceeded 30 seconds");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().expect("child output");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
}
