use super::*;
use crate::search::policy::SemanticPolicy;
use proptest::prelude::*;

mod helpers;
use helpers::*;

mod legacy;

// The pre-review tests below are retained verbatim as schema archaeology:
// they describe the rejected shadow-identity/full-coverage-only draft and
// intentionally do not compile into the test target. The active tests
// following the module exercise the accepted canonical identity and
// compare-and-swap contract.
#[rustfmt::skip]
#[cfg(any())]
mod superseded_pre_review_generation_contract;

mod accepted_contract;
mod accepted_pointer_and_resolution;

#[test]
fn accepted_structured_events_include_context_and_redact_paths_and_full_hashes() {
    use std::process::{Command, Stdio};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing_subscriber::prelude::*;
    use wait_timeout::ChildExt;

    const TRACE_CAPTURE_CHILD_ENV: &str = "CASS_SEMANTIC_MANIFEST_TRACE_CAPTURE_ISOLATED_V1";
    const TRACE_CAPTURE_TEST_NAME: &str = "search::semantic_manifest::tests::\
        accepted_structured_events_include_context_and_redact_paths_and_full_hashes";
    const TRACE_CAPTURE_TIMEOUT: Duration = Duration::from_secs(30);

    if std::env::var_os(TRACE_CAPTURE_CHILD_ENV).is_none() {
        let mut child = Command::new(std::env::current_exe().expect("current library test binary")) // ubs:ignore — fixed current test binary.
            .arg("--exact")
            .arg(TRACE_CAPTURE_TEST_NAME)
            .arg("--nocapture")
            .env(TRACE_CAPTURE_CHILD_ENV, "1")
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn isolated semantic-manifest tracing test");
        let status = child
            .wait_timeout(TRACE_CAPTURE_TIMEOUT)
            .expect("wait for isolated semantic-manifest tracing test");
        if status.is_none() {
            child
                .kill()
                .expect("kill timed-out semantic-manifest tracing test");
            let _ = child.wait();
        }
        let status =
            status.expect("isolated semantic-manifest tracing test exceeded 30-second timeout");
        assert!(
            status.success(),
            "isolated semantic-manifest tracing test failed with {status}"
        );
        return;
    }

    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("log buffer").extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let corpus = test_corpus_identity();
    let bytes = b"logged-vector".to_vec();
    let artifact = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        &bytes,
        &corpus,
    );
    let manifest = generation_with_id(
        "gen-structured-log",
        SemanticGenerationTopology::FastOnly,
        vec![artifact],
    );
    write_generation_artifacts(
        temp.path(),
        &manifest,
        &BTreeMap::from([(SemanticArtifactRole::FastVector, bytes)]),
    );
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let filter = tracing_subscriber::filter::dynamic_filter_fn(|_, _| true)
        .with_max_level_hint(tracing_subscriber::filter::LevelFilter::TRACE);
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .without_time()
        .with_writer(SharedWriter(Arc::clone(&buffer)))
        .with_filter(filter);
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        tracing::callsite::rebuild_interest_cache();
        manifest.write_immutable(temp.path()).unwrap();
        let pointer =
            SemanticCurrentPointerV1::for_manifest(&manifest, manifest.sealed_at_ms + 1, 1)
                .unwrap();
        pointer
            .publish_atomic(temp.path(), &SemanticExpectedCurrentV1::Absent)
            .unwrap();
        load_current_semantic_generation(temp.path(), Some(&corpus)).unwrap();
        let artifact_path = manifest
            .generation_dir(temp.path())
            .unwrap()
            .join("fast/primary.fsvi");
        fs::write(artifact_path, b"tamper-vector").unwrap();
        assert!(load_current_semantic_generation(temp.path(), Some(&corpus)).is_err());
    });
    let logs = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
    for field in [
        "semantic.manifest.artifact.validate",
        "semantic.manifest.seal",
        "semantic.pointer.swap",
        "semantic.manifest.load",
        "semantic.manifest.validate",
        "build",
        "request",
        "requested_topology",
        "realized_topology",
        "tier",
        "coverage",
        "artifact",
        "expected_identity",
        "actual_identity",
        "reason_code",
    ] {
        assert!(
            logs.contains(field),
            "missing structured field/event {field}; captured logs:\n{logs}"
        );
    }
    assert!(logs.contains("fast/primary.fsvi"));
    assert!(!logs.contains(&temp.path().display().to_string()));
    assert!(!logs.contains(&manifest.artifacts[0].artifact_sha256));
    assert!(!logs.contains(&manifest.producer.binary_elf_sha256));
    assert!(logs.contains(fingerprint_prefix(&manifest.artifacts[0].artifact_sha256)));
    assert!(logs.contains(fingerprint_prefix(&sha256_hex(b"tamper-vector"))));
}

proptest! {
    #[test]
    fn accepted_property_rejects_parent_alias_paths(
        directory in "[A-Za-z0-9_-]{1,64}",
        filename in "[A-Za-z0-9_-]{1,64}\\.fsvi",
    ) {
        let corpus = test_corpus_identity();
        let traversal = format!("{directory}/../{filename}");
        let artifact = complete_generation_artifact(
            SemanticArtifactRole::FastVector,
            &traversal,
            b"property-vector",
            &corpus,
        );
        let candidate =
            test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact]);
        let rejected_as_path_invariant = matches!(
            candidate.validate(),
            Err(SemanticGenerationError::InvalidManifest {
                class: SemanticManifestInvariantClass::Path,
                ..
            })
        );
        prop_assert!(
            rejected_as_path_invariant,
            "parent-alias paths must fail the semantic manifest path invariant"
        );
    }

    #[test]
    fn accepted_property_digest_is_stable_and_generation_sensitive(
        suffix in "[A-Za-z0-9_-]{1,32}",
    ) {
        let corpus = test_corpus_identity();
        let artifact = complete_generation_artifact(
            SemanticArtifactRole::FastVector,
            "fast/primary.fsvi",
            b"property-vector",
            &corpus,
        );
        let base =
            test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact]);
        let mut changed = base.clone();
        changed.generation_id = format!("gen-{suffix}");
        prop_assert_eq!(base.sha256().unwrap(), base.sha256().unwrap());
        prop_assert_ne!(base.sha256().unwrap(), changed.sha256().unwrap());
    }
}
