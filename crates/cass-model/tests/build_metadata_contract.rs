use cass_model::dependency_pin_correlation::build_metadata::{BuildMetadata, current};

const CHILD_ENV: &str = "CASS_MODEL_BUILD_METADATA_CONTRACT_CHILD";
const CHILD_META: BuildMetadata = BuildMetadata {
    ingest_contract: "child_process_contract",
    ingest_reuse: true,
    ingest_reuse_blocker: "",
    build_commit: Some("child-commit"),
    build_commit_date: Some("child-date"),
    git_sha: Some("child-sha"),
};
const CONFLICT_META: BuildMetadata = BuildMetadata {
    ingest_contract: "conflicting_contract",
    ingest_reuse: false,
    ingest_reuse_blocker: "conflict",
    build_commit: None,
    build_commit_date: None,
    git_sha: None,
};

#[test]
fn metadata_child() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }

    assert!(std::panic::catch_unwind(BuildMetadata::current).is_err());
    assert!(std::panic::catch_unwind(current).is_err());

    BuildMetadata::register_idempotent(CHILD_META);
    assert_eq!(BuildMetadata::current(), &CHILD_META);
    assert_eq!(current(), &CHILD_META);
    BuildMetadata::register_idempotent(CHILD_META);
    assert!(
        std::panic::catch_unwind(|| { BuildMetadata::register_idempotent(CONFLICT_META) }).is_err()
    );
    assert_eq!(BuildMetadata::current(), &CHILD_META);
    println!("metadata child contract complete");
}

#[test]
fn metadata_registration_contract_child_process() {
    let exe = std::env::current_exe().expect("current test executable");
    let output = std::process::Command::new(exe)
        .args(["--exact", "metadata_child", "--nocapture"])
        .env(CHILD_ENV, "1")
        .output()
        .expect("spawn metadata child");
    assert!(
        output.status.success(),
        "child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("metadata child contract complete"));
}
