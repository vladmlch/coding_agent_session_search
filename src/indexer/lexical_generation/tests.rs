use super::cleanup::*;
use super::*;
use tempfile::TempDir;

#[path = "cleanup_tests.rs"]
mod cleanup_tests;

#[test]
fn manifest_round_trips_through_json() {
    let mut manifest = LexicalGenerationManifest::new_scratch(
        "gen-00000001-abc",
        "attempt-00000001",
        "fp-deadbeef",
        1_700_000_000_000,
    );
    manifest.set_shard_plan_and_budget(
        LexicalGenerationShardPlan {
            plan_id: "plan-fp-deadbeef-2".into(),
            planner_version: 1,
            shard_count: 2,
            packet_contract_version: 1,
            source_db_fingerprint: "fp-deadbeef".into(),
        },
        LexicalGenerationBuildBudget {
            policy_id: "responsive-default".into(),
            effective_settings_fingerprint: "settings-fp-1".into(),
            max_inflight_message_bytes: 8 * 1024 * 1024,
            producer_queue_pages: 4,
            batch_conversation_limit: 64,
            worker_threads: 6,
            controller_reason: Some("reserved_2_cores_for_interactive_use".into()),
            extra_limits: BTreeMap::from([("staged_merge_jobs".into(), 2)]),
        },
        1_700_000_000_250,
    );
    let mut shard_a = LexicalShardManifest::planned("shard-0000", 0, 1_700_000_000_250);
    shard_a.indexed_doc_count = 20;
    shard_a.message_count = 20;
    shard_a.artifact_bytes = 4096;
    shard_a.stable_hash = Some("shard-hash-a".into());
    shard_a.transition(LexicalShardLifecycleState::Published, 1_700_000_000_900);
    shard_a.pinned = true;
    shard_a.reclaimable = false;
    let mut shard_b = LexicalShardManifest::planned("shard-0001", 1, 1_700_000_000_250);
    shard_b.indexed_doc_count = 14;
    shard_b.message_count = 14;
    shard_b.artifact_bytes = 2048;
    shard_b.stable_hash = Some("shard-hash-b".into());
    shard_b.transition(LexicalShardLifecycleState::Published, 1_700_000_000_900);
    shard_b.pinned = true;
    shard_b.reclaimable = false;
    manifest.set_shards(vec![shard_a, shard_b], 1_700_000_000_900);
    manifest.conversation_count = 12;
    manifest.message_count = 34;
    manifest.indexed_doc_count = 34;
    manifest.equivalence_manifest_fingerprint = Some("eq-fp-123".into());
    manifest.transition_build(LexicalGenerationBuildState::Validated, 1_700_000_000_500);
    manifest.transition_publish(LexicalGenerationPublishState::Published, 1_700_000_001_000);
    manifest.record_merge_debt(
        2,
        6144,
        "shard segments are queryable before background consolidation",
        1_700_000_001_100,
    );

    let bytes = serde_json::to_vec(&manifest).unwrap();
    let parsed: LexicalGenerationManifest = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed, manifest);
    assert_eq!(
        parsed.shard_plan.as_ref().unwrap().plan_id,
        "plan-fp-deadbeef-2"
    );
    assert_eq!(
        parsed
            .build_budget
            .as_ref()
            .unwrap()
            .effective_settings_fingerprint,
        "settings-fp-1"
    );
    assert_eq!(parsed.shards.len(), 2);
    assert!(parsed.is_serveable());
    assert!(parsed.merge_debt.has_pending_work());
    assert!(!parsed.is_fully_consolidated());
}

#[test]
fn build_and_publish_states_serialize_as_snake_case_strings() {
    let states: Vec<(LexicalGenerationBuildState, &str)> = vec![
        (LexicalGenerationBuildState::Scratch, "scratch"),
        (LexicalGenerationBuildState::Building, "building"),
        (LexicalGenerationBuildState::Built, "built"),
        (LexicalGenerationBuildState::Validating, "validating"),
        (LexicalGenerationBuildState::Validated, "validated"),
        (LexicalGenerationBuildState::Failed, "failed"),
    ];
    for (state, expected) in states {
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, format!("\"{expected}\""));
    }
    let publish_states: Vec<(LexicalGenerationPublishState, &str)> = vec![
        (LexicalGenerationPublishState::Staged, "staged"),
        (LexicalGenerationPublishState::Published, "published"),
        (LexicalGenerationPublishState::Superseded, "superseded"),
        (LexicalGenerationPublishState::Quarantined, "quarantined"),
    ];
    for (state, expected) in publish_states {
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, format!("\"{expected}\""));
    }
    let merge_debt_states: Vec<(LexicalGenerationMergeDebtState, &str)> = vec![
        (LexicalGenerationMergeDebtState::None, "none"),
        (LexicalGenerationMergeDebtState::Pending, "pending"),
        (LexicalGenerationMergeDebtState::Running, "running"),
        (LexicalGenerationMergeDebtState::Paused, "paused"),
        (LexicalGenerationMergeDebtState::Blocked, "blocked"),
        (LexicalGenerationMergeDebtState::Complete, "complete"),
        (LexicalGenerationMergeDebtState::Cancelled, "cancelled"),
    ];
    for (state, expected) in merge_debt_states {
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, format!("\"{expected}\""));
    }
    let shard_states: Vec<(LexicalShardLifecycleState, &str)> = vec![
        (LexicalShardLifecycleState::Planned, "planned"),
        (LexicalShardLifecycleState::Building, "building"),
        (LexicalShardLifecycleState::Staged, "staged"),
        (LexicalShardLifecycleState::Validated, "validated"),
        (LexicalShardLifecycleState::Published, "published"),
        (LexicalShardLifecycleState::Resumable, "resumable"),
        (LexicalShardLifecycleState::Quarantined, "quarantined"),
        (LexicalShardLifecycleState::Abandoned, "abandoned"),
    ];
    for (state, expected) in shard_states {
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, format!("\"{expected}\""));
    }
}

#[test]
fn failure_history_is_append_only_and_bumps_updated_at() {
    let mut manifest =
        LexicalGenerationManifest::new_scratch("gen-x", "attempt-1", "fp-x", 1_000_000);
    assert_eq!(manifest.updated_at_ms, 1_000_000);
    manifest.record_failure("attempt-1", "build", "oom during Tantivy merge", 2_000_000);
    manifest.record_failure("attempt-2", "validate", "doc count mismatch", 3_000_000);
    assert_eq!(manifest.failure_history.len(), 2);
    assert_eq!(manifest.failure_history[0].attempt_id, "attempt-1");
    assert_eq!(manifest.failure_history[0].phase, "build");
    assert_eq!(manifest.failure_history[1].attempt_id, "attempt-2");
    assert_eq!(manifest.failure_history[1].phase, "validate");
    assert_eq!(manifest.updated_at_ms, 3_000_000);
}

#[test]
fn store_and_load_round_trip_through_disk() {
    let tmp = TempDir::new().unwrap();
    let gen_dir = tmp.path().join("gen-1");
    assert_eq!(load_manifest(&gen_dir).unwrap(), None);

    let manifest =
        LexicalGenerationManifest::new_scratch("gen-1", "attempt-1", "fp-abc", 1_700_000_000_000);
    store_manifest(&gen_dir, &manifest).unwrap();
    let loaded = load_manifest(&gen_dir).unwrap().unwrap();
    assert_eq!(loaded, manifest);
    assert!(manifest_path(&gen_dir).exists());
}

#[test]
fn load_refuses_future_manifest_version() {
    let tmp = TempDir::new().unwrap();
    let gen_dir = tmp.path().join("gen-future");
    fs::create_dir_all(&gen_dir).unwrap();
    let future = serde_json::json!({
        "manifest_version": LEXICAL_GENERATION_MANIFEST_VERSION + 99,
        "generation_id": "gen-future",
        "attempt_id": "attempt-future",
        "created_at_ms": 1i64,
        "updated_at_ms": 1i64,
        "source_db_fingerprint": "fp-future",
        "conversation_count": 0u64,
        "message_count": 0u64,
        "indexed_doc_count": 0u64,
        "equivalence_manifest_fingerprint": null,
        "build_state": "scratch",
        "publish_state": "staged",
        "failure_history": [],
    });
    fs::write(
        manifest_path(&gen_dir),
        serde_json::to_vec(&future).unwrap(),
    )
    .unwrap();
    let err = load_manifest(&gen_dir).unwrap_err().to_string();
    assert!(
        err.contains("future manifest_version"),
        "expected future-version rejection, got {err}"
    );
}

#[test]
fn store_is_atomic_rename_and_overwrites_existing_manifest() {
    let tmp = TempDir::new().unwrap();
    let gen_dir = tmp.path().join("gen-atomic");
    let mut manifest =
        LexicalGenerationManifest::new_scratch("gen-atomic", "attempt-a", "fp-v1", 1_000_000);
    store_manifest(&gen_dir, &manifest).unwrap();

    manifest.transition_build(LexicalGenerationBuildState::Built, 2_000_000);
    manifest.attempt_id = "attempt-b".into();
    store_manifest(&gen_dir, &manifest).unwrap();

    // No leftover tmp files — the rename should have swept them.
    let entries: Vec<_> = fs::read_dir(&gen_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().into_string().unwrap())
        .collect();
    assert_eq!(entries, vec![LEXICAL_GENERATION_MANIFEST_FILE.to_string()]);

    let reloaded = load_manifest(&gen_dir).unwrap().unwrap();
    assert_eq!(reloaded.attempt_id, "attempt-b");
    assert_eq!(reloaded.build_state, LexicalGenerationBuildState::Built);
}

#[cfg(unix)]
#[test]
fn manifest_temp_creation_refuses_preexisting_symlink() {
    use std::os::unix::fs::symlink;

    let tmp = TempDir::new().unwrap();
    let protected = tmp.path().join("protected.json");
    let temp_path = tmp.path().join("manifest.tmp");
    fs::write(&protected, b"protected").unwrap();
    symlink(&protected, &temp_path).unwrap();

    let err = create_new_manifest_temp_file(&temp_path).expect_err("symlink collision should fail");
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(fs::read(&protected).unwrap(), b"protected");
}

#[test]
fn store_manifest_keeps_attempt_id_out_of_temp_filename() {
    let tmp = TempDir::new().unwrap();
    let gen_dir = tmp.path().join("gen-path-like-attempt");
    let manifest = LexicalGenerationManifest::new_scratch(
        "gen-path-like-attempt",
        "../../not-a-temp-path",
        "fp-abc",
        1_700_000_000_000,
    );

    store_manifest(&gen_dir, &manifest).unwrap();

    let loaded = load_manifest(&gen_dir).unwrap().unwrap();
    assert_eq!(loaded.attempt_id, "../../not-a-temp-path");
    assert!(manifest_path(&gen_dir).exists());
    assert!(!tmp.path().join("not-a-temp-path").exists());
}

#[test]
fn is_serveable_requires_validated_and_published() {
    let mut manifest = LexicalGenerationManifest::new_scratch("gen-serve", "attempt-1", "fp", 1);
    assert!(!manifest.is_serveable());
    manifest.transition_build(LexicalGenerationBuildState::Validated, 2);
    assert!(!manifest.is_serveable(), "validated but not yet published");
    manifest.transition_publish(LexicalGenerationPublishState::Published, 3);
    assert!(manifest.is_serveable());
    manifest.transition_publish(LexicalGenerationPublishState::Superseded, 4);
    assert!(!manifest.is_serveable(), "superseded must not serve");
}

#[test]
fn published_generation_can_serve_before_deferred_merge_debt_settles() {
    let mut manifest = LexicalGenerationManifest::new_scratch("gen-debt", "attempt-1", "fp", 1);
    manifest.transition_build(LexicalGenerationBuildState::Validated, 2);
    manifest.transition_publish(LexicalGenerationPublishState::Published, 3);

    manifest.record_merge_debt(
        3,
        12_288,
        "segment consolidation is safe to defer after atomic publish",
        4,
    );

    assert!(
        manifest.is_serveable(),
        "merge debt must not drag safe published assets off the query path"
    );
    assert!(
        !manifest.is_fully_consolidated(),
        "pending debt should keep fully-settled status false"
    );
    assert_eq!(
        manifest.merge_debt.state,
        LexicalGenerationMergeDebtState::Pending
    );
    assert_eq!(manifest.merge_debt.pending_shard_count, 3);
    assert_eq!(manifest.merge_debt.pending_artifact_bytes, 12_288);
    assert!(manifest.merge_debt.has_pending_work());
}

#[test]
fn merge_debt_tracks_background_pause_block_and_completion_reasons() {
    let mut manifest =
        LexicalGenerationManifest::new_scratch("gen-debt-flow", "attempt-1", "fp", 1);
    manifest.record_merge_debt(2, 2048, "two shard fragments need compaction", 2);

    manifest.transition_merge_debt(
        LexicalGenerationMergeDebtState::Running,
        3,
        Some("background worker acquired consolidation lease".into()),
        Some("controller admitted one low-priority merge job".into()),
    );
    assert_eq!(
        manifest.merge_debt.state,
        LexicalGenerationMergeDebtState::Running
    );
    assert_eq!(
        manifest.merge_debt.controller_reason.as_deref(),
        Some("controller admitted one low-priority merge job")
    );

    manifest.transition_merge_debt(
        LexicalGenerationMergeDebtState::Paused,
        4,
        Some("foreground search pressure exceeded reserve budget".into()),
        Some("controller yielded to interactive workload".into()),
    );
    assert_eq!(
        manifest.merge_debt.state,
        LexicalGenerationMergeDebtState::Paused
    );
    assert!(manifest.merge_debt.has_pending_work());

    manifest.transition_merge_debt(
        LexicalGenerationMergeDebtState::Blocked,
        5,
        Some("publish lock held by another generation".into()),
        Some("single-flight lock prevented duplicate compaction".into()),
    );
    assert_eq!(
        manifest.merge_debt.state,
        LexicalGenerationMergeDebtState::Blocked
    );

    manifest.transition_merge_debt(
        LexicalGenerationMergeDebtState::Complete,
        6,
        Some("background consolidation finished".into()),
        Some("controller budget remained below pressure threshold".into()),
    );
    assert!(manifest.merge_debt.is_fully_settled());
    assert_eq!(manifest.merge_debt.pending_shard_count, 0);
    assert_eq!(manifest.merge_debt.pending_artifact_bytes, 0);
    assert_eq!(manifest.updated_at_ms, 6);
}

#[test]
fn recovery_decision_attaches_published_generation() {
    let mut manifest =
        LexicalGenerationManifest::new_scratch("gen-published", "attempt-1", "fp-published", 10);
    manifest.set_shard_plan_and_budget(test_shard_plan("fp-published", 2), test_budget(), 11);
    let mut shard_a = LexicalShardManifest::planned("shard-a", 0, 11);
    shard_a.transition(LexicalShardLifecycleState::Published, 20);
    let mut shard_b = LexicalShardManifest::planned("shard-b", 1, 11);
    shard_b.transition(LexicalShardLifecycleState::Published, 20);
    manifest.set_shards(vec![shard_a, shard_b], 20);
    manifest.transition_build(LexicalGenerationBuildState::Validated, 30);
    manifest.transition_publish(LexicalGenerationPublishState::Published, 31);

    let decision = manifest.recovery_decision();
    assert_eq!(
        decision.action,
        LexicalGenerationRecoveryAction::AttachPublished
    );
    assert!(decision.resumable_shards.is_empty());
    assert!(decision.quarantined_shards.is_empty());
}

#[test]
fn recovery_decision_publishes_complete_validated_shard_set() {
    let mut manifest =
        LexicalGenerationManifest::new_scratch("gen-validated", "attempt-1", "fp-validated", 10);
    manifest.set_shard_plan_and_budget(test_shard_plan("fp-validated", 2), test_budget(), 11);
    let mut shard_a = LexicalShardManifest::planned("shard-a", 0, 11);
    shard_a.transition(LexicalShardLifecycleState::Validated, 20);
    let mut shard_b = LexicalShardManifest::planned("shard-b", 1, 11);
    shard_b.transition(LexicalShardLifecycleState::Validated, 20);
    manifest.set_shards(vec![shard_a, shard_b], 20);
    manifest.transition_build(LexicalGenerationBuildState::Validated, 30);

    let decision = manifest.recovery_decision();
    assert_eq!(
        decision.action,
        LexicalGenerationRecoveryAction::PublishValidated
    );
    assert!(decision.reason.contains("complete publish-ready shard set"));
}

#[test]
fn recovery_decision_resumes_resumable_staged_shards() {
    let mut manifest =
        LexicalGenerationManifest::new_scratch("gen-resume", "attempt-1", "fp-resume", 10);
    manifest.set_shard_plan_and_budget(test_shard_plan("fp-resume", 2), test_budget(), 11);
    manifest.set_shards(
        vec![
            LexicalShardManifest::planned("shard-a", 0, 11),
            LexicalShardManifest::planned("shard-b", 1, 11),
        ],
        12,
    );
    assert!(manifest.transition_shard(
        "shard-a",
        LexicalShardLifecycleState::Resumable,
        20,
        Some("builder checkpoint reached after doc flush".into()),
    ));
    assert!(manifest.transition_shard("shard-b", LexicalShardLifecycleState::Staged, 21, None));
    manifest.transition_build(LexicalGenerationBuildState::Building, 30);

    let decision = manifest.recovery_decision();
    assert_eq!(
        decision.action,
        LexicalGenerationRecoveryAction::ResumeStaged
    );
    assert_eq!(
        decision.resumable_shards,
        vec!["shard-a".to_string(), "shard-b".to_string()]
    );
    assert!(decision.quarantined_shards.is_empty());
}

#[test]
fn recovery_decision_keeps_quarantined_shards_out_of_search() {
    let mut manifest =
        LexicalGenerationManifest::new_scratch("gen-quarantine", "attempt-1", "fp-quarantine", 10);
    manifest.set_shard_plan_and_budget(test_shard_plan("fp-quarantine", 2), test_budget(), 11);
    manifest.set_shards(
        vec![
            LexicalShardManifest::planned("shard-a", 0, 11),
            LexicalShardManifest::planned("shard-b", 1, 11),
        ],
        12,
    );
    assert!(manifest.transition_shard(
        "shard-b",
        LexicalShardLifecycleState::Quarantined,
        20,
        Some("tantivy open probe failed".into()),
    ));
    manifest.transition_build(LexicalGenerationBuildState::Validated, 30);

    let decision = manifest.recovery_decision();
    assert_eq!(
        decision.action,
        LexicalGenerationRecoveryAction::KeepQuarantined
    );
    assert_eq!(decision.quarantined_shards, vec!["shard-b".to_string()]);
    assert!(decision.reason.contains("must stay out of search"));
}

#[test]
fn recovery_decision_discards_abandoned_or_failed_generation() {
    let mut manifest =
        LexicalGenerationManifest::new_scratch("gen-abandoned", "attempt-1", "fp-abandoned", 10);
    manifest.set_shard_plan_and_budget(test_shard_plan("fp-abandoned", 1), test_budget(), 11);
    manifest.set_shards(vec![LexicalShardManifest::planned("shard-a", 0, 11)], 12);
    assert!(manifest.transition_shard(
        "shard-a",
        LexicalShardLifecycleState::Abandoned,
        20,
        Some("source fingerprint changed mid-build".into()),
    ));
    manifest.transition_build(LexicalGenerationBuildState::Failed, 30);

    let decision = manifest.recovery_decision();
    assert_eq!(
        decision.action,
        LexicalGenerationRecoveryAction::DiscardAndRebuild
    );
    assert_eq!(decision.abandoned_shards, vec!["shard-a".to_string()]);
    assert!(decision.reason.contains("must rebuild from source"));
}

#[test]
fn shard_transition_records_retention_and_recovery_reasons() {
    let mut manifest =
        LexicalGenerationManifest::new_scratch("gen-retention", "attempt-1", "fp", 1);
    manifest.set_shards(vec![LexicalShardManifest::planned("shard-a", 0, 1)], 2);

    assert!(manifest.transition_shard(
        "shard-a",
        LexicalShardLifecycleState::Quarantined,
        3,
        Some("checksum mismatch".into()),
    ));
    let shard = &manifest.shards[0];
    assert!(!shard.reclaimable);
    assert!(!shard.pinned);
    assert_eq!(
        shard.quarantine_reason.as_deref(),
        Some("checksum mismatch")
    );

    assert!(manifest.transition_shard("shard-a", LexicalShardLifecycleState::Published, 4, None,));
    let shard = &manifest.shards[0];
    assert!(shard.pinned);
    assert!(!shard.reclaimable);
}

fn test_shard_plan(source_db_fingerprint: &str, shard_count: u32) -> LexicalGenerationShardPlan {
    LexicalGenerationShardPlan {
        plan_id: format!("plan-{source_db_fingerprint}-{shard_count}"),
        planner_version: 1,
        shard_count,
        packet_contract_version: 1,
        source_db_fingerprint: source_db_fingerprint.into(),
    }
}

fn test_budget() -> LexicalGenerationBuildBudget {
    LexicalGenerationBuildBudget {
        policy_id: "test-policy".into(),
        effective_settings_fingerprint: "settings-fp-test".into(),
        max_inflight_message_bytes: 4 * 1024 * 1024,
        producer_queue_pages: 2,
        batch_conversation_limit: 16,
        worker_threads: 2,
        controller_reason: Some("test budget".into()),
        extra_limits: BTreeMap::from([("staged_merge_jobs".into(), 1)]),
    }
}

pub(super) fn test_shard(
    shard_id: &str,
    shard_ordinal: u32,
    state: LexicalShardLifecycleState,
    artifact_bytes: u64,
) -> LexicalShardManifest {
    let mut shard = LexicalShardManifest::planned(shard_id, shard_ordinal, 1);
    shard.transition(state, 2);
    shard.artifact_bytes = artifact_bytes;
    shard.reclaimable = matches!(
        state,
        LexicalShardLifecycleState::Planned | LexicalShardLifecycleState::Abandoned
    );
    shard.pinned = matches!(state, LexicalShardLifecycleState::Published);
    shard
}

/// `coding_agent_session_search-ibuuh.19` golden gate: every
/// LexicalCleanupDisposition variant's `as_str()` must equal its
/// serde-serialized name AND must be unique. Pre-fix this gate
/// did not exist; the duplicate-naming class that bit ErrorKind
/// (al19b — 3 real duplicates discovered in production)
/// could land here unnoticed because the disposition feeds the
/// machine-readable cleanup inventory operators read from
/// `cass diag --quarantine`. A regression that:
/// - added a new variant without an `as_str()` arm,
/// - drifted serde rename_all away from snake_case,
/// - introduced a duplicate string,
///   would trip this immediately.
#[test]
fn cleanup_disposition_as_str_matches_serde_serialization_byte_for_byte() {
    use std::collections::HashSet;

    let mut seen_strs: HashSet<&'static str> = HashSet::new();
    for &variant in LexicalCleanupDisposition::all_variants() {
        let as_str = variant.as_str();
        // Uniqueness gate: catches the al19b-class duplicate bug.
        assert!(
            seen_strs.insert(as_str),
            "duplicate disposition string detected: {variant:?} maps to {as_str:?} \
                 which was already registered by an earlier variant"
        );
        // serde alignment: rename_all = snake_case must produce
        // the exact same string as_str() returns. A regression in
        // either direction (variant rename vs as_str() drift, or
        // serde attribute change) trips here.
        let serde_str = serde_json::to_string(&variant).expect("serialize disposition");
        // serde wraps strings in quotes — strip them.
        let serde_str = serde_str.trim_matches('"');
        assert_eq!(
            serde_str, as_str,
            "serde serialization {serde_str:?} must equal as_str() {as_str:?} for {variant:?}"
        );
    }
    // All eight variants must be covered. A new variant added
    // without registering it in all_variants() would shrink the
    // count and fail this assertion.
    assert_eq!(
        LexicalCleanupDisposition::all_variants().len(),
        8,
        "disposition enum has 8 variants at landing time; bump this count + add the new \
             variant + extend is_protected_retention_disposition for any addition"
    );
}

/// `coding_agent_session_search-ibuuh.19` classification gate:
/// `is_protected_retention_disposition()` must classify every
/// LexicalCleanupDisposition variant — exactly six are protected
/// (kept on disk) and exactly two are reclaimable. Pre-fix, a
/// new variant added without thinking about retention safety
/// would default to "not protected" silently and risk reclaiming
/// state the operator wanted preserved (or vice versa). Pinning
/// the partition explicitly closes that hole.
#[test]
fn cleanup_disposition_protected_retention_classification_is_exhaustive() {
    let protected: Vec<LexicalCleanupDisposition> = LexicalCleanupDisposition::all_variants()
        .iter()
        .copied()
        .filter(|d| is_protected_retention_disposition(*d))
        .collect();
    let reclaimable: Vec<LexicalCleanupDisposition> = LexicalCleanupDisposition::all_variants()
        .iter()
        .copied()
        .filter(|d| !is_protected_retention_disposition(*d))
        .collect();

    // Six protected + two reclaimable = eight variants total.
    // A new variant that defaults to non-protected without an
    // explicit policy decision will shift these counts and trip
    // the test, forcing a maintainer to think about retention.
    assert_eq!(
        protected.len(),
        6,
        "expected exactly 6 protected variants; got {protected:?}"
    );
    assert_eq!(
        reclaimable.len(),
        2,
        "expected exactly 2 reclaimable variants; got {reclaimable:?}"
    );

    // Pin the *exact* protected set so a regression that
    // misclassifies (e.g. moves CurrentPublished out of the
    // protected set, which would let cleanup nuke the live
    // search asset) trips the assertion with the variant name.
    for required_protected in [
        LexicalCleanupDisposition::CurrentPublished,
        LexicalCleanupDisposition::ActiveWork,
        LexicalCleanupDisposition::QuarantinedRetained,
        LexicalCleanupDisposition::SupersededRetained,
        LexicalCleanupDisposition::FailedRetained,
        LexicalCleanupDisposition::PinnedRetained,
    ] {
        assert!(
            protected.contains(&required_protected),
            "{required_protected:?} MUST be classified as protected — moving it out \
                 of the protected set risks reclaiming live or operator-flagged state"
        );
    }
    // Pin the reclaimable set too: a regression that
    // accidentally protected SupersededReclaimable would prevent
    // disk-budget pruning from ever reclaiming superseded
    // generations, leading to unbounded disk bloat (the bead
    // BACKGROUND warns about exactly this).
    for required_reclaimable in [
        LexicalCleanupDisposition::SupersededReclaimable,
        LexicalCleanupDisposition::FailedReclaimable,
    ] {
        assert!(
            reclaimable.contains(&required_reclaimable),
            "{required_reclaimable:?} MUST be reclaimable — protecting it would block \
                 disk-budget pruning of superseded/failed generations"
        );
    }
}

/// `coding_agent_session_search-ibuuh.19` round-trip gate: every
/// disposition string MUST round-trip through
/// `serde_json::to_string` → `serde_json::from_str` and yield the
/// same variant. Pre-fix, a regression that broke deserialization
/// (e.g. by dropping `Deserialize` derive or changing the rename
/// strategy in one direction) would silently break operators
/// reading `.lexical-rebuild-cleanup.json` artifacts back from
/// disk for trend analysis.
#[test]
fn cleanup_disposition_serde_round_trips_for_every_variant() {
    for &variant in LexicalCleanupDisposition::all_variants() {
        let json = serde_json::to_string(&variant).expect("serialize");
        let parsed: LexicalCleanupDisposition = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            parsed, variant,
            "disposition round-trip mismatch for {variant:?}: serialized as {json}, \
                 parsed as {parsed:?}"
        );
    }
}

/// `coding_agent_session_search-ibuuh.19` structured-logs gate:
/// every cleanup classification MUST emit a tracing event whose
/// fields let an operator answer "what got pruned?" and "why was
/// X kept?" from logs alone — directly addressing the bead's
/// "structured logs that let a future agent understand exactly
/// why disk was reclaimed or preserved" SCOPE bullet. Pre-fix,
/// `record_inventory` had ZERO tracing emissions (verified via
/// grep -nE "tracing::" returning empty in this file). Post-fix,
/// every classification routes to debug/info/warn per severity
/// tier (reclaimable=debug, retained-by-policy=info,
/// quarantined/failed-retained=warn).
///
/// This test pins:
/// 1. The QuarantinedRetained classification emits a `warn`
///    event (operator must see it in default logs).
/// 2. The event carries `disposition`, `generation_id`,
///    `reason`, and `retained_bytes` fields so a single log
///    line is fully diagnostic.
/// 3. The event target is `cass::indexer::lexical_cleanup`
///    (operators can grep / filter by target).
#[test]
fn record_inventory_emits_structured_classification_event_for_quarantined_generation() {
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    #[derive(Debug, Clone, Default)]
    struct CapturedEvent {
        level: String,
        target: String,
        fields: std::collections::HashMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct ClassificationCollector {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
    }

    impl<S: Subscriber> Layer<S> for ClassificationCollector {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            if event.metadata().target() != "cass::indexer::lexical_cleanup" {
                return;
            }
            let mut visitor = StringVisitor::default();
            event.record(&mut visitor);
            self.events
                .lock()
                .expect("collector lock")
                .push(CapturedEvent {
                    level: event.metadata().level().to_string(),
                    target: event.metadata().target().to_string(),
                    fields: visitor.fields,
                });
        }
    }

    #[derive(Default)]
    struct StringVisitor {
        fields: std::collections::HashMap<String, String>,
    }

    impl Visit for StringVisitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
        fn record_u64(&mut self, field: &Field, value: u64) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
        fn record_i64(&mut self, field: &Field, value: i64) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
        fn record_bool(&mut self, field: &Field, value: bool) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.fields
                .insert(field.name().to_string(), format!("{:?}", value));
        }
    }

    let collector = ClassificationCollector::default();
    let subscriber = Registry::default().with(collector.clone());

    // Build a quarantined fixture identical to the existing
    // `cleanup_inventory_keeps_quarantined_artifacts_for_inspection`
    // test (line ~2364) so the two tests share a factual basis.
    let mut manifest =
        LexicalGenerationManifest::new_scratch("gen-traced-quarantined", "attempt-1", "fp", 1);
    let shard = test_shard(
        "shard-bad",
        0,
        LexicalShardLifecycleState::Quarantined,
        4096,
    );
    manifest.set_shards(vec![shard], 2);
    assert!(manifest.transition_shard(
        "shard-bad",
        LexicalShardLifecycleState::Quarantined,
        3,
        Some("traced classification event regression".into()),
    ));
    manifest.transition_publish(LexicalGenerationPublishState::Quarantined, 4);

    // Drive record_inventory under our collector subscriber via
    // the existing public `from_manifests` entrypoint, which calls
    // record_inventory internally — same code path operators
    // exercise via cass diag/doctor.
    tracing::subscriber::with_default(subscriber, || {
        let _plan = LexicalCleanupDryRunPlan::from_manifests([&manifest]);
    });

    let captured = collector.events.lock().expect("collector lock").clone();
    // Exactly one classification event for one generation.
    assert_eq!(
        captured.len(),
        1,
        "record_inventory must emit exactly one classification event per generation; \
             got {captured:?}"
    );
    let event = &captured[0];

    // Invariant 1: target.
    assert_eq!(
        event.target, "cass::indexer::lexical_cleanup",
        "classification event must use the lexical_cleanup target so operators can \
             grep/filter by it"
    );

    // Invariant 2: WARN severity for quarantined (must surface in
    // default-level logs without dredging).
    assert_eq!(
        event.level,
        Level::WARN.to_string(),
        "QuarantinedRetained MUST emit at warn level so the inspection-required \
             state shows up in default operator logs; got {event:?}"
    );

    // Invariant 3: every diagnostic field is present so a single
    // log line answers "what got pruned?" and "why was X kept?"
    for required in [
        "disposition",
        "generation_id",
        "reason",
        "reclaimable_bytes",
        "retained_bytes",
        "artifact_bytes",
    ] {
        assert!(
            event.fields.contains_key(required),
            "classification event MUST include {required} field; got fields {:?}",
            event.fields.keys().collect::<Vec<_>>()
        );
    }

    // Invariant 4: field VALUES match the classification.
    assert_eq!(
        event.fields.get("disposition"),
        Some(&"quarantined_retained".to_string())
    );
    assert_eq!(
        event.fields.get("generation_id"),
        Some(&"gen-traced-quarantined".to_string())
    );
    assert_eq!(
        event.fields.get("retained_bytes"),
        Some(&"4096".to_string())
    );
    assert_eq!(
        event.fields.get("reclaimable_bytes"),
        Some(&"0".to_string())
    );
    // The reason string must explain WHY the generation is being
    // retained (operator inspection hold).
    let reason = event.fields.get("reason").expect("reason field present");
    assert!(
        reason.contains("operator inspection") || reason.contains("quarantined"),
        "reason field must explain the inspection hold; got {reason:?}"
    );
}

/// Bead coding_agent_session_search-yv5fn: coverage gap in 7d3297c7.
/// The sibling test above exercises only `QuarantinedRetained`.
/// This table-driven companion asserts correct severity routing
/// for EVERY `LexicalCleanupDisposition` variant — 8 total across
/// 3 severity tiers (2 DEBUG / 4 INFO / 2 WARN). A future refactor
/// that re-routes any variant to the wrong tier, or drops emission
/// entirely for one specific variant, would trip this test where
/// the Quarantined-only test would have stayed green.
///
/// Fixture shapes follow `classify_generation_for_cleanup` at
/// src/indexer/lexical_generation.rs:~1522:
///   - `CurrentPublished`: build=Validated, publish=Published, a
///     single Validated shard so `is_serveable` returns true.
///   - `ActiveWork`: default `new_scratch` state (build=Scratch).
///   - `QuarantinedRetained`: publish=Quarantined.
///   - `SupersededReclaimable`: publish=Superseded with a Planned
///     shard (reclaimable=true, artifact_bytes>0).
///   - `SupersededRetained`: publish=Superseded with a Published
///     shard (reclaimable=false via test_shard) — no reclaim
///     bytes, so the classifier picks the retained variant.
///   - `FailedReclaimable`: build=Failed with an Abandoned shard
///     (reclaimable=true) carrying artifact_bytes>0.
///   - `FailedRetained`: build=Failed with a Published shard
///     (pinned=true, reclaimable=false).
///   - `PinnedRetained`: build=Validated, publish=Staged, zero
///     shards — classifier falls through to the pinned arm.
#[test]
fn record_inventory_emits_correct_severity_for_every_disposition_variant() {
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    #[derive(Debug, Clone, Default)]
    struct CapturedEvent {
        level: String,
        target: String,
        fields: std::collections::HashMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct ClassificationCollector {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
    }

    impl<S: Subscriber> Layer<S> for ClassificationCollector {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            if event.metadata().target() != "cass::indexer::lexical_cleanup" {
                return;
            }
            let mut visitor = StringVisitor::default();
            event.record(&mut visitor);
            self.events
                .lock()
                .expect("collector lock")
                .push(CapturedEvent {
                    level: event.metadata().level().to_string(),
                    target: event.metadata().target().to_string(),
                    fields: visitor.fields,
                });
        }
    }

    #[derive(Default)]
    struct StringVisitor {
        fields: std::collections::HashMap<String, String>,
    }

    impl Visit for StringVisitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
        fn record_u64(&mut self, field: &Field, value: u64) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
        fn record_i64(&mut self, field: &Field, value: i64) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
        fn record_bool(&mut self, field: &Field, value: bool) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.fields
                .insert(field.name().to_string(), format!("{:?}", value));
        }
    }

    // Build a manifest that classifies to the requested disposition.
    fn fixture_for(
        disposition: LexicalCleanupDisposition,
        generation_id: &str,
    ) -> LexicalGenerationManifest {
        let mut m = LexicalGenerationManifest::new_scratch(generation_id, "attempt-1", "fp", 1);
        match disposition {
            LexicalCleanupDisposition::CurrentPublished => {
                // `all_shards_publish_ready` accepts Validated /
                // Published shards; with shard_plan=None the
                // plan-count check is skipped, so a single shard
                // is enough to make `is_serveable` true once
                // build=Validated + publish=Published land.
                let shard = test_shard(
                    "shard-current",
                    0,
                    LexicalShardLifecycleState::Validated,
                    128,
                );
                m.set_shards(vec![shard], 2);
                m.transition_build(LexicalGenerationBuildState::Validated, 3);
                m.transition_publish(LexicalGenerationPublishState::Published, 4);
            }
            LexicalCleanupDisposition::ActiveWork => {
                // Default new_scratch has build=Scratch → active.
            }
            LexicalCleanupDisposition::QuarantinedRetained => {
                let shard = test_shard("shard-q", 0, LexicalShardLifecycleState::Quarantined, 256);
                m.set_shards(vec![shard], 2);
                m.transition_publish(LexicalGenerationPublishState::Quarantined, 3);
            }
            LexicalCleanupDisposition::SupersededReclaimable => {
                // Planned shards are reclaimable=true per test_shard.
                let shard = test_shard("shard-s-r", 0, LexicalShardLifecycleState::Planned, 512);
                m.set_shards(vec![shard], 2);
                m.transition_build(LexicalGenerationBuildState::Validated, 3);
                m.transition_publish(LexicalGenerationPublishState::Superseded, 4);
            }
            LexicalCleanupDisposition::SupersededRetained => {
                // Published shards are pinned=true, reclaimable=false.
                let shard = test_shard(
                    "shard-s-keep",
                    0,
                    LexicalShardLifecycleState::Published,
                    512,
                );
                m.set_shards(vec![shard], 2);
                m.transition_build(LexicalGenerationBuildState::Validated, 3);
                m.transition_publish(LexicalGenerationPublishState::Superseded, 4);
            }
            LexicalCleanupDisposition::FailedReclaimable => {
                // Abandoned shards are reclaimable=true per test_shard.
                let shard = test_shard("shard-f-r", 0, LexicalShardLifecycleState::Abandoned, 1024);
                m.set_shards(vec![shard], 2);
                m.transition_build(LexicalGenerationBuildState::Failed, 3);
            }
            LexicalCleanupDisposition::FailedRetained => {
                // Published shards are pinned=true, reclaimable=false.
                let shard = test_shard(
                    "shard-f-keep",
                    0,
                    LexicalShardLifecycleState::Published,
                    1024,
                );
                m.set_shards(vec![shard], 2);
                m.transition_build(LexicalGenerationBuildState::Failed, 3);
            }
            LexicalCleanupDisposition::PinnedRetained => {
                // No shards, build=Validated, publish=Staged →
                // falls through every branch in the classifier.
                m.transition_build(LexicalGenerationBuildState::Validated, 2);
            }
        }
        m
    }

    // Table of (variant → expected severity tier).
    let cases: &[(LexicalCleanupDisposition, Level)] = &[
        (
            LexicalCleanupDisposition::SupersededReclaimable,
            Level::DEBUG,
        ),
        (LexicalCleanupDisposition::FailedReclaimable, Level::DEBUG),
        (LexicalCleanupDisposition::ActiveWork, Level::INFO),
        (LexicalCleanupDisposition::CurrentPublished, Level::INFO),
        (LexicalCleanupDisposition::SupersededRetained, Level::INFO),
        (LexicalCleanupDisposition::PinnedRetained, Level::INFO),
        (LexicalCleanupDisposition::QuarantinedRetained, Level::WARN),
        (LexicalCleanupDisposition::FailedRetained, Level::WARN),
    ];

    // Sanity: exactly 8 variants in the table — if a new variant is
    // ever added, `LexicalCleanupDisposition::all_variants` would
    // count higher than this table, so this assertion fires the
    // update before the mismatched-severity scenarios can slip.
    assert_eq!(
        LexicalCleanupDisposition::all_variants().len(),
        cases.len(),
        "table must cover every LexicalCleanupDisposition variant; \
             the classifier adds variants and this list must follow"
    );

    for (variant, expected_level) in cases {
        let collector = ClassificationCollector::default();
        let subscriber = Registry::default().with(collector.clone());
        let manifest = fixture_for(*variant, &format!("gen-{}", variant.as_str()));

        // Sanity: the fixture actually classifies to the variant
        // we intended. A mismatch here points to a fixture bug,
        // not to the tracing emission — keeps the severity
        // assertion below meaningful.
        let inventory_disposition = manifest.cleanup_inventory().disposition;
        assert_eq!(
            inventory_disposition, *variant,
            "fixture for variant {variant:?} must actually classify to {variant:?}, \
                 got {inventory_disposition:?}"
        );

        tracing::subscriber::with_default(subscriber, || {
            let _plan = LexicalCleanupDryRunPlan::from_manifests([&manifest]);
        });

        let captured = collector.events.lock().expect("collector lock").clone();
        assert_eq!(
            captured.len(),
            1,
            "variant {variant:?}: record_inventory must emit exactly one classification event; \
                 got {captured:?}"
        );
        let event = &captured[0];
        assert_eq!(
            event.level,
            expected_level.to_string(),
            "variant {variant:?} must emit at {expected_level:?} tier; got {event:?}"
        );
        assert_eq!(
            event.target, "cass::indexer::lexical_cleanup",
            "variant {variant:?}: target must stay grep-stable"
        );
        assert_eq!(
            event.fields.get("disposition").map(String::as_str),
            Some(variant.as_str()),
            "variant {variant:?}: disposition field must match the enum as_str"
        );
        for required in [
            "generation_id",
            "reason",
            "reclaimable_bytes",
            "retained_bytes",
            "artifact_bytes",
        ] {
            assert!(
                event.fields.contains_key(required),
                "variant {variant:?}: field {required} must be present; got {:?}",
                event.fields.keys().collect::<Vec<_>>()
            );
        }
    }
}
