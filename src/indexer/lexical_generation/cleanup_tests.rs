use super::super::cleanup::*;
use super::super::*;
use super::test_shard;

#[test]
fn cleanup_inventory_retains_current_published_generation() {
    let mut manifest = LexicalGenerationManifest::new_scratch("gen-current", "attempt-1", "fp", 1);
    let mut shard = test_shard("shard-live", 0, LexicalShardLifecycleState::Published, 4096);
    shard.pinned = true;
    shard.reclaimable = false;
    manifest.set_shards(vec![shard], 2);
    manifest.transition_build(LexicalGenerationBuildState::Validated, 3);
    manifest.transition_publish(LexicalGenerationPublishState::Published, 4);

    let inventory = manifest.cleanup_inventory();
    assert_eq!(
        inventory.disposition,
        LexicalCleanupDisposition::CurrentPublished
    );
    assert_eq!(inventory.artifact_bytes, 4096);
    assert_eq!(inventory.reclaimable_bytes, 0);
    assert_eq!(inventory.retained_bytes, 4096);
    assert_eq!(
        inventory.shards[0].disposition,
        LexicalCleanupDisposition::CurrentPublished
    );
}

#[test]
fn cleanup_inventory_marks_superseded_unpinned_shards_reclaimable() {
    let mut manifest = LexicalGenerationManifest::new_scratch("gen-old", "attempt-1", "fp", 1);
    let mut reclaimable = test_shard(
        "shard-old-a",
        0,
        LexicalShardLifecycleState::Published,
        8192,
    );
    reclaimable.pinned = false;
    reclaimable.reclaimable = true;
    let mut retained = test_shard(
        "shard-old-b",
        1,
        LexicalShardLifecycleState::Published,
        2048,
    );
    retained.pinned = true;
    retained.reclaimable = false;
    manifest.set_shards(vec![reclaimable, retained], 2);
    manifest.transition_build(LexicalGenerationBuildState::Validated, 3);
    manifest.transition_publish(LexicalGenerationPublishState::Superseded, 4);

    let inventory = manifest.cleanup_inventory();
    assert_eq!(
        inventory.disposition,
        LexicalCleanupDisposition::SupersededReclaimable
    );
    assert_eq!(inventory.artifact_bytes, 10_240);
    assert_eq!(inventory.reclaimable_bytes, 8192);
    assert_eq!(inventory.retained_bytes, 2048);
    assert_eq!(
        inventory.retain_until_ms,
        Some(4),
        "reclaimable superseded generations should expose the retention-window boundary"
    );
    assert!(
        inventory.retention_reason.contains("superseded generation"),
        "superseded retention classification should explain why it is reclaimable"
    );
    assert_eq!(
        inventory.shards[0].disposition,
        LexicalCleanupDisposition::SupersededReclaimable
    );
    assert_eq!(
        inventory.shards[1].disposition,
        LexicalCleanupDisposition::PinnedRetained
    );
}

#[test]
fn cleanup_inventory_keeps_quarantined_artifacts_for_inspection() {
    let mut manifest =
        LexicalGenerationManifest::new_scratch("gen-quarantined", "attempt-1", "fp", 1);
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
        Some("manifest checksum mismatch".into()),
    ));
    manifest.transition_publish(LexicalGenerationPublishState::Quarantined, 4);

    let inventory = manifest.cleanup_inventory();
    assert_eq!(
        inventory.disposition,
        LexicalCleanupDisposition::QuarantinedRetained
    );
    assert_eq!(inventory.reclaimable_bytes, 0);
    assert_eq!(inventory.retained_bytes, 4096);
    assert_eq!(
        inventory.retain_until_ms, None,
        "quarantined generations are retained indefinitely until inspection"
    );
    assert!(
        inventory.retention_reason.contains("operator inspection"),
        "quarantined retention classification should explain the inspection hold"
    );
    assert_eq!(
        inventory.shards[0].reason,
        "manifest checksum mismatch".to_string()
    );
}

#[test]
fn cleanup_inventory_preserves_active_merge_debt() {
    let mut manifest =
        LexicalGenerationManifest::new_scratch("gen-debt-active", "attempt-1", "fp", 1);
    let mut shard = test_shard(
        "shard-pending",
        0,
        LexicalShardLifecycleState::Published,
        1024,
    );
    shard.pinned = false;
    shard.reclaimable = true;
    manifest.set_shards(vec![shard], 2);
    manifest.transition_build(LexicalGenerationBuildState::Validated, 3);
    manifest.transition_publish(LexicalGenerationPublishState::Superseded, 4);
    manifest.record_merge_debt(1, 2048, "background merge still running", 5);

    let inventory = manifest.cleanup_inventory();
    assert_eq!(inventory.disposition, LexicalCleanupDisposition::ActiveWork);
    assert_eq!(inventory.artifact_bytes, 3072);
    assert_eq!(inventory.reclaimable_bytes, 0);
    assert_eq!(inventory.retained_bytes, 3072);
    assert!(inventory.reason.contains("active"));

    let plan = LexicalCleanupDryRunPlan::from_manifests([&manifest]);
    assert_eq!(plan.total_retained_bytes, 3072);
    assert_eq!(plan.protected_retained_bytes, 3072);
    assert_eq!(
        plan.protected_generation_ids,
        vec!["gen-debt-active".to_string()]
    );

    let gate = plan.apply_gate_with_fingerprint(true, Some(&plan.approval_fingerprint));
    assert_eq!(gate.protected_retained_bytes, 3072);
    assert_eq!(
        gate.protected_generation_ids,
        vec!["gen-debt-active".to_string()]
    );
}

#[test]
fn cleanup_dry_run_plan_summarizes_reclaim_retain_and_quarantine_buckets() {
    let mut current = LexicalGenerationManifest::new_scratch("gen-current", "attempt-1", "fp", 1);
    let mut current_shard =
        test_shard("shard-live", 0, LexicalShardLifecycleState::Published, 4096);
    current_shard.pinned = true;
    current_shard.reclaimable = false;
    current.set_shards(vec![current_shard], 2);
    current.transition_build(LexicalGenerationBuildState::Validated, 3);
    current.transition_publish(LexicalGenerationPublishState::Published, 4);

    let mut superseded = LexicalGenerationManifest::new_scratch("gen-old", "attempt-2", "fp", 10);
    let mut reclaimable = test_shard(
        "shard-old-a",
        0,
        LexicalShardLifecycleState::Published,
        8192,
    );
    reclaimable.pinned = false;
    reclaimable.reclaimable = true;
    let mut retained = test_shard(
        "shard-old-b",
        1,
        LexicalShardLifecycleState::Published,
        1024,
    );
    retained.pinned = true;
    retained.reclaimable = false;
    superseded.set_shards(vec![reclaimable, retained], 11);
    superseded.transition_build(LexicalGenerationBuildState::Validated, 12);
    superseded.transition_publish(LexicalGenerationPublishState::Superseded, 13);

    let mut quarantined =
        LexicalGenerationManifest::new_scratch("gen-quarantined", "attempt-3", "fp", 20);
    let quarantined_shard = test_shard(
        "shard-bad",
        0,
        LexicalShardLifecycleState::Quarantined,
        2048,
    );
    quarantined.set_shards(vec![quarantined_shard], 21);
    assert!(quarantined.transition_shard(
        "shard-bad",
        LexicalShardLifecycleState::Quarantined,
        22,
        Some("checksum mismatch".into()),
    ));
    quarantined.transition_publish(LexicalGenerationPublishState::Quarantined, 23);

    let plan = LexicalCleanupDryRunPlan::from_manifests([&current, &superseded, &quarantined]);

    assert!(plan.dry_run);
    assert!(plan.has_reclaimable_artifacts());
    assert_eq!(plan.generation_count, 3);
    assert_eq!(plan.total_artifact_bytes, 15_360);
    assert_eq!(plan.total_reclaimable_bytes, 8192);
    assert_eq!(plan.total_retained_bytes, 7168);
    assert_eq!(plan.protected_retained_bytes, 7168);
    assert_eq!(plan.reclaimable_generation_ids, vec!["gen-old"]);
    assert_eq!(
        plan.fully_retained_generation_ids,
        vec!["gen-current", "gen-quarantined"]
    );
    assert_eq!(
        plan.protected_generation_ids,
        vec!["gen-current", "gen-old", "gen-quarantined"]
    );
    assert_eq!(plan.quarantined_generation_ids, vec!["gen-quarantined"]);
    assert_eq!(
        plan.inspection_required_generation_ids(),
        vec!["gen-quarantined".to_string()]
    );
    assert_eq!(plan.inspection_required_count, 1);
    assert_eq!(plan.inspection_required_retained_bytes, 2048);
    assert_eq!(plan.inspection_required_retained_bytes(), 2048);
    assert_eq!(
        plan.inspection_items,
        vec![LexicalCleanupInspectionItem {
            generation_id: "gen-quarantined".to_string(),
            shard_id: Some("shard-bad".to_string()),
            disposition: LexicalCleanupDisposition::QuarantinedRetained,
            reason: "checksum mismatch".to_string(),
            retained_bytes: 2048,
        }]
    );
    assert_eq!(
        plan.disposition_counts
            .get(&LexicalCleanupDisposition::CurrentPublished),
        Some(&1)
    );
    assert_eq!(
        plan.disposition_counts
            .get(&LexicalCleanupDisposition::SupersededReclaimable),
        Some(&1)
    );
    assert_eq!(
        plan.disposition_counts
            .get(&LexicalCleanupDisposition::QuarantinedRetained),
        Some(&1)
    );
    let current_generation_summary = plan
        .generation_disposition_summaries
        .get(&LexicalCleanupDisposition::CurrentPublished)
        .expect("current published generation summary");
    assert_eq!(current_generation_summary.generation_count, 1);
    assert_eq!(current_generation_summary.artifact_bytes, 4096);
    assert_eq!(current_generation_summary.reclaimable_bytes, 0);
    assert_eq!(current_generation_summary.retained_bytes, 4096);

    let superseded_generation_summary = plan
        .generation_disposition_summaries
        .get(&LexicalCleanupDisposition::SupersededReclaimable)
        .expect("superseded reclaimable generation summary");
    assert_eq!(superseded_generation_summary.generation_count, 1);
    assert_eq!(superseded_generation_summary.artifact_bytes, 9216);
    assert_eq!(superseded_generation_summary.reclaimable_bytes, 8192);
    assert_eq!(superseded_generation_summary.retained_bytes, 1024);

    let quarantined_generation_summary = plan
        .generation_disposition_summaries
        .get(&LexicalCleanupDisposition::QuarantinedRetained)
        .expect("quarantined generation summary");
    assert_eq!(quarantined_generation_summary.generation_count, 1);
    assert_eq!(quarantined_generation_summary.artifact_bytes, 2048);
    assert_eq!(quarantined_generation_summary.reclaimable_bytes, 0);
    assert_eq!(quarantined_generation_summary.retained_bytes, 2048);

    let reclaimable_summary = plan
        .shard_disposition_summaries
        .get(&LexicalCleanupDisposition::SupersededReclaimable)
        .expect("superseded reclaimable shard summary");
    assert_eq!(reclaimable_summary.shard_count, 1);
    assert_eq!(reclaimable_summary.artifact_bytes, 8192);
    assert_eq!(reclaimable_summary.reclaimable_bytes, 8192);
    assert_eq!(reclaimable_summary.retained_bytes, 0);

    let pinned_summary = plan
        .shard_disposition_summaries
        .get(&LexicalCleanupDisposition::PinnedRetained)
        .expect("pinned retained shard summary");
    assert_eq!(pinned_summary.shard_count, 1);
    assert_eq!(pinned_summary.artifact_bytes, 1024);
    assert_eq!(pinned_summary.reclaimable_bytes, 0);
    assert_eq!(pinned_summary.retained_bytes, 1024);

    let json = serde_json::to_value(&plan).expect("serialize cleanup dry-run plan");
    assert_eq!(json["protected_retained_bytes"], 7168);
    assert_eq!(json["protected_generation_ids"][0], "gen-current");
    assert_eq!(json["protected_generation_ids"][1], "gen-old");
    assert_eq!(json["protected_generation_ids"][2], "gen-quarantined");
    assert_eq!(
        json["generation_disposition_summaries"]["current_published"]["retained_bytes"],
        4096
    );
    assert_eq!(
        json["generation_disposition_summaries"]["superseded_reclaimable"]["generation_count"],
        1
    );
    assert_eq!(
        json["generation_disposition_summaries"]["superseded_reclaimable"]["reclaimable_bytes"],
        8192
    );
    assert_eq!(
        json["generation_disposition_summaries"]["quarantined_retained"]["retained_bytes"],
        2048
    );
    assert_eq!(
        json["shard_disposition_summaries"]["superseded_reclaimable"]["reclaimable_bytes"],
        8192
    );
    assert_eq!(
        json["shard_disposition_summaries"]["pinned_retained"]["retained_bytes"],
        1024
    );
    assert_eq!(
        json["inspection_items"][0]["generation_id"],
        "gen-quarantined"
    );
    assert_eq!(
        json["inspection_required_generation_ids"][0],
        "gen-quarantined"
    );
    assert_eq!(json["inspection_required_count"], 1);
    assert_eq!(json["inspection_required_retained_bytes"], 2048);
    assert_eq!(json["inspection_items"][0]["shard_id"], "shard-bad");
    assert_eq!(
        json["inspection_items"][0]["disposition"],
        "quarantined_retained"
    );
    assert_eq!(json["inspection_items"][0]["retained_bytes"], 2048);
    assert_eq!(plan.inventories.len(), 3);
}

#[test]
fn cleanup_dry_run_plan_lists_only_reclaimable_shard_candidates() {
    let mut current = LexicalGenerationManifest::new_scratch("gen-current", "attempt-1", "fp", 1);
    let mut current_shard =
        test_shard("shard-live", 0, LexicalShardLifecycleState::Published, 4096);
    current_shard.pinned = true;
    current_shard.reclaimable = false;
    current.set_shards(vec![current_shard], 2);
    current.transition_build(LexicalGenerationBuildState::Validated, 3);
    current.transition_publish(LexicalGenerationPublishState::Published, 4);

    let mut superseded = LexicalGenerationManifest::new_scratch("gen-old", "attempt-2", "fp", 10);
    let mut old_a = test_shard(
        "shard-old-a",
        0,
        LexicalShardLifecycleState::Published,
        8192,
    );
    old_a.pinned = false;
    old_a.reclaimable = true;
    let mut old_b = test_shard(
        "shard-old-b",
        1,
        LexicalShardLifecycleState::Published,
        2048,
    );
    old_b.pinned = true;
    old_b.reclaimable = false;
    superseded.set_shards(vec![old_a, old_b], 11);
    superseded.transition_build(LexicalGenerationBuildState::Validated, 12);
    superseded.transition_publish(LexicalGenerationPublishState::Superseded, 13);

    let mut failed = LexicalGenerationManifest::new_scratch("gen-failed", "attempt-3", "fp", 20);
    let mut failed_shard = test_shard(
        "shard-failed",
        0,
        LexicalShardLifecycleState::Abandoned,
        1024,
    );
    failed_shard.reclaimable = true;
    failed.set_shards(vec![failed_shard], 21);
    assert!(failed.transition_shard(
        "shard-failed",
        LexicalShardLifecycleState::Abandoned,
        22,
        Some("source changed before publish".into()),
    ));
    failed.transition_build(LexicalGenerationBuildState::Failed, 23);

    let mut quarantined =
        LexicalGenerationManifest::new_scratch("gen-quarantined", "attempt-4", "fp", 30);
    let quarantined_shard =
        test_shard("shard-bad", 0, LexicalShardLifecycleState::Quarantined, 512);
    quarantined.set_shards(vec![quarantined_shard], 31);
    assert!(quarantined.transition_shard(
        "shard-bad",
        LexicalShardLifecycleState::Quarantined,
        32,
        Some("checksum mismatch".into()),
    ));
    quarantined.transition_publish(LexicalGenerationPublishState::Quarantined, 33);

    let plan =
        LexicalCleanupDryRunPlan::from_manifests([&current, &superseded, &failed, &quarantined]);
    let candidates = plan.reclaim_candidates();

    assert_eq!(plan.reclaim_candidates, candidates);
    assert_eq!(
        candidates,
        vec![
            LexicalCleanupReclaimCandidate {
                generation_id: "gen-old".to_string(),
                shard_id: "shard-old-a".to_string(),
                disposition: LexicalCleanupDisposition::SupersededReclaimable,
                reason: "superseded shard is unpinned and safe to reclaim after dry-run approval"
                    .to_string(),
                reclaimable_bytes: 8192,
            },
            LexicalCleanupReclaimCandidate {
                generation_id: "gen-failed".to_string(),
                shard_id: "shard-failed".to_string(),
                disposition: LexicalCleanupDisposition::FailedReclaimable,
                reason: "source changed before publish".to_string(),
                reclaimable_bytes: 1024,
            },
        ]
    );
    assert_eq!(plan.total_reclaimable_bytes, 9216);
    assert_eq!(plan.total_retained_bytes, 6656);
    assert_eq!(plan.protected_retained_bytes, 6656);
    assert_eq!(
        plan.protected_generation_ids,
        vec!["gen-current", "gen-old", "gen-quarantined"]
    );

    let json = serde_json::to_value(&plan).expect("serialize cleanup dry-run plan");
    assert_eq!(json["protected_retained_bytes"], 6656);
    assert_eq!(json["reclaim_candidates"][0]["generation_id"], "gen-old");
    assert_eq!(json["reclaim_candidates"][0]["shard_id"], "shard-old-a");
    assert_eq!(
        json["reclaim_candidates"][0]["disposition"],
        "superseded_reclaimable"
    );
    assert_eq!(json["reclaim_candidates"][0]["reclaimable_bytes"], 8192);
    assert_eq!(json["reclaim_candidates"][1]["generation_id"], "gen-failed");
    assert_eq!(
        json["reclaim_candidates"][1]["disposition"],
        "failed_reclaimable"
    );
    assert_eq!(
        json["reclaim_candidates"]
            .as_array()
            .expect("reclaim_candidates must serialize as an array")
            .len(),
        2
    );
}

#[test]
fn cleanup_apply_gate_requires_approval_and_blocks_active_work() {
    let mut superseded = LexicalGenerationManifest::new_scratch("gen-old", "attempt-1", "fp", 1);
    let mut reclaimable_shard =
        test_shard("shard-old", 0, LexicalShardLifecycleState::Published, 4096);
    reclaimable_shard.pinned = false;
    reclaimable_shard.reclaimable = true;
    superseded.set_shards(vec![reclaimable_shard], 2);
    superseded.transition_build(LexicalGenerationBuildState::Validated, 3);
    superseded.transition_publish(LexicalGenerationPublishState::Superseded, 4);

    let mut active = LexicalGenerationManifest::new_scratch("gen-active", "attempt-2", "fp", 10);
    active.set_shards(
        vec![test_shard(
            "shard-active",
            0,
            LexicalShardLifecycleState::Building,
            2048,
        )],
        11,
    );
    active.transition_build(LexicalGenerationBuildState::Building, 12);

    let mut quarantined =
        LexicalGenerationManifest::new_scratch("gen-quarantined", "attempt-3", "fp", 20);
    quarantined.set_shards(
        vec![test_shard(
            "shard-bad",
            0,
            LexicalShardLifecycleState::Quarantined,
            512,
        )],
        21,
    );
    assert!(quarantined.transition_shard(
        "shard-bad",
        LexicalShardLifecycleState::Quarantined,
        22,
        Some("checksum mismatch".into()),
    ));
    quarantined.transition_publish(LexicalGenerationPublishState::Quarantined, 23);

    let plan = LexicalCleanupDryRunPlan::from_manifests([&superseded, &active, &quarantined]);

    let blocked = plan.apply_gate(false);
    assert!(!blocked.apply_allowed);
    assert!(blocked.dry_run);
    assert!(!blocked.explicit_operator_approval);
    assert_eq!(
        blocked.approval_fingerprint_status,
        LexicalCleanupApprovalFingerprintStatus::NotRequested
    );
    assert_eq!(blocked.generation_count, 3);
    assert_eq!(blocked.total_artifact_bytes, 6656);
    assert_eq!(blocked.total_retained_bytes, 2560);
    assert_eq!(
        blocked
            .disposition_counts
            .get(&LexicalCleanupDisposition::SupersededReclaimable),
        Some(&1)
    );
    assert_eq!(
        blocked
            .generation_disposition_summaries
            .get(&LexicalCleanupDisposition::ActiveWork)
            .map(|summary| summary.retained_bytes),
        Some(2048)
    );
    assert_eq!(
        blocked
            .shard_disposition_summaries
            .get(&LexicalCleanupDisposition::QuarantinedRetained)
            .map(|summary| summary.retained_bytes),
        Some(512)
    );
    assert_eq!(
        blocked.blocker_codes,
        vec![
            LexicalCleanupApplyBlocker::OperatorApprovalRequired,
            LexicalCleanupApplyBlocker::ActiveGenerationWork,
        ]
    );
    assert_eq!(blocked.active_generation_ids, vec!["gen-active"]);
    assert_eq!(
        blocked.reclaimable_generation_ids,
        vec!["gen-old".to_string()]
    );
    assert_eq!(
        blocked.fully_retained_generation_ids,
        vec!["gen-active".to_string(), "gen-quarantined".to_string()]
    );
    assert_eq!(
        blocked.quarantined_generation_ids,
        vec!["gen-quarantined".to_string()]
    );
    assert_eq!(blocked.candidate_count, 1);
    assert_eq!(blocked.reclaimable_bytes, 4096);
    assert_eq!(
        blocked.candidate_previews,
        vec![LexicalCleanupReclaimCandidate {
            generation_id: "gen-old".to_string(),
            shard_id: "shard-old".to_string(),
            disposition: LexicalCleanupDisposition::SupersededReclaimable,
            reason: "superseded shard is unpinned and safe to reclaim after dry-run approval"
                .to_string(),
            reclaimable_bytes: 4096,
        }]
    );
    assert_eq!(
        blocked.inspection_required_generation_ids,
        vec!["gen-quarantined".to_string()]
    );
    assert_eq!(
        blocked.inspection_previews,
        vec![LexicalCleanupInspectionItem {
            generation_id: "gen-quarantined".to_string(),
            shard_id: Some("shard-bad".to_string()),
            disposition: LexicalCleanupDisposition::QuarantinedRetained,
            reason: "checksum mismatch".to_string(),
            retained_bytes: 512,
        }]
    );
    assert_eq!(blocked.inspection_required_count, 1);
    assert_eq!(blocked.inspection_required_retained_bytes, 512);
    assert_eq!(
        blocked.protected_generation_ids,
        vec!["gen-active".to_string(), "gen-quarantined".to_string()]
    );
    assert_eq!(blocked.protected_retained_bytes, 2560);
    assert!(
        blocked
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("explicit operator approval")),
        "missing approval blocker: {:?}",
        blocked.blocked_reasons
    );
    assert!(
        blocked
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("gen-active")),
        "missing active-work blocker: {:?}",
        blocked.blocked_reasons
    );

    let active_still_blocks = plan.apply_gate(true);
    assert!(!active_still_blocks.apply_allowed);
    assert!(active_still_blocks.explicit_operator_approval);
    assert_eq!(
        active_still_blocks.approval_fingerprint_status,
        LexicalCleanupApprovalFingerprintStatus::Missing
    );
    assert_eq!(
        active_still_blocks.blocker_codes,
        vec![
            LexicalCleanupApplyBlocker::ApprovalFingerprintMissing,
            LexicalCleanupApplyBlocker::ActiveGenerationWork,
        ]
    );
    assert!(!active_still_blocks.approval_fingerprint_matches);
    assert!(
        active_still_blocks
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("approval fingerprint")),
        "missing fingerprint blocker: {:?}",
        active_still_blocks.blocked_reasons
    );

    let active_fingerprint_still_blocks =
        plan.apply_gate_with_fingerprint(true, Some(&plan.approval_fingerprint));
    assert!(!active_fingerprint_still_blocks.apply_allowed);
    assert_eq!(
        active_fingerprint_still_blocks.approval_fingerprint_status,
        LexicalCleanupApprovalFingerprintStatus::Matched
    );
    assert_eq!(
        active_fingerprint_still_blocks.blocker_codes,
        vec![LexicalCleanupApplyBlocker::ActiveGenerationWork]
    );
    assert!(active_fingerprint_still_blocks.approval_fingerprint_matches);
    assert_eq!(active_fingerprint_still_blocks.blocked_reasons.len(), 1);

    let safe_plan = LexicalCleanupDryRunPlan::from_manifests([&superseded]);
    let allowed =
        safe_plan.apply_gate_with_fingerprint(true, Some(&safe_plan.approval_fingerprint));
    assert!(allowed.apply_allowed);
    assert!(allowed.blocker_codes.is_empty());
    assert!(allowed.active_generation_ids.is_empty());
    assert!(allowed.protected_generation_ids.is_empty());
    assert_eq!(
        allowed.reclaimable_generation_ids,
        vec!["gen-old".to_string()]
    );
    assert!(allowed.fully_retained_generation_ids.is_empty());
    assert!(allowed.quarantined_generation_ids.is_empty());
    assert_eq!(allowed.protected_retained_bytes, 0);
    assert_eq!(allowed.inspection_required_count, 0);
    assert_eq!(allowed.inspection_required_retained_bytes, 0);
    assert!(allowed.blocked_reasons.is_empty());
    assert_eq!(
        allowed.approval_fingerprint_status,
        LexicalCleanupApprovalFingerprintStatus::Matched
    );
    assert!(allowed.approval_fingerprint_matches);
    assert_eq!(
        allowed.provided_approval_fingerprint.as_deref(),
        Some(safe_plan.approval_fingerprint.as_str())
    );
    assert_eq!(allowed.generation_count, 1);
    assert_eq!(allowed.total_artifact_bytes, 4096);
    assert_eq!(allowed.total_retained_bytes, 0);
    assert_eq!(
        allowed
            .disposition_counts
            .get(&LexicalCleanupDisposition::SupersededReclaimable),
        Some(&1)
    );
    assert_eq!(allowed.candidate_count, 1);
    assert_eq!(allowed.reclaimable_bytes, 4096);
    let allowed_json =
        serde_json::to_value(&allowed).expect("serialize cleanup apply gate preview");
    assert_eq!(
        allowed_json["provided_approval_fingerprint"],
        safe_plan.approval_fingerprint
    );
    assert_eq!(allowed_json["approval_fingerprint_matches"], true);
    assert_eq!(allowed_json["approval_fingerprint_status"], "matched");
    assert_eq!(allowed_json["blocker_codes"], serde_json::json!([]));
    assert_eq!(allowed_json["active_generation_ids"], serde_json::json!([]));
    assert_eq!(
        allowed_json["reclaimable_generation_ids"],
        serde_json::json!(["gen-old"])
    );
    assert_eq!(
        allowed_json["fully_retained_generation_ids"],
        serde_json::json!([])
    );
    assert_eq!(
        allowed_json["quarantined_generation_ids"],
        serde_json::json!([])
    );
    assert_eq!(allowed_json["generation_count"], 1);
    assert_eq!(allowed_json["total_artifact_bytes"], 4096);
    assert_eq!(allowed_json["total_retained_bytes"], 0);
    assert_eq!(
        allowed_json["disposition_counts"]["superseded_reclaimable"],
        1
    );
    assert_eq!(
        allowed_json["generation_disposition_summaries"]["superseded_reclaimable"]["reclaimable_bytes"],
        4096
    );
    assert_eq!(
        allowed_json["shard_disposition_summaries"]["superseded_reclaimable"]["shard_count"],
        1
    );
    assert_eq!(
        allowed_json["protected_generation_ids"],
        serde_json::json!([])
    );
    assert_eq!(allowed_json["protected_retained_bytes"], 0);
    assert_eq!(allowed_json["inspection_required_count"], 0);
    assert_eq!(allowed_json["inspection_required_retained_bytes"], 0);
    assert_eq!(allowed_json["inspection_previews"], serde_json::json!([]));
    assert_eq!(
        allowed_json["candidate_previews"][0]["generation_id"],
        "gen-old"
    );
    assert_eq!(
        allowed_json["candidate_previews"][0]["shard_id"],
        "shard-old"
    );
    assert_eq!(
        allowed_json["candidate_previews"][0]["reclaimable_bytes"],
        4096
    );

    let stale_fingerprint = safe_plan.apply_gate_with_fingerprint(true, Some("cleanup-v1-stale"));
    assert!(!stale_fingerprint.apply_allowed);
    assert_eq!(
        stale_fingerprint.approval_fingerprint_status,
        LexicalCleanupApprovalFingerprintStatus::Mismatched
    );
    assert_eq!(
        stale_fingerprint.blocker_codes,
        vec![LexicalCleanupApplyBlocker::ApprovalFingerprintMismatched]
    );
    assert!(!stale_fingerprint.approval_fingerprint_matches);
    assert!(
        stale_fingerprint
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("does not match")),
        "missing stale fingerprint blocker: {:?}",
        stale_fingerprint.blocked_reasons
    );

    let empty_plan = LexicalCleanupDryRunPlan::from_manifests([&quarantined]);
    let no_candidates =
        empty_plan.apply_gate_with_fingerprint(true, Some(&empty_plan.approval_fingerprint));
    assert!(!no_candidates.apply_allowed);
    assert_eq!(
        no_candidates.blocker_codes,
        vec![LexicalCleanupApplyBlocker::NoReclaimableCandidates]
    );
    assert_eq!(no_candidates.generation_count, 1);
    assert_eq!(no_candidates.total_artifact_bytes, 512);
    assert_eq!(no_candidates.total_retained_bytes, 512);
    assert!(no_candidates.reclaimable_generation_ids.is_empty());
    assert_eq!(
        no_candidates.fully_retained_generation_ids,
        vec!["gen-quarantined".to_string()]
    );
    assert_eq!(
        no_candidates.quarantined_generation_ids,
        vec!["gen-quarantined".to_string()]
    );
    assert_eq!(
        no_candidates.protected_generation_ids,
        vec!["gen-quarantined".to_string()]
    );
    assert_eq!(no_candidates.protected_retained_bytes, 512);
    assert_eq!(no_candidates.inspection_required_count, 1);
    assert_eq!(no_candidates.inspection_required_retained_bytes, 512);
    let no_candidates_json =
        serde_json::to_value(&no_candidates).expect("serialize no-candidate apply gate");
    assert_eq!(
        no_candidates_json["inspection_previews"][0]["generation_id"],
        "gen-quarantined"
    );
    assert_eq!(
        no_candidates_json["inspection_previews"][0]["retained_bytes"],
        512
    );
}

#[test]
fn cleanup_apply_gate_deserializes_legacy_payload_without_lifecycle_summaries() {
    let legacy = serde_json::json!({
        "apply_allowed": false,
        "dry_run": true,
        "explicit_operator_approval": false,
        "candidate_count": 0,
        "reclaimable_bytes": 0,
        "blocked_reasons": []
    });

    let gate: LexicalCleanupApplyGate =
        serde_json::from_value(legacy).expect("legacy cleanup apply gate should deserialize");
    assert_eq!(
        gate.approval_fingerprint_status,
        LexicalCleanupApprovalFingerprintStatus::NotRequested
    );
    assert!(!gate.approval_fingerprint_matches);
    assert!(gate.active_generation_ids.is_empty());
    assert!(gate.protected_generation_ids.is_empty());
    assert_eq!(gate.protected_retained_bytes, 0);
    assert!(gate.inspection_previews.is_empty());
    assert_eq!(gate.inspection_required_count, 0);
    assert_eq!(gate.inspection_required_retained_bytes, 0);
    assert!(gate.inspection_required_generation_ids.is_empty());
}

#[test]
fn cleanup_dry_run_plan_fingerprints_approval_surface() -> Result<(), serde_json::Error> {
    let mut superseded = LexicalGenerationManifest::new_scratch("gen-old", "attempt-1", "fp", 1);
    let mut reclaimable_shard =
        test_shard("shard-old", 0, LexicalShardLifecycleState::Published, 4096);
    reclaimable_shard.pinned = false;
    reclaimable_shard.reclaimable = true;
    superseded.set_shards(vec![reclaimable_shard], 2);
    superseded.transition_build(LexicalGenerationBuildState::Validated, 3);
    superseded.transition_publish(LexicalGenerationPublishState::Superseded, 4);

    let mut larger = LexicalGenerationManifest::new_scratch("gen-old", "attempt-2", "fp", 10);
    let mut larger_shard = test_shard("shard-old", 0, LexicalShardLifecycleState::Published, 8192);
    larger_shard.pinned = false;
    larger_shard.reclaimable = true;
    larger.set_shards(vec![larger_shard], 11);
    larger.transition_build(LexicalGenerationBuildState::Validated, 12);
    larger.transition_publish(LexicalGenerationPublishState::Superseded, 13);

    let mut quarantined =
        LexicalGenerationManifest::new_scratch("gen-quarantined", "attempt-3", "fp", 20);
    quarantined.set_shards(
        vec![test_shard(
            "shard-bad",
            0,
            LexicalShardLifecycleState::Quarantined,
            512,
        )],
        21,
    );
    assert!(quarantined.transition_shard(
        "shard-bad",
        LexicalShardLifecycleState::Quarantined,
        22,
        Some("checksum mismatch".into()),
    ));
    quarantined.transition_publish(LexicalGenerationPublishState::Quarantined, 23);

    let plan = LexicalCleanupDryRunPlan::from_manifests([&superseded, &quarantined]);
    let changed_plan = LexicalCleanupDryRunPlan::from_manifests([&larger, &quarantined]);

    assert!(plan.approval_fingerprint.starts_with("cleanup-v1-"));
    assert_eq!(plan.approval_fingerprint.len(), "cleanup-v1-".len() + 64);
    assert_ne!(
        plan.approval_fingerprint, changed_plan.approval_fingerprint,
        "approval fingerprint must change when reclaimable candidate bytes change"
    );

    let mut current =
        LexicalGenerationManifest::new_scratch("gen-current", "attempt-current", "fp", 30);
    current.set_shards(
        vec![test_shard(
            "shard-current",
            0,
            LexicalShardLifecycleState::Published,
            100,
        )],
        31,
    );
    current.transition_build(LexicalGenerationBuildState::Validated, 32);
    current.transition_publish(LexicalGenerationPublishState::Published, 33);

    let mut pinned =
        LexicalGenerationManifest::new_scratch("gen-pinned", "attempt-pinned", "fp", 40);
    pinned.set_shards(
        vec![test_shard(
            "shard-pinned",
            0,
            LexicalShardLifecycleState::Published,
            200,
        )],
        41,
    );
    pinned.transition_build(LexicalGenerationBuildState::Validated, 42);

    let mut larger_current =
        LexicalGenerationManifest::new_scratch("gen-current", "attempt-current", "fp", 50);
    larger_current.set_shards(
        vec![test_shard(
            "shard-current",
            0,
            LexicalShardLifecycleState::Published,
            200,
        )],
        51,
    );
    larger_current.transition_build(LexicalGenerationBuildState::Validated, 52);
    larger_current.transition_publish(LexicalGenerationPublishState::Published, 53);

    let mut smaller_pinned =
        LexicalGenerationManifest::new_scratch("gen-pinned", "attempt-pinned", "fp", 60);
    smaller_pinned.set_shards(
        vec![test_shard(
            "shard-pinned",
            0,
            LexicalShardLifecycleState::Published,
            100,
        )],
        61,
    );
    smaller_pinned.transition_build(LexicalGenerationBuildState::Validated, 62);

    let retained_plan = LexicalCleanupDryRunPlan::from_manifests([&current, &pinned]);
    let shifted_retained_plan =
        LexicalCleanupDryRunPlan::from_manifests([&larger_current, &smaller_pinned]);
    assert_eq!(
        retained_plan.total_retained_bytes,
        shifted_retained_plan.total_retained_bytes
    );
    assert_eq!(
        retained_plan.disposition_counts,
        shifted_retained_plan.disposition_counts
    );
    assert_ne!(
        retained_plan.approval_fingerprint, shifted_retained_plan.approval_fingerprint,
        "approval fingerprint must change when retained bytes move between protected disposition summaries"
    );

    let mut duplicate_key_a =
        LexicalGenerationManifest::new_scratch("gen-dup", "attempt-dup", "fp", 70);
    let mut dup_small = test_shard("dup-shard", 0, LexicalShardLifecycleState::Published, 100);
    dup_small.pinned = false;
    dup_small.reclaimable = true;
    let mut dup_large = test_shard("dup-shard", 1, LexicalShardLifecycleState::Published, 200);
    dup_large.pinned = false;
    dup_large.reclaimable = true;
    duplicate_key_a.set_shards(vec![dup_small.clone(), dup_large.clone()], 71);
    duplicate_key_a.transition_build(LexicalGenerationBuildState::Validated, 72);
    duplicate_key_a.transition_publish(LexicalGenerationPublishState::Superseded, 73);

    let mut duplicate_key_b =
        LexicalGenerationManifest::new_scratch("gen-dup", "attempt-dup", "fp", 80);
    duplicate_key_b.set_shards(vec![dup_large, dup_small], 81);
    duplicate_key_b.transition_build(LexicalGenerationBuildState::Validated, 82);
    duplicate_key_b.transition_publish(LexicalGenerationPublishState::Superseded, 83);

    let duplicate_order_plan_a = LexicalCleanupDryRunPlan::from_manifests([&duplicate_key_a]);
    let duplicate_order_plan_b = LexicalCleanupDryRunPlan::from_manifests([&duplicate_key_b]);
    assert_eq!(
        duplicate_order_plan_a.approval_fingerprint, duplicate_order_plan_b.approval_fingerprint,
        "approval fingerprint must sort equal generation/shard/disposition keys by the rest of the hashed candidate payload"
    );

    let gate = plan.apply_gate_with_fingerprint(true, Some(&plan.approval_fingerprint));
    assert_eq!(gate.approval_fingerprint, plan.approval_fingerprint);
    assert_eq!(
        gate.provided_approval_fingerprint.as_deref(),
        Some(plan.approval_fingerprint.as_str())
    );
    assert!(gate.approval_fingerprint_matches);
    let plan_json = serde_json::to_value(&plan)?;
    let gate_json = serde_json::to_value(&gate)?;
    assert_eq!(plan_json["approval_fingerprint"], plan.approval_fingerprint);
    assert_eq!(gate_json["approval_fingerprint"], plan.approval_fingerprint);
    assert_eq!(
        gate_json["provided_approval_fingerprint"],
        plan.approval_fingerprint
    );
    Ok(())
}

#[test]
fn cleanup_dry_run_plan_lists_inspection_items_for_retained_risky_artifacts() {
    let mut quarantined =
        LexicalGenerationManifest::new_scratch("gen-quarantined", "attempt-1", "fp", 1);
    quarantined.set_shards(
        vec![test_shard(
            "shard-bad",
            0,
            LexicalShardLifecycleState::Quarantined,
            512,
        )],
        2,
    );
    assert!(quarantined.transition_shard(
        "shard-bad",
        LexicalShardLifecycleState::Quarantined,
        3,
        Some("checksum mismatch".into()),
    ));
    quarantined.transition_publish(LexicalGenerationPublishState::Quarantined, 4);

    let mut failed =
        LexicalGenerationManifest::new_scratch("gen-failed-retained", "attempt-2", "fp", 10);
    let mut failed_shard = test_shard(
        "shard-failed",
        0,
        LexicalShardLifecycleState::Abandoned,
        256,
    );
    failed_shard.reclaimable = false;
    failed.set_shards(vec![failed_shard], 11);
    assert!(failed.transition_shard(
        "shard-failed",
        LexicalShardLifecycleState::Abandoned,
        12,
        Some("operator retained failed shard for postmortem".into()),
    ));
    failed.shards[0].reclaimable = false;
    failed.transition_build(LexicalGenerationBuildState::Failed, 13);

    let plan = LexicalCleanupDryRunPlan::from_manifests([&quarantined, &failed]);

    assert_eq!(
        plan.inspection_required_generation_ids(),
        vec![
            "gen-quarantined".to_string(),
            "gen-failed-retained".to_string()
        ]
    );
    assert_eq!(
        plan.inspection_items,
        vec![
            LexicalCleanupInspectionItem {
                generation_id: "gen-quarantined".to_string(),
                shard_id: Some("shard-bad".to_string()),
                disposition: LexicalCleanupDisposition::QuarantinedRetained,
                reason: "checksum mismatch".to_string(),
                retained_bytes: 512,
            },
            LexicalCleanupInspectionItem {
                generation_id: "gen-failed-retained".to_string(),
                shard_id: Some("shard-failed".to_string()),
                disposition: LexicalCleanupDisposition::FailedRetained,
                reason: "operator retained failed shard for postmortem".to_string(),
                retained_bytes: 256,
            },
        ]
    );
    assert_eq!(plan.inspection_required_count, 2);
    assert_eq!(plan.inspection_required_retained_bytes, 768);
    assert_eq!(plan.inspection_required_retained_bytes(), 768);

    let json = serde_json::to_value(&plan).expect("serialize cleanup inspection dry-run plan");
    assert_eq!(json["inspection_required_count"], 2);
    assert_eq!(json["inspection_required_retained_bytes"], 768);
    assert_eq!(
        json["inspection_items"][0]["disposition"],
        "quarantined_retained"
    );
    assert_eq!(
        json["inspection_items"][1]["generation_id"],
        "gen-failed-retained"
    );
    assert_eq!(json["inspection_items"][1]["retained_bytes"], 256);
}
