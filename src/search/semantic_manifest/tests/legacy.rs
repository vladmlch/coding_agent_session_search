use super::*;

// ── Manifest load/save round-trip ──────────────────────────────────

#[test]
fn manifest_round_trip_via_disk() {
    let temp = tempfile::tempdir().unwrap();
    let mut manifest = SemanticManifest {
        fast_tier: Some(test_artifact(TierKind::Fast, true)),
        quality_tier: Some(test_artifact(TierKind::Quality, true)),
        hnsw: Some(test_hnsw()),
        checkpoint: Some(test_checkpoint(TierKind::Quality)),
        backlog: BacklogLedger {
            total_conversations: 2000,
            fast_tier_processed: 1000,
            quality_tier_processed: 500,
            db_fingerprint: "fp-1234".to_owned(),
            computed_at_ms: 1_700_000_000_000,
        },
        ..Default::default()
    };

    manifest.save(temp.path()).unwrap();
    let loaded = SemanticManifest::load(temp.path()).unwrap().unwrap();

    assert_eq!(loaded.manifest_version, MANIFEST_FORMAT_VERSION);
    assert!(loaded.fast_tier.is_some());
    assert!(loaded.quality_tier.is_some());
    assert!(loaded.hnsw.is_some());
    assert!(loaded.checkpoint.is_some());
    assert_eq!(loaded.backlog.total_conversations, 2000);
    assert!(loaded.updated_at_ms > 0);
}

#[test]
fn manifest_save_overwrites_existing_file() {
    let temp = tempfile::tempdir().unwrap();
    let mut first = SemanticManifest {
        fast_tier: Some(test_artifact(TierKind::Fast, true)),
        ..Default::default()
    };
    first.save(temp.path()).unwrap();

    let mut second = SemanticManifest {
        quality_tier: Some(test_artifact(TierKind::Quality, true)),
        backlog: BacklogLedger {
            total_conversations: 99,
            fast_tier_processed: 0,
            quality_tier_processed: 99,
            db_fingerprint: "fp-overwrite".to_owned(),
            computed_at_ms: 1_700_000_000_123,
        },
        ..Default::default()
    };
    second.save(temp.path()).unwrap();

    let loaded = SemanticManifest::load(temp.path()).unwrap().unwrap();
    assert!(loaded.fast_tier.is_none());
    assert!(loaded.quality_tier.is_some());
    assert_eq!(loaded.backlog.total_conversations, 99);
}

#[cfg(unix)]
#[test]
fn manifest_replacement_path_entry_exists_detects_dangling_symlink() -> Result<(), String> {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let link_path = SemanticManifest::path(temp.path());
    let manifest_dir = link_path
        .parent()
        .ok_or_else(|| "semantic manifest path should have a parent directory".to_string())?;
    fs::create_dir_all(manifest_dir).map_err(|e| e.to_string())?;
    let missing_target = manifest_dir.join("missing-semantic-manifest.json");

    symlink(&missing_target, &link_path).map_err(|e| e.to_string())?;

    if link_path.exists() {
        return Err("dangling manifest symlink unexpectedly resolved".to_string());
    }
    if !replacement_path_entry_exists(&link_path).map_err(|e| e.to_string())? {
        return Err(format!(
            "semantic manifest replacement entry check missed dangling symlink {}",
            link_path.display()
        ));
    }

    Ok(())
}

#[test]
fn manifest_temp_file_creation_is_exclusive_and_unique() -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let final_path = SemanticManifest::path(temp.path());
    let manifest_dir = final_path
        .parent()
        .ok_or_else(|| "semantic manifest path should have a parent directory".to_string())?;
    fs::create_dir_all(manifest_dir).map_err(|e| e.to_string())?;

    let (first_path, mut first_file) =
        create_unique_manifest_temp_file(&final_path).map_err(|e| e.to_string())?;
    first_file.write_all(b"first").map_err(|e| e.to_string())?;
    let (second_path, mut second_file) =
        create_unique_manifest_temp_file(&final_path).map_err(|e| e.to_string())?;
    second_file
        .write_all(b"second")
        .map_err(|e| e.to_string())?;

    if first_path == second_path {
        return Err("exclusive temp creation reused the same path".to_string());
    }
    if !first_path.exists() {
        return Err(format!(
            "first temp file is missing: {}",
            first_path.display()
        ));
    }
    if !second_path.exists() {
        return Err(format!(
            "second temp file is missing: {}",
            second_path.display()
        ));
    }
    if first_path.parent() != Some(manifest_dir) {
        return Err(format!(
            "first temp path escaped manifest directory: {}",
            first_path.display()
        ));
    }
    if second_path.parent() != Some(manifest_dir) {
        return Err(format!(
            "second temp path escaped manifest directory: {}",
            second_path.display()
        ));
    }

    Ok(())
}

#[test]
fn manifest_load_missing_returns_none() {
    let temp = tempfile::tempdir().unwrap();
    let loaded = SemanticManifest::load(temp.path()).unwrap();
    assert!(loaded.is_none());
}

#[test]
fn manifest_load_or_default_returns_defaults() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = SemanticManifest::load_or_default(temp.path()).unwrap();
    assert_eq!(manifest.manifest_version, MANIFEST_FORMAT_VERSION);
    assert!(manifest.fast_tier.is_none());
    assert!(manifest.quality_tier.is_none());
}

#[test]
fn manifest_load_corrupt_returns_parse_error() {
    let temp = tempfile::tempdir().unwrap();
    let path = SemanticManifest::path(temp.path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, b"not json").unwrap();

    let result = SemanticManifest::load(temp.path());
    assert!(matches!(result, Err(ManifestError::Parse { .. })));
}

#[test]
fn manifest_load_future_version_returns_error() {
    let temp = tempfile::tempdir().unwrap();
    let path = SemanticManifest::path(temp.path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();

    let manifest = SemanticManifest {
        manifest_version: MANIFEST_FORMAT_VERSION + 1,
        ..Default::default()
    };
    let json = serde_json::to_string(&manifest).unwrap();
    fs::write(&path, json).unwrap();

    let result = SemanticManifest::load(temp.path());
    assert!(matches!(
        result,
        Err(ManifestError::UnsupportedVersion { .. })
    ));
}

// ── Tier readiness (table-driven) ──────────────────────────────────

#[test]
fn tier_readiness_cases() {
    let policy = test_policy();
    let db_fp = "fp-1234";
    let model_rev = "abc123";
    let cases: &[TierReadinessCase] = &[
        (
            "ready artifact with matching fingerprint",
            TierKind::Fast,
            true,
            db_fp,
            model_rev,
            no_artifact_mutation,
            ExpectedTierReadiness::Ready,
        ),
        (
            "ready artifact with changed DB fingerprint",
            TierKind::Fast,
            true,
            "different-fp",
            model_rev,
            no_artifact_mutation,
            ExpectedTierReadiness::Stale,
        ),
        (
            "ready artifact with changed model revision",
            TierKind::Quality,
            true,
            db_fp,
            "new-revision",
            no_artifact_mutation,
            ExpectedTierReadiness::Stale,
        ),
        (
            "schema version mismatch",
            TierKind::Quality,
            true,
            db_fp,
            model_rev,
            set_schema_version_to_zero,
            ExpectedTierReadiness::Incompatible,
        ),
        (
            "not yet published artifact",
            TierKind::Fast,
            false,
            db_fp,
            model_rev,
            no_artifact_mutation,
            ExpectedTierReadiness::Building(100),
        ),
    ];

    for (label, tier, ready, current_db_fp, current_model_rev, mutate, expected) in cases {
        let mut artifact = test_artifact(*tier, *ready);
        mutate(&mut artifact);
        assert_tier_readiness(
            artifact.readiness(&policy, current_db_fp, current_model_rev),
            *expected,
            label,
        );
    }
}

// ── Manifest-level readiness ───────────────────────────────────────

#[test]
fn manifest_tier_readiness_missing() {
    let manifest = SemanticManifest::default();
    let policy = test_policy();
    assert_eq!(
        manifest.fast_tier_readiness(&policy, "fp", "rev"),
        TierReadiness::Missing,
    );
    assert_eq!(
        manifest.quality_tier_readiness(&policy, "fp", "rev"),
        TierReadiness::Missing,
    );
}

#[test]
fn manifest_tier_readiness_with_checkpoint() {
    let manifest = SemanticManifest {
        checkpoint: Some(test_checkpoint(TierKind::Quality)),
        ..Default::default()
    };

    let policy = test_policy();
    // Fast tier has no checkpoint → Missing
    assert_eq!(
        manifest.fast_tier_readiness(&policy, "fp-1234", "rev"),
        TierReadiness::Missing,
    );
    // Quality tier has a valid checkpoint → Building
    assert!(matches!(
        manifest.quality_tier_readiness(&policy, "fp-1234", "rev"),
        TierReadiness::Building { progress_pct: 50 },
    ));
}

#[test]
fn manifest_tier_readiness_checkpoint_invalid_db() {
    let manifest = SemanticManifest {
        checkpoint: Some(test_checkpoint(TierKind::Quality)),
        ..Default::default()
    };

    let policy = test_policy();
    // Checkpoint DB doesn't match → Missing (checkpoint invalid)
    assert_eq!(
        manifest.quality_tier_readiness(&policy, "other-fp", "rev"),
        TierReadiness::Missing,
    );
}

// ── Hybrid search check ────────────────────────────────────────────

#[test]
fn can_hybrid_search_requires_usable_fast_tier() {
    let policy = test_policy();
    let db_fp = "fp-1234";
    let rev = "abc123";

    // No fast tier → can't hybrid
    let manifest = SemanticManifest::default();
    assert!(!manifest.can_hybrid_search(&policy, db_fp, rev));

    // Fast tier present → can hybrid
    let manifest = SemanticManifest {
        fast_tier: Some(test_artifact(TierKind::Fast, true)),
        ..Default::default()
    };
    assert!(manifest.can_hybrid_search(&policy, db_fp, rev));
}

// ── Backlog ledger ─────────────────────────────────────────────────

#[test]
fn backlog_remaining_and_pending() {
    let ledger = BacklogLedger {
        total_conversations: 1000,
        fast_tier_processed: 800,
        quality_tier_processed: 300,
        db_fingerprint: "fp".to_owned(),
        computed_at_ms: 0,
    };

    assert_eq!(ledger.fast_tier_remaining(), 200);
    assert_eq!(ledger.quality_tier_remaining(), 700);
    assert!(ledger.has_pending_work());
    assert!(ledger.is_current("fp"));
    assert!(!ledger.is_current("other"));
}

#[test]
fn backlog_no_pending_when_fully_processed() {
    let ledger = BacklogLedger {
        total_conversations: 500,
        fast_tier_processed: 500,
        quality_tier_processed: 500,
        db_fingerprint: "fp".to_owned(),
        computed_at_ms: 0,
    };

    assert_eq!(ledger.fast_tier_remaining(), 0);
    assert_eq!(ledger.quality_tier_remaining(), 0);
    assert!(!ledger.has_pending_work());
}

// ── Build checkpoint ───────────────────────────────────────────────

#[test]
fn checkpoint_progress_and_completion() -> Result<(), String> {
    let cp = test_checkpoint(TierKind::Quality);
    assert_eq!(cp.progress_pct(), 50);
    assert!(!cp.is_complete());
    assert!(cp.is_valid("fp-1234"));
    assert!(!cp.is_valid("other-fp"));

    // Complete checkpoint
    let mut cp = test_checkpoint(TierKind::Quality);
    cp.conversations_processed = 1000;
    assert_eq!(cp.progress_pct(), 99);
    if cp.is_complete() {
        return Err("count parity alone should not complete a checkpoint".to_string());
    }
    cp.cursor_exhausted = true;
    assert_eq!(cp.progress_pct(), 100);
    assert!(cp.is_complete());
    Ok(())
}

#[test]
fn checkpoint_zero_total_gives_zero_pct() {
    let mut cp = test_checkpoint(TierKind::Fast);
    cp.total_conversations = 0;
    cp.conversations_processed = 0;
    assert_eq!(cp.progress_pct(), 0);
}

// ── Publish and clear ──────────────────────────────────────────────

#[test]
fn publish_artifact_clears_matching_checkpoint() {
    let mut manifest = SemanticManifest {
        checkpoint: Some(test_checkpoint(TierKind::Quality)),
        ..Default::default()
    };

    manifest.publish_artifact(test_artifact(TierKind::Quality, true));
    assert!(manifest.checkpoint.is_none());
    assert!(manifest.quality_tier.is_some());
}

#[test]
fn publish_artifact_keeps_non_matching_checkpoint() {
    let mut manifest = SemanticManifest {
        checkpoint: Some(test_checkpoint(TierKind::Quality)),
        ..Default::default()
    };

    manifest.publish_artifact(test_artifact(TierKind::Fast, true));
    assert!(manifest.checkpoint.is_some()); // Quality checkpoint survives
    assert!(manifest.fast_tier.is_some());
}

// ── Refresh backlog ────────────────────────────────────────────────

#[test]
fn refresh_backlog_computes_from_ready_artifacts() {
    let mut manifest = SemanticManifest {
        fast_tier: Some(test_artifact(TierKind::Fast, true)),
        quality_tier: Some(test_artifact(TierKind::Quality, true)),
        ..Default::default()
    };

    manifest.refresh_backlog(2000, "fp-1234");
    assert_eq!(manifest.backlog.total_conversations, 2000);
    assert_eq!(manifest.backlog.fast_tier_processed, 250);
    assert_eq!(manifest.backlog.quality_tier_processed, 250);
}

#[test]
fn refresh_backlog_ignores_stale_artifacts() {
    let mut manifest = SemanticManifest {
        fast_tier: Some(test_artifact(TierKind::Fast, true)),
        ..Default::default()
    };

    // DB fingerprint doesn't match → artifact not counted
    manifest.refresh_backlog(2000, "different-fp");
    assert_eq!(manifest.backlog.fast_tier_processed, 0);
}

// ── Invalidation ───────────────────────────────────────────────────

#[test]
fn invalidate_incompatible_removes_schema_mismatch() {
    let mut artifact = test_artifact(TierKind::Quality, true);
    artifact.schema_version = 0; // mismatch
    let mut manifest = SemanticManifest {
        quality_tier: Some(artifact),
        hnsw: Some(test_hnsw()), // depends on quality tier
        ..Default::default()
    };

    let policy = test_policy();
    let count = manifest.invalidate_incompatible(&policy, "abc123");

    assert_eq!(count, 2); // quality + hnsw
    assert!(manifest.quality_tier.is_none());
    assert!(manifest.hnsw.is_none());
}

#[test]
fn invalidate_incompatible_keeps_compatible() {
    let mut manifest = SemanticManifest {
        fast_tier: Some(test_artifact(TierKind::Fast, true)),
        quality_tier: Some(test_artifact(TierKind::Quality, true)),
        ..Default::default()
    };

    let policy = test_policy();
    let count = manifest.invalidate_incompatible(&policy, "abc123");

    assert_eq!(count, 0);
    assert!(manifest.fast_tier.is_some());
    assert!(manifest.quality_tier.is_some());
}

// ── Legacy adoption ────────────────────────────────────────────────

#[test]
fn adopt_legacy_artifact() {
    let mut manifest = SemanticManifest::default();
    let doc_count = 500;
    let conversation_count = 125;
    let index_path = "vector_index/index-fnv1a-384.fsvi";
    let db_fingerprint = "fp-old";
    let size_bytes = 75_000;
    let adopted = manifest.adopt_legacy_artifact(
        TierKind::Fast,
        "fnv1a-384",
        "hash",
        384,
        doc_count,
        conversation_count,
        db_fingerprint,
        index_path,
        size_bytes,
    );

    assert!(adopted);
    let fast = manifest.fast_tier.as_ref().unwrap();
    assert_eq!(fast.embedder_id, "fnv1a-384");
    assert!(fast.ready);
    assert_eq!(fast.schema_version, SEMANTIC_SCHEMA_VERSION);
}

// ── Size accounting ────────────────────────────────────────────────

#[test]
fn total_size_accounts_for_all_artifacts() {
    let manifest = SemanticManifest {
        fast_tier: Some(test_artifact(TierKind::Fast, true)),
        quality_tier: Some(test_artifact(TierKind::Quality, true)),
        hnsw: Some(test_hnsw()),
        ..Default::default()
    };

    assert_eq!(manifest.total_size_bytes(), 150_000 + 150_000 + 50_000);
    assert_eq!(manifest.total_size_mb(), 1); // 350KB rounds up to 1MB
}

#[test]
fn total_size_empty_is_zero() {
    let manifest = SemanticManifest::default();
    assert_eq!(manifest.total_size_bytes(), 0);
    assert_eq!(manifest.total_size_mb(), 0);
}

// ── JSON round-trip ────────────────────────────────────────────────

#[test]
fn manifest_json_round_trip() {
    let manifest = SemanticManifest {
        fast_tier: Some(test_artifact(TierKind::Fast, true)),
        quality_tier: Some(test_artifact(TierKind::Quality, true)),
        hnsw: Some(test_hnsw()),
        checkpoint: Some(test_checkpoint(TierKind::Quality)),
        ..Default::default()
    };

    let json = serde_json::to_string_pretty(&manifest).unwrap();
    let deser: SemanticManifest = serde_json::from_str(&json).unwrap();
    assert_eq!(deser.fast_tier, manifest.fast_tier);
    assert_eq!(deser.quality_tier, manifest.quality_tier);
    assert_eq!(deser.hnsw, manifest.hnsw);
    assert_eq!(deser.checkpoint, manifest.checkpoint);
}

// ── Shard sidecar manifest ─────────────────────────────────────────

#[test]
fn shard_manifest_round_trip_via_sidecar() {
    let temp = tempfile::tempdir().unwrap();
    let mut shards = SemanticShardManifest::default();
    shards.replace_shards_for_generation(
        TierKind::Fast,
        "fnv1a-384",
        "fp-sharded",
        vec![test_shard(1, 2, true), test_shard(0, 2, true)],
    );

    shards.save(temp.path()).unwrap();
    let loaded = SemanticShardManifest::load(temp.path()).unwrap().unwrap();

    assert_eq!(loaded.manifest_version, MANIFEST_FORMAT_VERSION);
    assert_eq!(loaded.shards.len(), 2);
    assert_eq!(loaded.shards[0].shard_index, 0);
    assert_eq!(loaded.shards[1].shard_index, 1);
    assert!(loaded.updated_at_ms > 0);
}

#[test]
fn invalidating_embedder_shards_preserves_records_and_other_generations() {
    let hash_fast = test_shard(0, 1, true);
    let mut hash_quality = test_shard(0, 1, true);
    hash_quality.tier = TierKind::Quality;
    hash_quality.db_fingerprint = "fp-quality-hash".to_string();
    let mut minilm_quality = test_shard(0, 1, true);
    minilm_quality.tier = TierKind::Quality;
    minilm_quality.embedder_id = "minilm-384".to_string();
    minilm_quality.db_fingerprint = "fp-quality-minilm".to_string();
    let mut shards = SemanticShardManifest {
        shards: vec![hash_fast, hash_quality, minilm_quality],
        ..Default::default()
    };

    assert_eq!(shards.mark_shards_stale_for_embedder("fnv1a-384"), 2);
    assert_eq!(shards.shards.len(), 3);
    assert!(
        shards
            .shards
            .iter()
            .filter(|shard| shard.embedder_id == "fnv1a-384")
            .all(|shard| !shard.ready && !shard.ann_ready)
    );
    assert!(
        shards
            .shards
            .iter()
            .find(|shard| shard.embedder_id == "minilm-384")
            .is_some_and(|shard| shard.ready)
    );
}

#[test]
fn shard_summary_requires_every_ready_shard() {
    let mut shards = SemanticShardManifest::default();
    shards.replace_shards_for_generation(
        TierKind::Fast,
        "fnv1a-384",
        "fp-sharded",
        vec![test_shard(0, 3, true), test_shard(2, 3, true)],
    );

    let partial = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
    assert_eq!(partial.shard_count, 3);
    assert_eq!(partial.ready_shards, 2);
    assert!(!partial.complete);

    shards.replace_shards_for_generation(
        TierKind::Fast,
        "fnv1a-384",
        "fp-sharded",
        vec![
            test_shard(0, 3, true),
            test_shard(1, 3, true),
            test_shard(2, 3, true),
        ],
    );

    let complete = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
    assert_eq!(complete.ready_shards, 3);
    assert!(complete.complete);
    assert_eq!(complete.doc_count, 75);
    assert_eq!(complete.total_conversations, 10);
}

#[test]
fn shard_summary_rejects_non_mmap_ready_or_inconsistent_shards() {
    let mut non_mmap = test_shard(0, 1, true);
    non_mmap.mmap_ready = false;
    let mut shards = SemanticShardManifest::default();
    shards.replace_shards_for_generation(TierKind::Fast, "fnv1a-384", "fp-sharded", vec![non_mmap]);

    let non_mmap_summary = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
    assert_eq!(non_mmap_summary.ready_shards, 0);
    assert!(!non_mmap_summary.complete);

    let mut inconsistent = test_shard(1, 3, true);
    inconsistent.ann_ready = true;
    inconsistent.ann_index_path = None;
    inconsistent.ann_size_bytes = 4096;
    shards.replace_shards_for_generation(
        TierKind::Fast,
        "fnv1a-384",
        "fp-sharded",
        vec![test_shard(0, 2, true), inconsistent],
    );

    let inconsistent_summary = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
    assert_eq!(inconsistent_summary.shard_count, 3);
    assert_eq!(inconsistent_summary.ready_shards, 2);
    assert_eq!(inconsistent_summary.ann_ready_shards, 0);
    assert!(!inconsistent_summary.complete);

    shards.replace_shards_for_generation(
        TierKind::Fast,
        "fnv1a-384",
        "fp-sharded",
        vec![
            test_shard(0, 2, true),
            test_shard(1, 2, true),
            test_shard(1, 2, false),
        ],
    );
    let duplicate_summary = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
    assert_eq!(duplicate_summary.shard_count, 2);
    assert_eq!(duplicate_summary.ready_shards, 2);
    assert!(
        !duplicate_summary.complete,
        "duplicate shard indexes must not summarize as a complete generation"
    );

    let mut duplicate_path = test_shard(1, 2, true);
    duplicate_path.index_path = test_shard(0, 2, true).index_path;
    shards.replace_shards_for_generation(
        TierKind::Fast,
        "fnv1a-384",
        "fp-sharded",
        vec![test_shard(0, 2, true), duplicate_path],
    );
    let duplicate_path_summary = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
    assert_eq!(duplicate_path_summary.shard_count, 2);
    assert_eq!(duplicate_path_summary.ready_shards, 2);
    assert!(
        !duplicate_path_summary.complete,
        "duplicate shard index paths must not summarize as a complete generation"
    );

    let mut blank_path = test_shard(0, 1, true);
    blank_path.index_path.clear();
    shards.replace_shards_for_generation(
        TierKind::Fast,
        "fnv1a-384",
        "fp-sharded",
        vec![blank_path],
    );
    let blank_path_summary = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
    assert_eq!(blank_path_summary.shard_count, 1);
    assert_eq!(blank_path_summary.ready_shards, 1);
    assert!(
        !blank_path_summary.complete,
        "blank shard index paths must not summarize as complete"
    );

    for unsafe_path in [
        tempfile::tempdir()
            .unwrap()
            .path()
            .join("outside.fsvi")
            .to_string_lossy()
            .to_string(),
        "vector_index/shards/../outside.fsvi".to_string(),
        "./vector_index/shards/fast/hash.fsvi".to_string(),
        " vector_index/shards/fast/hash.fsvi".to_string(),
    ] {
        let mut unsafe_shard = test_shard(0, 1, true);
        unsafe_shard.index_path = unsafe_path;
        shards.replace_shards_for_generation(
            TierKind::Fast,
            "fnv1a-384",
            "fp-sharded",
            vec![unsafe_shard],
        );
        let unsafe_path_summary = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
        assert_eq!(unsafe_path_summary.shard_count, 1);
        assert_eq!(unsafe_path_summary.ready_shards, 1);
        assert!(
            !unsafe_path_summary.complete,
            "unsafe shard index paths must not summarize as complete"
        );
    }

    let outside_ann_dir = tempfile::tempdir().unwrap();
    for unsafe_ann_path in [
        outside_ann_dir
            .path()
            .join("outside.chsw")
            .to_string_lossy()
            .to_string(),
        "vector_index/shards/../outside.chsw".to_string(),
        "./vector_index/shards/fast/hash.chsw".to_string(),
        " vector_index/shards/fast/hash.chsw".to_string(),
    ] {
        let mut unsafe_ann = test_shard(0, 1, true);
        unsafe_ann.ann_ready = true;
        unsafe_ann.ann_index_path = Some(unsafe_ann_path);
        unsafe_ann.ann_size_bytes = 4096;
        shards.replace_shards_for_generation(
            TierKind::Fast,
            "fnv1a-384",
            "fp-sharded",
            vec![unsafe_ann],
        );
        let unsafe_ann_summary = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
        assert_eq!(unsafe_ann_summary.shard_count, 1);
        assert_eq!(unsafe_ann_summary.ready_shards, 1);
        assert_eq!(unsafe_ann_summary.ann_ready_shards, 0);
        assert!(
            unsafe_ann_summary.complete,
            "unsafe optional ANN paths must not invalidate the vector shard generation"
        );
    }

    let mut duplicate_ann_path = test_shard(1, 2, true);
    duplicate_ann_path.ann_ready = true;
    duplicate_ann_path.ann_index_path =
        Some("vector_index/shards/fast-fnv1a-384/shared-ann.chsw".to_owned());
    duplicate_ann_path.ann_size_bytes = 4096;
    let mut first_ann_path = test_shard(0, 2, true);
    first_ann_path.ann_ready = true;
    first_ann_path.ann_index_path = duplicate_ann_path.ann_index_path.clone();
    first_ann_path.ann_size_bytes = 4096;
    shards.replace_shards_for_generation(
        TierKind::Fast,
        "fnv1a-384",
        "fp-sharded",
        vec![first_ann_path, duplicate_ann_path],
    );
    let duplicate_ann_summary = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
    assert_eq!(duplicate_ann_summary.shard_count, 2);
    assert_eq!(duplicate_ann_summary.ready_shards, 2);
    assert_eq!(duplicate_ann_summary.ann_ready_shards, 1);
    assert!(
        duplicate_ann_summary.complete,
        "duplicate optional ANN paths must not invalidate the vector shard generation"
    );

    shards.replace_shards_for_generation(
        TierKind::Fast,
        "fnv1a-384",
        "fp-sharded",
        vec![test_shard(2, 2, true)],
    );
    let out_of_range_summary = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
    assert_eq!(out_of_range_summary.shard_count, 2);
    assert_eq!(out_of_range_summary.ready_shards, 1);
    assert!(
        !out_of_range_summary.complete,
        "shard indexes outside the declared shard count are malformed"
    );

    let mut mismatched_metadata = test_shard(1, 2, true);
    mismatched_metadata.dimension = 768;
    shards.replace_shards_for_generation(
        TierKind::Fast,
        "fnv1a-384",
        "fp-sharded",
        vec![test_shard(0, 2, true), mismatched_metadata],
    );
    let metadata_summary = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
    assert_eq!(metadata_summary.shard_count, 2);
    assert_eq!(metadata_summary.ready_shards, 2);
    assert!(
        !metadata_summary.complete,
        "complete shard generations require consistent shard metadata"
    );

    let mut stale_schema = test_shard(0, 1, true);
    stale_schema.schema_version = SEMANTIC_SCHEMA_VERSION.saturating_sub(1);
    shards.replace_shards_for_generation(
        TierKind::Fast,
        "fnv1a-384",
        "fp-sharded",
        vec![stale_schema],
    );
    let stale_schema_summary = shards.summary(TierKind::Fast, "fnv1a-384", "fp-sharded");
    assert_eq!(stale_schema_summary.shard_count, 1);
    assert_eq!(stale_schema_summary.ready_shards, 1);
    assert!(
        !stale_schema_summary.complete,
        "stale schema shards must not summarize as complete"
    );
}

#[test]
fn shard_sidecar_does_not_make_main_manifest_ready() {
    let mut shards = SemanticShardManifest::default();
    shards.replace_shards_for_generation(
        TierKind::Fast,
        "fnv1a-384",
        "fp-sharded",
        vec![test_shard(0, 1, true)],
    );
    assert!(
        shards
            .summary(TierKind::Fast, "fnv1a-384", "fp-sharded")
            .complete
    );

    let manifest = SemanticManifest::default();
    let policy = test_policy();
    assert_eq!(
        manifest.fast_tier_readiness(&policy, "fp-sharded", "hash"),
        TierReadiness::Missing,
        "sidecar shards must not publish runtime semantic readiness"
    );
}

#[test]
fn shard_manifest_invalidates_incompatible_shards() {
    let mut bad = test_shard(0, 1, true);
    bad.schema_version = 0;
    let mut shards = SemanticShardManifest {
        shards: vec![bad, test_shard(0, 1, true)],
        ..Default::default()
    };

    let invalidated = shards.invalidate_incompatible(&test_policy(), "hash");

    assert_eq!(invalidated, 1);
    assert_eq!(shards.shards.len(), 1);
    assert_eq!(shards.total_size_bytes(), 4096);
}
