use super::*;

#[test]
fn lexical_states_serialize_as_snake_case() {
    let pairs: &[(LexicalReadinessState, &str)] = &[
        (LexicalReadinessState::Missing, "missing"),
        (LexicalReadinessState::Repairing, "repairing"),
        (
            LexicalReadinessState::StaleButSearchable,
            "stale_but_searchable",
        ),
        (LexicalReadinessState::Ready, "ready"),
        (
            LexicalReadinessState::CorruptQuarantined,
            "corrupt_quarantined",
        ),
    ];
    for (state, expected) in pairs {
        assert_eq!(
            serde_json::to_string(state).unwrap(),
            format!("\"{expected}\"")
        );
    }
}

#[test]
fn semantic_states_serialize_as_snake_case() {
    let pairs: &[(SemanticReadinessState, &str)] = &[
        (SemanticReadinessState::Absent, "absent"),
        (SemanticReadinessState::Backfilling, "backfilling"),
        (SemanticReadinessState::FastTierReady, "fast_tier_ready"),
        (SemanticReadinessState::HybridReady, "hybrid_ready"),
        (SemanticReadinessState::PolicyDisabled, "policy_disabled"),
    ];
    for (state, expected) in pairs {
        assert_eq!(
            serde_json::to_string(state).unwrap(),
            format!("\"{expected}\"")
        );
    }
}

#[test]
fn refinement_levels_serialize_as_snake_case() {
    let pairs: &[(SearchRefinementLevel, &str)] = &[
        (SearchRefinementLevel::LexicalOnly, "lexical_only"),
        (SearchRefinementLevel::FastTierRefined, "fast_tier_refined"),
        (
            SearchRefinementLevel::FullyHybridRefined,
            "fully_hybrid_refined",
        ),
    ];
    for (level, expected) in pairs {
        assert_eq!(
            serde_json::to_string(level).unwrap(),
            format!("\"{expected}\"")
        );
    }
}

#[test]
fn is_searchable_distinguishes_lexical_failure_modes() {
    let cases = [
        (LexicalReadinessState::Missing, false),
        (LexicalReadinessState::CorruptQuarantined, false),
        (LexicalReadinessState::Repairing, true),
        (LexicalReadinessState::StaleButSearchable, true),
        (LexicalReadinessState::Ready, true),
    ];

    for (state, expected) in cases {
        assert_eq!(state.is_searchable(), expected, "{state:?}");
    }
}

#[test]
fn semantic_can_refine_only_when_at_least_fast_tier_ready() {
    let cases = [
        (SemanticReadinessState::Absent, false),
        (SemanticReadinessState::Backfilling, false),
        (SemanticReadinessState::PolicyDisabled, false),
        (SemanticReadinessState::FastTierReady, true),
        (SemanticReadinessState::HybridReady, true),
    ];

    for (state, expected) in cases {
        assert_eq!(state.can_refine(), expected, "{state:?}");
    }
}

#[test]
fn recommended_actions_serialize_as_snake_case() {
    let pairs: &[(RecommendedAction, &str)] = &[
        (RecommendedAction::NothingRequired, "nothing_required"),
        (RecommendedAction::RepairLexicalNow, "repair_lexical_now"),
        (
            RecommendedAction::WaitForLexicalRepair,
            "wait_for_lexical_repair",
        ),
        (
            RecommendedAction::WaitForSemanticCatchUp,
            "wait_for_semantic_catch_up",
        ),
        (
            RecommendedAction::RefreshLexicalSoon,
            "refresh_lexical_soon",
        ),
        (
            RecommendedAction::SemanticDisabledByPolicy,
            "semantic_disabled_by_policy",
        ),
    ];
    for (action, expected) in pairs {
        let expected_json = format!("\"{expected}\"");
        assert!(
            matches!(
                serde_json::to_string(action).as_deref(),
                Ok(actual) if actual == expected_json.as_str()
            ),
            "action should serialize as {expected_json}"
        );
    }
}

#[test]
fn recommended_action_missing_lexical_always_repair_now() {
    for sem in [
        SemanticReadinessState::Absent,
        SemanticReadinessState::Backfilling,
        SemanticReadinessState::FastTierReady,
        SemanticReadinessState::HybridReady,
        SemanticReadinessState::PolicyDisabled,
    ] {
        let snap = ReadinessSnapshot::new(LexicalReadinessState::Missing, sem);
        assert_eq!(
            snap.recommended_action(),
            RecommendedAction::RepairLexicalNow
        );
    }
}

#[test]
fn recommended_action_corrupt_lexical_always_repair_now() {
    let snap = ReadinessSnapshot::new(
        LexicalReadinessState::CorruptQuarantined,
        SemanticReadinessState::HybridReady,
    );
    assert_eq!(
        snap.recommended_action(),
        RecommendedAction::RepairLexicalNow
    );
}

#[test]
fn recommended_action_active_lexical_repair_dominates_semantic_state() {
    for sem in [
        SemanticReadinessState::Absent,
        SemanticReadinessState::Backfilling,
        SemanticReadinessState::FastTierReady,
        SemanticReadinessState::HybridReady,
        SemanticReadinessState::PolicyDisabled,
    ] {
        let snap = ReadinessSnapshot::new(LexicalReadinessState::Repairing, sem);
        assert_eq!(
            snap.recommended_action(),
            RecommendedAction::WaitForLexicalRepair
        );
        assert!(snap.is_searchable());
    }
}

#[test]
fn recommended_action_stale_lexical_requests_refresh() {
    for sem in [
        SemanticReadinessState::Absent,
        SemanticReadinessState::HybridReady,
    ] {
        let snap = ReadinessSnapshot::new(LexicalReadinessState::StaleButSearchable, sem);
        assert_eq!(
            snap.recommended_action(),
            RecommendedAction::RefreshLexicalSoon
        );
    }
}

#[test]
fn recommended_action_ready_plus_hybrid_is_nothing_required() {
    let snap = ReadinessSnapshot::new(
        LexicalReadinessState::Ready,
        SemanticReadinessState::HybridReady,
    );
    assert_eq!(
        snap.recommended_action(),
        RecommendedAction::NothingRequired
    );
}

#[test]
fn recommended_action_ready_plus_policy_disabled_acknowledges_policy() {
    let snap = ReadinessSnapshot::new(
        LexicalReadinessState::Ready,
        SemanticReadinessState::PolicyDisabled,
    );
    assert_eq!(
        snap.recommended_action(),
        RecommendedAction::SemanticDisabledByPolicy
    );
}

#[test]
fn recommended_action_ready_plus_semantic_converging_waits() {
    for sem in [
        SemanticReadinessState::Absent,
        SemanticReadinessState::Backfilling,
    ] {
        let snap = ReadinessSnapshot::new(LexicalReadinessState::Ready, sem);
        assert_eq!(
            snap.recommended_action(),
            RecommendedAction::WaitForSemanticCatchUp
        );
    }
}

#[test]
fn snapshot_with_last_search_refinement_round_trips_through_json() {
    let snap = ReadinessSnapshot::new(
        LexicalReadinessState::Ready,
        SemanticReadinessState::FastTierReady,
    )
    .with_last_search_refinement(SearchRefinementLevel::FastTierRefined);

    let json = serde_json::to_string(&snap).unwrap();
    assert!(json.contains("\"lexical\":\"ready\""));
    assert!(json.contains("\"semantic\":\"fast_tier_ready\""));
    assert!(json.contains("\"last_search_refinement\":\"fast_tier_refined\""));

    let parsed: ReadinessSnapshot = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, snap);
}

#[test]
fn snapshot_defaults_last_search_refinement_to_none() {
    let snap = ReadinessSnapshot::new(
        LexicalReadinessState::Ready,
        SemanticReadinessState::HybridReady,
    );
    assert!(snap.last_search_refinement.is_none());
    let json = serde_json::to_string(&snap).unwrap();
    assert!(json.contains("\"last_search_refinement\":null"));
}

// -----------------------------------------------------------------
// DerivedAssetTruthTable (.1.1)
// -----------------------------------------------------------------

#[test]
fn truth_table_enums_serialize_as_snake_case() {
    let db: &[(CanonicalDbAvailability, &str)] = &[
        (CanonicalDbAvailability::Available, "available"),
        (CanonicalDbAvailability::Missing, "missing"),
        (CanonicalDbAvailability::OpenFailed, "open_failed"),
        (CanonicalDbAvailability::Corrupt, "corrupt"),
        (CanonicalDbAvailability::Unreachable, "unreachable"),
    ];
    for (v, want) in db {
        assert_eq!(serde_json::to_string(v).unwrap(), format!("\"{want}\""));
    }
    let cov: &[(SourceCoverageState, &str)] = &[
        (SourceCoverageState::Complete, "complete"),
        (SourceCoverageState::Partial, "partial"),
        (SourceCoverageState::Unavailable, "unavailable"),
        (SourceCoverageState::Unconfigured, "unconfigured"),
        (SourceCoverageState::Unknown, "unknown"),
    ];
    for (v, want) in cov {
        assert_eq!(serde_json::to_string(v).unwrap(), format!("\"{want}\""));
    }
    let maint: &[(MaintenanceActivity, &str)] = &[
        (MaintenanceActivity::Idle, "idle"),
        (MaintenanceActivity::LexicalRebuild, "lexical_rebuild"),
        (MaintenanceActivity::SemanticBackfill, "semantic_backfill"),
        (MaintenanceActivity::StorageRepair, "storage_repair"),
        (MaintenanceActivity::Unknown, "unknown"),
    ];
    for (v, want) in maint {
        assert_eq!(serde_json::to_string(v).unwrap(), format!("\"{want}\""));
    }
    let risk: &[(ArchiveRiskLevel, &str)] = &[
        (ArchiveRiskLevel::None, "none"),
        (ArchiveRiskLevel::Low, "low"),
        (ArchiveRiskLevel::Elevated, "elevated"),
        (ArchiveRiskLevel::High, "high"),
        (ArchiveRiskLevel::Unknown, "unknown"),
    ];
    for (v, want) in risk {
        assert_eq!(serde_json::to_string(v).unwrap(), format!("\"{want}\""));
    }
    let bin: &[(BinaryCompatibility, &str)] = &[
        (BinaryCompatibility::Current, "current"),
        (BinaryCompatibility::Outdated, "outdated"),
        (BinaryCompatibility::Ahead, "ahead"),
        (BinaryCompatibility::Unknown, "unknown"),
    ];
    for (v, want) in bin {
        assert_eq!(serde_json::to_string(v).unwrap(), format!("\"{want}\""));
    }
}

#[test]
fn safe_next_action_serializes_as_snake_case() {
    let pairs: &[(SafeNextAction, &str)] = &[
        (SafeNextAction::None, "none"),
        (SafeNextAction::IndexFull, "index_full"),
        (SafeNextAction::BackupThenRepair, "backup_then_repair"),
        (SafeNextAction::InspectCanonicalDb, "inspect_canonical_db"),
        (SafeNextAction::RepairLexical, "repair_lexical"),
        (SafeNextAction::RefreshLexical, "refresh_lexical"),
        (SafeNextAction::WaitForMaintenance, "wait_for_maintenance"),
        (SafeNextAction::WaitForSemantic, "wait_for_semantic"),
        (SafeNextAction::ConfigureSources, "configure_sources"),
        (SafeNextAction::ReconnectSource, "reconnect_source"),
        (SafeNextAction::InspectQuarantine, "inspect_quarantine"),
        (
            SafeNextAction::InstallSemanticModel,
            "install_semantic_model",
        ),
        (SafeNextAction::UpgradeBinary, "upgrade_binary"),
        (
            SafeNextAction::RebuildForCurrentBinary,
            "rebuild_for_current_binary",
        ),
        (SafeNextAction::HostUnreachable, "host_unreachable"),
    ];
    for (v, want) in pairs {
        assert_eq!(serde_json::to_string(v).unwrap(), format!("\"{want}\""));
    }
}

/// Look up a fixture by name; panics with a clear message if missing.
fn fixture(name: &str) -> DerivedAssetTruthTable {
    fleet_fixtures()
        .into_iter()
        .find(|(n, _)| *n == name)
        .unwrap_or_else(|| panic!("missing fleet fixture {name}"))
        .1
}

#[test]
fn fleet_fixtures_cover_all_seven_named_states_in_stable_order() {
    let names: Vec<&str> = fleet_fixtures().into_iter().map(|(n, _)| n).collect();
    assert_eq!(
        names,
        vec![
            "local_stale_quarantine",
            "ts1_high_archive_risk",
            "ts2_fast_health_slow_status",
            "csd_missing_lexical_metadata",
            "css_stale_existing_index",
            "mac_mini_max_stale_old_binary",
            "mac_mini_old_unreachable",
        ]
    );
}

#[test]
fn fleet_fixtures_round_trip_through_json() {
    for (name, table) in fleet_fixtures() {
        let json = serde_json::to_string(&table).unwrap();
        let parsed: DerivedAssetTruthTable = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, table, "fixture {name} should round-trip");
    }
}

#[test]
fn local_stale_quarantine_recommends_refresh_over_advisory_quarantine() {
    let t = fixture("local_stale_quarantine");
    assert!(t.is_searchable());
    assert!(t.quarantine.has_exclusions());
    // Stale-refresh dominates the advisory quarantine signal.
    assert_eq!(t.safe_next_command().action, SafeNextAction::RefreshLexical);
}

#[test]
fn ts1_high_archive_risk_is_backup_first() {
    let t = fixture("ts1_high_archive_risk");
    let cmd = t.safe_next_command();
    assert_eq!(cmd.action, SafeNextAction::BackupThenRepair);
    assert!(!cmd.action.is_mutating());
}

#[test]
fn ts2_fast_health_slow_status_waits_for_semantic() {
    let t = fixture("ts2_fast_health_slow_status");
    assert!(t.is_searchable());
    assert_eq!(
        t.safe_next_command().action,
        SafeNextAction::WaitForSemantic
    );
}

#[test]
fn csd_missing_lexical_metadata_repairs_lexical() {
    let t = fixture("csd_missing_lexical_metadata");
    assert!(!t.is_searchable());
    assert!(!t.lexical_metadata.present);
    assert_eq!(t.safe_next_command().action, SafeNextAction::RepairLexical);
}

#[test]
fn css_stale_existing_index_refreshes_lexical() {
    let t = fixture("css_stale_existing_index");
    assert!(t.is_searchable());
    assert_eq!(t.safe_next_command().action, SafeNextAction::RefreshLexical);
}

#[test]
fn mac_mini_max_stale_old_binary_upgrades_binary_first() {
    let t = fixture("mac_mini_max_stale_old_binary");
    // Binary skew is surfaced ahead of the stale-lexical refresh.
    assert_eq!(t.safe_next_command().action, SafeNextAction::UpgradeBinary);
}

#[test]
fn newer_binary_rebuilds_assets_for_current_schema() {
    let t = DerivedAssetTruthTable {
        db: CanonicalDbAvailability::Available,
        source_coverage: SourceCoverageState::Complete,
        scan_watermark_ms: Some(1),
        last_projection_ms: Some(1),
        lexical_metadata: LexicalMetadata {
            present: true,
            schema_hash: Some("schema-previous".to_string()),
            storage_fingerprint: Some("fp-current".to_string()),
            built_at_ms: Some(1),
        },
        readiness: ReadinessSnapshot::new(
            LexicalReadinessState::Ready,
            SemanticReadinessState::HybridReady,
        ),
        quarantine: QuarantineSummary::default(),
        maintenance: MaintenanceActivity::Idle,
        archive_risk: ArchiveRiskLevel::Low,
        binary: BinaryCompatibility::Ahead,
    };
    let cmd = t.safe_next_command();
    assert_eq!(cmd.action, SafeNextAction::RebuildForCurrentBinary);
    assert!(cmd.action.is_mutating());
    assert_eq!(cmd.command.as_deref(), Some("cass index --full"));
}

#[test]
fn mac_mini_old_unreachable_yields_host_unreachable() {
    let t = fixture("mac_mini_old_unreachable");
    assert!(!t.is_searchable());
    let cmd = t.safe_next_command();
    assert_eq!(cmd.action, SafeNextAction::HostUnreachable);
    assert!(cmd.command.is_none());
}

#[test]
fn fresh_install_recommends_index_full() {
    let t = DerivedAssetTruthTable {
        db: CanonicalDbAvailability::Missing,
        source_coverage: SourceCoverageState::Complete,
        scan_watermark_ms: None,
        last_projection_ms: None,
        lexical_metadata: LexicalMetadata::default(),
        readiness: ReadinessSnapshot::new(
            LexicalReadinessState::Missing,
            SemanticReadinessState::Absent,
        ),
        quarantine: QuarantineSummary::default(),
        maintenance: MaintenanceActivity::Idle,
        archive_risk: ArchiveRiskLevel::None,
        binary: BinaryCompatibility::Current,
    };
    assert_eq!(t.safe_next_command().action, SafeNextAction::IndexFull);
}

#[test]
fn corrupt_db_inspects_before_repair_even_under_high_risk() {
    let t = DerivedAssetTruthTable {
        db: CanonicalDbAvailability::Corrupt,
        source_coverage: SourceCoverageState::Complete,
        scan_watermark_ms: None,
        last_projection_ms: None,
        lexical_metadata: LexicalMetadata::default(),
        readiness: ReadinessSnapshot::new(
            LexicalReadinessState::CorruptQuarantined,
            SemanticReadinessState::Absent,
        ),
        quarantine: QuarantineSummary::default(),
        maintenance: MaintenanceActivity::Idle,
        archive_risk: ArchiveRiskLevel::High,
        binary: BinaryCompatibility::Current,
    };
    let cmd = t.safe_next_command();
    // Read-only inspection wins even when archive risk is high.
    assert_eq!(cmd.action, SafeNextAction::InspectCanonicalDb);
    assert!(!cmd.action.is_mutating());
}

#[test]
fn active_lexical_rebuild_waits_instead_of_duplicating() {
    let t = DerivedAssetTruthTable {
        db: CanonicalDbAvailability::Available,
        source_coverage: SourceCoverageState::Complete,
        scan_watermark_ms: None,
        last_projection_ms: None,
        lexical_metadata: LexicalMetadata::default(),
        readiness: ReadinessSnapshot::new(
            LexicalReadinessState::Repairing,
            SemanticReadinessState::Absent,
        ),
        quarantine: QuarantineSummary::default(),
        maintenance: MaintenanceActivity::LexicalRebuild,
        archive_risk: ArchiveRiskLevel::Low,
        binary: BinaryCompatibility::Current,
    };
    assert_eq!(
        t.safe_next_command().action,
        SafeNextAction::WaitForMaintenance
    );
}

#[test]
fn unconfigured_sources_recommend_setup() {
    let t = DerivedAssetTruthTable {
        db: CanonicalDbAvailability::Available,
        source_coverage: SourceCoverageState::Unconfigured,
        scan_watermark_ms: None,
        last_projection_ms: None,
        lexical_metadata: LexicalMetadata::default(),
        readiness: ReadinessSnapshot::new(
            LexicalReadinessState::Missing,
            SemanticReadinessState::Absent,
        ),
        quarantine: QuarantineSummary::default(),
        maintenance: MaintenanceActivity::Idle,
        archive_risk: ArchiveRiskLevel::None,
        binary: BinaryCompatibility::Current,
    };
    assert_eq!(
        t.safe_next_command().action,
        SafeNextAction::ConfigureSources
    );
}

#[test]
fn fully_converged_table_requires_no_action() {
    let t = DerivedAssetTruthTable {
        db: CanonicalDbAvailability::Available,
        source_coverage: SourceCoverageState::Complete,
        scan_watermark_ms: Some(1),
        last_projection_ms: Some(1),
        lexical_metadata: LexicalMetadata {
            present: true,
            schema_hash: Some("schema-current".to_string()),
            storage_fingerprint: Some("fp-current".to_string()),
            built_at_ms: Some(1),
        },
        readiness: ReadinessSnapshot::new(
            LexicalReadinessState::Ready,
            SemanticReadinessState::HybridReady,
        ),
        quarantine: QuarantineSummary::default(),
        maintenance: MaintenanceActivity::Idle,
        archive_risk: ArchiveRiskLevel::None,
        binary: BinaryCompatibility::Current,
    };
    let cmd = t.safe_next_command();
    assert_eq!(cmd.action, SafeNextAction::None);
    assert!(cmd.command.is_none());
    assert!(t.is_searchable());
}

// -----------------------------------------------------------------
// ArchiveSafetyEnvelope (.1.3)
// -----------------------------------------------------------------

#[test]
fn high_archive_risk_envelope_is_backup_first_and_hard_gates_mutating_actions() {
    let t = fixture("ts1_high_archive_risk");
    let env = t.archive_safety_envelope(Some("/data/cass"));
    assert_eq!(env.archive_risk, ArchiveRiskLevel::High);
    assert!(env.backup_recommended);
    assert_eq!(env.data_dir.as_deref(), Some("/data/cass"));
    // Every coverage-reducing rebuild/repair is gated unsafe-until-backup.
    for gated in [
        SafeNextAction::IndexFull,
        SafeNextAction::RepairLexical,
        SafeNextAction::RefreshLexical,
        SafeNextAction::RebuildForCurrentBinary,
    ] {
        assert!(
            env.is_gated(gated),
            "{gated:?} must be gated under high risk"
        );
        assert!(gated.is_mutating());
    }
    // The node's own safe-next-command stays backup-first, never a
    // casual rebuild.
    assert_eq!(
        t.safe_next_command().action,
        SafeNextAction::BackupThenRepair
    );
    assert!(env.reason.contains("back up"));
}

#[test]
fn low_risk_stale_node_is_not_gated_and_keeps_normal_refresh_guidance() {
    for name in ["local_stale_quarantine", "css_stale_existing_index"] {
        let t = fixture(name);
        let env = t.archive_safety_envelope(Some("/data/cass"));
        assert!(matches!(env.archive_risk, ArchiveRiskLevel::Low));
        assert!(!env.backup_recommended, "{name} low risk: no backup gate");
        assert!(env.unsafe_until_backup.is_empty(), "{name} nothing gated");
        assert!(!env.is_gated(SafeNextAction::RefreshLexical));
        // Normal refresh guidance is preserved.
        assert_eq!(t.safe_next_command().action, SafeNextAction::RefreshLexical);
    }
}

#[test]
fn elevated_risk_recommends_backup_without_hard_gating() {
    let t = DerivedAssetTruthTable {
        db: CanonicalDbAvailability::Available,
        source_coverage: SourceCoverageState::Complete,
        scan_watermark_ms: Some(1),
        last_projection_ms: Some(1),
        lexical_metadata: LexicalMetadata::default(),
        readiness: ReadinessSnapshot::new(
            LexicalReadinessState::StaleButSearchable,
            SemanticReadinessState::HybridReady,
        ),
        quarantine: QuarantineSummary::default(),
        maintenance: MaintenanceActivity::Idle,
        archive_risk: ArchiveRiskLevel::Elevated,
        binary: BinaryCompatibility::Current,
    };
    let env = t.archive_safety_envelope(Some("/data/cass"));
    assert!(env.backup_recommended);
    assert!(
        env.unsafe_until_backup.is_empty(),
        "elevated risk is advisory, not a hard gate"
    );
}

#[test]
fn unreachable_node_envelope_has_no_data_dir_and_no_gate() {
    let t = fixture("mac_mini_old_unreachable");
    let env = t.archive_safety_envelope(Some("/data/cass"));
    assert!(
        env.data_dir.is_none(),
        "unreachable host surfaces no data_dir"
    );
    assert!(!env.backup_recommended);
    assert!(env.unsafe_until_backup.is_empty());
}

#[test]
fn archive_safety_envelope_round_trips_through_json_snake_case() {
    let t = fixture("ts1_high_archive_risk");
    let env = t.archive_safety_envelope(Some("/data/cass"));
    let json = serde_json::to_string(&env).unwrap();
    assert!(json.contains("\"archive_risk\":\"high\""));
    assert!(json.contains("\"backup_recommended\":true"));
    assert!(json.contains("\"unsafe_until_backup\":[\"index_full\""));
    assert!(json.contains("\"data_dir\":\"/data/cass\""));
    let parsed: ArchiveSafetyEnvelope = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, env);
}

#[test]
fn healthy_node_with_only_quarantine_surfaces_advisory_inspection() {
    let t = DerivedAssetTruthTable {
        db: CanonicalDbAvailability::Available,
        source_coverage: SourceCoverageState::Complete,
        scan_watermark_ms: Some(1),
        last_projection_ms: Some(1),
        lexical_metadata: LexicalMetadata {
            present: true,
            schema_hash: Some("schema-current".to_string()),
            storage_fingerprint: Some("fp-current".to_string()),
            built_at_ms: Some(1),
        },
        readiness: ReadinessSnapshot::new(
            LexicalReadinessState::Ready,
            SemanticReadinessState::HybridReady,
        ),
        quarantine: QuarantineSummary {
            quarantined_count: 1,
            total_quarantined_bytes: 1024,
            causes: vec!["ingest_oom".to_string()],
        },
        maintenance: MaintenanceActivity::Idle,
        archive_risk: ArchiveRiskLevel::None,
        binary: BinaryCompatibility::Current,
    };
    let cmd = t.safe_next_command();
    assert_eq!(cmd.action, SafeNextAction::InspectQuarantine);
    assert!(!cmd.action.is_mutating());
}

// --- truth_table_from_state bridge (bead envu1) ----------------------

#[test]
fn bridge_fresh_install_maps_to_missing_db_and_index_full() {
    // A fresh data dir: no canonical DB, no lexical index, nothing
    // rebuilding. The bridge must surface a Missing DB and the safe next
    // command must be the initial `cass index --full` build.
    let state = serde_json::json!({
        "database": { "exists": false, "opened": false, "open_skipped": false },
        "index": { "exists": false },
        "rebuild": { "active": false },
    });
    let readiness = ReadinessSnapshot::new(
        LexicalReadinessState::Missing,
        SemanticReadinessState::Absent,
    );
    let table = truth_table_from_state(&state, readiness);
    assert_eq!(table.db, CanonicalDbAvailability::Missing);
    assert!(!table.lexical_metadata.present);
    assert_eq!(table.source_coverage, SourceCoverageState::Unknown);
    assert_eq!(table.archive_risk, ArchiveRiskLevel::Unknown);
    assert_eq!(table.maintenance, MaintenanceActivity::Idle);
    assert!(!table.is_searchable());
    assert_eq!(table.safe_next_command().action, SafeNextAction::IndexFull);
}

#[test]
fn bridge_healthy_node_maps_to_available_and_converged() {
    // A healthy node: DB opened, lexical index present + fresh, no
    // rebuild, no quarantine. With a Ready/HybridReady search axis the
    // bridge must converge to "nothing required".
    let state = serde_json::json!({
        "database": { "exists": true, "opened": true, "open_skipped": false },
        "index": {
            "exists": true,
            "fingerprint": { "checkpoint_fingerprint": "10:42:0:0" },
            "last_indexed_at": "2026-06-08T00:00:00+00:00",
            "checkpoint": { "schema_matches": true },
        },
        "rebuild": { "active": false },
        "semantic": { "checkpoint": { "active": false } },
        "ingest_quarantine": { "quarantined_conversations": 0 },
    });
    let readiness = ReadinessSnapshot::new(
        LexicalReadinessState::Ready,
        SemanticReadinessState::HybridReady,
    );
    let table = truth_table_from_state(&state, readiness);
    assert_eq!(table.db, CanonicalDbAvailability::Available);
    assert!(table.lexical_metadata.present);
    assert_eq!(
        table.lexical_metadata.storage_fingerprint.as_deref(),
        Some("10:42:0:0")
    );
    assert_eq!(
        table.lexical_metadata.built_at_ms,
        Some(1_780_876_800_000) // 2026-06-08T00:00:00Z in epoch millis
    );
    assert_eq!(table.last_projection_ms, table.lexical_metadata.built_at_ms);
    assert_eq!(table.binary, BinaryCompatibility::Current);
    assert!(table.is_searchable());
    assert_eq!(table.safe_next_command().action, SafeNextAction::None);
}

#[test]
fn bridge_present_unopened_db_maps_to_open_failed() {
    // The file exists but a real open attempt failed (not skipped):
    // surface OpenFailed → inspect before any repair, never rebuild.
    let state = serde_json::json!({
        "database": { "exists": true, "opened": false, "open_skipped": false },
        "index": { "exists": true },
        "rebuild": { "active": false },
    });
    let table = truth_table_from_state(
        &state,
        ReadinessSnapshot::new(LexicalReadinessState::Ready, SemanticReadinessState::Absent),
    );
    assert_eq!(table.db, CanonicalDbAvailability::OpenFailed);
    let cmd = table.safe_next_command();
    assert_eq!(cmd.action, SafeNextAction::InspectCanonicalDb);
    assert!(!cmd.action.is_mutating());
}

#[test]
fn bridge_skipped_open_on_present_db_is_available() {
    // Health's fast path elides the DB open (open_skipped=true) on a
    // present file: the bridge must treat it as Available, not OpenFailed.
    let state = serde_json::json!({
        "database": { "exists": true, "opened": false, "open_skipped": true },
        "index": { "exists": true },
        "rebuild": { "active": false },
    });
    let table = truth_table_from_state(
        &state,
        ReadinessSnapshot::new(LexicalReadinessState::Ready, SemanticReadinessState::Absent),
    );
    assert_eq!(table.db, CanonicalDbAvailability::Available);
}

#[test]
fn bridge_maps_active_rebuild_and_semantic_backfill() {
    let rebuilding = serde_json::json!({
        "database": { "exists": true, "opened": true },
        "index": { "exists": true },
        "rebuild": { "active": true },
        "semantic": { "checkpoint": { "active": false } },
    });
    assert_eq!(
        truth_table_from_state(
            &rebuilding,
            ReadinessSnapshot::new(
                LexicalReadinessState::Repairing,
                SemanticReadinessState::Absent,
            ),
        )
        .maintenance,
        MaintenanceActivity::LexicalRebuild
    );

    // Lexical rebuild takes precedence; when it is idle and the semantic
    // checkpoint is active the bridge reports SemanticBackfill.
    let backfilling = serde_json::json!({
        "database": { "exists": true, "opened": true },
        "index": { "exists": true },
        "rebuild": { "active": false },
        "semantic": { "checkpoint": { "active": true } },
    });
    assert_eq!(
        truth_table_from_state(
            &backfilling,
            ReadinessSnapshot::new(
                LexicalReadinessState::Ready,
                SemanticReadinessState::Backfilling,
            ),
        )
        .maintenance,
        MaintenanceActivity::SemanticBackfill
    );
}

#[test]
fn bridge_schema_mismatch_marks_binary_ahead() {
    let state = serde_json::json!({
        "database": { "exists": true, "opened": true },
        "index": { "exists": true, "checkpoint": { "schema_matches": false } },
        "rebuild": { "active": false },
    });
    let table = truth_table_from_state(
        &state,
        ReadinessSnapshot::new(
            LexicalReadinessState::Ready,
            SemanticReadinessState::HybridReady,
        ),
    );
    assert_eq!(table.binary, BinaryCompatibility::Ahead);
    // A binary ahead of the on-disk assets must drive a rebuild-for-schema
    // recommendation ahead of the converged path.
    assert_eq!(
        table.safe_next_command().action,
        SafeNextAction::RebuildForCurrentBinary
    );
}

#[test]
fn bridge_maps_quarantine_count_and_feeds_projections() {
    // Quarantine is advisory: it never blocks search, but the count must
    // flow through, and the bridged table must feed both typed
    // projections (readiness summary + next-command envelope) cleanly.
    let state = serde_json::json!({
        "database": { "exists": true, "opened": true },
        "index": {
            "exists": true,
            "checkpoint": { "schema_matches": true },
        },
        "rebuild": { "active": false },
        "semantic": { "checkpoint": { "active": false } },
        "ingest_quarantine": { "quarantined_conversations": 3 },
    });
    let table = truth_table_from_state(
        &state,
        ReadinessSnapshot::new(
            LexicalReadinessState::Ready,
            SemanticReadinessState::HybridReady,
        ),
    );
    assert_eq!(table.quarantine.quarantined_count, 3);
    assert!(table.quarantine.has_exclusions());

    // Prove the bridged table is consumable by the typed projection layer
    // that 6f1lm / v6vuz assemble over.
    let summary = crate::search::readiness_projection::project(
        &table,
        crate::search::readiness_projection::SurfaceKind::Status,
    );
    assert!(summary.is_searchable);
    assert!(summary.quarantine_incomplete);
    let envelope = table.next_command_envelope(Some("/tmp/cass-data"));
    assert!(!envelope.recommended_commands.is_empty());
}
