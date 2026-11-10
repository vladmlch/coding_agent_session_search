use super::response_schema_tests_part_2::test_full_rebuild_readiness;
use crate::cli::commands::capabilities_introspect::capabilitiesresponse::build_introspect_subsystem_coverage;
use crate::cli::output::cli_error_json_payload::cass_not_initialized_recommended_action;
use crate::doctor::repair_state::finalize_cleanup_apply_contract::DiagQuarantineSummary;
use crate::doctor::report::coverage::{
    DoctorRuntimeSummaryInput, build_doctor_runtime_summary, doctor_fast_coverage_risk_unchecked,
};
use crate::doctor::report::inventory::DoctorCoverageRiskSummary;
use crate::doctor::report::storage::build_doctor_storage_pressure_report;
use crate::robot_schema::build_response_schemas::build_response_schemas;
use crate::robot_schema::response_schema_index_state::{
    DOCTOR_V2_COMMON_BRANCH_FIELDS, DOCTOR_V2_RESPONSE_SCHEMA_KEYS,
};
use crate::robot_schema::response_schema_pack::response_schema_subsystem_coverage_matrix;
use anyhow::Result;
use std::path::Path;

#[test]
pub(crate) fn introspect_coverage_source_is_the_executable_matrix_report() -> Result<(), String> {
    let embedded = serde_json::to_value(build_introspect_subsystem_coverage())
        .map_err(|error| error.to_string())?;
    let expected = serde_json::to_value(crate::subsystem_coverage_matrix::matrix_report())
        .map_err(|error| error.to_string())?;

    if !embedded.eq(&expected) {
        return Err("introspect subsystem coverage drifted from matrix_report()".to_string());
    }
    let coverage = build_introspect_subsystem_coverage();
    if !coverage
        .subsystem_count
        .eq(&crate::subsystem_coverage_matrix::REPORT_SUBSYSTEM_FILES.len())
        || !coverage.complete
    {
        return Err("introspect published an incomplete subsystem coverage report".to_string());
    }
    Ok(())
}

#[test]
pub(crate) fn introspect_schema_describes_subsystem_coverage_contract() -> Result<(), String> {
    let schemas = build_response_schemas();
    let published = schemas
        .get("introspect")
        .and_then(|schema| schema.pointer("/properties/subsystem_coverage"))
        .ok_or_else(|| "introspect schema omits subsystem_coverage".to_string())?;
    if !published.eq(&response_schema_subsystem_coverage_matrix()) {
        return Err("published subsystem coverage schema drifted from its source".to_string());
    }

    let proof_levels = published
        .pointer("/properties/subsystems/items/properties/mandatory_proofs/items/enum")
        .ok_or_else(|| "subsystem coverage schema omits proof-level enum".to_string())?;
    if !proof_levels.eq(&serde_json::json!([
        "unit",
        "integration",
        "golden",
        "e2e",
        "logs"
    ])) {
        return Err("subsystem coverage proof-level enum drifted".to_string());
    }
    Ok(())
}

#[test]
pub(crate) fn analytics_incidents_schema_is_discoverable_and_explicit() {
    let schemas = build_response_schemas();
    let schema = &schemas["analytics-incidents"];
    assert_eq!(
        schema["properties"]["command"]["const"],
        "analytics/incidents"
    );
    assert_eq!(
        schema["properties"]["data"]["properties"]["schema_version"]["const"],
        2
    );

    let data_required = schema["properties"]["data"]["required"]
        .as_array()
        .expect("analytics incidents data required fields");
    for field in [
        "discovery",
        "top_sessions",
        "total_sessions",
        "total_hits",
        "top_sessions_truncated",
        "redaction",
    ] {
        assert!(
            data_required
                .iter()
                .any(|required| required.as_str() == Some(field)),
            "analytics incidents schema omits required field {field}"
        );
    }
    assert_eq!(
        schema["properties"]["data"]["properties"]["redaction"]["properties"]["hash_strategy"]["const"],
        "blake3_256_v1"
    );
    assert_eq!(
        schema["properties"]["data"]["properties"]["top_sessions"]["items"]["properties"]["suggested_command"]
            ["properties"]["argv"]["type"],
        "array"
    );
}

#[test]
pub(crate) fn b7tb0_runtime_and_cache_outcome_schemas_are_discoverable() {
    let schemas = build_response_schemas();
    assert!(
        schemas["status"]["properties"]
            .get("daemon_runtime")
            .is_some()
    );
    assert!(
        schemas["doctor"]["properties"]
            .get("daemon_runtime")
            .is_some()
    );
    assert!(
        schemas["doctor"]["required"]
            .as_array()
            .expect("doctor required fields")
            .iter()
            .any(|field| field.as_str() == Some("daemon_runtime"))
    );

    let outcomes = &schemas["search"]["properties"]["_meta"]["properties"]["cache_stats"]["properties"]
        ["outcomes"]["properties"];
    for field in [
        "hit",
        "cold_miss",
        "stale_generation_miss",
        "forced_reload",
        "reload_failure",
        "fallback",
    ] {
        assert!(
            outcomes.get(field).is_some(),
            "missing cache outcome {field}"
        );
    }
}

#[test]
pub(crate) fn status_schema_includes_semantic_and_rebuild_truth() {
    let schemas = build_response_schemas();
    let status = &schemas["status"]["properties"];

    assert!(
        status.get("semantic").is_some(),
        "status schema missing semantic block"
    );
    assert!(
        status.get("rebuild").is_some(),
        "status schema missing rebuild block"
    );
    assert!(
        status.get("rebuild_progress").is_some(),
        "status schema missing rebuild_progress block"
    );
    assert_eq!(
        status["rebuild_progress"]["properties"]["completion_ratio"]["type"],
        serde_json::json!(["number", "null"])
    );
    assert_eq!(
        status["index"]["properties"]["status"]["type"],
        serde_json::json!("string")
    );
    assert_eq!(
        status["database"]["properties"]["open_retryable"]["type"],
        serde_json::json!("boolean")
    );
}

#[test]
pub(crate) fn health_schema_tracks_full_nested_state() {
    let schemas = build_response_schemas();
    let state = &schemas["health"]["properties"]["state"]["properties"];

    assert!(
        state.get("semantic").is_some(),
        "health.state schema missing semantic block"
    );
    assert!(
        state.get("rebuild").is_some(),
        "health.state schema missing rebuild block"
    );
    assert!(
        schemas["health"]["properties"]
            .get("rebuild_progress")
            .is_some(),
        "health schema missing top-level rebuild_progress block"
    );
    assert_eq!(
        schemas["health"]["properties"]["rebuild_progress"]["properties"]["updated_at"]["type"],
        serde_json::json!(["string", "null"])
    );
    assert_eq!(
        schemas["health"]["properties"]["db"]["properties"]["open_error"]["type"],
        serde_json::json!(["string", "null"])
    );
    assert_eq!(
        schemas["health"]["properties"]["db"]["properties"]["open_skipped"]["type"],
        serde_json::json!("boolean")
    );
}

#[test]
pub(crate) fn search_schema_includes_mode_and_fallback_metadata() {
    let schemas = build_response_schemas();
    let meta = &schemas["search"]["properties"]["_meta"]["properties"];

    for key in [
        "search_mode",
        "requested_search_mode",
        "fallback_tier",
        "fallback_reason",
        "semantic_refinement",
        "timing",
        "state",
        "cursor_manifest",
        "explanation_cards",
    ] {
        assert!(meta.get(key).is_some(), "search _meta schema missing {key}");
    }
    assert!(
        meta["cursor_manifest"]["properties"]
            .get("continuation_safe")
            .is_some(),
        "cursor manifest schema should explain continuation safety"
    );
    assert_eq!(
        meta["explanation_cards"]["items"]["properties"]["fallback_contract"]["type"],
        serde_json::json!(["object", "null"])
    );
}

#[test]
pub(crate) fn doctor_v2_response_schemas_cover_branchable_surfaces() {
    let schemas = build_response_schemas();
    let keys: Vec<_> = schemas.keys().map(String::as_str).collect();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort_unstable();
    assert_eq!(
        keys, sorted_keys,
        "response_schemas must stay alphabetically deterministic"
    );

    for key in DOCTOR_V2_RESPONSE_SCHEMA_KEYS {
        assert!(schemas.contains_key(*key), "missing {key} schema");
    }

    for key in DOCTOR_V2_RESPONSE_SCHEMA_KEYS
        .iter()
        .copied()
        .filter(|key| *key != "doctor-error-envelope" && *key != "doctor-failure-context")
    {
        let properties = schemas[key]["properties"]
            .as_object()
            .unwrap_or_else(|| panic!("{key} schema should expose object properties"));
        for field in DOCTOR_V2_COMMON_BRANCH_FIELDS {
            assert!(
                properties.contains_key(*field),
                "{key} schema missing common branch field {field}"
            );
        }
        let required = schemas[key]["required"]
            .as_array()
            .unwrap_or_else(|| panic!("{key} schema should list required branch fields"));
        for field in DOCTOR_V2_COMMON_BRANCH_FIELDS {
            assert!(
                required.iter().any(|value| value.as_str() == Some(*field)),
                "{key} required list missing {field}"
            );
        }
    }

    let err = &schemas["doctor-error-envelope"]["properties"]["err"]["properties"];
    for field in [
        "kind",
        "code",
        "surface",
        "mode",
        "risk_level",
        "asset_class",
        "blocked_reasons",
        "recommended_action",
    ] {
        assert!(
            err.get(field).is_some(),
            "doctor error envelope missing {field}"
        );
    }
}

#[test]
pub(crate) fn doctor_failure_context_schema_matches_artifact_contract() {
    let schemas = build_response_schemas();
    let schema = &schemas["doctor-failure-context"];
    let properties = schema["properties"]
        .as_object()
        .expect("failure context schema properties");

    for field in [
        "schema_version",
        "failed_phase",
        "failed_check",
        "artifacts",
        "repro",
        "captured_logs",
        "recent_events",
        "redaction_status",
    ] {
        assert!(
            properties.contains_key(field),
            "failure context schema missing {field}"
        );
    }
    assert!(
        !properties.contains_key("repro_command"),
        "failure context schema must advertise the structured repro object, not the old repro_command field"
    );
    assert_eq!(
        properties["repro"]["properties"]["mutates_live_archive"]["type"],
        serde_json::json!("boolean")
    );
    assert_eq!(
        properties["repro"]["properties"]["command_json"]["items"]["type"],
        serde_json::json!("string")
    );
    assert_eq!(
        properties["artifacts"]["properties"]["failure_context_path"]["type"],
        serde_json::json!(["string", "null"])
    );
    let required = schema["required"]
        .as_array()
        .expect("failure context required fields");
    for field in [
        "schema_version",
        "failed_phase",
        "failed_check",
        "artifacts",
        "repro",
    ] {
        assert!(
            required.iter().any(|value| value.as_str() == Some(field)),
            "failure context required list missing {field}"
        );
    }
}

#[test]
pub(crate) fn health_and_status_schemas_advertise_doctor_summary() {
    let schemas = build_response_schemas();

    for key in ["health", "status", "state"] {
        assert!(
            schemas[key]["properties"].get("doctor_summary").is_some(),
            "{key} schema should expose doctor_summary"
        );
    }

    let summary = &schemas["status"]["properties"]["doctor_summary"]["properties"];
    for field in [
        "status",
        "risk_level",
        "fallback_mode",
        "coverage_delta",
        "recommended_action",
        "redaction_status",
        "archive_coverage_state",
        "source_mirror_state",
        "doctor_check_recommended",
        "coverage_source",
        "quarantine_summary",
    ] {
        assert!(
            summary.get(field).is_some(),
            "doctor_summary schema missing {field}"
        );
    }
}

#[test]
pub(crate) fn doctor_runtime_summary_marks_health_coverage_not_checked() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state = serde_json::json!({
        "semantic": { "fallback_mode": "lexical" },
        "rebuild": { "active": false },
    });
    let coverage_risk = doctor_fast_coverage_risk_unchecked(true);

    let summary = build_doctor_runtime_summary(DoctorRuntimeSummaryInput {
        surface: "health-summary",
        state: &state,
        status: "healthy",
        healthy: true,
        initialized: true,
        db_exists: true,
        rebuild_active: false,
        coverage_risk: &coverage_risk,
        coverage_source: "health-fast-state",
        coverage_checked: false,
        quarantine_summary: None,
        recommended_action: None,
        data_dir: temp.path(),
    });

    assert_eq!(summary["archive_coverage_state"], "not_checked");
    assert_eq!(summary["coverage_source"]["status"], "not_checked");
    assert_eq!(summary["coverage_source"]["source"], "health-fast-state");
    assert_eq!(summary["doctor_check_recommended"], true);
    assert_eq!(summary["quarantine_summary"], serde_json::Value::Null);
    assert_eq!(
        summary["cleanup_reclaimable_bytes"],
        serde_json::Value::Null
    );
    assert_eq!(
        summary["operation_outcome"]["action_not_taken"],
        "did not run deep doctor collectors, source sync, rebuild, model verification, or filesystem-wide repair work"
    );
}

#[test]
pub(crate) fn doctor_runtime_summary_routes_uninitialized_archives_to_indexing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state = serde_json::json!({
        "semantic": { "fallback_mode": "lexical" },
        "rebuild": { "active": false },
    });
    let coverage_risk = doctor_fast_coverage_risk_unchecked(false);
    let recommended_action = cass_not_initialized_recommended_action();

    let summary = build_doctor_runtime_summary(DoctorRuntimeSummaryInput {
        surface: "health-summary",
        state: &state,
        status: "not_initialized",
        healthy: false,
        initialized: false,
        db_exists: false,
        rebuild_active: false,
        coverage_risk: &coverage_risk,
        coverage_source: "health-fast-state",
        coverage_checked: false,
        quarantine_summary: None,
        recommended_action: Some(&recommended_action),
        data_dir: temp.path(),
    });

    assert_eq!(summary["archive_coverage_state"], "not_initialized");
    assert_eq!(summary["doctor_check_recommended"], false);
    assert_eq!(summary["recommended_action"], recommended_action);
    assert_eq!(
        summary["coverage_source"]["recommended_action"],
        "Run cass index --full once before archive coverage can be assessed."
    );
    assert_eq!(
        summary["operation_outcome"]["next_command"],
        "cass index --full"
    );
    assert_eq!(
        summary["operation_outcome"]["reason"],
        "archive database is not initialized yet, so doctor coverage cannot be checked"
    );
}

#[test]
pub(crate) fn doctor_runtime_summary_carries_checked_status_coverage_and_quarantine_bytes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state = serde_json::json!({
        "semantic": { "fallback_mode": "lexical" },
        "rebuild": { "active": false },
        "_meta": { "timestamp": "2026-05-06T00:00:00Z" },
    });
    let coverage_risk = DoctorCoverageRiskSummary {
        schema_version: 1,
        status: "sole_copy_risk".to_string(),
        confidence_tier: "archive_db_without_raw_mirror".to_string(),
        archive_conversation_count: 7,
        missing_current_source_count: 2,
        db_without_raw_mirror_count: 1,
        db_projection_only_count: 1,
        sole_copy_warning_count: 2,
        recommended_action: "preserve archive and inspect doctor repair".to_string(),
        ..DoctorCoverageRiskSummary::default()
    };
    let quarantine_summary = DiagQuarantineSummary {
        cleanup_dry_run_reclaimable_bytes: 4096,
        ..DiagQuarantineSummary::default()
    };

    let summary = build_doctor_runtime_summary(DoctorRuntimeSummaryInput {
        surface: "status-summary",
        state: &state,
        status: "degraded",
        healthy: false,
        initialized: true,
        db_exists: true,
        rebuild_active: false,
        coverage_risk: &coverage_risk,
        coverage_source: "status-inline-small-archive",
        coverage_checked: true,
        quarantine_summary: Some(&quarantine_summary),
        recommended_action: None,
        data_dir: temp.path(),
    });

    assert_eq!(summary["archive_coverage_state"], "sole_copy_risk");
    assert_eq!(
        summary["source_mirror_state"],
        "archive_rows_without_raw_mirror"
    );
    assert_eq!(summary["risk_level"], "high");
    assert_eq!(summary["health_class"], "degraded-archive-risk");
    assert_eq!(summary["repair_recommended"], true);
    assert_eq!(summary["sole_copy_conversation_count"], 2);
    assert_eq!(summary["cleanup_reclaimable_bytes"], 4096);
    assert_eq!(summary["coverage_delta"]["archive_conversation_count"], 7);
    assert_eq!(
        summary["coverage_delta"]["visible_source_conversation_count"],
        5
    );
    assert_eq!(
        summary["coverage_source"]["source"],
        "status-inline-small-archive"
    );
    assert_eq!(
        summary["coverage_source"]["generated_at"],
        "2026-05-06T00:00:00Z"
    );
}

pub(crate) fn write_storage_fixture_file(root: &Path, relative_path: &str, bytes: usize) {
    let path = root.join(relative_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create fixture parent");
    }
    std::fs::write(path, vec![b'x'; bytes]).expect("write storage fixture file");
}

#[test]
pub(crate) fn doctor_storage_pressure_classifies_precious_and_reclaimable_bytes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path();
    write_storage_fixture_file(root, "agent_search.db", 5);
    write_storage_fixture_file(root, "raw-mirror/v1/codex/blob.jsonl", 7);
    write_storage_fixture_file(root, "backups/agent_search.db.bak", 3);
    write_storage_fixture_file(root, "doctor/receipts/cleanup.json", 2);
    write_storage_fixture_file(root, "index/generation-failed-reclaimable/chunk", 11);
    write_storage_fixture_file(root, "index/.lexical-publish-backups/old/shard", 13);
    write_storage_fixture_file(root, "index/live/shard", 17);
    write_storage_fixture_file(root, "index/generation-quarantined/manifest.json", 19);

    let report = build_doctor_storage_pressure_report(
        root,
        root.to_path_buf(),
        Ok(1024),
        true,
        test_full_rebuild_readiness(root, 1024),
    );

    assert_eq!(report.schema_version, 3);
    assert_eq!(report.full_rebuild_readiness.status, "blocked");
    assert!(
            report
                .notes
                .iter()
                .any(|note| note
                    .contains("A full rebuild (cass index --full / --force-rebuild) needs")),
            "storage notes should carry the full-rebuild readiness verdict: {:?}",
            report.notes
        );
    assert_eq!(report.status, "warn");
    assert_eq!(report.low_disk_risk, "low_free_space");
    assert_eq!(report.total_accounted_bytes, 77);
    assert_eq!(report.reclaimable_derived_bytes, 24);
    assert_eq!(report.precious_evidence_bytes, 17);
    assert_eq!(
        report.reclaimable_bytes_by_class["reclaimable_derived_cache"],
        11
    );
    assert_eq!(
        report.reclaimable_bytes_by_class["retained_publish_backup"],
        13
    );
    assert_eq!(report.precious_bytes_by_class["canonical_archive_db"], 5);
    assert_eq!(report.precious_bytes_by_class["raw_mirror_blob"], 7);
    assert_eq!(report.precious_bytes_by_class["backup_bundle"], 3);
    assert_eq!(report.precious_bytes_by_class["operation_receipt"], 2);
    assert_eq!(
        report.protected_bytes_by_class["quarantined_lexical_generation"],
        19
    );
    assert!(
        !report
            .reclaimable_bytes_by_class
            .contains_key("raw_mirror_blob"),
        "raw mirrors must never appear in reclaimable storage classes"
    );
    assert!(
        report
            .safe_cleanup_scope
            .contains(&"reclaimable_derived_cache".to_string())
    );
    assert!(
        report
            .recommended_action
            .contains("fingerprint-approved derived cleanup"),
        "low-disk derived cleanup guidance should require fingerprint approval: {}",
        report.recommended_action
    );
}
