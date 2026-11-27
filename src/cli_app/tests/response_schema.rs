#[allow(unused_imports)]
pub(super) use super::*;

#[cfg(test)]
mod response_schema_tests {
    use super::*;

    #[test]
    fn introspect_coverage_source_is_the_executable_matrix_report() -> Result<(), String> {
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
    fn introspect_schema_describes_subsystem_coverage_contract() -> Result<(), String> {
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
    fn analytics_incidents_schema_removed_with_phase0_cli() {
        let schemas = build_response_schemas();
        assert!(
            !schemas.contains_key("analytics-incidents"),
            "retired analytics-incidents schema must not be advertised"
        );
    }

    #[test]
    fn b7tb0_runtime_and_cache_outcome_schemas_are_discoverable() {
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
    fn status_schema_includes_semantic_and_rebuild_truth() {
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
    fn health_schema_tracks_full_nested_state() {
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
        let memory = &schemas["health"]["properties"]["responsiveness"]["properties"]["memory"]["properties"];
        for field in [
            "capacity_pct",
            "inflight_byte_limit",
            "target_resident_bytes",
            "resident_bytes",
            "available_bytes",
            "effective_total_bytes",
            "healthy_streak",
            "reason",
        ] {
            assert!(
                memory.get(field).is_some(),
                "health.responsiveness.memory schema missing field {field}"
            );
        }
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
    fn search_schema_includes_mode_and_fallback_metadata() {
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
    fn doctor_v2_response_schemas_cover_branchable_surfaces() {
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
    fn doctor_failure_context_schema_matches_artifact_contract() {
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
    fn health_and_status_schemas_advertise_doctor_summary() {
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
    fn doctor_runtime_summary_marks_health_coverage_not_checked() {
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
    fn doctor_runtime_summary_routes_uninitialized_archives_to_indexing() {
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
    fn doctor_runtime_summary_carries_checked_status_coverage_and_quarantine_bytes() {
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

    fn write_storage_fixture_file(root: &Path, relative_path: &str, bytes: usize) {
        let path = root.join(relative_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create fixture parent");
        }
        std::fs::write(path, vec![b'x'; bytes]).expect("write storage fixture file");
    }

    #[test]
    fn doctor_storage_pressure_classifies_precious_and_reclaimable_bytes() {
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

    #[cfg(unix)]
    #[test]
    fn doctor_storage_pressure_accounts_for_symlinked_archive_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let real_root = temp.path().join("real-cass-data");
        std::fs::create_dir_all(&real_root).expect("create real root");
        write_storage_fixture_file(&real_root, "bookmarks.json", 4);
        let link_root = temp.path().join("cass-data-link");
        std::os::unix::fs::symlink(&real_root, &link_root).expect("symlink data root");

        let report = build_doctor_storage_pressure_report(
            &link_root,
            link_root.clone(),
            Ok(DOCTOR_STORAGE_MIN_FREE_BYTES),
            false,
            test_full_rebuild_readiness(&link_root, DOCTOR_STORAGE_MIN_FREE_BYTES),
        );

        assert!(report.data_dir_exists);
        assert_eq!(report.precious_bytes_by_class["bookmark_store"], 4);
        assert!(
            !report
                .reclaimable_bytes_by_class
                .contains_key("bookmark_store"),
            "bookmarks are user state and must not become reclaimable through a symlinked root"
        );
    }

    fn test_full_rebuild_readiness(
        root: &Path,
        available_bytes: u64,
    ) -> DoctorFullRebuildReadinessReport {
        let db_path = root.join("agent_search.db");
        build_doctor_full_rebuild_readiness(
            crate::indexer::full_rebuild_headroom_projection(root, &db_path),
            vec![(root.to_path_buf(), Ok(available_bytes))],
            true,
        )
    }

    /// GH#442: doctor's full-rebuild readiness must be the indexer's own
    /// rule and probe paths, not a parallel approximation.
    #[test]
    fn doctor_full_rebuild_readiness_uses_the_indexer_rule_and_probe_paths() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        write_storage_fixture_file(root, "agent_search.db", 5);
        write_storage_fixture_file(root, "agent_search.db-wal", 3);
        write_storage_fixture_file(root, "index/live/shard", 17);
        let db_path = root.join("agent_search.db");

        let projection = crate::indexer::full_rebuild_headroom_projection(root, &db_path);
        assert_eq!(projection.db_bundle_bytes, 8, "db + wal sidecar");
        assert_eq!(projection.lexical_index_bytes, 17);
        assert_eq!(projection.floor_bytes, 512 * 1024 * 1024);
        assert_eq!(
            projection.required_bytes, projection.floor_bytes,
            "2*8 + 2*17 is below the floor, so the floor applies"
        );
        let probe_paths = crate::indexer::existing_headroom_probe_paths(root, &db_path);
        assert_eq!(
            probe_paths,
            vec![root.to_path_buf()],
            "data dir and db parent are the same path and must be probed once"
        );

        let required = projection.required_bytes;
        let ready = build_doctor_full_rebuild_readiness(
            projection,
            vec![(root.to_path_buf(), Ok(required))],
            true,
        );
        assert_eq!(ready.status, "ready");
        assert!(ready.enforced);
        assert_eq!(ready.required_bytes, required);
        assert_eq!(ready.available_bytes, Some(required));
        assert_eq!(ready.shortfall_bytes, 0);
        assert_eq!(ready.probe_path, root.display().to_string());
        assert_eq!(ready.probe_paths, vec![root.display().to_string()]);
        assert_eq!(ready.formula, crate::indexer::FULL_REBUILD_HEADROOM_FORMULA);
        assert!(
            ready.notes[0].contains("would pass"),
            "ready note should say the preflight passes: {:?}",
            ready.notes
        );

        // The tightest probe path decides, exactly as the indexer's loop does.
        let other = root.join("elsewhere");
        let blocked = build_doctor_full_rebuild_readiness(
            projection,
            vec![
                (root.to_path_buf(), Ok(required + 1)),
                (other.clone(), Ok(required - 10)),
            ],
            true,
        );
        assert_eq!(blocked.status, "blocked");
        assert_eq!(blocked.available_bytes, Some(required - 10));
        assert_eq!(blocked.shortfall_bytes, 10);
        assert_eq!(blocked.probe_path, other.display().to_string());
        assert_eq!(blocked.probe_paths.len(), 2);
        assert!(
            blocked.notes[0].contains("10 bytes short")
                && blocked.notes[0].contains("would refuse"),
            "blocked note should name the shortfall: {:?}",
            blocked.notes
        );

        let unknown = build_doctor_full_rebuild_readiness(
            projection,
            vec![(root.to_path_buf(), Err(io::Error::other("probe failed")))],
            true,
        );
        assert_eq!(unknown.status, "unknown");
        assert_eq!(unknown.available_bytes, None);
        assert_eq!(unknown.shortfall_bytes, 0);
        assert!(unknown.notes[0].contains("probe failed"));

        let advisory = build_doctor_full_rebuild_readiness(
            projection,
            vec![(root.to_path_buf(), Ok(0))],
            false,
        );
        assert_eq!(advisory.status, "blocked");
        assert!(!advisory.enforced);
        assert!(
            advisory
                .notes
                .iter()
                .any(|note| note.contains("CASS_INDEX_SKIP_DISK_HEADROOM_CHECK")),
            "advisory verdict must say the indexer would not enforce it: {:?}",
            advisory.notes
        );

        // The check line classifies as its own anomaly: a warning that keeps
        // doctor healthy and carries no data-loss risk.
        let check = doctor_check_report(
            "full_rebuild_readiness",
            "warn",
            "Full rebuild blocked by disk headroom",
            false,
            false,
        );
        assert_eq!(check.anomaly_class, DoctorAnomaly::FullRebuildHeadroom);
        assert_eq!(check.health_class, DoctorHealth::Healthy);
        assert_eq!(check.data_loss_risk, DoctorDataLossRisk::None);
        assert!(!check.safe_for_auto_repair);
        assert_eq!(
            doctor_check_report("full_rebuild_readiness", "pass", "ready", false, false)
                .anomaly_class,
            DoctorAnomaly::Healthy
        );
    }

    /// GH#442 reproduction shape: a 100 GiB (sparse) archive with no engine
    /// migration marker and no lexical index projects a 331.25 GiB
    /// requirement (a rebuild sized as the bundle, 9/4 of it, plus one copy
    /// for the pending migration repair and the 1/16 growth allowance; nw61n), and
    /// doctor reports it blocked against the free space the indexer's refusal
    /// named, even though the 1 GiB general pressure floor is satisfied.
    #[test]
    fn doctor_full_rebuild_readiness_reports_the_indexer_refusal_numbers() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        let db_path = root.join("agent_search.db");
        let db = std::fs::File::create(&db_path).expect("create sparse db");
        db.set_len(100 * 1024 * 1024 * 1024)
            .expect("extend sparse db");
        drop(db);

        let projection = crate::indexer::full_rebuild_headroom_projection(root, &db_path);
        assert!(projection.migration_repair_pending);
        assert_eq!(projection.lexical_index_bytes, 0);
        assert_eq!(
            projection.lexical_rebuild_basis_bytes,
            projection.db_bundle_bytes
        );
        assert_eq!(projection.required_bytes, 355_676_979_200);

        let available = 45_252_415_488_u64;
        let report = build_doctor_storage_pressure_report(
            root,
            root.to_path_buf(),
            Ok(available),
            true,
            build_doctor_full_rebuild_readiness(
                projection,
                vec![(root.to_path_buf(), Ok(available))],
                true,
            ),
        );
        assert_eq!(report.status, "ok", "general pressure floor is satisfied");
        assert_eq!(report.low_disk_risk, "none");
        let readiness = &report.full_rebuild_readiness;
        assert_eq!(readiness.status, "blocked");
        assert_eq!(readiness.required_bytes, 355_676_979_200);
        assert_eq!(readiness.available_bytes, Some(available));
        assert_eq!(readiness.shortfall_bytes, 355_676_979_200 - available);
        assert_eq!(
            readiness.lexical_rebuild_basis_bytes,
            projection.db_bundle_bytes
        );
        assert!(
            readiness
                .notes
                .iter()
                .any(|note| note.starts_with("There is no live lexical generation")),
            "{:?}",
            readiness.notes
        );

        let json = serde_json::to_value(&report).expect("serialize storage pressure");
        assert_eq!(json["schema_version"], 3);
        assert_eq!(
            json["full_rebuild_readiness"]["status"].as_str(),
            Some("blocked")
        );
        assert_eq!(
            json["full_rebuild_readiness"]["required_bytes"].as_u64(),
            Some(355_676_979_200)
        );
        assert_eq!(
            json["full_rebuild_readiness"]["formula"].as_str(),
            Some(crate::indexer::FULL_REBUILD_HEADROOM_FORMULA)
        );
    }

    /// #453: reclaimable lexical bytes are reported next to the requirement
    /// they are excluded from, with the path that reclaims each class, and
    /// the remedy never points at the rebuild the shortfall blocks.
    #[test]
    fn doctor_full_rebuild_readiness_reports_reclaimable_lexical_bytes_and_their_remedy() {
        let projection = crate::indexer::FullRebuildHeadroomProjection {
            required_bytes: 37_000_000_000,
            floor_bytes: 512 * 1024 * 1024,
            db_bundle_bytes: 11_900_000_000,
            lexical_index_bytes: 6_800_000_000,
            lexical_rebuild_basis_bytes: 6_800_000_000,
            retired_segment_bytes: 4_200_000_000,
            retired_segment_files: 1_642,
            retained_backup_bytes: 6_400_000_000,
            rebuild_staging_bytes: 32_000_000_000,
            quarantine_bytes: 34_600_000_000,
            db_growth_allowance_bytes: 11_900_000_000 / 16,
            migration_repair_pending: false,
            migration_copy_bytes: 0,
        };
        let root = PathBuf::from("/probe");
        let blocked = build_doctor_full_rebuild_readiness(
            projection,
            vec![(root.clone(), Ok(30_000_000_000))],
            true,
        );
        assert_eq!(blocked.status, "blocked");
        assert_eq!(blocked.lexical_index_bytes, 6_800_000_000);
        assert_eq!(blocked.retired_segment_bytes, 4_200_000_000);
        assert_eq!(blocked.retired_segment_files, 1_642);
        assert_eq!(blocked.retained_publish_backup_bytes, 6_400_000_000);
        let retired_note = blocked
            .notes
            .iter()
            .find(|note| note.contains("merge-retired segment file"))
            .expect("retired-segment note");
        assert!(retired_note.contains("1642"), "{retired_note}");
        assert!(retired_note.contains("4200000000 bytes"), "{retired_note}");
        assert!(retired_note.contains("cass index --gc"), "{retired_note}");
        assert!(
            retired_note.contains(&format!(
                "{}-second grace",
                frankensearch::quill::DEFAULT_GARBAGE_GRACE.as_secs()
            )),
            "{retired_note}"
        );
        let backup_note = blocked
            .notes
            .iter()
            .find(|note| note.contains(".lexical-publish-backups"))
            .expect("retained-backup note");
        assert!(backup_note.contains("6400000000 bytes"), "{backup_note}");
        assert!(backup_note.contains("cass doctor cleanup"), "{backup_note}");
        let staging_note = blocked
            .notes
            .iter()
            .find(|note| note.contains(".rebuild-staging"))
            .expect("rebuild-staging note (GH #496)");
        assert!(staging_note.contains("32000000000 bytes"), "{staging_note}");
        assert!(
            staging_note.contains("subtracted from the requirement"),
            "{staging_note}"
        );
        let quarantine_note = blocked
            .notes
            .iter()
            .find(|note| note.contains("quarantine bundles"))
            .expect("quarantine note (nw61n)");
        assert!(
            quarantine_note.contains("34600000000 bytes"),
            "{quarantine_note}"
        );
        assert!(quarantine_note.contains("not charged"), "{quarantine_note}");
        assert!(
            blocked
                .notes
                .iter()
                .all(|note| !note.starts_with("There is no live lexical generation")),
            "a live index sizes the rebuild: {:?}",
            blocked.notes
        );
        assert!(
            blocked
                .notes
                .iter()
                .all(|note| !note.contains("Run `cass index --full`")),
            "a blocked rebuild must not be prescribed as its own remedy: {:?}",
            blocked.notes
        );

        let json = serde_json::to_value(&blocked).expect("serialize readiness");
        assert_eq!(json["retired_segment_bytes"].as_u64(), Some(4_200_000_000));
        assert_eq!(json["retired_segment_files"].as_u64(), Some(1_642));
        assert_eq!(
            json["retained_publish_backup_bytes"].as_u64(),
            Some(6_400_000_000)
        );
        assert_eq!(json["rebuild_staging_bytes"].as_u64(), Some(32_000_000_000));
        assert_eq!(json["quarantine_bytes"].as_u64(), Some(34_600_000_000));
        assert_eq!(
            json["lexical_rebuild_basis_bytes"].as_u64(),
            Some(6_800_000_000)
        );

        // With nothing reclaimable the notes stay exactly the two-line shape
        // the robot goldens pin.
        let clean = build_doctor_full_rebuild_readiness(
            crate::indexer::FullRebuildHeadroomProjection {
                retired_segment_bytes: 0,
                retired_segment_files: 0,
                retained_backup_bytes: 0,
                rebuild_staging_bytes: 0,
                quarantine_bytes: 0,
                ..projection
            },
            vec![(root, Ok(37_000_000_000))],
            true,
        );
        assert_eq!(clean.status, "ready");
        assert_eq!(clean.notes.len(), 2, "{:?}", clean.notes);
    }

    #[test]
    fn doctor_config_exclusion_risks_detect_cache_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let data_dir = temp.path().join(".cache").join("cass");
        std::fs::create_dir_all(&data_dir).expect("create cache data dir");

        let risks = collect_doctor_config_exclusion_risks(
            &data_dir,
            &data_dir.join("agent_search.db"),
            &data_dir.join("config.toml"),
            &temp.path().join("sources.toml"),
        );

        assert!(
            risks
                .iter()
                .any(|risk| risk.risk_kind == "volatile-cache-location"
                    && risk.confidence == "high"
                    && !risk.auto_fix_available),
            "cache placement should be reported as a high-confidence preservation risk: {risks:#?}"
        );
    }

    #[test]
    fn doctor_config_exclusion_risks_detect_explicit_filter_patterns() {
        let temp = tempfile::tempdir().expect("tempdir");
        let data_dir = temp.path().join("cass-data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        std::fs::write(data_dir.join(".rsync-filter"), "- raw-mirror/**\n")
            .expect("write rsync filter");

        let risks = collect_doctor_config_exclusion_risks(
            &data_dir,
            &data_dir.join("agent_search.db"),
            &data_dir.join("config.toml"),
            &temp.path().join("sources.toml"),
        );

        assert!(
            risks.iter().any(|risk| {
                risk.risk_kind == "backup-filter-excludes-cass-evidence"
                    && std::path::Path::new(&risk.path)
                        .ends_with(std::path::Path::new("raw-mirror").join("v1"))
                    && risk
                        .evidence
                        .iter()
                        .any(|line| line.contains(".rsync-filter:1"))
            }),
            "local backup filter should identify raw mirror exclusion: {risks:#?}"
        );
    }

    #[test]
    fn doctor_config_exclusion_risks_detect_repo_gitignore_matches() {
        let temp = tempfile::tempdir().expect("tempdir");
        let repo = temp.path().join("repo");
        let data_dir = repo.join("cass-data");
        std::fs::create_dir_all(repo.join(".git")).expect("create repo marker");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        std::fs::write(repo.join(".gitignore"), "cass-data/raw-mirror/**\n")
            .expect("write repo gitignore");

        let risks = collect_doctor_config_exclusion_risks(
            &data_dir,
            &data_dir.join("agent_search.db"),
            &data_dir.join("config.toml"),
            &temp.path().join("sources.toml"),
        );

        assert!(
            risks.iter().any(|risk| {
                risk.risk_kind == "repo-ignore-excludes-cass-evidence"
                    && std::path::Path::new(&risk.path).ends_with(
                        std::path::Path::new("cass-data")
                            .join("raw-mirror")
                            .join("v1"),
                    )
                    && risk
                        .evidence
                        .iter()
                        .any(|line| line.contains(".gitignore:1"))
            }),
            "repo-local .gitignore should be scanned only for cass paths under that repo: {risks:#?}"
        );
    }

    #[test]
    fn doctor_config_exclusion_risks_no_config_found_is_clean() {
        let temp = tempfile::tempdir().expect("tempdir");
        // RCH may place TMPDIR beneath the checkout. Establish a fixture-local
        // repository boundary so ambient parent .gitignore rules cannot turn
        // this no-config case into a repository-policy test.
        std::fs::create_dir_all(temp.path().join(".git")).expect("create repo boundary");
        let data_dir = temp.path().join("cass-data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");

        let risks = collect_doctor_config_exclusion_risks(
            &data_dir,
            &data_dir.join("agent_search.db"),
            &data_dir.join("missing-config.toml"),
            &temp.path().join("missing-sources.toml"),
        );

        assert!(
            risks.is_empty(),
            "absence of directly scanned config should not invent a backup failure: {risks:#?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn doctor_config_exclusion_risks_respect_symlinked_data_dirs() {
        let temp = tempfile::tempdir().expect("tempdir");
        let repo = temp.path().join("repo");
        let real_data = temp.path().join("real-cass-data");
        let link_data = repo.join("cass-data-link");
        std::fs::create_dir_all(repo.join(".git")).expect("create repo marker");
        std::fs::create_dir_all(&real_data).expect("create real data dir");
        std::os::unix::fs::symlink(&real_data, &link_data).expect("symlink data dir");
        std::fs::write(repo.join(".gitignore"), "cass-data-link/raw-mirror/**\n")
            .expect("write repo gitignore");

        let risks = collect_doctor_config_exclusion_risks(
            &link_data,
            &link_data.join("agent_search.db"),
            &link_data.join("config.toml"),
            &temp.path().join("sources.toml"),
        );

        assert!(
            risks.iter().any(|risk| {
                risk.risk_kind == "repo-ignore-excludes-cass-evidence"
                    && std::path::Path::new(&risk.path).ends_with(
                        std::path::Path::new("cass-data-link")
                            .join("raw-mirror")
                            .join("v1"),
                    )
            }),
            "logical symlinked data-dir placement should still be checked against repo ignores: {risks:#?}"
        );
    }

    #[test]
    fn doctor_config_exclusion_risks_suppress_later_include_rules() {
        let temp = tempfile::tempdir().expect("tempdir");
        let repo = temp.path().join("repo");
        let data_dir = repo.join("cass-data");
        std::fs::create_dir_all(repo.join(".git")).expect("create repo marker");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        std::fs::write(
            repo.join(".gitignore"),
            "cass-data/raw-mirror/**\n!cass-data/raw-mirror/v1/**\n",
        )
        .expect("write repo gitignore");

        let risks = collect_doctor_config_exclusion_risks(
            &data_dir,
            &data_dir.join("agent_search.db"),
            &data_dir.join("config.toml"),
            &temp.path().join("sources.toml"),
        );

        assert!(
            !risks
                .iter()
                .any(|risk| risk.risk_kind == "repo-ignore-excludes-cass-evidence"),
            "last matching include rule should suppress the earlier repo ignore: {risks:#?}"
        );
    }

    #[test]
    fn doctor_config_exclusion_risks_scan_cass_config_and_sources_text() {
        let temp = tempfile::tempdir().expect("tempdir");
        let data_dir = temp.path().join("cass-data");
        let config_path = data_dir.join("config.toml");
        let sources_path = temp.path().join("sources.toml");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        std::fs::write(&config_path, "backup_exclude = [\"backups/**\"]\n").expect("write config");
        std::fs::write(&sources_path, "sync_ignore = [\"raw-mirror/**\"]\n")
            .expect("write sources");

        let risks = collect_doctor_config_exclusion_risks(
            &data_dir,
            &data_dir.join("agent_search.db"),
            &config_path,
            &sources_path,
        );

        assert!(
            risks.iter().any(
                |risk| risk.risk_kind == "cass-config-excludes-cass-evidence"
                    && risk.path.ends_with("backups")
            ),
            "cass config exclusion-like entries should be surfaced with uncertainty: {risks:#?}"
        );
        assert!(
            risks.iter().any(
                |risk| risk.risk_kind == "sources-config-excludes-cass-evidence"
                    && std::path::Path::new(&risk.path)
                        .ends_with(std::path::Path::new("raw-mirror").join("v1"))
            ),
            "sources config exclusion-like entries should be surfaced with uncertainty: {risks:#?}"
        );
    }

    #[test]
    fn doctor_v2_schema_examples_are_redacted_contract_examples() {
        let schemas = build_response_schemas();

        for key in DOCTOR_V2_RESPONSE_SCHEMA_KEYS {
            let examples = schemas[*key]
                .get("examples")
                .or_else(|| schemas[*key]["properties"]["err"].get("examples"))
                .cloned()
                .unwrap_or_else(|| {
                    panic!("{key} schema must carry at least one redacted contract example")
                });
            let encoded = serde_json::to_string(&examples).expect("serialize doctor examples");
            for forbidden in [
                "/home/",
                ".codex",
                ".claude",
                "sk-",
                "BEGIN RSA PRIVATE KEY",
            ] {
                assert!(
                    !encoded.contains(forbidden),
                    "{key} example leaked forbidden sentinel {forbidden}: {encoded}"
                );
            }
            if *key != "doctor-error-envelope" {
                assert!(
                    encoded.contains("contract-example"),
                    "{key} examples should declare contract-example provenance"
                );
            }
        }
    }
}
