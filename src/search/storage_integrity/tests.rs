use super::*;

const ALL_STATES: &[StorageState] = &[
    StorageState::Ok,
    StorageState::DerivedOnlyDrift,
    StorageState::BusyOrLocked,
    StorageState::WalSidecarSuspect,
    StorageState::SchemaDrift,
    StorageState::OpenreadFailed,
    StorageState::IntegrityFailed,
    StorageState::LegacyInteropFailed,
    StorageState::FtsMetadataFailed,
    StorageState::UnsafeSqlShape,
    StorageState::UnknownDeferred,
    StorageState::Unchecked,
];

#[test]
fn storage_state_values_serialize_snake_case_and_are_stable() {
    let pairs: &[(StorageState, &str)] = &[
        (StorageState::Ok, "ok"),
        (StorageState::DerivedOnlyDrift, "derived_only_drift"),
        (StorageState::BusyOrLocked, "busy_or_locked"),
        (StorageState::WalSidecarSuspect, "wal_sidecar_suspect"),
        (StorageState::SchemaDrift, "schema_drift"),
        (StorageState::OpenreadFailed, "openread_failed"),
        (StorageState::IntegrityFailed, "integrity_failed"),
        (StorageState::LegacyInteropFailed, "legacy_interop_failed"),
        (StorageState::FtsMetadataFailed, "fts_metadata_failed"),
        (StorageState::UnsafeSqlShape, "unsafe_sql_shape"),
        (StorageState::UnknownDeferred, "unknown_deferred"),
        (StorageState::Unchecked, "unchecked"),
    ];
    for (v, want) in pairs {
        assert_eq!(serde_json::to_string(v).unwrap(), format!("\"{want}\""));
    }
    // Every variant is in the pinned list (count guard catches additions).
    assert_eq!(pairs.len(), ALL_STATES.len());
}

#[test]
fn risk_and_readability_serialize_snake_case() {
    let risk: &[(SourceOfTruthRisk, &str)] = &[
        (SourceOfTruthRisk::None, "none"),
        (SourceOfTruthRisk::Low, "low"),
        (SourceOfTruthRisk::Medium, "medium"),
        (SourceOfTruthRisk::High, "high"),
        (SourceOfTruthRisk::Unknown, "unknown"),
    ];
    for (v, want) in risk {
        assert_eq!(serde_json::to_string(v).unwrap(), format!("\"{want}\""));
    }
    let read: &[(ArchiveReadability, &str)] = &[
        (ArchiveReadability::Readable, "readable"),
        (ArchiveReadability::PartiallyReadable, "partially_readable"),
        (ArchiveReadability::Unreadable, "unreadable"),
        (ArchiveReadability::NotChecked, "not_checked"),
        (ArchiveReadability::TimedOut, "timed_out"),
    ];
    for (v, want) in read {
        assert_eq!(serde_json::to_string(v).unwrap(), format!("\"{want}\""));
    }
}

#[test]
fn every_state_has_a_defined_risk_and_storage_family() {
    for &s in ALL_STATES {
        // default_risk is total; Ok is the only None.
        let risk = s.default_risk();
        if s == StorageState::Ok {
            assert_eq!(risk, SourceOfTruthRisk::None);
        }
        // Every non-deferred, non-unchecked state attributes to
        // frankensqlite-storage.
        let fam = s.root_cause_family();
        if matches!(s, StorageState::UnknownDeferred | StorageState::Unchecked) {
            assert_eq!(fam, RootCauseFamily::Unknown);
        } else {
            assert_eq!(fam, RootCauseFamily::FrankensqliteStorage);
        }
    }
}

#[test]
fn read_failures_are_high_risk_and_untrustworthy() {
    for s in [StorageState::OpenreadFailed, StorageState::IntegrityFailed] {
        assert_eq!(s.default_risk(), SourceOfTruthRisk::High, "{s:?}");
        assert!(!s.canonical_trustworthy(), "{s:?}");
    }
    // Derived-only / FTS / busy keep the canonical rows trustworthy.
    for s in [
        StorageState::DerivedOnlyDrift,
        StorageState::FtsMetadataFailed,
        StorageState::BusyOrLocked,
    ] {
        assert!(s.canonical_trustworthy(), "{s:?}");
        assert_eq!(s.default_risk(), SourceOfTruthRisk::Low, "{s:?}");
    }
}

/// Fixtures for the report's named failure modes.
fn fixture(state: StorageState) -> StorageIntegrityReport {
    let (readability, checks) = match state {
        StorageState::OpenreadFailed => (
            ArchiveReadability::Unreadable,
            vec![StorageCheck::ran("open_read", 12)],
        ),
        StorageState::IntegrityFailed => (
            ArchiveReadability::PartiallyReadable,
            vec![StorageCheck::ran("integrity_check", 240)],
        ),
        StorageState::SchemaDrift => (
            ArchiveReadability::Readable,
            vec![StorageCheck::ran("schema_version", 3)],
        ),
        StorageState::BusyOrLocked => (
            ArchiveReadability::NotChecked,
            vec![StorageCheck::skipped(
                "integrity_check",
                "database is locked",
            )],
        ),
        StorageState::WalSidecarSuspect => (
            ArchiveReadability::Readable,
            vec![StorageCheck::ran("wal_sidecar", 5)],
        ),
        StorageState::UnsafeSqlShape => (
            ArchiveReadability::Readable,
            vec![StorageCheck::ran("sql_shape_lint", 1)],
        ),
        StorageState::FtsMetadataFailed => (
            ArchiveReadability::Readable,
            vec![StorageCheck::ran("fts_metadata", 8)],
        ),
        StorageState::LegacyInteropFailed => (
            ArchiveReadability::Unreadable,
            vec![StorageCheck::ran("legacy_open", 40)],
        ),
        _ => (
            ArchiveReadability::Readable,
            vec![StorageCheck::ran("open_read", 2)],
        ),
    };
    StorageIntegrityReport::derive(state, readability, checks)
}

#[test]
fn fixtures_cover_the_named_failure_modes_with_consistent_risk() {
    let cases = [
        (StorageState::OpenreadFailed, SourceOfTruthRisk::High),
        (StorageState::IntegrityFailed, SourceOfTruthRisk::High),
        (StorageState::SchemaDrift, SourceOfTruthRisk::Medium),
        (StorageState::BusyOrLocked, SourceOfTruthRisk::Low),
        (StorageState::WalSidecarSuspect, SourceOfTruthRisk::Medium),
        (StorageState::UnsafeSqlShape, SourceOfTruthRisk::Medium),
        (StorageState::FtsMetadataFailed, SourceOfTruthRisk::Low),
        (StorageState::LegacyInteropFailed, SourceOfTruthRisk::Medium),
    ];
    for (state, risk) in cases {
        let r = fixture(state);
        assert_eq!(r.storage_state, state);
        assert_eq!(r.source_of_truth_risk, risk, "{state:?} risk");
        // Diagnostics never mutate the archive.
        assert!(
            r.all_checks_read_only(),
            "{state:?} checks must be read-only"
        );
    }
}

#[test]
fn busy_lock_fixture_skips_with_a_reason() {
    let r = fixture(StorageState::BusyOrLocked);
    let check = &r.checks_attempted[0];
    assert!(check.skipped_reason.is_some());
    assert_eq!(r.archive_readability, ArchiveReadability::NotChecked);
}

#[test]
fn human_summary_shares_the_robot_vocabulary() {
    let r = fixture(StorageState::OpenreadFailed);
    let summary = r.human_summary();
    // The human one-liner uses the exact serialized enum labels.
    assert!(summary.contains("openread_failed"), "{summary}");
    assert!(summary.contains("high"), "{summary}");
    assert!(summary.contains("unreadable"), "{summary}");
}

#[test]
fn report_round_trips_through_json() {
    let r = fixture(StorageState::IntegrityFailed);
    let json = serde_json::to_string(&r).unwrap();
    assert!(json.contains("\"storage_state\":\"integrity_failed\""));
    assert!(json.contains("\"source_of_truth_risk\":\"high\""));
    assert!(json.contains("\"archive_readability\":\"partially_readable\""));
    assert!(json.contains("\"read_only\":true"));
    let parsed: StorageIntegrityReport = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, r);
}

#[test]
fn doctor_signals_classify_each_derivable_state() {
    // Healthy canonical DB, healthy derived assets.
    let ok = DoctorStorageSignals {
        db_file_present: true,
        ..Default::default()
    };
    assert_eq!(
        ok.classify(),
        (StorageState::Ok, ArchiveReadability::Readable)
    );

    // DB file present but the read-only opener failed.
    let open_failed = DoctorStorageSignals {
        db_file_present: true,
        db_open_failed: true,
        ..Default::default()
    };
    assert_eq!(
        open_failed.classify(),
        (StorageState::OpenreadFailed, ArchiveReadability::Unreadable)
    );

    // Opened but the integrity probe reported failure.
    let integrity = DoctorStorageSignals {
        db_file_present: true,
        integrity_failed: true,
        ..Default::default()
    };
    assert_eq!(
        integrity.classify(),
        (
            StorageState::IntegrityFailed,
            ArchiveReadability::PartiallyReadable
        )
    );

    // Opened but a read failed mid-probe so integrity is unconfirmed.
    let unverified = DoctorStorageSignals {
        db_file_present: true,
        integrity_unverified: true,
        ..Default::default()
    };
    assert_eq!(
        unverified.classify(),
        (
            StorageState::IntegrityFailed,
            ArchiveReadability::Unreadable
        )
    );

    // Healthy DB, drifted derived lexical index.
    let drift = DoctorStorageSignals {
        db_file_present: true,
        lexical_index_drifted: true,
        ..Default::default()
    };
    assert_eq!(
        drift.classify(),
        (StorageState::DerivedOnlyDrift, ArchiveReadability::Readable)
    );
}

#[test]
fn doctor_signals_classify_deferred_and_absent_cases() {
    // A bounded probe timeout never claims health.
    let timed_out = DoctorStorageSignals {
        db_file_present: true,
        probe_timed_out: true,
        // Even with a downstream integrity_failed signal set, the timeout
        // (deferred verdict) wins so we never over-claim corruption.
        integrity_failed: true,
        ..Default::default()
    };
    assert_eq!(
        timed_out.classify(),
        (StorageState::UnknownDeferred, ArchiveReadability::TimedOut)
    );

    // Fresh install: no DB file, never indexed → vacuously ok, not_checked.
    let fresh = DoctorStorageSignals {
        db_file_present: false,
        not_initialized: true,
        ..Default::default()
    };
    assert_eq!(
        fresh.classify(),
        (StorageState::Ok, ArchiveReadability::NotChecked)
    );

    // Expected-but-missing archive: missing != corrupt → deferred, not a
    // failure state.
    let missing = DoctorStorageSignals {
        db_file_present: false,
        not_initialized: false,
        ..Default::default()
    };
    assert_eq!(
        missing.classify(),
        (
            StorageState::UnknownDeferred,
            ArchiveReadability::NotChecked
        )
    );
}

#[test]
fn doctor_signal_precedence_is_most_severe_first() {
    // Open failure dominates every downstream derived-asset signal.
    let everything = DoctorStorageSignals {
        db_file_present: true,
        db_open_failed: true,
        integrity_failed: true,
        lexical_index_drifted: true,
        ..Default::default()
    };
    assert_eq!(everything.classify().0, StorageState::OpenreadFailed);

    // Real DB-level integrity failure outranks a derived-only drift.
    let integ_over_drift = DoctorStorageSignals {
        db_file_present: true,
        integrity_failed: true,
        lexical_index_drifted: true,
        ..Default::default()
    };
    assert_eq!(integ_over_drift.classify().0, StorageState::IntegrityFailed);
}

#[test]
fn build_doctor_report_derives_risk_from_state() {
    let signals = DoctorStorageSignals {
        db_file_present: true,
        integrity_failed: true,
        ..Default::default()
    };
    let report =
        build_doctor_storage_integrity(signals, vec![StorageCheck::ran("archive_integrity", 7)]);
    assert_eq!(report.storage_state, StorageState::IntegrityFailed);
    // Risk is the state's default, never hand-set.
    assert_eq!(
        report.source_of_truth_risk,
        StorageState::IntegrityFailed.default_risk()
    );
    assert_eq!(report.source_of_truth_risk, SourceOfTruthRisk::High);
    assert!(report.all_checks_read_only());
}

#[test]
fn readiness_builder_records_db_open_not_archive_integrity() {
    // #331: a healthy open WITHOUT a cached attestation projects
    // `unchecked` (risk unknown) — never a synthesized `ok` — and the
    // ONLY recorded check is `db_open`; the deliberately-skipped
    // `quick_check` is listed under checks_not_attempted.
    let unchecked = build_readiness_storage_integrity(
        DoctorStorageSignals {
            db_file_present: true,
            ..Default::default()
        },
        None,
    );
    assert_eq!(unchecked.storage_state, StorageState::Unchecked);
    assert_eq!(unchecked.source_of_truth_risk, SourceOfTruthRisk::Unknown);
    assert_eq!(unchecked.archive_readability, ArchiveReadability::Readable);
    assert_eq!(unchecked.checks_attempted.len(), 1);
    assert_eq!(unchecked.checks_attempted[0].name, "db_open");
    assert!(unchecked.checks_attempted[0].read_only);
    assert!(
        unchecked
            .checks_attempted
            .iter()
            .all(|c| c.name != "archive_integrity"),
        "readiness surfaces never claim a deep integrity probe ran"
    );
    assert_eq!(unchecked.attestation_source.as_deref(), Some("none"));
    assert_eq!(unchecked.checks_not_attempted.len(), 1);
    assert_eq!(unchecked.checks_not_attempted[0].name, "quick_check");
    assert_eq!(
        unchecked.checks_not_attempted[0].reason,
        "outside_status_budget"
    );
}

fn test_attestation(verdict: IntegrityAttestationVerdict) -> IntegrityAttestation {
    IntegrityAttestation {
        version: 1,
        verdict,
        check_depth: "quick_check".to_string(),
        checked_at_ms: 1_700_000_000_000,
        db_size_bytes: 4096,
        db_mtime_ns: 1_700_000_000_000_000_000,
        wal_size_bytes: 0,
        wal_mtime_ns: 0,
        detail: None,
    }
}

#[test]
fn readiness_builder_projects_cached_pass_attestation_as_ok() {
    let att = test_attestation(IntegrityAttestationVerdict::Pass);
    let ok = build_readiness_storage_integrity(
        DoctorStorageSignals {
            db_file_present: true,
            ..Default::default()
        },
        Some(&att),
    );
    assert_eq!(ok.storage_state, StorageState::Ok);
    assert_eq!(ok.source_of_truth_risk, SourceOfTruthRisk::None);
    assert_eq!(ok.attestation_source.as_deref(), Some("cached"));
    assert_eq!(ok.attested_at_ms, Some(att.checked_at_ms));
    assert_eq!(ok.attestation_check_depth.as_deref(), Some("quick_check"));
    assert_eq!(
        ok.attested_db_fingerprint.as_deref(),
        Some(integrity_attestation_fingerprint(&att).as_str())
    );
}

#[test]
fn readiness_builder_projects_cached_fail_attestation_as_integrity_failed() {
    // The #331 scenario: the archive still opens and bounded reads work,
    // but a deep probe proved committed corruption. Status must NOT
    // report ok/none; the cached failure projects integrity_failed/high.
    let att = test_attestation(IntegrityAttestationVerdict::Fail);
    for signals in [
        DoctorStorageSignals {
            db_file_present: true,
            ..Default::default()
        },
        DoctorStorageSignals {
            db_file_present: true,
            lexical_index_drifted: true,
            ..Default::default()
        },
    ] {
        let failed = build_readiness_storage_integrity(signals, Some(&att));
        assert_eq!(failed.storage_state, StorageState::IntegrityFailed);
        assert_eq!(failed.source_of_truth_risk, SourceOfTruthRisk::High);
        assert_eq!(
            failed.archive_readability,
            ArchiveReadability::PartiallyReadable
        );
        assert_eq!(failed.attestation_source.as_deref(), Some("cached"));
    }
}

#[test]
fn attestation_round_trips_and_fingerprint_gates_reuse() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path();
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"not empty").expect("seed db file");

    // No attestation stored yet.
    assert!(load_matching_integrity_attestation(data_dir, &db_path).is_none());

    let captured = capture_integrity_attestation(
        &db_path,
        IntegrityAttestationVerdict::Pass,
        "quick_check",
        None,
    )
    .expect("capture live attestation");
    assert_eq!(captured.check_depth, "quick_check");
    assert!(
        !integrity_attestation_path(data_dir).exists(),
        "capturing live doctor evidence must remain read-only"
    );

    store_integrity_attestation(
        data_dir,
        &db_path,
        IntegrityAttestationVerdict::Pass,
        "quick_check",
        None,
    )
    .expect("capture and persist pass attestation");
    let loaded = load_matching_integrity_attestation(data_dir, &db_path)
        .expect("fingerprint-matched attestation loads");
    assert_eq!(loaded.verdict, IntegrityAttestationVerdict::Pass);
    assert_eq!(loaded.check_depth, "quick_check");

    // Any archive byte change (size differs) invalidates the attestation.
    std::fs::write(&db_path, b"not empty but longer now").expect("mutate db file");
    assert!(
        load_matching_integrity_attestation(data_dir, &db_path).is_none(),
        "a changed archive must not reuse the stale attestation"
    );

    // A fresh probe over the changed bytes re-arms it, including FAIL.
    store_integrity_attestation(
        data_dir,
        &db_path,
        IntegrityAttestationVerdict::Fail,
        "quick_check",
        Some("wrong # of entries in index sqlite_autoindex_conversations_1".to_string()),
    )
    .expect("capture and persist fail attestation");
    let failed = load_matching_integrity_attestation(data_dir, &db_path)
        .expect("fail attestation loads while fingerprint matches");
    assert_eq!(failed.verdict, IntegrityAttestationVerdict::Fail);
    assert!(failed.detail.is_some());

    // WAL sidecar activity also invalidates.
    std::fs::write(data_dir.join("agent_search.db-wal"), b"wal bytes").expect("seed wal");
    assert!(
        load_matching_integrity_attestation(data_dir, &db_path).is_none(),
        "WAL sidecar activity must invalidate the attestation"
    );
}

#[test]
fn readiness_builder_skips_open_when_no_archive_present() {
    // No DB file but never initialized → vacuously ok, the db_open check is
    // recorded as skipped with the initialization reason.
    let fresh = build_readiness_storage_integrity(
        DoctorStorageSignals {
            db_file_present: false,
            not_initialized: true,
            ..Default::default()
        },
        None,
    );
    assert_eq!(fresh.storage_state, StorageState::Ok);
    assert_eq!(fresh.archive_readability, ArchiveReadability::NotChecked);
    assert_eq!(fresh.checks_attempted[0].name, "db_open");
    assert!(fresh.checks_attempted[0].skipped_reason.is_some());
}

#[test]
fn readiness_builder_agrees_with_doctor_classification_on_shared_signals() {
    // For every non-Ok signal BOTH surfaces can observe (open failure,
    // derived-only drift, missing/uninitialized, expected-but-missing), the
    // readiness builder yields the SAME storage state the shared
    // classifier does — the "all truth surfaces agree" invariant. Without
    // an attestation the readiness builder never produces
    // `integrity_failed` because it never sets the integrity signals; a
    // clean open is downgraded to the honest `unchecked` (#331) and is
    // asserted separately above.
    let cases = [
        DoctorStorageSignals {
            db_file_present: true,
            db_open_failed: true,
            ..Default::default()
        },
        DoctorStorageSignals {
            db_file_present: true,
            lexical_index_drifted: true,
            ..Default::default()
        },
        DoctorStorageSignals {
            db_file_present: false,
            not_initialized: true,
            ..Default::default()
        },
        DoctorStorageSignals {
            db_file_present: false,
            ..Default::default()
        },
    ];
    for signals in cases {
        // DoctorStorageSignals is Copy, so classify() after the move is fine.
        let report = build_readiness_storage_integrity(signals, None);
        let (state, _) = signals.classify();
        assert_eq!(report.storage_state, state, "{signals:?}");
        // Risk is always derived from the state, never hand-set.
        assert_eq!(report.source_of_truth_risk, state.default_risk());
        assert!(report.all_checks_read_only());
        assert_ne!(report.storage_state, StorageState::IntegrityFailed);
    }
}

#[test]
fn timed_out_check_is_recorded() {
    let r = StorageIntegrityReport::derive(
        StorageState::UnknownDeferred,
        ArchiveReadability::TimedOut,
        vec![StorageCheck::timed_out("integrity_check", 5000)],
    );
    assert_eq!(r.source_of_truth_risk, SourceOfTruthRisk::Unknown);
    assert!(r.checks_attempted[0].timed_out);
    assert_eq!(r.archive_readability, ArchiveReadability::TimedOut);
}

#[test]
fn dedicated_probe_precedence_keeps_typed_busy_above_generic_open_failure() {
    let open_failed = StorageIntegrityReport::derive(
        StorageState::OpenreadFailed,
        ArchiveReadability::Unreadable,
        Vec::new(),
    );
    let busy = apply_dedicated_storage_probe(
        open_failed,
        DedicatedStorageProbe {
            busy_or_locked: true,
            ..Default::default()
        },
    );
    assert_eq!(busy.storage_state, StorageState::BusyOrLocked);
    assert_eq!(busy.source_of_truth_risk, SourceOfTruthRisk::Low);
    assert_eq!(busy.archive_readability, ArchiveReadability::NotChecked);

    let open_failed = StorageIntegrityReport::derive(
        StorageState::OpenreadFailed,
        ArchiveReadability::Unreadable,
        Vec::new(),
    );
    let schema = apply_dedicated_storage_probe(
        open_failed,
        DedicatedStorageProbe {
            schema_drift: true,
            ..Default::default()
        },
    );
    assert_eq!(schema.storage_state, StorageState::OpenreadFailed);

    let plausible_main_with_orphan = apply_dedicated_storage_probe(
        StorageIntegrityReport::derive(
            StorageState::OpenreadFailed,
            ArchiveReadability::Unreadable,
            Vec::new(),
        ),
        DedicatedStorageProbe {
            wal_sidecar_suspect: true,
            main_db_header_plausible: true,
            ..Default::default()
        },
    );
    assert_eq!(
        plausible_main_with_orphan.storage_state,
        StorageState::WalSidecarSuspect
    );

    let broken_main_with_sidecar = apply_dedicated_storage_probe(
        StorageIntegrityReport::derive(
            StorageState::OpenreadFailed,
            ArchiveReadability::Unreadable,
            Vec::new(),
        ),
        DedicatedStorageProbe {
            wal_sidecar_suspect: true,
            main_db_header_plausible: false,
            ..Default::default()
        },
    );
    assert_eq!(
        broken_main_with_sidecar.storage_state,
        StorageState::OpenreadFailed
    );
}

fn seed_schema_version(path: &std::path::Path, version: i64) -> anyhow::Result<()> {
    use crate::franken_sync::compat::{ConnectionExt as _, ParamValue};

    let storage = crate::storage::sqlite::FrankenStorage::open(path)?;
    storage.raw().execute_compat(
        "UPDATE meta SET value = ?1 WHERE key = 'schema_version'",
        &[ParamValue::from(version.to_string())],
    )?;
    storage.close_without_checkpoint()?;
    Ok(())
}

#[test]
fn dedicated_probe_distinguishes_openable_schema_drift_from_legacy_layout() -> anyhow::Result<()> {
    let schema_dir = tempfile::tempdir()?;
    let schema_db = schema_dir.path().join("agent_search.db");
    seed_schema_version(
        &schema_db,
        crate::storage::sqlite::CURRENT_SCHEMA_VERSION + 1,
    )?;
    let schema = probe_dedicated_storage_state(&schema_db, std::time::Duration::from_secs(1));
    assert!(schema.schema_drift);
    assert!(!schema.legacy_interop_failed);
    assert!(!schema.busy_or_locked);

    let legacy_dir = tempfile::tempdir()?;
    let legacy_db = legacy_dir.path().join("agent_search.db");
    seed_schema_version(
        &legacy_db,
        crate::storage::sqlite::MIN_IN_PLACE_MIGRATION_SCHEMA_VERSION - 1,
    )?;
    let legacy = probe_dedicated_storage_state(&legacy_db, std::time::Duration::from_secs(1));
    assert!(legacy.legacy_interop_failed);
    assert!(!legacy.schema_drift);
    assert!(!legacy.busy_or_locked);
    Ok(())
}

#[test]
fn dedicated_probe_skips_oversized_snapshot_without_mutating_canonical_bytes() -> anyhow::Result<()>
{
    use std::io::{Seek as _, SeekFrom, Write as _};

    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("agent_search.db");
    let mut db = std::fs::File::create(&db_path)?;
    db.write_all(b"SQLite format 3\0")?;
    db.seek(SeekFrom::Start(16))?;
    db.write_all(&4096_u16.to_be_bytes())?;
    db.set_len(SCHEMA_SNAPSHOT_MAX_BYTES + 1)?;
    drop(db);

    let before = blake3::hash(&std::fs::read(&db_path)?);
    let probe = probe_dedicated_storage_state(&db_path, std::time::Duration::from_secs(1));
    let after = blake3::hash(&std::fs::read(&db_path)?);

    assert_eq!(before, after, "oversized preflight must not rewrite the DB");
    assert!(!probe.schema_drift);
    assert!(!probe.legacy_interop_failed);
    let schema_check = probe
        .checks_attempted
        .iter()
        .find(|check| check.name == "schema_version")
        .expect("schema check must report why it was skipped");
    assert_eq!(
        schema_check.skipped_reason.as_deref(),
        Some(SCHEMA_SNAPSHOT_OVERSIZED_REASON)
    );
    assert!(!schema_check.timed_out);
    Ok(())
}

#[test]
fn dedicated_probe_requires_orphan_or_malformed_sidecar_evidence() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("agent_search.db");
    std::fs::write(&db_path, b"SQLite format 3\0diagnostic")?;
    let wal_path = wal_sidecar_path(&db_path);
    let shm_path = shm_sidecar_path(&db_path);

    std::fs::write(&wal_path, [])?;
    assert!(
        !wal_sidecars_are_structurally_suspect(&db_path),
        "mere WAL presence is not suspect"
    );

    // An SHM without its source WAL is an actual orphan signal.
    std::fs::write(&shm_path, [0_u8; 32])?;
    let renamed_wal = dir.path().join("retained-wal-fixture");
    std::fs::rename(&wal_path, renamed_wal)?;
    assert!(wal_sidecars_are_structurally_suspect(&db_path));
    Ok(())
}
