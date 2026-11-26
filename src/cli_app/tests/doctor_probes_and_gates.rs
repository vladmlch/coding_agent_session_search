#[allow(unused_imports)]
pub(super) use super::*;

#[cfg(test)]
mod doctor_probe_timeout_tests {
    use super::doctor_probe_timeout_for_bundle;

    #[test]
    fn budget_scales_with_started_gib_and_is_capped() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let secs = |bytes: u64| doctor_probe_timeout_for_bundle(bytes).as_secs();
        // (bundle bytes, expected seconds): 30s base + 20s per started GiB,
        // capped at 600s. 1.4GB is the reporter's healthy archive -> 70s.
        let table = [
            (0_u64, 30_u64),
            (200 * 1024 * 1024, 50),
            (1_400_000_000, 70),
            (9 * GIB, 210),
            (100 * GIB, 600),
            (u64::MAX, 600),
        ];
        for (bytes, want) in table {
            assert!(
                secs(bytes).cmp(&want).is_eq(),
                "{bytes} bytes -> {} s",
                secs(bytes)
            );
        }
    }
}

#[cfg(test)]
#[test]
fn doctor_deferred_archive_recommended_action_names_the_next_command_for_each_cause() {
    let reason =
        "archive_wide_collectors_deferred: archive database open did not complete successfully";
    let action = |staged, buildable, busy, timed_out, deferred| {
        doctor_deferred_archive_recommended_action(
            reason, staged, buildable, busy, timed_out, deferred,
        )
    };
    assert!(action(false, false, false, false, false).starts_with(reason));
    // A staged candidate outranks building another one, which outranks
    // waiting on a holder; the cause-specific reruns come after both.
    let staged = action(true, true, true, true, true);
    assert!(
        staged.contains("'cass doctor repair --dry-run --json'"),
        "{staged}"
    );
    assert!(!staged.contains("'cass doctor --fix --json'"), "{staged}");
    let buildable = action(false, true, true, true, true);
    assert!(
        buildable.contains("'cass doctor --fix --json'"),
        "{buildable}"
    );
    assert!(
        buildable.contains("'cass doctor repair --dry-run --json'"),
        "{buildable}"
    );
    let busy = action(false, false, true, true, true);
    assert!(busy.contains("holds the archive"), "{busy}");
    assert!(busy.contains("'cass doctor check --json'"), "{busy}");
    let timed_out = action(false, false, false, true, true);
    assert!(
        timed_out.contains("CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS"),
        "{timed_out}"
    );
    assert!(
        !timed_out.contains("CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE"),
        "{timed_out}"
    );
    let deferred = action(false, false, false, false, true);
    assert!(
        deferred.contains("CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE=1"),
        "{deferred}"
    );
    // With no repair authority and no transient cause there is no command to
    // name yet; the previous guidance stays.
    assert!(
        action(false, false, false, false, false)
            .ends_with("Run process-isolated archive coverage probes before archive repair.")
    );
}

#[cfg(test)]
#[test]
fn doctor_franken_deep_integrity_size_gate_has_an_exact_boundary() {
    assert!(
        doctor_franken_deep_integrity_skip_reason_for_bytes(
            DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES,
            false
        )
        .is_none()
    );
    let reason = doctor_franken_deep_integrity_skip_reason_for_bytes(
        DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES + 1,
        false,
    )
    .expect("one byte above the bounded budget must defer the full-page PRAGMA");
    assert!(reason.contains("full_page_integrity_probe_deferred"));
    assert!(
        reason.contains(CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE),
        "the deferral must name its own override (GH #390): {reason}"
    );
}

#[cfg(test)]
#[test]
fn doctor_franken_deep_integrity_size_gate_is_lifted_by_explicit_opt_in() {
    assert!(
        doctor_franken_deep_integrity_skip_reason_for_bytes(
            DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES * 8,
            true
        )
        .is_none(),
        "GH #390: an explicit full-probe request must lift the byte cap"
    );
}

#[cfg(test)]
#[test]
fn doctor_franken_deep_integrity_size_gate_counts_the_wal_bundle() {
    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("agent_search.db");
    std::fs::File::create(&db_path)
        .and_then(|file| file.set_len(DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES - 1))
        .expect("create sparse main DB fixture");
    assert!(doctor_franken_deep_integrity_skip_reason(&db_path).is_none());

    let wal_path = temp.path().join("agent_search.db-wal");
    std::fs::File::create(wal_path)
        .and_then(|file| file.set_len(2))
        .expect("create sparse WAL fixture");
    let reason = doctor_franken_deep_integrity_skip_reason(&db_path)
        .expect("main plus WAL above the budget must defer the full-page PRAGMA");
    assert!(reason.contains("268435457 bytes"));
}

#[cfg(test)]
#[test]
fn doctor_deferred_integrity_never_implies_corruption_or_destructive_rebuild() {
    let deferred_message = "full_page_integrity_probe_deferred: bounded probe skipped";
    assert_eq!(
        doctor_anomaly_for_check("database", "warn", deferred_message),
        DoctorAnomaly::RepairBlocked
    );
    assert_eq!(
        doctor_anomaly_for_check(
            "database",
            "fail",
            "cached quick_check attestation records structural failure; full_page_integrity_probe_deferred"
        ),
        DoctorAnomaly::ArchiveDbCorrupt,
        "a current cached FAIL must not be hidden by the deferral marker"
    );
    assert_eq!(
        doctor_anomaly_for_check(
            "database",
            "fail",
            "Database failed SQLite quick_check: malformed page"
        ),
        DoctorAnomaly::ArchiveDbCorrupt,
        "a completed affirmative live integrity failure remains corruption evidence"
    );
    assert!(doctor_archive_queryable_for_non_destructive_derived_rebuild(false, true, true, false));
    assert!(!doctor_archive_queryable_for_non_destructive_derived_rebuild(false, true, true, true));
    assert!(
        !doctor_archive_queryable_for_non_destructive_derived_rebuild(false, true, false, false)
    );
    assert!(!doctor_canonical_candidate_replacement_authorized(
        true, false, false
    ));
    assert!(doctor_canonical_candidate_replacement_authorized(
        true, false, true
    ));
    assert!(doctor_canonical_candidate_replacement_authorized(
        false, false, false
    ));
    assert_eq!(
        doctor_anomaly_for_check(
            "database",
            "timeout",
            "Database health probe timed out; archive health is unverified"
        ),
        DoctorAnomaly::RepairBlocked,
        "a timeout is diagnostic uncertainty, not evidence that the archive is unreadable"
    );
    for unverified_message in [
        "Database health probe failed via SQLite: running PRAGMA quick_check(1): unsupported page state",
        "Database health probe failed via SQLite: running PRAGMA integrity_check: unsupported page state",
        "Database row counts completed but no integrity verdict was returned",
        "Database query failed",
    ] {
        assert_eq!(
            doctor_anomaly_for_check("database", "fail", unverified_message),
            DoctorAnomaly::RepairBlocked,
            "an unverified probe is diagnostic uncertainty, not replacement authority: {unverified_message}"
        );
    }
}

#[cfg(test)]
#[test]
fn doctor_open_failure_authority_accepts_only_affirmative_corruption_classes() -> Result<(), String>
{
    let db_open_error = |message: &str| CliError {
        code: 9,
        kind: CliErrorKind::DbOpen.kind_str(),
        message: message.to_string(),
        hint: None,
        retryable: true,
    };
    for message in [
        "file is too small to contain a SQLite header (possible corruption)",
        "file header is not SQLite format 3 (possible corruption)",
        "database disk image is malformed",
        "file is not a database",
    ] {
        if !doctor_db_open_error_is_affirmative_integrity_failure(&db_open_error(message)) {
            return Err(format!(
                "deterministic corruption-class open failure must authorize candidate recovery: {message}"
            ));
        }
    }
    for message in [
        "open timed out after 30s (possible corruption or lock contention)",
        "database is busy",
        "permission denied",
        "generic database open failed",
    ] {
        if doctor_db_open_error_is_affirmative_integrity_failure(&db_open_error(message)) {
            return Err(format!(
                "unknown/transient open failure must remain fail-closed: {message}"
            ));
        }
    }
    Ok(())
}

/// 9v9xi: a path that does not exist cannot be canonicalized, so it must still
/// compare with an existing one in the same spelling (Windows `\\?\` prefix,
/// macOS `/var` -> `/private/var`).
#[cfg(test)]
mod path_spelling_tests {
    use super::*;

    #[test]
    fn session_workspace_filter_matches_a_stored_workspace_that_no_longer_exists() {
        let temp = tempfile::tempdir().expect("tempdir");
        // Resolve the temp dir's own symlinks first, so only the spelling the
        // filter produces can differ.
        let base = dunce::canonicalize(temp.path()).expect("resolve temp dir");
        let target = normalize_session_filter_path(&base).expect("filter target");
        let deleted = base.join("deleted-project");
        assert!(!deleted.exists());
        assert_eq!(workspace_match_distance(Some(&base), &target), Some(0));
        assert_eq!(workspace_match_distance(Some(&deleted), &target), Some(1));
        let sibling = PathBuf::from(format!("{}-sibling", base.display()));
        assert_eq!(workspace_match_distance(Some(&sibling), &target), None);
    }

    #[test]
    fn scratch_data_dir_is_recognized_before_it_exists() {
        let temp = tempfile::tempdir().expect("tempdir");
        let data_dir = temp.path().join("not-created-yet").join("cass-data");
        assert!(!data_dir.exists());
        assert!(auto_refresh_is_scratch_data_dir(&data_dir));
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        assert!(auto_refresh_is_scratch_data_dir(&data_dir));
        // The root of the temp dir's filesystem is not scratch space.
        let root = std::env::temp_dir()
            .ancestors()
            .last()
            .expect("temp dir root")
            .to_path_buf();
        let outside = root.join(format!("cass-not-scratch-{}", std::process::id()));
        assert!(!outside.exists());
        assert!(!auto_refresh_is_scratch_data_dir(&outside));
    }
}

#[cfg(test)]
mod source_ingest_reuse_capability_tests {
    use super::SourceIngestReuseCapability;

    /// The compiled policy and its reason never disagree: reuse is on exactly
    /// when build.rs found no unpinned package to blame, and the reason field
    /// is always present so agents can branch on it.
    #[test]
    fn compiled_reuse_policy_names_a_blocker_exactly_when_disabled() {
        let policy = SourceIngestReuseCapability::compiled();
        assert_eq!(policy.enabled, policy.disabled_by.is_none(), "{policy:?}");
        let json = serde_json::to_value(&policy).unwrap();
        assert!(json.get("disabled_by").is_some(), "{json}");
        assert_eq!(json["enabled"], policy.enabled);
    }
}
