use super::super::*;
use super::*;

#[test]
fn maintenance_mode_round_trips_lock_values() {
    for mode in [
        SearchMaintenanceMode::Index,
        SearchMaintenanceMode::WatchStartup,
        SearchMaintenanceMode::Watch,
        SearchMaintenanceMode::WatchOnce,
    ] {
        assert_eq!(
            SearchMaintenanceMode::parse_lock_value(mode.as_lock_value()),
            Some(mode)
        );
    }
}

#[test]
fn maintenance_job_kind_round_trips_lock_values() {
    for kind in [
        SearchMaintenanceJobKind::LexicalRefresh,
        SearchMaintenanceJobKind::SemanticAcquire,
        SearchMaintenanceJobKind::SemanticRebuild,
        SearchMaintenanceJobKind::AnalyticsRebuild,
    ] {
        assert_eq!(
            SearchMaintenanceJobKind::parse_lock_value(kind.as_lock_value()),
            Some(kind)
        );
    }
}

#[test]
fn issue_342_semantic_hnsw_is_not_mislabeled_as_stalled_lexical_work() {
    let now_ms = 1_733_000_300_000_i64;
    let mut snapshot = SearchMaintenanceSnapshot {
        active: true,
        mode: Some(SearchMaintenanceMode::Index),
        job_kind: Some(SearchMaintenanceJobKind::SemanticRebuild),
        phase: Some("semantic:hnsw".to_string()),
        last_progress_at_ms: Some(now_ms - 300_000),
        ..SearchMaintenanceSnapshot::default()
    };

    assert_eq!(maintenance_stall_age_ms(&snapshot, now_ms), None);

    snapshot.phase = Some("semantic:embedding".to_string());
    assert_eq!(
        maintenance_stall_age_ms(&snapshot, now_ms),
        Some(300_000),
        "measured semantic phases must retain stale-progress diagnostics"
    );
}

#[test]
fn gh422_stale_lock_metadata_is_ignored_without_mutating_it_on_read() {
    // Regression for issue #176: the TUI used to see a permanent
    // `orphaned: true` state when the index-run.lock file contained
    // metadata from a crashed process. A successful shared-lock probe now
    // returns a clean snapshot without rewriting the file, preserving both
    // the old spin fix and search's strict read-only contract.
    let temp = tempfile::tempdir().expect("tempdir");
    let lock_path = temp.path().join("index-run.lock");
    let stale_metadata = concat!(
        "pid=4242\n",
        "started_at_ms=1733000111000\n",
        "updated_at_ms=1733000112000\n",
        "db_path=/tmp/cass/agent_search.db\n",
        "mode=index\n",
        "job_id=lexical-refresh-1733000111000-4242\n",
        "job_kind=lexical_refresh\n",
        "phase=rebuilding\n"
    );
    std::fs::write(&lock_path, stale_metadata).expect("write lock metadata");

    let snapshot = read_search_maintenance_snapshot(temp.path());
    assert!(!snapshot.active, "no owner, must not be reported active");
    assert!(
        !snapshot.orphaned,
        "stale metadata must be ignored, not reported as orphaned"
    );
    assert!(
        snapshot.pid.is_none(),
        "stale pid must not escape the probe"
    );
    assert!(
        snapshot.job_id.is_none(),
        "stale job_id must not escape the probe"
    );
    assert!(
        snapshot.phase.is_none(),
        "stale phase must not escape the probe"
    );
    assert_eq!(
        std::fs::read(&lock_path).expect("read unchanged lock metadata"),
        stale_metadata.as_bytes(),
        "the PURE probe (gh#422 search paths) must preserve every byte"
    );

    // Second read also returns a clean default snapshot.
    let snapshot2 = read_search_maintenance_snapshot(temp.path());
    assert!(!snapshot2.active);
    assert!(!snapshot2.orphaned);
    assert_eq!(
        std::fs::read(&lock_path).expect("read lock metadata after second probe"),
        stale_metadata.as_bytes(),
        "repeated pure observation must remain byte-for-byte read-only"
    );

    // #176 / bd-k9jb9 contract: the REAPING probe (status/health/TUI
    // observation surfaces) truncates the stale metadata in place —
    // never deleting the file — so subsequent readers observe a clean
    // lock without re-doing the staleness dance.
    let reaped = read_search_maintenance_snapshot_reaping(temp.path());
    assert!(!reaped.active);
    assert!(!reaped.orphaned);
    assert_eq!(
        std::fs::metadata(&lock_path)
            .expect("stat lock after reaping probe")
            .len(),
        0,
        "stale lock metadata must be truncated in place by the reaping probe"
    );
    assert!(
        lock_path.exists(),
        "the lock file must be truncated, never deleted"
    );
    let after_reap = read_search_maintenance_snapshot_reaping(temp.path());
    assert!(!after_reap.active);
    assert!(!after_reap.orphaned);
}

#[test]
fn live_owner_metadata_is_preserved_when_flock_is_held() -> std::io::Result<()> {
    // When the lock is actually held by a live owner, the snapshot
    // must report the owner faithfully and must NOT reap the file.
    use fs2::FileExt;
    let temp = tempfile::tempdir()?;
    let lock_path = temp.path().join("index-run.lock");
    let owner_pid = active_lock_fixture_pid(4242);
    let mut owner = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)?;
    owner.try_lock_exclusive()?;
    // Write metadata while holding the lock, matching acquire_index_run_lock's order.
    write_locked_metadata(
        &lock_path,
        &mut owner,
        format!(
            concat!(
                "pid={}\n",
                "started_at_ms=1733000111000\n",
                "updated_at_ms=1733000112000\n",
                "db_path=/tmp/cass/agent_search.db\n",
                "mode=index\n",
                "job_id=lexical-refresh-1733000111000-4242\n",
                "job_kind=lexical_refresh\n",
                "phase=rebuilding\n"
            ),
            owner_pid
        )
        .as_str(),
    )?;

    let snapshot = read_search_maintenance_snapshot(temp.path());
    assert!(snapshot.active, "live owner must be reported active");
    assert!(!snapshot.orphaned);
    assert_eq!(snapshot.pid, Some(owner_pid));
    assert_eq!(
        snapshot.job_id.as_deref(),
        Some("lexical-refresh-1733000111000-4242")
    );
    assert_eq!(
        snapshot.job_kind,
        Some(SearchMaintenanceJobKind::LexicalRefresh)
    );
    assert_eq!(snapshot.phase.as_deref(), Some("rebuilding"));
    assert_eq!(snapshot.updated_at_ms, Some(1_733_000_112_000));

    // Metadata must still be present — reaping must NOT have happened.
    let post = std::fs::metadata(&lock_path)?;
    assert!(post.len() > 0, "live-owner metadata must not be truncated");

    FileExt::unlock(&owner)?;
    Ok(())
}

/// Coordination outcome layer must also degrade to `Stale` when
/// forward progress is stuck — search-side single-flight callers
/// then route around the wedged worker instead of attaching to it.
#[test]
fn coordination_reports_stale_when_forward_progress_is_stuck() {
    let now_ms: i64 = 1_733_000_300_000;
    let snapshot = SearchMaintenanceSnapshot {
        active: true,
        pid: Some(12345),
        started_at_ms: Some(now_ms - 600_000),
        db_path: Some(PathBuf::from("/tmp/cass/agent_search.db")),
        mode: Some(SearchMaintenanceMode::WatchStartup),
        job_id: Some("lexical_refresh-1-12345".to_string()),
        job_kind: Some(SearchMaintenanceJobKind::LexicalRefresh),
        phase: Some("watch_startup".to_string()),
        updated_at_ms: Some(now_ms - 500),
        last_progress_at_ms: Some(now_ms - 300_000),
        orphaned: false,
    };

    let outcome = evaluate_maintenance_coordination_from_snapshot(&snapshot, now_ms);
    match outcome {
        MaintenanceCoordinationOutcome::Stale { ref reason, .. } => {
            assert!(
                reason.contains("forward progress")
                    && (reason.contains("#258") || reason.contains("issue #258")),
                "stalled coordination reason should mention forward progress and #258: {reason}",
            );
        }
        other => assert!(
            matches!(other, MaintenanceCoordinationOutcome::Stale { .. }),
            "stalled forward-progress snapshot must coordinate as Stale, got {other:?}",
        ),
    }
}

// -----------------------------------------------------------------------
// Maintenance coordination tests
// -----------------------------------------------------------------------

fn make_active_snapshot(now_ms: i64) -> SearchMaintenanceSnapshot {
    SearchMaintenanceSnapshot {
        active: true,
        pid: Some(12345),
        started_at_ms: Some(now_ms - 5_000),
        db_path: Some(PathBuf::from("/tmp/cass/agent_search.db")),
        mode: Some(SearchMaintenanceMode::Index),
        job_id: Some("lexical_refresh-1000-12345".to_string()),
        job_kind: Some(SearchMaintenanceJobKind::LexicalRefresh),
        phase: Some("scanning".to_string()),
        updated_at_ms: Some(now_ms - 500),
        last_progress_at_ms: Some(now_ms - 500),
        orphaned: false,
    }
}

#[test]
fn coordination_no_active_job_when_snapshot_inactive() {
    let snapshot = SearchMaintenanceSnapshot::default();
    let outcome = evaluate_maintenance_coordination_from_snapshot(&snapshot, 1_733_000_000_000);
    assert_eq!(outcome, MaintenanceCoordinationOutcome::Idle);
}

#[test]
fn coordination_tracks_active_legacy_lock_without_job_id() {
    let snapshot = SearchMaintenanceSnapshot {
        active: true,
        pid: Some(12345),
        job_id: None,
        mode: Some(SearchMaintenanceMode::Index),
        ..Default::default()
    };
    let outcome = evaluate_maintenance_coordination_from_snapshot(&snapshot, 1_733_000_000_000);
    if let MaintenanceCoordinationOutcome::Active {
        ref job_id,
        job_kind,
        ..
    } = outcome
    {
        assert_eq!(job_id, "index-active-lock-12345");
        assert_eq!(job_kind, SearchMaintenanceJobKind::LexicalRefresh);
    } else {
        assert!(
            matches!(outcome, MaintenanceCoordinationOutcome::Active { .. }),
            "legacy active lock must remain active, got {outcome:?}"
        );
    }
}

#[test]
fn coordination_active_job_with_fresh_heartbeat() {
    let now_ms = 1_733_000_000_000i64;
    let snapshot = make_active_snapshot(now_ms);
    let outcome = evaluate_maintenance_coordination_from_snapshot(&snapshot, now_ms);
    if let MaintenanceCoordinationOutcome::Active {
        ref job_id,
        ref phase,
        ..
    } = outcome
    {
        assert_eq!(job_id, "lexical_refresh-1000-12345");
        assert_eq!(phase.as_deref(), Some("scanning"));
    } else {
        assert!(
            matches!(outcome, MaintenanceCoordinationOutcome::Active { .. }),
            "expected ActiveJob, got {outcome:?}"
        );
    }
}

#[test]
fn coordination_stale_job_with_old_heartbeat() {
    let now_ms = 1_733_000_000_000i64;
    let snapshot = SearchMaintenanceSnapshot {
        updated_at_ms: Some(now_ms - 60_000),
        ..make_active_snapshot(now_ms)
    };
    let outcome = evaluate_maintenance_coordination_from_snapshot(&snapshot, now_ms);
    if let MaintenanceCoordinationOutcome::Stale {
        ref job_id,
        ref reason,
    } = outcome
    {
        assert_eq!(job_id, "lexical_refresh-1000-12345");
        assert!(reason.contains("60000ms"), "reason={reason}");
    } else {
        assert!(
            matches!(outcome, MaintenanceCoordinationOutcome::Stale { .. }),
            "expected StaleJob, got {outcome:?}"
        );
    }
}

#[test]
fn coordination_missing_heartbeat_timestamp_still_respects_active_flock() {
    let now_ms = 1_733_000_000_000i64;
    let snapshot = SearchMaintenanceSnapshot {
        updated_at_ms: None,
        ..make_active_snapshot(now_ms)
    };
    let outcome = evaluate_maintenance_coordination_from_snapshot(&snapshot, now_ms);
    if let MaintenanceCoordinationOutcome::Active { updated_at_ms, .. } = outcome {
        assert_eq!(updated_at_ms, now_ms);
    } else {
        assert!(
            matches!(outcome, MaintenanceCoordinationOutcome::Active { .. }),
            "missing heartbeat metadata must not hide an active flock, got {outcome:?}"
        );
    }
}

#[test]
fn decision_launch_when_no_job() {
    let snapshot = SearchMaintenanceSnapshot::default();
    let decision = decide_maintenance_action_from_snapshot(&snapshot, 1_733_000_000_000);
    assert_eq!(decision, MaintenanceDecision::Launch);
}

#[test]
fn decision_launch_when_no_lock_file() {
    let temp = tempfile::tempdir().expect("tempdir");
    let decision = decide_maintenance_action(temp.path(), 1_733_000_000_000);
    assert_eq!(decision, MaintenanceDecision::Launch);
}

#[test]
fn decision_attaches_when_active_lock_has_stale_heartbeat() {
    let now_ms = 1_733_000_000_000i64;
    let snapshot = SearchMaintenanceSnapshot {
        updated_at_ms: Some(now_ms - 60_000),
        ..make_active_snapshot(now_ms)
    };
    let decision = decide_maintenance_action_from_snapshot(&snapshot, now_ms);
    if let MaintenanceDecision::AttachOrWait {
        ref job_id,
        ref phase,
        elapsed_ms,
        ..
    } = decision
    {
        assert_eq!(job_id, "lexical_refresh-1000-12345");
        assert_eq!(phase.as_deref(), Some("scanning"));
        assert_eq!(elapsed_ms, 5_000);
    } else {
        assert!(
            matches!(decision, MaintenanceDecision::AttachOrWait { .. }),
            "stale heartbeat still has an active lock, got {decision:?}"
        );
    }
}

#[test]
fn decision_attach_when_active_fresh_job() {
    let now_ms = 1_733_000_000_000i64;
    let snapshot = make_active_snapshot(now_ms);
    let decision = decide_maintenance_action_from_snapshot(&snapshot, now_ms);
    if let MaintenanceDecision::AttachOrWait {
        ref job_id,
        elapsed_ms,
        ..
    } = decision
    {
        assert_eq!(job_id, "lexical_refresh-1000-12345");
        assert_eq!(elapsed_ms, 5_000);
    } else {
        assert!(
            matches!(decision, MaintenanceDecision::AttachOrWait { .. }),
            "expected AttachOrWait, got {decision:?}"
        );
    }
}

#[test]
fn poll_returns_immediately_when_no_active_job() {
    let temp = tempfile::tempdir().expect("tempdir");
    let result = poll_maintenance_until_idle(
        temp.path(),
        Some(Duration::from_millis(500)),
        Some(Duration::from_millis(50)),
    );
    assert!(!result.timed_out);
    assert_eq!(result.polls, 1);
    assert!(
        matches!(result.outcome, MaintenanceCoordinationOutcome::Idle),
        "expected NoActiveJob"
    );
    assert!(
        result.elapsed <= Duration::from_millis(500),
        "immediate idle poll should finish before timeout, elapsed={:?}",
        result.elapsed
    );
}

#[test]
fn poll_returns_active_on_timeout_when_lock_held() {
    use fs2::FileExt;
    let temp = tempfile::tempdir().expect("tempdir");
    let lock_path = temp.path().join("index-run.lock");
    let now_ms = crate::storage::sqlite::FrankenStorage::now_millis();
    let pid = active_lock_fixture_pid(99999);
    let mut owner = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)
        .expect("open owner handle");
    owner.try_lock_exclusive().expect("acquire lock");
    write_locked_metadata(
        &lock_path,
        &mut owner,
        format!(
            "pid={pid}\nstarted_at_ms={}\nupdated_at_ms={}\ndb_path=/tmp/test.db\nmode=index\njob_id=test-job-1\njob_kind=lexical_refresh\nphase=scanning\n",
            now_ms - 1_000,
            now_ms,
        )
        .as_str(),
    )
    .expect("write lock metadata");

    let result = poll_maintenance_until_idle(
        temp.path(),
        Some(Duration::from_millis(300)),
        Some(Duration::from_millis(50)),
    );
    assert!(result.timed_out, "should time out when lock is held");
    assert!(result.polls >= 2, "should have polled multiple times");
    assert!(
        matches!(
            result.outcome,
            MaintenanceCoordinationOutcome::Active { .. }
        ),
        "expected ActiveJob on timeout"
    );

    let _ = FileExt::unlock(&owner);
}

#[test]
fn poll_times_out_instead_of_declaring_stale_held_lock_idle() {
    use fs2::FileExt;
    let temp = tempfile::tempdir().expect("tempdir");
    let lock_path = temp.path().join("index-run.lock");
    let now_ms = crate::storage::sqlite::FrankenStorage::now_millis();
    let pid = active_lock_fixture_pid(99999);
    let mut owner = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)
        .expect("open owner handle");
    owner.try_lock_exclusive().expect("acquire lock");
    write_locked_metadata(
        &lock_path,
        &mut owner,
        format!(
            "pid={pid}\nstarted_at_ms={}\nupdated_at_ms={}\ndb_path=/tmp/test.db\nmode=index\njob_id=test-job-stale\njob_kind=lexical_refresh\nphase=scanning\n",
            now_ms - 120_000,
            now_ms - 120_000,
        )
        .as_str(),
    )
    .expect("write lock metadata");

    let result = poll_maintenance_until_idle(
        temp.path(),
        Some(Duration::from_millis(150)),
        Some(Duration::from_millis(25)),
    );
    assert!(result.timed_out, "held stale lock is still not idle");
    assert!(
        matches!(result.outcome, MaintenanceCoordinationOutcome::Stale { .. }),
        "expected stale held lock on timeout, got {:?}",
        result.outcome
    );

    let _ = FileExt::unlock(&owner);
}

#[test]
fn poll_detects_release_mid_wait() {
    use fs2::FileExt;
    let temp = tempfile::tempdir().expect("tempdir");
    let lock_path = temp.path().join("index-run.lock");
    let now_ms = crate::storage::sqlite::FrankenStorage::now_millis();
    let pid = active_lock_fixture_pid(99999);
    let mut owner = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)
        .expect("open owner handle");
    owner.try_lock_exclusive().expect("acquire lock");
    write_locked_metadata(
        &lock_path,
        &mut owner,
        format!(
            "pid={pid}\nstarted_at_ms={}\nupdated_at_ms={}\ndb_path=/tmp/test.db\nmode=index\njob_id=test-job-2\njob_kind=lexical_refresh\nphase=committing\n",
            now_ms - 1_000,
            now_ms,
        )
        .as_str(),
    )
    .expect("write lock metadata");

    let temp_path = temp.path().to_path_buf();
    let lock_path_for_release = lock_path.clone();
    let release_thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        let _ = owner.set_len(0);
        let _ = clear_index_run_lock_metadata_sidecar(&lock_path_for_release);
        let _ = FileExt::unlock(&owner);
        drop(owner);
    });

    let result = poll_maintenance_until_idle(
        &temp_path,
        Some(Duration::from_secs(2)),
        Some(Duration::from_millis(50)),
    );
    assert!(!result.timed_out, "should detect release before timeout");
    release_thread.join().expect("release thread");
}

#[test]
fn failopen_returns_failopen_when_lexical_available_and_job_active() {
    let temp = tempfile::tempdir().expect("tempdir");
    let lock_path = temp.path().join("index-run.lock");
    let now_ms = crate::storage::sqlite::FrankenStorage::now_millis();
    let pid = active_lock_fixture_pid(99999);

    use fs2::FileExt;
    let mut owner = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)
        .expect("open owner handle");
    owner.try_lock_exclusive().expect("acquire lock");
    write_locked_metadata(
        &lock_path,
        &mut owner,
        format!(
            "pid={pid}\nstarted_at_ms={}\nupdated_at_ms={}\ndb_path=/tmp/test.db\nmode=index\njob_id=fo-job-1\njob_kind=lexical_refresh\nphase=indexing\n",
            now_ms - 1_000,
            now_ms,
        )
        .as_str(),
    )
    .expect("write lock metadata");

    let decision = decide_search_failopen(temp.path(), now_ms, true);
    if let MaintenanceDecision::FailOpen { ref reason } = decision {
        assert!(reason.contains("fo-job-1"), "reason={reason}");
        assert!(reason.contains("failing open"), "reason={reason}");
    } else {
        assert!(
            matches!(decision, MaintenanceDecision::FailOpen { .. }),
            "expected FailOpen, got {decision:?}"
        );
    }

    let decision_no_lexical = decide_search_failopen(temp.path(), now_ms, false);
    assert!(
        matches!(
            decision_no_lexical,
            MaintenanceDecision::AttachOrWait { .. }
        ),
        "without lexical must attach, got {decision_no_lexical:?}"
    );

    let _ = FileExt::unlock(&owner);
}

#[test]
fn failopen_handles_active_stale_heartbeat_without_launching_repair() {
    let temp = tempfile::tempdir().expect("tempdir");
    let lock_path = temp.path().join("index-run.lock");
    let now_ms = crate::storage::sqlite::FrankenStorage::now_millis();
    let pid = active_lock_fixture_pid(99999);

    use fs2::FileExt;
    let mut owner = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)
        .expect("open owner handle");
    owner.try_lock_exclusive().expect("acquire lock");
    write_locked_metadata(
        &lock_path,
        &mut owner,
        format!(
            "pid={pid}\nstarted_at_ms={}\nupdated_at_ms={}\ndb_path=/tmp/test.db\nmode=index\njob_id=fo-stale-1\njob_kind=lexical_refresh\nphase=indexing\n",
            now_ms - 120_000,
            now_ms - 120_000,
        )
        .as_str(),
    )
    .expect("write lock metadata");

    let decision = decide_search_failopen(temp.path(), now_ms, true);
    if let MaintenanceDecision::FailOpen { ref reason } = decision {
        assert!(reason.contains("fo-stale-1"), "reason={reason}");
        assert!(reason.contains("stale heartbeat"), "reason={reason}");
        assert!(reason.contains("failing open"), "reason={reason}");
    } else {
        assert!(
            matches!(decision, MaintenanceDecision::FailOpen { .. }),
            "expected FailOpen for searchable stale active lock, got {decision:?}"
        );
    }

    let decision_no_lexical = decide_search_failopen(temp.path(), now_ms, false);
    assert!(
        matches!(
            decision_no_lexical,
            MaintenanceDecision::AttachOrWait { .. }
        ),
        "without lexical must wait for the held lock, got {decision_no_lexical:?}"
    );

    let _ = FileExt::unlock(&owner);
}

// -----------------------------------------------------------------------
// ibuuh.22: Event log, yield signaling, unified view tests
// -----------------------------------------------------------------------

#[test]
fn event_log_append_and_read() {
    let temp = tempfile::tempdir().expect("tempdir");
    let event = MaintenanceEvent {
        timestamp_ms: 1_733_000_000_000,
        job_id: "test-job-1".to_string(),
        actor_pid: 42,
        kind: MaintenanceEventKind::Started {
            job_kind: "lexical_refresh".to_string(),
            phase: "scanning".to_string(),
        },
    };
    append_maintenance_event(temp.path(), &event).expect("append");
    let events = read_maintenance_events(temp.path(), None, None);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].job_id, "test-job-1");
    assert_eq!(events[0].actor_pid, 42);
    assert!(matches!(
        events[0].kind,
        MaintenanceEventKind::Started { .. }
    ));
}

#[test]
fn event_log_filters_by_timestamp() {
    let temp = tempfile::tempdir().expect("tempdir");
    for i in 0..5 {
        let event = MaintenanceEvent {
            timestamp_ms: 1_000 + i,
            job_id: format!("job-{i}"),
            actor_pid: 1,
            kind: MaintenanceEventKind::Progress {
                processed: i as u64,
                total: 5,
            },
        };
        append_maintenance_event(temp.path(), &event).expect("append");
    }
    let events = read_maintenance_events(temp.path(), Some(1_002), None);
    assert_eq!(events.len(), 2, "should only get events after ts 1002");
    assert_eq!(events[0].timestamp_ms, 1_003);
    assert_eq!(events[1].timestamp_ms, 1_004);
}

#[test]
fn event_log_respects_limit() {
    let temp = tempfile::tempdir().expect("tempdir");
    for i in 0..10 {
        let event = MaintenanceEvent {
            timestamp_ms: 1_000 + i,
            job_id: format!("job-{i}"),
            actor_pid: 1,
            kind: MaintenanceEventKind::Resumed,
        };
        append_maintenance_event(temp.path(), &event).expect("append");
    }
    let events = read_maintenance_events(temp.path(), None, Some(3));
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].timestamp_ms, 1_007);
    assert_eq!(events[2].timestamp_ms, 1_009);
}

#[test]
fn event_log_returns_empty_when_missing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let events = read_maintenance_events(temp.path(), None, None);
    assert!(events.is_empty());
}

#[test]
fn event_log_truncation_retains_tail() {
    let temp = tempfile::tempdir().expect("tempdir");
    for i in 0..550 {
        let event = MaintenanceEvent {
            timestamp_ms: i,
            job_id: format!("job-{i}"),
            actor_pid: 1,
            kind: MaintenanceEventKind::Resumed,
        };
        append_maintenance_event(temp.path(), &event).expect("append");
    }
    let before = read_maintenance_events(temp.path(), None, Some(600));
    assert_eq!(before.len(), 550);
    truncate_maintenance_event_log(temp.path()).expect("truncate");
    let after = read_maintenance_events(temp.path(), None, Some(600));
    assert_eq!(after.len(), MAX_EVENT_LOG_ENTRIES);
    assert_eq!(after[0].timestamp_ms, 50);
    assert_eq!(after[MAX_EVENT_LOG_ENTRIES - 1].timestamp_ms, 549);
}

#[test]
fn yield_signal_round_trip() {
    let temp = tempfile::tempdir().expect("tempdir");
    assert!(
        check_yield_requested(temp.path()).is_none(),
        "no signal initially"
    );
    request_yield(temp.path(), "foreground search pressure").expect("request yield");
    let req = check_yield_requested(temp.path()).expect("yield should be present");
    assert_eq!(req.requester_pid, std::process::id());
    assert_eq!(req.reason, "foreground search pressure");
    assert!(req.requested_at_ms > 0);
    clear_yield_signal(temp.path()).expect("clear");
    assert!(
        check_yield_requested(temp.path()).is_none(),
        "signal cleared"
    );
}

#[test]
fn clear_yield_signal_is_idempotent() {
    let temp = tempfile::tempdir().expect("tempdir");
    clear_yield_signal(temp.path()).expect("clear nonexistent");
    clear_yield_signal(temp.path()).expect("clear again");
}

#[test]
fn unified_view_idle_no_events() {
    let temp = tempfile::tempdir().expect("tempdir");
    let view = unified_maintenance_view(temp.path(), true);
    assert!(matches!(
        view.coordination,
        MaintenanceCoordinationOutcome::Idle
    ));
    assert!(view.yield_pending.is_none());
    assert!(view.recent_events.is_empty());
    assert_eq!(view.decision, MaintenanceDecision::Launch);
}

#[test]
fn unified_view_active_with_lexical_fails_open() {
    use fs2::FileExt;
    let temp = tempfile::tempdir().expect("tempdir");
    let lock_path = temp.path().join("index-run.lock");
    let now_ms = crate::storage::sqlite::FrankenStorage::now_millis();
    let pid = active_lock_fixture_pid(99999);
    let mut owner = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)
        .expect("open");
    owner.try_lock_exclusive().expect("lock");
    write_locked_metadata(
        &lock_path,
        &mut owner,
        format!(
            "pid={pid}\nstarted_at_ms={}\nupdated_at_ms={}\ndb_path=/tmp/t.db\nmode=index\njob_id=uv-1\njob_kind=lexical_refresh\nphase=indexing\n",
            now_ms - 1_000,
            now_ms,
        )
        .as_str(),
    )
    .expect("write metadata");

    let event = MaintenanceEvent {
        timestamp_ms: now_ms,
        job_id: "uv-1".to_string(),
        actor_pid: pid,
        kind: MaintenanceEventKind::Started {
            job_kind: "lexical_refresh".to_string(),
            phase: "indexing".to_string(),
        },
    };
    append_maintenance_event(temp.path(), &event).expect("append");

    let view = unified_maintenance_view(temp.path(), true);
    assert!(matches!(
        view.coordination,
        MaintenanceCoordinationOutcome::Active { .. }
    ));
    assert!(matches!(
        view.decision,
        MaintenanceDecision::FailOpen { .. }
    ));
    assert_eq!(view.recent_events.len(), 1);

    let _ = FileExt::unlock(&owner);
}

#[test]
fn unified_view_includes_yield_signal() {
    let temp = tempfile::tempdir().expect("tempdir");
    request_yield(temp.path(), "test yield").expect("yield");
    let view = unified_maintenance_view(temp.path(), true);
    assert!(view.yield_pending.is_some());
    assert_eq!(view.yield_pending.as_ref().unwrap().reason, "test yield");
    clear_yield_signal(temp.path()).expect("clear");
}

#[test]
fn event_kinds_serialize_round_trip() {
    let temp = tempfile::tempdir().expect("tempdir");
    let kinds = vec![
        MaintenanceEventKind::Started {
            job_kind: "lexical_refresh".to_string(),
            phase: "init".to_string(),
        },
        MaintenanceEventKind::PhaseChanged {
            from: "init".to_string(),
            to: "scanning".to_string(),
        },
        MaintenanceEventKind::Progress {
            processed: 50,
            total: 100,
        },
        MaintenanceEventKind::YieldRequested {
            requester_pid: 42,
            reason: "foreground".to_string(),
        },
        MaintenanceEventKind::Paused {
            reason: "yield".to_string(),
        },
        MaintenanceEventKind::Resumed,
        MaintenanceEventKind::Completed {
            summary: "done".to_string(),
        },
        MaintenanceEventKind::Failed {
            error: "oops".to_string(),
        },
        MaintenanceEventKind::Cancelled {
            reason: "user".to_string(),
        },
    ];
    for (i, kind) in kinds.into_iter().enumerate() {
        let event = MaintenanceEvent {
            timestamp_ms: i as i64,
            job_id: "rt-test".to_string(),
            actor_pid: 1,
            kind,
        };
        append_maintenance_event(temp.path(), &event).expect("append");
    }
    let events = read_maintenance_events(temp.path(), None, None);
    assert_eq!(events.len(), 9);
    assert!(matches!(
        events[0].kind,
        MaintenanceEventKind::Started { .. }
    ));
    assert!(matches!(events[5].kind, MaintenanceEventKind::Resumed));
    assert!(matches!(
        events[8].kind,
        MaintenanceEventKind::Cancelled { .. }
    ));
}
