use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::franken_sync::compat::RowExt;
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::sources::provenance::SourceKind;
use serial_test::serial;
use tempfile::TempDir;

/// Regression for cass#265.
///
/// Before this fix, every preflight step inside `run_index`'s
/// `phase=watch_startup` block ran with no progress bump and no
/// sub-phase breadcrumb. If any one of them wedged (e.g. a multi-
/// second-to-multi-hour COUNT scan against `messages` triggering a
/// SQLite B-tree descent bug), operators saw only the opaque
/// `phase=watch_startup` for hours, with no way to tell whether
/// the wedge was in `cleanup_orphan_fk_rows`, the lexical-
/// checkpoint probe, the tantivy reader preflight, or somewhere
/// else. The reporter on cass#265 specifically asked: "Get a
/// thread-by-thread backtrace … I can produce one if the
/// maintainer can share … a guidance recipe for `lldb` to attach
/// to PID and dump symbolicated `bt all`." This sub-phase
/// breadcrumb is the same information, surfaced without requiring
/// the operator to attach `lldb` to a frozen process.
///
/// The fix routes each preflight step through
/// `IndexRunLockGuard::set_phase`, which:
/// 1. Writes a fine-grained sub-phase string to the lock file's
///    `phase=` line (e.g. `watch_startup:count_total_conversations`).
/// 2. Bumps `last_progress_at_ms` (both the on-disk field and the
///    atomic the heartbeat folds in) so a real wedge inside ONE
///    preflight surfaces at +120 s with the sub-phase pinpointed.
/// 3. Forces `IndexStallWatchdog` to reset its
///    `last_progress_advance` timer at each sub-phase boundary so
///    the watchdog scopes its stall window to a single preflight
///    rather than the whole watch_startup block.
///
/// This test exercises `set_phase` directly (without spinning up
/// the full indexer): acquire the lock as `WatchStartup`, call
/// `set_phase` with several sub-phases, and assert each call
/// updates the on-disk `phase=` line while leaving `mode=` at
/// `watch_startup` and advancing `last_progress_at_ms` strictly
/// monotonically.
#[test]
fn set_phase_writes_sub_phase_breadcrumb_and_bumps_progress() -> Result<()> {
    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("agent_search.db");
    std::fs::write(&db_path, b"placeholder")?;
    let mut guard =
        acquire_index_run_lock(tmp.path(), &db_path, SearchMaintenanceMode::WatchStartup)?;
    let lock_path = tmp.path().join("index-run.lock");

    let initial = read_index_run_lock_metadata_for_test(&lock_path)?;
    let initial_phase = initial
        .lines()
        .find_map(|line| line.strip_prefix("phase="))
        .map(str::to_string);
    assert_eq!(
        initial_phase.as_deref(),
        Some("watch_startup"),
        "initial phase must equal the mode for the WatchStartup acquire"
    );
    let initial_progress: i64 = initial
        .lines()
        .find_map(|line| line.strip_prefix("last_progress_at_ms="))
        .context("initial lock file must have last_progress_at_ms")?
        .parse()?;

    // Each preflight step writes a strictly different sub-phase
    // and a strictly larger last_progress_at_ms. The sleep is
    // tiny because `FrankenStorage::now_millis()` ticks every
    // millisecond — production preflight steps take seconds, so
    // monotonic strictness is the easy case.
    let sub_phases = [
        "watch_startup:ensure_index_dir",
        "watch_startup:open_storage",
        "watch_startup:validate_fts_messages",
        "watch_startup:cleanup_orphan_fk_rows",
        "watch_startup:count_total_conversations",
        "watch_startup:probe_lexical_checkpoint",
        "watch_startup:tantivy_reader_preflight",
        "watch_startup:scan_entry",
    ];
    let mut prev_progress = initial_progress;
    for sub_phase in sub_phases {
        std::thread::sleep(std::time::Duration::from_millis(2));
        guard.set_phase(SearchMaintenanceMode::WatchStartup, sub_phase)?;
        let raw = read_index_run_lock_metadata_for_test(&lock_path)?;
        let phase_line = raw
            .lines()
            .find_map(|line| line.strip_prefix("phase="))
            .map(str::to_string);
        assert_eq!(
            phase_line.as_deref(),
            Some(sub_phase),
            "set_phase must write the sub-phase string to the on-disk phase= field; got {raw:?}"
        );
        let mode_line = raw
            .lines()
            .find_map(|line| line.strip_prefix("mode="))
            .map(str::to_string);
        assert_eq!(
            mode_line.as_deref(),
            Some("watch_startup"),
            "set_phase must leave mode= unchanged so SearchMaintenanceSnapshot.mode keeps reflecting WatchStartup; got {raw:?}"
        );
        let progress: i64 = raw
            .lines()
            .find_map(|line| line.strip_prefix("last_progress_at_ms="))
            .context("last_progress_at_ms after set_phase")?
            .parse()?;
        assert!(
            progress > prev_progress,
            "set_phase must bump last_progress_at_ms (prev={prev_progress}, now={progress}); without this the stall detector cannot scope its window to a single sub-phase"
        );
        // The atomic mirror must also move; this is what the
        // heartbeat thread reads.
        let atomic_progress = guard.last_progress_at_ms_atomic.load(Ordering::Relaxed);
        assert_eq!(
            atomic_progress, progress,
            "the atomic mirror used by the heartbeat thread must match the on-disk last_progress_at_ms after set_phase"
        );
        prev_progress = progress;
    }

    drop(guard);
    Ok(())
}

/// Regression for GH #443.
///
/// A plain incremental run (`mode=index`) used to write only the
/// `classify_nonresumable_checkpoint` breadcrumb and then leave it in
/// place for every later preflight step, so a wedge inside the writable
/// storage open was reported against a sidecar read that had already
/// returned. Every taxonomy step must now be visible in every mode.
#[test]
fn preflight_breadcrumbs_are_visible_in_every_index_mode() {
    for mode in [
        SearchMaintenanceMode::Index,
        SearchMaintenanceMode::WatchStartup,
        SearchMaintenanceMode::Watch,
        SearchMaintenanceMode::WatchOnce,
    ] {
        for sub_phase in crate::indexer::WATCH_STARTUP_SUB_PHASE_TAXONOMY {
            assert!(
                crate::indexer::preflight_breadcrumb_visible(mode, sub_phase),
                "{sub_phase} must be written to the lock file under mode {:?}; a breadcrumb \
                 that stops advancing after classify_nonresumable_checkpoint misattributes \
                 an open_storage wedge (GH #443)",
                mode.as_lock_value()
            );
        }
    }
}

/// GH #443 companion: under `mode=index` the lock file carries the
/// sub-phase string while `mode=` stays `index`, so `cass status` shows
/// the running preflight step for a plain incremental run and does not
/// mistake it for a watch-startup job.
#[test]
fn set_phase_under_plain_index_mode_keeps_mode_index_and_advances_breadcrumb() -> Result<()> {
    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("agent_search.db");
    std::fs::write(&db_path, b"placeholder")?;
    let mut guard = acquire_index_run_lock(tmp.path(), &db_path, SearchMaintenanceMode::Index)?;
    let lock_path = tmp.path().join("index-run.lock");

    for sub_phase in [
        "watch_startup:classify_nonresumable_checkpoint",
        "watch_startup:open_storage",
        "watch_startup:writable_preflight",
        "watch_startup:count_total_conversations",
    ] {
        guard.set_phase(SearchMaintenanceMode::Index, sub_phase)?;
        let raw = read_index_run_lock_metadata_for_test(&lock_path)?;
        let phase_line = raw.lines().find_map(|line| line.strip_prefix("phase="));
        assert_eq!(
            phase_line,
            Some(sub_phase),
            "plain index mode must advance the on-disk phase= breadcrumb; got {raw:?}"
        );
        let mode_line = raw.lines().find_map(|line| line.strip_prefix("mode="));
        assert_eq!(
            mode_line,
            Some("index"),
            "sub-phase breadcrumbs must not rewrite mode= for a plain index run; got {raw:?}"
        );
    }

    drop(guard);
    Ok(())
}

/// Regression for cass#265.
///
/// The `IndexStallWatchdog` resets its `last_progress_advance`
/// timer whenever `phase_code` changes. The new sub-phase
/// breadcrumbs live in the LOCK FILE's `phase=` field (a string),
/// not in `IndexingProgress::phase` (an atomic enum). So a
/// wedged preflight is detected by `last_progress_at_ms` (which
/// `set_phase` bumps) and surfaced to operators via the lock-file
/// `phase=` string. This test verifies the lock-file `phase=`
/// values are exactly the strings the README/issue triage docs
/// reference, so a future refactor that renames them will be
/// flagged here before it ships and silently breaks operator
/// runbooks.
///
/// The taxonomy of sub-phases under `watch_startup:` is the
/// public contract — cass#265 hinge.
#[test]
fn watch_startup_sub_phase_taxonomy_is_documented_and_stable() -> Result<()> {
    // This list MUST match the `preflight_phase!(...)` call sites
    // in `run_index`. When you add or remove a preflight step,
    // update this list AND the cass#265 release-note section so
    // operators have a complete map of what `phase=` strings can
    // surface in `cass health --robot`.
    let documented_sub_phases = [
        "watch_startup:ensure_index_dir",
        "watch_startup:ensure_storage_headroom",
        "watch_startup:open_storage",
        "watch_startup:writable_preflight",
        "watch_startup:validate_fts_messages",
        "watch_startup:cleanup_orphan_fk_rows",
        "watch_startup:count_total_conversations",
        "watch_startup:probe_lexical_checkpoint",
        "watch_startup:repair_daily_stats",
        "watch_startup:tantivy_reader_preflight",
        "watch_startup:historical_salvage",
        "watch_startup:count_total_messages",
        "watch_startup:published_index_validate",
        "watch_startup:scan_entry",
        "watch_startup:reclassify_legacy_omp",
        "watch_startup:classify_nonresumable_checkpoint",
        "watch_startup:count_nonresumable_checkpoint_conversations",
        "watch_startup:fts_shadow_viability",
        "watch_startup:raw_mirror_maintenance",
    ];
    for sub_phase in documented_sub_phases {
        assert!(
            sub_phase.starts_with("watch_startup:"),
            "sub-phase {sub_phase} must live under the watch_startup: namespace so health-check JSON consumers can route on prefix"
        );
        assert!(
            !sub_phase.contains(char::is_whitespace),
            "sub-phase {sub_phase} must not contain whitespace; the lock file parser splits on `=` and expects a single line value"
        );
        // No colons after the namespace separator: keep sub-phases
        // simple identifiers so future structured-log consumers
        // (e.g. Datadog facets) can treat them as enum values.
        let after_namespace = sub_phase
            .strip_prefix("watch_startup:")
            .expect("namespace check above");
        assert!(
            !after_namespace.contains(':'),
            "sub-phase {sub_phase} must not contain a second colon; keep the breadcrumb taxonomy flat"
        );
    }

    // Cross-check against the source: every documented sub-phase
    // must appear in src/indexer/mod.rs (this file). This makes
    // a refactor that drops a preflight call fail this test
    // before the next release ships.
    let indexer_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/indexer");
    let mut src = String::new();
    for rel in [
        "mod.rs",
        "run_lock.rs",
        "run_index_preflight.rs",
        "run_index_lexical.rs",
        "run_index_finalize.rs",
    ] {
        let path = indexer_dir.join(rel);
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("read production indexer source {}", path.display()))?;
        src.push_str(&content);
        src.push('\n');
    }
    for sub_phase in documented_sub_phases {
        assert!(
            src.contains(sub_phase),
            "documented sub-phase {sub_phase} is missing from src/indexer/mod.rs; if the preflight step was renamed, update the test AND the release-note operator runbook"
        );
    }

    // v0.6.7: the taxonomy must also be consumable as a runtime
    // const so the watchdog can map a `step_idx` -> sub_phase
    // string without re-parsing source. Cross-check the test
    // list against the const so a refactor that updates one but
    // not the other fails here.
    assert_eq!(
        documented_sub_phases.as_slice(),
        crate::indexer::WATCH_STARTUP_SUB_PHASE_TAXONOMY,
        "documented taxonomy and the WATCH_STARTUP_SUB_PHASE_TAXONOMY const must match \
         — the watchdog uses the const as authority; if they diverge, the lock-file \
         `<step>_TIMEOUT` breadcrumb will name the wrong step on a wedge"
    );
    Ok(())
}

/// Regression for cass#265 v0.6.7 hardening.
///
/// The v0.6.6 fix shipped sub-phase breadcrumbs but didn't fail
/// the run on a wedge — operators still had to attach lldb or
/// kill the process manually after watching the lock file for
/// hours. The reporter on cass#265 confirmed the wedge persists
/// at `cleanup_orphan_fk_rows` on v0.6.6, so v0.6.7 ships a
/// proactive watchdog that aborts the process when any preflight
/// step exceeds `CASS_PREFLIGHT_OP_TIMEOUT_SECS` (default 180 s).
///
/// This test simulates a wedged preflight by:
/// 1. Starting the watchdog with a 100 ms timeout and a
///    `RecordOnly` policy (so the test process survives).
/// 2. Calling `state.enter(step_idx_for_cleanup_orphan_fk_rows, now_ms)`.
/// 3. Polling for the trip with a 750 ms deadline.
/// 4. Asserting:
///    - `state.tripped` is true (watchdog fired).
///    - The operator-visible metadata `phase=` line was rewritten to
///      `watch_startup:cleanup_orphan_fk_rows_TIMEOUT`.
///    - The lock-file `updated_at_ms=` advanced.
///    - The other fields (pid, started_at_ms, db_path, mode,
///      job_id, job_kind) are preserved verbatim — the watchdog
///      must only touch `phase=` and `updated_at_ms=`, not the
///      operator-visible job identity.
#[test]
fn watch_startup_preflight_watchdog_fires_on_wedged_step() -> Result<()> {
    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("agent_search.db");
    std::fs::write(&db_path, b"placeholder")?;
    let guard = acquire_index_run_lock(tmp.path(), &db_path, SearchMaintenanceMode::WatchStartup)?;
    let lock_path = tmp.path().join("index-run.lock");

    // Snapshot the pre-trip lock-file fields so we can prove
    // the watchdog preserved everything except `phase=` and
    // `updated_at_ms=`.
    let pre = read_index_run_lock_metadata_for_test(&lock_path)?;
    let extract = |key: &str, raw: &str| -> Option<String> {
        raw.lines()
            .find_map(|line| line.strip_prefix(key))
            .map(str::to_string)
    };
    let pre_pid = extract("pid=", &pre).context("pre pid=")?;
    let pre_started = extract("started_at_ms=", &pre).context("pre started_at_ms=")?;
    let pre_db = extract("db_path=", &pre).context("pre db_path=")?;
    let pre_mode = extract("mode=", &pre).context("pre mode=")?;
    let pre_job_id = extract("job_id=", &pre).context("pre job_id=")?;
    let pre_job_kind = extract("job_kind=", &pre).context("pre job_kind=")?;
    let pre_updated: i64 = extract("updated_at_ms=", &pre)
        .context("pre updated_at_ms=")?
        .parse()?;

    // Locate the wedge-candidate step (the one cass#265 confirmed
    // wedges on v0.6.6). Hard-coding the string keeps the
    // taxonomy test as the single source of truth.
    let wedged = "watch_startup:cleanup_orphan_fk_rows";
    let step_idx = crate::indexer::watch_startup_step_idx(wedged)
        .context("cleanup_orphan_fk_rows must be in the taxonomy")?;

    let state = crate::indexer::WatchStartupPreflightState::new();
    // Pass a dedicated write lock (matches the production call
    // site) so the INV3 fix compiles and is exercised here.
    let test_write_lock = std::sync::Arc::new(std::sync::Mutex::new(()));
    let _watchdog = crate::indexer::WatchStartupPreflightWatchdog::start(
        Arc::clone(&state),
        crate::indexer::WATCH_STARTUP_SUB_PHASE_TAXONOMY,
        lock_path.clone(),
        tmp.path().to_path_buf(),
        std::time::Duration::from_millis(100),
        std::time::Duration::from_millis(25),
        crate::indexer::PreflightTimeoutPolicy::RecordOnly,
        test_write_lock,
    );

    // Simulate the wedge: claim we entered the step `now` and
    // never exit. The watchdog will trip after ~125 ms (timeout
    // 100 ms, poll 25 ms).
    let started_at_ms = FrankenStorage::now_millis();
    state.enter(step_idx, started_at_ms);

    // Give the watchdog enough wall-clock to poll, decide, and
    // rewrite the lock file. 3pp5m: 750 ms was observed to be too
    // tight on a saturated worker (the trip landed just after the
    // deadline, then its timeout event was emitted — the mechanism
    // worked, the window did not). 5 s is still a hard bound on a
    // genuinely broken watchdog while absorbing scheduler starvation.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if state.tripped.load(Ordering::Relaxed) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        state.tripped.load(Ordering::Relaxed),
        "watchdog must trip when a preflight step exceeds its timeout; otherwise the cass#265 wedge persists silently"
    );

    // Verify the operator-visible metadata now carries the
    // TIMEOUT breadcrumb. This is the surface the operator's
    // `cass health --json` reads: the lock file on POSIX, and the
    // sidecar on Windows when the held lock prevents same-process
    // lock-file rewrites. The watchdog sets `state.tripped` only
    // after this metadata path is durable, but the bounded poll
    // keeps the test robust to reader/writer scheduling jitter on
    // heavily loaded CI runners.
    let read_deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
    let post = loop {
        let raw = read_index_run_lock_metadata_for_test(&lock_path)?;
        if raw.contains("_TIMEOUT") || std::time::Instant::now() > read_deadline {
            break raw;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    let post_phase = extract("phase=", &post).context("post phase=")?;
    assert_eq!(
        post_phase,
        format!("{wedged}_TIMEOUT"),
        "watchdog must rewrite phase= to `<step>_TIMEOUT`; got {post:?}"
    );
    let sidecar_path = crate::search::asset_state::index_run_lock_metadata_sidecar_path(&lock_path);
    let sidecar = std::fs::read_to_string(&sidecar_path).with_context(|| {
        format!(
            "reading watchdog-rewritten metadata sidecar {}",
            sidecar_path.display()
        )
    })?;
    let sidecar_phase = extract("phase=", &sidecar).context("sidecar phase=")?;
    assert_eq!(
        sidecar_phase,
        format!("{wedged}_TIMEOUT"),
        "watchdog must also rewrite the sidecar phase= for lock-blocked readers; got {sidecar:?}"
    );
    // Everything else preserved.
    assert_eq!(
        extract("pid=", &post).as_deref(),
        Some(pre_pid.as_str()),
        "watchdog must NOT change pid="
    );
    assert_eq!(
        extract("started_at_ms=", &post).as_deref(),
        Some(pre_started.as_str()),
        "watchdog must NOT change started_at_ms="
    );
    assert_eq!(
        extract("db_path=", &post).as_deref(),
        Some(pre_db.as_str()),
        "watchdog must NOT change db_path="
    );
    assert_eq!(
        extract("mode=", &post).as_deref(),
        Some(pre_mode.as_str()),
        "watchdog must NOT change mode= — that field reflects user-facing SearchMaintenanceMode and must stay `watch_startup`"
    );
    assert_eq!(
        extract("job_id=", &post).as_deref(),
        Some(pre_job_id.as_str()),
        "watchdog must NOT change job_id="
    );
    assert_eq!(
        extract("job_kind=", &post).as_deref(),
        Some(pre_job_kind.as_str()),
        "watchdog must NOT change job_kind="
    );
    let post_updated: i64 = extract("updated_at_ms=", &post)
        .context("post updated_at_ms=")?
        .parse()?;
    assert!(
        post_updated >= pre_updated,
        "watchdog must bump updated_at_ms= (pre={pre_updated}, post={post_updated}); without this operators can't distinguish a fresh TIMEOUT from a stale lock"
    );

    drop(guard);
    Ok(())
}

/// Regression for INV3 (v0.6.9 hardening).
///
/// When the watchdog fires it holds `metadata_write_lock` across
/// the full `rewrite_lock_phase_for_timeout` call. This serialises
/// the watchdog's `_TIMEOUT` rewrite against the heartbeat thread,
/// which also takes the same lock before every write. Without this
/// serialisation the heartbeat can read, then overwrite, the lock
/// file between the watchdog's `set_len(0)` and `write_all`, leaving
/// the lock file without the `_TIMEOUT` breadcrumb even though
/// `process::exit(70)` was called.
///
/// The test simulates the race by:
/// 1. Pre-holding `metadata_write_lock` from the test thread.
/// 2. Starting the watchdog (RecordOnly) with the same lock.
/// 3. Entering a step and verifying the watchdog trips but cannot
///    write the lock file while the lock is held.
/// 4. Releasing the lock and confirming the rewrite then lands.
#[test]
fn watchdog_timeout_rewrite_serialised_by_metadata_write_lock() -> Result<()> {
    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("agent_search.db");
    std::fs::write(&db_path, b"placeholder")?;
    let guard = acquire_index_run_lock(tmp.path(), &db_path, SearchMaintenanceMode::WatchStartup)?;
    let lock_path = tmp.path().join("index-run.lock");

    let metadata_write_lock = std::sync::Arc::clone(&guard.metadata_write_lock);

    // Pre-hold the lock so the watchdog's rewrite is blocked.
    let held = metadata_write_lock
        .lock()
        .expect("metadata_write_lock poisoned");

    let wedged = "watch_startup:cleanup_orphan_fk_rows";
    let step_idx = crate::indexer::watch_startup_step_idx(wedged)
        .context("cleanup_orphan_fk_rows must be in taxonomy")?;

    let state = crate::indexer::WatchStartupPreflightState::new();
    let _watchdog = crate::indexer::WatchStartupPreflightWatchdog::start(
        Arc::clone(&state),
        crate::indexer::WATCH_STARTUP_SUB_PHASE_TAXONOMY,
        lock_path.clone(),
        tmp.path().to_path_buf(),
        std::time::Duration::from_millis(50),
        std::time::Duration::from_millis(10),
        crate::indexer::PreflightTimeoutPolicy::RecordOnly,
        std::sync::Arc::clone(&metadata_write_lock),
    );

    let started_at_ms = FrankenStorage::now_millis();
    state.enter(step_idx, started_at_ms);

    // Wait long enough for the watchdog to have tripped internally
    // (it sets `tripped` after acquiring the lock, so it must wait).
    // 400 ms >> 50 ms timeout + 10 ms poll + acquisition delay.
    std::thread::sleep(std::time::Duration::from_millis(400));

    // Lock file must NOT yet have _TIMEOUT because the watchdog is
    // blocked waiting for `metadata_write_lock`.
    let before_release = read_index_run_lock_metadata_for_test(&lock_path).unwrap_or_default();
    assert!(
        !before_release.contains("_TIMEOUT"),
        "watchdog must not rewrite the lock file while metadata_write_lock is held; \
         got: {before_release:?}"
    );

    // Release the lock. Now the watchdog can complete its write.
    drop(held);

    let read_deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
    let after_release = loop {
        let raw = read_index_run_lock_metadata_for_test(&lock_path).unwrap_or_default();
        if raw.contains("_TIMEOUT") || std::time::Instant::now() > read_deadline {
            break raw;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    assert!(
        after_release.contains("_TIMEOUT"),
        "watchdog must rewrite lock file to _TIMEOUT after metadata_write_lock is released; \
         got: {after_release:?}"
    );

    drop(guard);
    Ok(())
}

/// Regression for cass#265 v0.6.7 hardening.
///
/// The skip env var lets operators bypass a single wedged
/// preflight step without disabling the rest of the preflight.
/// This test pins the env-var naming convention (lossless from
/// sub-phase string) and the parsing rules.
#[test]
fn preflight_skip_env_var_naming_is_lossless() {
    // Mapping is lossless: sub-phase -> CASS_SKIP_PREFLIGHT_<UPPER>.
    assert_eq!(
        crate::indexer::watch_startup_preflight::skip_env_name(
            "watch_startup:cleanup_orphan_fk_rows"
        ),
        "CASS_SKIP_PREFLIGHT_CLEANUP_ORPHAN_FK_ROWS"
    );
    assert_eq!(
        crate::indexer::watch_startup_preflight::skip_env_name(
            "watch_startup:count_total_messages"
        ),
        "CASS_SKIP_PREFLIGHT_COUNT_TOTAL_MESSAGES"
    );
    assert_eq!(
        crate::indexer::watch_startup_preflight::skip_env_name(
            "watch_startup:validate_fts_messages"
        ),
        "CASS_SKIP_PREFLIGHT_VALIDATE_FTS_MESSAGES"
    );

    // Round-trip every documented sub-phase so a future renamed
    // entry surfaces here.
    for sub_phase in crate::indexer::WATCH_STARTUP_SUB_PHASE_TAXONOMY {
        let env_name = crate::indexer::watch_startup_preflight::skip_env_name(sub_phase);
        assert!(
            env_name.starts_with("CASS_SKIP_PREFLIGHT_"),
            "skip env var for {sub_phase} must follow the documented prefix; got {env_name}"
        );
        assert!(
            !env_name.contains(':'),
            "skip env var for {sub_phase} must strip the watch_startup: namespace; got {env_name}"
        );
    }
}

#[test]
fn validate_fts_messages_preflight_is_opt_in() {
    {
        let _guard = unset_env_var("CASS_PREFLIGHT_VALIDATE_FTS_MESSAGES");
        assert!(
            !crate::indexer::preflight_validate_fts_messages_enabled(),
            "routine indexing must not run the derived fallback FTS validation probe by default"
        );
    }

    for value in ["1", "true", "TRUE", "yes", "YES"] {
        let _guard = set_env_var("CASS_PREFLIGHT_VALIDATE_FTS_MESSAGES", value);
        assert!(
            crate::indexer::preflight_validate_fts_messages_enabled(),
            "{value:?} should enable the opt-in fallback FTS validation preflight"
        );
    }

    for value in ["0", "false", "FALSE", "no", "NO", ""] {
        let _guard = set_env_var("CASS_PREFLIGHT_VALIDATE_FTS_MESSAGES", value);
        assert!(
            !crate::indexer::preflight_validate_fts_messages_enabled(),
            "{value:?} should leave fallback FTS validation skipped for routine indexing"
        );
    }
}

#[test]
fn validate_fts_messages_skip_does_not_imply_opt_in() {
    let _validate_guard = unset_env_var("CASS_PREFLIGHT_VALIDATE_FTS_MESSAGES");
    let _skip_guard = set_env_var("CASS_SKIP_PREFLIGHT_VALIDATE_FTS_MESSAGES", "1");

    assert!(
        !crate::indexer::preflight_validate_fts_messages_enabled(),
        "the legacy skip knob must not turn the expensive fallback FTS validation probe on"
    );
    assert!(
        crate::indexer::preflight_skip("watch_startup:validate_fts_messages"),
        "the skip knob should still parse truthfully when an explicit opt-in path consults it"
    );
}

#[test]
#[serial]
fn cleanup_orphan_fk_rows_preflight_is_opt_in() {
    {
        let _guard = unset_env_var("CASS_PREFLIGHT_CLEANUP_ORPHAN_FK_ROWS");
        assert!(
            !crate::indexer::preflight_cleanup_orphan_fk_rows_enabled(),
            "routine indexing must not run the broad orphan-FK cleanup sweep by default"
        );
    }

    for value in ["1", "true", "TRUE", "yes", "YES"] {
        let _guard = set_env_var("CASS_PREFLIGHT_CLEANUP_ORPHAN_FK_ROWS", value);
        assert!(
            crate::indexer::preflight_cleanup_orphan_fk_rows_enabled(),
            "{value:?} should enable the opt-in orphan-FK cleanup preflight"
        );
    }

    for value in ["0", "false", "FALSE", "no", "NO", ""] {
        let _guard = set_env_var("CASS_PREFLIGHT_CLEANUP_ORPHAN_FK_ROWS", value);
        assert!(
            !crate::indexer::preflight_cleanup_orphan_fk_rows_enabled(),
            "{value:?} should leave orphan-FK cleanup skipped for routine indexing"
        );
    }
}

#[test]
fn cleanup_orphan_fk_rows_skip_does_not_imply_opt_in() {
    let _cleanup_guard = unset_env_var("CASS_PREFLIGHT_CLEANUP_ORPHAN_FK_ROWS");
    let _skip_guard = set_env_var("CASS_SKIP_PREFLIGHT_CLEANUP_ORPHAN_FK_ROWS", "1");

    assert!(
        !crate::indexer::preflight_cleanup_orphan_fk_rows_enabled(),
        "the legacy skip knob must not turn the expensive orphan-FK cleanup sweep on"
    );
    assert!(
        crate::indexer::preflight_skip("watch_startup:cleanup_orphan_fk_rows"),
        "the skip knob should still parse truthfully when an explicit opt-in path consults it"
    );
}

/// Regression for #258. The heartbeat thread must refresh
/// `updated_at_ms` (its job: prove the process is alive) but MUST
/// NOT touch `last_progress_at_ms` (which the indexing thread alone
/// updates on real forward progress). If the heartbeat were to
/// refresh both, the stall detector in
/// `crate::search::asset_state::maintenance_stall_age_ms` would
/// silently never fire, recreating the bug.
#[test]
fn heartbeat_preserves_last_progress_at_ms_field_for_stall_detection() {
    let tmp = TempDir::new().unwrap();
    let lock_path = tmp.path().join("index-run.lock");
    // Note: last_progress_at_ms is set to a deliberately small
    // value (333) representing "indexing thread last reported
    // forward progress at t=333ms".
    std::fs::write(
        &lock_path,
        "pid=123\nstarted_at_ms=111\nupdated_at_ms=222\nlast_progress_at_ms=333\ndb_path=/tmp/db.sqlite\nmode=watch_startup\njob_id=lexical_refresh-123\njob_kind=lexical_refresh\nphase=watch_startup\n",
    )
    .unwrap();

    // Heartbeat twice — simulate ~5 s of clock advance with no
    // indexing-thread activity.
    heartbeat_index_run_lock(tmp.path()).unwrap();
    heartbeat_index_run_lock(tmp.path()).unwrap();

    let refreshed = read_index_run_lock_metadata_for_test(&lock_path).unwrap();
    let last_progress_lines: Vec<&str> = refreshed
        .lines()
        .filter_map(|line| line.strip_prefix("last_progress_at_ms="))
        .collect();
    assert_eq!(
        last_progress_lines.len(),
        1,
        "heartbeat must keep exactly one last_progress_at_ms line, got {refreshed:?}",
    );
    assert_eq!(
        last_progress_lines.first().copied(),
        Some("333"),
        "heartbeat must preserve last_progress_at_ms verbatim — refreshing it would silently disable the #258 stall detector",
    );
    // updated_at_ms must have moved.
    let updated_at_value: Option<i64> = refreshed
        .lines()
        .filter_map(|line| line.strip_prefix("updated_at_ms="))
        .next()
        .and_then(|raw| raw.parse().ok());
    assert!(
        updated_at_value.is_some_and(|value| value > 222),
        "updated_at_ms must advance past initial 222, got {refreshed:?}",
    );
}

#[test]
fn heartbeat_folds_indexer_progress_atomic_into_last_progress_at_ms() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    std::fs::write(&db_path, b"placeholder").unwrap();
    let guard = acquire_index_run_lock(tmp.path(), &db_path, SearchMaintenanceMode::Index)
        .expect("acquire index run lock");
    let lock_path = tmp.path().join("index-run.lock");
    let before = read_index_run_lock_metadata_for_test(&lock_path).unwrap();
    let old_progress = before
        .lines()
        .find_map(|line| line.strip_prefix("last_progress_at_ms="))
        .and_then(|raw| raw.parse::<i64>().ok())
        .expect("initial progress line");
    let bumped_progress = old_progress.saturating_add(1_000);
    guard
        .last_progress_at_ms_atomic
        .store(bumped_progress, Ordering::Relaxed);

    heartbeat_index_run_lock_with_lock_and_progress(
        tmp.path(),
        Some(&guard.metadata_write_lock),
        guard.last_progress_at_ms_atomic.load(Ordering::Relaxed),
    )
    .expect("heartbeat should persist indexer-owned progress");

    let refreshed = read_index_run_lock_metadata_for_test(&lock_path).unwrap();
    let expected_line = format!("last_progress_at_ms={bumped_progress}");
    assert!(
        refreshed.lines().any(|line| line == expected_line),
        "heartbeat must persist the indexer-owned progress bump, got {refreshed:?}"
    );
    drop(guard);
}

#[test]
fn heartbeat_index_run_lock_surfaces_in_place_open_errors() {
    let tmp = TempDir::new().unwrap();
    let parent_as_file = tmp.path().join("not-a-directory");
    std::fs::write(&parent_as_file, b"not a directory").unwrap();
    let lock_path = parent_as_file.join("index-run.lock");

    let err = write_index_run_lock_heartbeat_in_place(&lock_path, "updated_at_ms=123\n")
        .expect_err("non-directory parent should make heartbeat open fail");

    let err_text = format!("{err:#}");
    assert!(
        err_text.contains("opening index-run lock heartbeat"),
        "unexpected heartbeat error: {err_text}"
    );
}

#[cfg(unix)]
#[test]
fn heartbeat_index_run_lock_preserves_lock_file_inode() {
    use std::os::unix::fs::MetadataExt;

    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    std::fs::write(&db_path, b"placeholder").unwrap();
    let guard = acquire_index_run_lock(tmp.path(), &db_path, SearchMaintenanceMode::Index)
        .expect("acquire index run lock");
    let lock_path = tmp.path().join("index-run.lock");
    let before = std::fs::metadata(&lock_path).expect("lock metadata before heartbeat");

    heartbeat_index_run_lock_with_lock(tmp.path(), Some(&guard.metadata_write_lock))
        .expect("heartbeat refresh should succeed");

    let after = std::fs::metadata(&lock_path).expect("lock metadata after heartbeat");
    assert_eq!(
        (before.dev(), before.ino()),
        (after.dev(), after.ino()),
        "heartbeat refresh must not replace index-run.lock while a flock is held"
    );
    drop(guard);
}

#[test]
fn heartbeat_index_run_lock_waits_for_shared_metadata_write_lock() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    std::fs::write(&db_path, b"placeholder").unwrap();
    let guard = acquire_index_run_lock(tmp.path(), &db_path, SearchMaintenanceMode::Index)
        .expect("acquire index run lock");
    let metadata_write_lock = Arc::clone(&guard.metadata_write_lock);
    let held_lock = metadata_write_lock
        .lock()
        .expect("hold metadata write lock");
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started_flag = Arc::clone(&started);
    let finished_flag = Arc::clone(&finished);
    let data_dir = tmp.path().to_path_buf();
    let metadata_write_lock_for_thread = Arc::clone(&metadata_write_lock);

    let handle = thread::spawn(move || {
        started_flag.store(true, Ordering::SeqCst);
        heartbeat_index_run_lock_with_lock(&data_dir, Some(&metadata_write_lock_for_thread))
            .expect("heartbeat refresh with shared lock");
        finished_flag.store(true, Ordering::SeqCst);
    });

    while !started.load(Ordering::SeqCst) {
        thread::yield_now();
    }
    thread::sleep(Duration::from_millis(50));
    assert!(
        !finished.load(Ordering::SeqCst),
        "heartbeat must wait for the shared metadata write lock before rewriting the lock file"
    );

    drop(held_lock);
    handle.join().expect("heartbeat thread should join cleanly");
    assert!(finished.load(Ordering::SeqCst));
    drop(guard);
}

#[test]
fn gh473_short_writer_wait_preserves_live_reader_ingest() {
    const CHILD: &str = "CASS_TEST_GH473_CHILD";
    if dotenvy::var(CHILD).is_err() {
        for (value, expected) in [(None, "10"), (Some("0"), "10"), (Some("37"), "37")] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    &format!(
                        "{}::gh473_short_writer_wait_preserves_live_reader_ingest",
                        module_path!().split_once("::").unwrap().1
                    ),
                    "--nocapture",
                ])
                .env(CHILD, expected)
                .env_remove("CASS_INDEX_WRITER_BUSY_TIMEOUT_MS");
            if let Some(value) = value {
                command.env("CASS_INDEX_WRITER_BUSY_TIMEOUT_MS", value);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "child failed: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        }
        return;
    }
    let expected = dotenvy::var(CHILD).unwrap().parse::<u64>().unwrap();
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("archive.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    persist::apply_index_writer_busy_timeout(&storage);
    assert_eq!(storage.index_writer_busy_timeout_ms(), Some(expected));
    let reader = FrankenStorage::open_readonly(&db_path).unwrap();
    reader.raw().query("SELECT COUNT(*) FROM messages").unwrap();
    let conv = norm_conv(Some("gh473"), vec![norm_msg(0, 100)]);
    let completion = crate::storage::sqlite::SourceIngestLedgerEntry {
        key: "source_ingest_v1:gh473".into(),
        observation: "{\"complete\":true}".into(),
    };
    for pass in 0..3 {
        let started = Instant::now();
        persist::persist_conversations_batched_inner(
            &storage,
            None,
            std::slice::from_ref(&conv),
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            false,
            false,
            None,
            persist::PersistHeartbeat::NONE,
            Some(&completion),
        )
        .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "write/no-op pass {pass} must not incur a 60-second inner wait"
        );
        let rows = storage
            .raw()
            .query("SELECT COUNT(*) FROM messages")
            .unwrap();
        assert_eq!(rows[0].get_typed::<i64>(0).unwrap(), 1);
        let rows = storage
            .raw()
            .query("SELECT COUNT(*) FROM conversations")
            .unwrap();
        assert_eq!(rows[0].get_typed::<i64>(0).unwrap(), 1);
        assert_eq!(
            storage
                .source_ingest_ledger_entries()
                .unwrap()
                .get(&completion.key),
            Some(&completion.observation)
        );
    }
    reader.close_without_checkpoint().unwrap();
    storage
        .raw()
        .execute("CREATE TABLE gh473_retry(n INTEGER)")
        .unwrap();
    let writer = crate::franken_sync::Connection::open_existing_schema_only(
        db_path.to_string_lossy(),
    )
    .unwrap();
    writer.execute("PRAGMA busy_timeout = 10").unwrap();
    storage.raw().execute("BEGIN IMMEDIATE").unwrap();
    let mut attempts = 0;
    persist::with_writer_retry(2, || {
        attempts += 1;
        let result = writer
            .execute("INSERT INTO gh473_retry VALUES(1)")
            .map_err(anyhow::Error::new);
        if attempts == 1 {
            let err = result
                .as_ref()
                .expect_err("held writer lock must cause real contention");
            assert!(anyhow_chain_indicates_retryable_storage_contention(err));
            storage.raw().execute("ROLLBACK").unwrap();
        }
        result
    })
    .unwrap();
    assert_eq!(
        attempts, 2,
        "whole-operation retry succeeds after lock release"
    );
    let rows = writer.query("SELECT COUNT(*) FROM gh473_retry").unwrap();
    assert_eq!(rows[0].get_typed::<i64>(0).unwrap(), 1);
    writer.close_without_checkpoint().unwrap();
    storage.close().unwrap();
}

#[test]
fn gh459_workspace_repair_checkpoint_precedes_mutation_and_survives_reopen() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "cursor".into(),
            name: "Cursor".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    let mut conv = norm_conv(Some("gh459-durable"), vec![norm_msg(0, 100)]);
    conv.agent_slug = "cursor".into();
    conv.workspace = Some(PathBuf::from("/workspace/my/app"));
    conv.metadata = serde_json::json!({"cursor_format":"agent"});
    let wrong_id = storage
        .ensure_workspace(conv.workspace.as_ref().unwrap(), None)
        .unwrap();
    let original = storage
        .insert_conversation_tree(agent_id, Some(wrong_id), &persist::map_to_internal(&conv))
        .unwrap();
    let messages =
        serde_json::to_value(storage.fetch_messages(original.conversation_id).unwrap()).unwrap();
    rebuild_tantivy_from_db_deferred_startup(&db_path, &data_dir, 1, None).unwrap();
    let index_path = crate::search::tantivy::index_dir(&data_dir).unwrap();
    let published = index_meta_fingerprint(&index_path).unwrap();
    conv.workspace = Some(PathBuf::from("/workspace/my-app"));
    conv.metadata["cursor_workspace_attribution"] = serde_json::json!("workspace_trusted");
    persist::prepare_cursor_workspace_repair(
        &storage,
        Some(&data_dir),
        std::slice::from_ref(&conv),
    )
    .unwrap();
    let pending = load_lexical_rebuild_state(&index_path).unwrap().unwrap();
    assert!(!pending.completed);
    assert_eq!(
        pending.effective_execution_mode(),
        LexicalRebuildExecutionMode::CanonicalMetadataRepair
    );
    assert!(pending.requires_restart_from_zero_on_resume());
    assert_eq!(
        storage.list_conversations(10, 0).unwrap()[0].workspace,
        Some(PathBuf::from("/workspace/my/app")),
        "pending state must precede the canonical mutation"
    );
    let outcome = persist::persist_conversations_batched_with_raw_mirror_links(
        &storage,
        None,
        &data_dir,
        std::slice::from_ref(&conv),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
        persist::PersistHeartbeat::NONE,
    )
    .unwrap();
    assert_eq!(outcome.workspace_changes, 1);
    assert_eq!(outcome.inserted_conversations, 0);
    assert_eq!(outcome.inserted_messages, 0);
    assert!(outcome.lexical_update_deferred);
    storage.close().unwrap();
    // Stop before publication and reopen the real database and sidecar.
    assert_eq!(index_meta_fingerprint(&index_path).unwrap(), published);
    assert!(
        !load_lexical_rebuild_state(&index_path)
            .unwrap()
            .unwrap()
            .completed
    );
    assert!(
        nonresumable_pending_lexical_rebuild_status_without_fingerprint(&index_path, &db_path, 2)
            .unwrap()
            .unwrap()
            .has_pending_resume,
        "intervening inserts cannot erase metadata repair debt"
    );
    let storage = FrankenStorage::open(&db_path).unwrap();
    assert_eq!(
        storage.list_conversations(10, 0).unwrap()[0].workspace,
        conv.workspace
    );
    assert_eq!(
        serde_json::to_value(storage.fetch_messages(original.conversation_id).unwrap()).unwrap(),
        messages
    );
    rebuild_tantivy_from_db_deferred_startup(&db_path, &data_dir, 1, None).unwrap();
    assert!(
        load_lexical_rebuild_state(&index_path)
            .unwrap()
            .unwrap()
            .completed
    );
    assert_ne!(index_meta_fingerprint(&index_path).unwrap(), published);

    // A real checkpoint write failure must reject the batch before UPDATE.
    let checkpoint_path = lexical_rebuild_state_path(&index_path);
    fs::rename(
        &checkpoint_path,
        index_path.join("retained-gh459-checkpoint.json"),
    )
    .unwrap();
    fs::create_dir(&checkpoint_path).unwrap();
    conv.metadata["cursor_workspace_attribution"] = serde_json::json!("unresolved");
    conv.workspace = None;
    assert!(
        persist::persist_conversations_batched_with_raw_mirror_links(
            &storage,
            None,
            &data_dir,
            &[conv],
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            false,
            persist::PersistHeartbeat::NONE,
        )
        .is_err()
    );
    assert_eq!(
        storage.list_conversations(10, 0).unwrap()[0].workspace,
        Some(PathBuf::from("/workspace/my-app"))
    );
    assert_eq!(
        serde_json::to_value(storage.fetch_messages(original.conversation_id).unwrap()).unwrap(),
        messages
    );
}

#[test]
fn lexical_rebuild_packet_matches_canonical_and_normalized_semantics_for_host_only_remote() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("packet-contract.db");
    let storage = FrankenStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let raw_conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some("packet-host-only".into()),
        title: Some("Packet host-only remote".into()),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        source_path: PathBuf::from("/tmp/packet-host-only.jsonl"),
        started_at: Some(1_700_000_100_000),
        ended_at: Some(1_700_000_100_100),
        metadata: serde_json::json!({
            "cass": {
                "origin": {
                    "source_id": "   ",
                    "host": "builder-packet"
                }
            }
        }),
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: Some("user".into()),
                created_at: Some(1_700_000_100_010),
                content: "first packet message".into(),
                extra: serde_json::json!({}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".into(),
                author: Some("assistant".into()),
                created_at: Some(1_700_000_100_020),
                content: "second packet message".into(),
                extra: serde_json::json!({}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
        ],
    };

    let canonical = Conversation {
        id: None,
        agent_slug: raw_conv.agent_slug.clone(),
        workspace: raw_conv.workspace.clone(),
        external_id: raw_conv.external_id.clone(),
        title: raw_conv.title.clone(),
        source_path: raw_conv.source_path.clone(),
        started_at: raw_conv.started_at,
        ended_at: raw_conv.ended_at,
        approx_tokens: Some(64),
        metadata_json: raw_conv.metadata.clone(),
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_100_010),
                content: "first packet message".into(),
                extra_json: serde_json::json!({}),
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: Some("assistant".into()),
                created_at: Some(1_700_000_100_020),
                content: "second packet message".into(),
                extra_json: serde_json::json!({}),
                snippets: Vec::new(),
            },
        ],
        source_id: "   ".into(),
        origin_host: Some("builder-packet".into()),
    };

    let workspace_id = storage
        .ensure_workspace(raw_conv.workspace.as_deref().expect("workspace path"), None)
        .unwrap();
    let inserted = storage
        .insert_conversation_tree(agent_id, Some(workspace_id), &canonical)
        .unwrap();
    let (agent_slugs, workspace_paths) = storage.build_lexical_rebuild_lookups().unwrap();
    let replay_row = storage
        .list_conversations_for_lexical_rebuild_after_id(10, 0, &agent_slugs, &workspace_paths)
        .unwrap()
        .into_iter()
        .next()
        .expect("canonical replay row");
    let mut fetched = storage
        .fetch_messages_for_lexical_rebuild_batch(&[inserted.conversation_id], None, None)
        .unwrap();
    let replay_messages = fetched
        .remove(&inserted.conversation_id)
        .expect("canonical replay messages");
    let first_replay_content_ptr = replay_messages[0].content.as_ptr();
    let source_map = storage
        .list_sources()
        .unwrap_or_default()
        .into_iter()
        .map(|source| (source.id, (source.kind, source.host_label)))
        .collect::<HashMap<_, _>>();

    let canonical_packet = LexicalRebuildConversationPacket::from_canonical_replay_messages(
        replay_row,
        replay_messages,
        &source_map,
    )
    .unwrap();
    let normalized_packet =
        LexicalRebuildConversationPacket::from_normalized_conversation(&raw_conv);

    assert_eq!(
        canonical_packet.semantic_view(),
        normalized_packet.semantic_view(),
        "canonical replay and normalized builders should agree on the hot-path semantics"
    );
    assert_eq!(
        canonical_packet.fingerprint_input(),
        normalized_packet.fingerprint_input(),
        "canonical replay and normalized builders should agree on the fingerprint input contract"
    );
    assert_eq!(
        canonical_packet.diagnostics.provenance_mode,
        LexicalRebuildPacketProvenanceMode::SourceMapLookup
    );
    assert_eq!(
        canonical_packet.messages[0].content.as_ptr(),
        first_replay_content_ptr,
        "canonical replay must move fetched transcript content through packet normalization"
    );
    assert_eq!(
        normalized_packet.diagnostics.provenance_mode,
        LexicalRebuildPacketProvenanceMode::HostFallback
    );
}

#[test]
fn lexical_rebuild_packet_empty_conversation_has_zero_budget_and_no_docs() {
    let packet = LexicalRebuildConversationPacket::from_canonical_replay(
        crate::storage::sqlite::LexicalRebuildConversationRow {
            id: Some(77),
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some("empty-packet".into()),
            title: Some("Empty packet".into()),
            source_path: PathBuf::from("/tmp/empty-packet.jsonl"),
            started_at: Some(1_700_000_200_000),
            ended_at: Some(1_700_000_200_000),
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        },
        crate::storage::sqlite::LexicalRebuildGroupedMessageRows::new(),
        None,
        &HashMap::new(),
    );

    assert_eq!(packet.message_count, 0);
    assert_eq!(packet.message_bytes, 0);
    assert!(packet.prebuilt_docs().is_empty());
    assert!(!packet.diagnostics.missing_conversation_id);
}

#[test]
fn lexical_rebuild_packet_missing_conversation_id_is_explicit_and_non_indexable() {
    let mut grouped = crate::storage::sqlite::LexicalRebuildGroupedMessageRows::new();
    grouped.push(crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
        idx: 0,
        is_tool_role: false,
        created_at: Some(1_700_000_300_010),
        content: "missing-id packet body".into(),
    });

    let packet = LexicalRebuildConversationPacket::from_canonical_replay(
        crate::storage::sqlite::LexicalRebuildConversationRow {
            id: None,
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some("missing-id-packet".into()),
            title: Some("Missing id packet".into()),
            source_path: PathBuf::from("/tmp/missing-id-packet.jsonl"),
            started_at: Some(1_700_000_300_000),
            ended_at: Some(1_700_000_300_100),
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        },
        grouped,
        None,
        &HashMap::new(),
    );

    assert!(packet.diagnostics.missing_conversation_id);
    assert_eq!(packet.message_count, 1);
    assert_eq!(packet.message_bytes, "missing-id packet body".len());
    assert!(packet.prebuilt_docs().is_empty());
}

#[test]
fn lexical_rebuild_packet_large_payload_budget_is_exact_and_preserves_doc_fallbacks() {
    let large_body = "x".repeat(32 * 1024);
    let mut grouped = crate::storage::sqlite::LexicalRebuildGroupedMessageRows::new();
    grouped.push(crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
        idx: 0,
        is_tool_role: false,
        created_at: None,
        content: large_body.clone(),
    });
    grouped.push(crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
        idx: 1,
        is_tool_role: false,
        created_at: Some(1_700_000_400_020),
        content: "tail".into(),
    });

    let packet = LexicalRebuildConversationPacket::from_canonical_replay(
        crate::storage::sqlite::LexicalRebuildConversationRow {
            id: Some(88),
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some("large-packet".into()),
            title: Some("Large packet".into()),
            source_path: PathBuf::from("/tmp/large-packet.jsonl"),
            started_at: Some(1_700_000_400_000),
            ended_at: Some(1_700_000_400_100),
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        },
        grouped,
        Some(123),
        &HashMap::new(),
    );

    let docs = packet.prebuilt_docs();
    assert_eq!(packet.message_count, 2);
    assert_eq!(packet.message_bytes, large_body.len() + 4);
    assert_eq!(packet.last_message_id, Some(123));
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0].conversation_id, Some(88));
    assert_eq!(docs[0].created_at, Some(1_700_000_400_000));
    assert_eq!(docs[0].content.len(), large_body.len());
    assert_eq!(docs[0].source_id, LOCAL_SOURCE_ID);
    assert_eq!(docs[1].created_at, Some(1_700_000_400_020));
}

#[test]
fn prepare_lexical_rebuild_packet_batch_preserves_order_and_parallel_equivalence() {
    let source_map = HashMap::from([(
        "builder-prep".to_string(),
        (SourceKind::Ssh, Some("builder-prep".to_string())),
    )]);
    let conversation_page = vec![
        crate::storage::sqlite::LexicalRebuildConversationRow {
            id: Some(11),
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace-a")),
            external_id: Some("prep-a".into()),
            title: Some("Prep A".into()),
            source_path: PathBuf::from("/tmp/prep-a.jsonl"),
            started_at: Some(1_700_000_500_000),
            ended_at: Some(1_700_000_500_100),
            source_id: "builder-prep".into(),
            origin_host: Some("builder-prep".into()),
        },
        crate::storage::sqlite::LexicalRebuildConversationRow {
            id: Some(22),
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace-b")),
            external_id: Some("prep-empty".into()),
            title: Some("Prep Empty".into()),
            source_path: PathBuf::from("/tmp/prep-empty.jsonl"),
            started_at: Some(1_700_000_500_200),
            ended_at: Some(1_700_000_500_200),
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        },
        crate::storage::sqlite::LexicalRebuildConversationRow {
            id: Some(33),
            agent_slug: "codex".into(),
            workspace: None,
            external_id: Some("prep-c".into()),
            title: Some("Prep C".into()),
            source_path: PathBuf::from("/tmp/prep-c.jsonl"),
            started_at: Some(1_700_000_500_400),
            ended_at: Some(1_700_000_500_500),
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        },
    ];
    let grouped_messages = HashMap::from([
        (
            11_i64,
            vec![
                Message {
                    id: Some(501),
                    idx: 0,
                    role: MessageRole::User,
                    author: Some("user".into()),
                    created_at: Some(1_700_000_500_010),
                    content: "prep-a-0".into(),
                    extra_json: serde_json::json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: Some(502),
                    idx: 1,
                    role: MessageRole::Agent,
                    author: Some("assistant".into()),
                    created_at: Some(1_700_000_500_020),
                    content: "prep-a-1".into(),
                    extra_json: serde_json::json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
        (
            33_i64,
            vec![Message {
                id: Some(601),
                idx: 0,
                role: MessageRole::Tool,
                author: Some("tool".into()),
                created_at: Some(1_700_000_500_410),
                content: "prep-c-0".into(),
                extra_json: serde_json::json!({}),
                snippets: Vec::new(),
            }],
        ),
    ]);

    let serial_packets = prepare_lexical_rebuild_packet_batch(
        conversation_page.clone(),
        grouped_messages.clone(),
        &source_map,
        None,
    )
    .unwrap();
    let worker_pool = ThreadPoolBuilder::new().num_threads(2).build().unwrap();
    let parallel_packets = prepare_lexical_rebuild_packet_batch(
        conversation_page,
        grouped_messages,
        &source_map,
        Some(&worker_pool),
    )
    .unwrap();

    let serial_views = serial_packets
        .iter()
        .map(LexicalRebuildConversationPacket::semantic_view)
        .collect::<Vec<_>>();
    let parallel_views = parallel_packets
        .iter()
        .map(LexicalRebuildConversationPacket::semantic_view)
        .collect::<Vec<_>>();
    assert_eq!(serial_views, parallel_views);
    assert_eq!(
        parallel_packets
            .iter()
            .map(|packet| packet.identity.external_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("prep-a"), Some("prep-empty"), Some("prep-c")]
    );
    assert_eq!(parallel_packets[0].last_message_id, Some(502));
    assert_eq!(parallel_packets[1].message_count, 0);
    assert_eq!(parallel_packets[2].provenance.origin_kind, "local");
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LexicalRebuildEquivalenceEvidence {
    document_count: usize,
    manifest_fingerprint: String,
    golden_query_digest: String,
}

fn lexical_rebuild_packet_artifact(
    packets: &[LexicalRebuildConversationPacket],
) -> serde_json::Value {
    let packet_rows = packets
        .iter()
        .map(|packet| {
            serde_json::json!({
                "conversation_id": packet.identity.conversation_id,
                "external_id": packet.identity.external_id.as_deref(),
                "agent": &packet.identity.agent,
                "workspace": packet.identity.workspace.as_deref(),
                "source_path": &packet.identity.source_path,
                "source_id": &packet.provenance.source_id,
                "origin_kind": &packet.provenance.origin_kind,
                "origin_host": packet.provenance.origin_host.as_deref(),
                "message_count": packet.message_count,
                "message_bytes": packet.message_bytes,
                "last_message_id": packet.last_message_id,
                "messages": packet.messages.iter().map(|message| {
                    serde_json::json!({
                        "idx": message.idx,
                        "is_tool_role": message.is_tool_role,
                        "created_at": message.created_at,
                        "content": &message.content,
                    })
                }).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    let document_rows = packets
        .iter()
        .flat_map(|packet| {
            packet.prebuilt_docs().into_iter().map(|doc| {
                serde_json::json!({
                    "conversation_id": doc.conversation_id,
                    "agent": doc.agent,
                    "workspace": doc.workspace,
                    "source_path": doc.source_path,
                    "msg_idx": doc.msg_idx,
                    "created_at": doc.created_at,
                    "title": doc.title,
                    "content": doc.content,
                    "source_id": doc.source_id,
                    "origin_kind": doc.origin_kind,
                    "origin_host": doc.origin_host,
                })
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "packets": packet_rows,
        "documents": document_rows,
    })
}

fn stable_json_digest(value: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(value).expect("equivalence artifact should serialize");
    blake3::hash(&bytes).to_hex().to_string()
}

fn lexical_rebuild_golden_query_digest(packets: &[LexicalRebuildConversationPacket]) -> String {
    let queries = [
        "lexical-fixture-1",
        "lexical-fixture-2-second",
        "missing-golden-query",
    ];
    let hits = queries
        .iter()
        .flat_map(|query| {
            packets.iter().flat_map(move |packet| {
                packet.prebuilt_docs().into_iter().filter_map(move |doc| {
                    let title = doc.title.unwrap_or("");
                    let workspace = doc.workspace.unwrap_or("");
                    if doc.content.contains(query)
                        || title.contains(query)
                        || workspace.contains(query)
                        || doc.source_path.contains(query)
                    {
                        Some(serde_json::json!({
                            "query": query,
                            "conversation_id": doc.conversation_id,
                            "agent": doc.agent,
                            "source_path": doc.source_path,
                            "msg_idx": doc.msg_idx,
                            "created_at": doc.created_at,
                            "content": doc.content,
                        }))
                    } else {
                        None
                    }
                })
            })
        })
        .collect::<Vec<_>>();

    stable_json_digest(&serde_json::json!({
        "queries": queries,
        "hits": hits,
    }))
}

fn lexical_rebuild_equivalence_evidence(
    packets: &[LexicalRebuildConversationPacket],
) -> LexicalRebuildEquivalenceEvidence {
    let artifact = lexical_rebuild_packet_artifact(packets);
    LexicalRebuildEquivalenceEvidence {
        document_count: packets
            .iter()
            .map(|packet| packet.prebuilt_docs().len())
            .sum(),
        manifest_fingerprint: stable_json_digest(&artifact),
        golden_query_digest: lexical_rebuild_golden_query_digest(packets),
    }
}

#[test]
fn keyset_batched_lexical_rebuild_matches_legacy_offset_replay_evidence() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("lexical-equivalence.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let legacy_packets = legacy_offset_lexical_rebuild_packets(&storage, 1).unwrap();
    let keyset_packets = keyset_batched_lexical_rebuild_packets(&storage, 2, 2).unwrap();
    let legacy_evidence = lexical_rebuild_equivalence_evidence(&legacy_packets);
    let keyset_evidence = lexical_rebuild_equivalence_evidence(&keyset_packets);
    let legacy_artifact =
        serde_json::to_string_pretty(&lexical_rebuild_packet_artifact(&legacy_packets)).unwrap();
    let keyset_artifact =
        serde_json::to_string_pretty(&lexical_rebuild_packet_artifact(&keyset_packets)).unwrap();

    assert_eq!(
        legacy_packets
            .iter()
            .map(LexicalRebuildConversationPacket::semantic_view)
            .collect::<Vec<_>>(),
        keyset_packets
            .iter()
            .map(LexicalRebuildConversationPacket::semantic_view)
            .collect::<Vec<_>>(),
        "legacy offset replay and keyset batched replay diverged\nlegacy_artifact={legacy_artifact}\nkeyset_artifact={keyset_artifact}"
    );
    assert_eq!(
        legacy_evidence, keyset_evidence,
        "equivalence evidence diverged\nlegacy_artifact={legacy_artifact}\nkeyset_artifact={keyset_artifact}"
    );
    assert_eq!(keyset_evidence.document_count, 4);
}
