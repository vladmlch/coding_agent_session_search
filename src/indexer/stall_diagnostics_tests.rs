use super::stall_watchdog::collect_stall_diagnostics;
use clap::CommandFactory;
use tempfile::TempDir;

/// WS-F.2 / #439: the exit-70 path must hand agents the documented error
/// envelope. Positive observable: an abort payload yields the envelope
/// with `kind:"index-stalled"`, `code:70`, `retryable:true`, the phase and
/// stall duration, and the phase-aware hint carried through. Planted
/// negative: a warn-only stall payload (`abort_process` absent or false)
/// yields no envelope at all. No-claim: this does not exercise the process
/// exit or the watchdog timing.
#[test]
pub(crate) fn stall_abort_emits_index_stalled_error_envelope_only_when_aborting() {
    let abort = serde_json::json!({
        "event": "stall_detected",
        "phase": "preparing",
        "stall_elapsed_ms": 305_000_u64,
        "abort_threshold_secs": 300_u64,
        "abort_process": true,
        "exit_code": 70,
        "hint": "phase hint",
    });
    let envelope =
        crate::indexer::stall_watchdog::index_stall_abort_envelope(&abort).expect("abort envelope");
    assert_eq!(envelope["success"], serde_json::json!(false));
    assert_eq!(envelope["code"], serde_json::json!(70));
    assert_eq!(envelope["kind"], serde_json::json!("index-stalled"));
    assert_eq!(envelope["retryable"], serde_json::json!(true));
    assert_eq!(envelope["phase"], serde_json::json!("preparing"));
    assert_eq!(envelope["stall_elapsed_ms"], serde_json::json!(305_000_u64));
    assert_eq!(envelope["abort_threshold_secs"], serde_json::json!(300_u64));
    assert_eq!(envelope["hint"], serde_json::json!("phase hint"));
    assert!(
        envelope["error"]
            .as_str()
            .is_some_and(|message| message.contains("preparing") && message.contains("305s")),
        "error message must name the phase and the stall duration: {envelope}"
    );
    // The line agents read is the compact JSON of that same envelope.
    let line = envelope.to_string();
    let reparsed: serde_json::Value = serde_json::from_str(&line).expect("envelope parses");
    assert_eq!(reparsed, envelope);

    let warn_only = serde_json::json!({
        "event": "stall_detected",
        "phase": "preparing",
        "stall_elapsed_ms": 125_000_u64,
    });
    assert!(crate::indexer::stall_watchdog::index_stall_abort_envelope(&warn_only).is_none());
    let explicit_false = serde_json::json!({ "abort_process": false, "phase": "indexing" });
    assert!(crate::indexer::stall_watchdog::index_stall_abort_envelope(&explicit_false).is_none());
}

#[test]
pub(crate) fn issue_342_index_help_documents_semantic_stall_controls() {
    crate::cli::commands::run_on_large_stack::run_on_large_stack(|| {
        let mut command = crate::cli::args::Cli::command();
        let index = command
            .find_subcommand_mut("index")
            .expect("index subcommand");
        let help = index.render_long_help().to_string();
        let normalized_help = help.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(normalized_help.contains("CASS_INDEX_STALL_DETECT_SECS=0"));
        assert!(normalized_help.contains("CASS_INDEX_STALL_ABORT_SECS=0"));
        assert!(normalized_help.contains("opaque phase"));
    });
}

#[test]
pub(crate) fn empty_data_dir_produces_null_checkpoint_and_no_lock() {
    let tmp = TempDir::new().expect("temp dir");
    let diag = collect_stall_diagnostics(tmp.path());
    let obj = diag.as_object().expect("object");
    assert_eq!(obj["index_dir_exists"], serde_json::json!(false));
    assert!(obj["lexical_rebuild_checkpoint"].is_object());
    assert!(
        obj["lexical_rebuild_checkpoint"]
            .as_object()
            .expect("object")
            .is_empty()
    );
    assert!(!obj.contains_key("index_run_lock"));
}

#[test]
pub(crate) fn captures_checkpoint_contents_and_lock_when_present() {
    let tmp = TempDir::new().expect("temp dir");
    let index_dir = tmp.path().join("index");
    std::fs::create_dir_all(&index_dir).expect("create index dir");
    std::fs::write(
        index_dir.join("lexical_rebuild_state.json"),
        r#"{"completed": true, "committed_conversation_id": 42}"#,
    )
    .expect("write checkpoint");
    std::fs::write(tmp.path().join("index-run.lock"), "pid=12345\nmode=index\n")
        .expect("write lock");
    // Synthetic Tantivy-like segment so we exercise the segment counter.
    std::fs::write(index_dir.join("abcd.idx"), b"x").expect("write segment");

    let diag = collect_stall_diagnostics(tmp.path());
    let obj = diag.as_object().expect("object");
    assert_eq!(obj["index_dir_exists"], serde_json::json!(true));
    assert_eq!(obj["tantivy_segment_files"], serde_json::json!(1));
    let checkpoint = obj["lexical_rebuild_checkpoint"]
        .as_object()
        .expect("checkpoint object");
    let entry = checkpoint["lexical_rebuild_state.json"]
        .as_object()
        .expect("entry");
    assert_eq!(
        entry["content"]["completed"],
        serde_json::json!(true),
        "parsed checkpoint JSON should round-trip"
    );
    let lock = obj["index_run_lock"].as_object().expect("lock object");
    assert_eq!(lock["size_bytes"], serde_json::json!(21));
    assert!(lock.contains_key("modified_ms"));
    assert!(
        lock["content_raw"]
            .as_str()
            .expect("content_raw string")
            .contains("pid=12345")
    );
}

#[test]
pub(crate) fn oversized_checkpoint_omits_content_but_records_size() {
    let tmp = TempDir::new().expect("temp dir");
    let index_dir = tmp.path().join("index");
    std::fs::create_dir_all(&index_dir).expect("create index dir");
    let big = vec![b'x'; 128 * 1024];
    std::fs::write(index_dir.join("lexical_rebuild_state.json"), &big)
        .expect("write oversize checkpoint");

    let diag = collect_stall_diagnostics(tmp.path());
    let entry = diag["lexical_rebuild_checkpoint"]["lexical_rebuild_state.json"]
        .as_object()
        .expect("entry");
    assert!(entry.contains_key("content_omitted"));
    assert!(!entry.contains_key("content"));
    assert_eq!(entry["size_bytes"], serde_json::json!(big.len() as u64));
}

#[test]
pub(crate) fn gh538_stall_diagnostic_reads_enforce_the_limit_on_actual_bytes() -> anyhow::Result<()>
{
    let temp = TempDir::new()?;
    let path = temp.path().join("diagnostic.json");
    std::fs::write(&path, b"12345")?;
    assert_eq!(
        crate::indexer::stall_watchdog::read_bounded_stall_diagnostic(&path, 5)?,
        "12345"
    );
    assert!(crate::indexer::stall_watchdog::read_bounded_stall_diagnostic(&path, 4).is_err());
    Ok(())
}

#[test]
pub(crate) fn gh538_pending_diagnostics_cannot_delay_a_watchdog_abort() -> anyhow::Result<()> {
    use crate::indexer::stall_watchdog::{IndexStallFilesystemProbe, IndexStallWatchdog};
    use crate::indexer::{INDEX_PHASE_LEXICAL_INDEXING, IndexingProgress};
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    let temp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::aborting_lexical_phases(
        temp.path().to_path_buf(),
        Duration::from_millis(50),
    )
    .watch_aware(true);
    // Withhold a probe's result through the actual channel protocol. The
    // live Unix regression separately blocks a real diagnostic file read
    // and verifies process exit 70 while the raw mirror lease is held.
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let started = Instant::now();
    watchdog.filesystem_probe = IndexStallFilesystemProbe::Pending {
        receiver,
        started,
        started_at_ms: 1,
    };
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.last_phase = INDEX_PHASE_LEXICAL_INDEXING;
    watchdog.last_progress_advance = started - Duration::from_millis(100);
    let progress = IndexingProgress::default();
    progress
        .phase
        .store(INDEX_PHASE_LEXICAL_INDEXING, Ordering::Relaxed);
    progress.total.store(1, Ordering::Relaxed);

    let warning = watchdog.observe(&progress, 100).expect("stall warning");
    let abort = watchdog.observe(&progress, 200).expect("stall abort");
    assert_eq!(warning["event"], "stall_detected");
    assert_eq!(abort["event"], "stall_aborting");
    assert_eq!(abort["exit_code"], 70);
    assert_eq!(
        abort["diagnostics"]["filesystem_probe"]["status"],
        "pending"
    );
    assert!(abort["diagnostics"]["raw_mirror_io"].is_array());
    assert_eq!(progress.current.load(Ordering::Relaxed), 0);
    assert_eq!(progress.activity.load(Ordering::Relaxed), 0);

    // Genuine progress may reset stall detection, but cannot spawn more
    // workers while an earlier filesystem probe is still blocked.
    watchdog.filesystem_probe.reset_after_progress();
    let pending = watchdog.filesystem_probe.snapshot(temp.path());
    assert_eq!(pending["filesystem_probe"]["started_at_ms"], 1);
    sender.send((serde_json::json!({"index_dir_exists": true}), 2))?;
    let completed = watchdog.filesystem_probe.snapshot(temp.path());
    assert_eq!(completed["filesystem_probe"]["status"], "complete");
    assert_eq!(completed["filesystem_probe"]["completed_at_ms"], 2);
    assert_eq!(completed["index_dir_exists"], true);
    Ok(())
}

/// Regression for #258: `IndexStallWatchdog` previously short-
/// circuited on `phase_code == 0` (the "preparing" phase before
/// Scanning/Indexing), so any startup wedge that never advanced
/// past phase=0 was invisible. The v0.6.2 watcher reporter saw
/// 4+ hours of silence on exactly that path. The watchdog now
/// fires whenever no progress is observed within the threshold,
/// regardless of phase.
#[test]
pub(crate) fn watchdog_fires_on_phase_zero_startup_wedge() {
    use crate::indexer::IndexingProgress;
    use crate::indexer::stall_watchdog::IndexStallWatchdog;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new().expect("temp dir");
    let mut watchdog = IndexStallWatchdog::new(tmp.path().to_path_buf(), Duration::from_millis(50));
    // Force a tiny stall threshold so the test does not need to
    // sleep 120 s. Using a smaller-than-default value still
    // exercises the same code path because
    // `index_stall_threshold` floors at `progress_interval + 1s`,
    // but for the unit test we drive the watchdog by directly
    // tampering with `last_progress_advance`.
    watchdog.threshold = Some(Duration::from_millis(1));
    // Simulate "indexer just started, phase=0, current=0". Force
    // last_progress_advance into the past so the threshold has
    // already elapsed by the time we call `observe`.
    watchdog.last_phase = 0;
    watchdog.last_current = 0;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    // Caller has not touched phase/current — still 0/0.
    assert_eq!(progress.phase.load(Ordering::Relaxed), 0);
    assert_eq!(progress.current.load(Ordering::Relaxed), 0);

    let payload = watchdog.observe(&progress, 100);
    let payload = payload
        .expect("phase=0 wedges past the stall threshold must emit a stall_detected event (#258)");
    let obj = payload.as_object().expect("event payload is an object");
    assert_eq!(obj["event"], serde_json::json!("stall_detected"));
    assert!(
        obj.contains_key("stall_elapsed_ms"),
        "watchdog event missing stall_elapsed_ms",
    );

    // Second observe call with no progress must be silenced by
    // the per-phase repeat guard.
    let repeat = watchdog.observe(&progress, 200);
    assert!(
        repeat.is_none(),
        "watchdog must not spam repeated events for the same phase",
    );
}

#[test]
pub(crate) fn gh422_watchdog_aborts_a_phase_zero_lexical_startup_wedge() -> anyhow::Result<()> {
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use crate::indexer::{INDEX_PHASE_PREPARING, IndexingProgress};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    );
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.last_phase = INDEX_PHASE_PREPARING;
    watchdog.last_current = 0;
    watchdog.last_activity = 0;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    assert_eq!(
        progress.phase.load(Ordering::Relaxed),
        INDEX_PHASE_PREPARING
    );
    let report = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("preparing wedge did not report"))?;
    assert_eq!(report["event"], serde_json::json!("stall_detected"));
    assert_ne!(report["abort_process"], serde_json::json!(true));

    let abort = watchdog
        .observe(&progress, 200)
        .ok_or_else(|| anyhow::anyhow!("preparing wedge did not request a bounded abort"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["abort_process"], serde_json::json!(true));
    assert_eq!(abort["exit_code"], serde_json::json!(70));
    Ok(())
}

#[test]
pub(crate) fn gh426_watchdog_bounds_analytics_repair_in_all_index_modes() -> anyhow::Result<()> {
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use crate::indexer::{INDEX_PHASE_ANALYTICS_REBUILD, IndexingProgress};
    use anyhow::Context as _;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    for (watch, semantic) in [(false, false), (true, false), (false, true)] {
        let progress = IndexingProgress::default();
        progress.set_phase_progress(INDEX_PHASE_ANALYTICS_REBUILD, 128, 513);
        let mut watchdog = IndexStallWatchdog::with_abort_policy(
            tmp.path().to_path_buf(),
            Duration::from_millis(50),
            IndexStallAbortPolicy::AbortLexicalPhases,
        )
        .watch_aware(watch)
        .semantic_aware(semantic);
        watchdog.abort_all_phases = false;
        watchdog.threshold = Some(Duration::from_millis(1));
        watchdog.abort_threshold = Some(Duration::from_millis(2));
        watchdog.last_phase = INDEX_PHASE_ANALYTICS_REBUILD;
        watchdog.last_current = 128;
        watchdog.last_activity = progress.activity.load(Ordering::Relaxed);
        watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

        let report = watchdog
            .observe(&progress, 100)
            .context("analytics stall report")?;
        assert_eq!(report["event"], "stall_detected");
        assert_ne!(report["abort_process"], true);
        let abort = watchdog
            .observe(&progress, 200)
            .context("analytics stall bound")?;
        assert_eq!(abort["event"], "stall_aborting");
        assert_eq!(abort["exit_code"], 70);
        assert_eq!(abort["abort_process"], true);

        // Committed work resets the stall even in a phase that can abort;
        // an active repair is neither idle watch nor an opaque model build.
        progress.current.store(129, Ordering::Relaxed);
        assert!(watchdog.observe(&progress, 201).is_none());
        assert!(watchdog.stall_abort_reported_for_phase.is_none());
    }
    Ok(())
}

/// fr7wp / gh#413: a pre-index (preparing) stall whose process block IO
/// keeps advancing is a slow canonical open doing real work. The stall
/// REPORT still fires (with the io-active marker), but the abort defers
/// to the finalize-class bound; once IO stops advancing past the detect
/// window, the base abort applies again — the grace is bounded.
#[test]
pub(crate) fn watchdog_grants_bounded_preparing_grace_while_block_io_advances() -> anyhow::Result<()>
{
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use crate::indexer::{INDEX_PHASE_PREPARING, IndexingProgress};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    static IO_COUNTER: AtomicU64 = AtomicU64::new(0);
    fn growing_io() -> Option<u64> {
        Some(IO_COUNTER.fetch_add(4096, Ordering::Relaxed))
    }
    fn frozen_io() -> Option<u64> {
        Some(IO_COUNTER.load(Ordering::Relaxed))
    }

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    );
    watchdog.threshold = Some(Duration::from_secs(60));
    watchdog.abort_threshold = Some(Duration::from_secs(120));
    watchdog.finalize_abort_threshold = Some(Duration::from_secs(3600));
    watchdog.io_probe = growing_io;
    watchdog.last_phase = INDEX_PHASE_PREPARING;
    watchdog.last_current = 0;
    watchdog.last_activity = 0;
    // Base abort threshold (120s) already elapsed; the detect threshold
    // long passed too.
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_secs(200);
    // Seed a baseline below the counter's next sample (fetch_add returns
    // the PREVIOUS value) so the first observation registers an IO
    // advance.
    IO_COUNTER.store(4096, Ordering::Relaxed);
    watchdog.last_io_bytes = Some(0);
    watchdog.last_io_advance = std::time::Instant::now() - Duration::from_secs(200);

    let progress = Arc::new(IndexingProgress::default());
    let report = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("preparing stall did not report"))?;
    assert_eq!(report["event"], serde_json::json!("stall_detected"));
    assert_eq!(report["pre_index_io_active"], serde_json::json!(true));

    // IO advanced within the detect window -> the abort defers to the
    // finalize-class bound (3600s) even though the base 120s elapsed.
    let deferred = watchdog.observe(&progress, 200);
    assert!(
        deferred.is_none(),
        "advancing block IO must defer the pre-index abort to the finalize-class bound",
    );

    // Freeze IO and age the advance stamp past the detect window: the
    // grace lapses and the base abort fires (bounded, per #422). The
    // growing probe's fetch_add left the counter ahead of the last
    // sample, so align the baseline first — a frozen read must not
    // register as one more advance.
    watchdog.io_probe = frozen_io;
    watchdog.last_io_bytes = Some(IO_COUNTER.load(Ordering::Relaxed));
    watchdog.last_io_advance = std::time::Instant::now() - Duration::from_secs(90);
    let abort = watchdog
        .observe(&progress, 300)
        .ok_or_else(|| anyhow::anyhow!("stalled-io preparing wedge did not abort"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["abort_process"], serde_json::json!(true));
    assert_eq!(abort["exit_code"], serde_json::json!(70));
    Ok(())
}

/// Without a block-IO signal (non-Linux, unreadable /proc) the pre-index
/// abort keeps its base threshold — the grace never engages on absence of
/// evidence.
#[test]
pub(crate) fn watchdog_without_io_signal_keeps_base_preparing_abort() -> anyhow::Result<()> {
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use crate::indexer::{INDEX_PHASE_PREPARING, IndexingProgress};
    use std::sync::Arc;
    use std::time::Duration;

    fn no_io() -> Option<u64> {
        None
    }

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    );
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.io_probe = no_io;
    watchdog.last_phase = INDEX_PHASE_PREPARING;
    watchdog.last_current = 0;
    watchdog.last_activity = 0;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    let report = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("preparing wedge did not report"))?;
    assert_eq!(report["event"], serde_json::json!("stall_detected"));
    assert!(report.get("pre_index_io_active").is_none());
    let abort = watchdog
        .observe(&progress, 200)
        .ok_or_else(|| anyhow::anyhow!("preparing wedge did not abort at the base bound"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    Ok(())
}

/// Regression for #332: `current` measures batch publication, not worker
/// activity. While a producer is mid-parse of a large source artifact it
/// ticks `IndexingProgress::activity` per parsed conversation without
/// advancing `current` — the watchdog must treat that tick as forward
/// progress instead of emitting a false `stall_detected`.
#[test]
pub(crate) fn watchdog_treats_activity_ticks_as_progress() {
    use crate::indexer::IndexingProgress;
    use crate::indexer::stall_watchdog::IndexStallWatchdog;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new().expect("temp dir");
    let mut watchdog = IndexStallWatchdog::new(tmp.path().to_path_buf(), Duration::from_millis(50));
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.last_phase = 2;
    watchdog.last_current = 141;
    watchdog.last_activity = 0;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    progress.phase.store(2, Ordering::Relaxed);
    progress.total.store(221, Ordering::Relaxed);
    progress.current.store(141, Ordering::Relaxed);
    // Active parsing: the counter that CAN move during a long parse moved.
    progress.tick_activity();

    assert!(
        watchdog.observe(&progress, 100).is_none(),
        "an advancing activity tick is forward progress, not a stall (#332)"
    );

    // With current AND activity both flat past the threshold, the stall
    // genuinely fires.
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);
    let payload = watchdog
        .observe(&progress, 200)
        .expect("a genuinely idle phase must still stall-detect");
    assert_eq!(payload["event"], serde_json::json!("stall_detected"));
}

/// Regression for #332 (ETA half): while `total` is still an expanding
/// "discovered so far" count, the snapshot must not publish an ETA (it
/// would regress by hours as discovery expands the denominator), and it
/// must label the unit + finality so dashboards can tell the difference.
#[test]
pub(crate) fn snapshot_suppresses_eta_until_total_is_final() {
    use crate::indexer::IndexingProgress;
    use std::sync::atomic::Ordering;

    let progress = IndexingProgress::default();
    progress.phase.store(2, Ordering::Relaxed);
    progress.total.store(221, Ordering::Relaxed);
    progress.current.store(141, Ordering::Relaxed);
    progress.total_is_final.store(false, Ordering::Relaxed);

    let expanding = progress.snapshot_json(60_000);
    assert_eq!(expanding["total_is_final"], serde_json::json!(false));
    assert_eq!(expanding["unit"], serde_json::json!("conversations"));
    assert!(
        expanding["eta_seconds"].is_null(),
        "an expanding denominator must not publish a regressing ETA: {expanding}"
    );
    assert!(
        expanding["rate_per_sec"].is_number(),
        "the observed rate stays available while the total expands"
    );

    progress.total_is_final.store(true, Ordering::Relaxed);
    let stable = progress.snapshot_json(60_000);
    assert_eq!(stable["total_is_final"], serde_json::json!(true));
    assert!(
        stable["eta_seconds"].is_number(),
        "a final denominator publishes a real ETA: {stable}"
    );
}

#[test]
pub(crate) fn watchdog_reports_before_phase_two_abort_event() -> anyhow::Result<()> {
    use crate::indexer::IndexingProgress;
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    );
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.last_phase = 2;
    watchdog.last_current = 0;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    progress.phase.store(2, Ordering::Relaxed);

    let report = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("phase-2 stall observation did not report"))?;
    assert_eq!(report["event"], serde_json::json!("stall_detected"));
    assert_ne!(report["abort_process"], serde_json::json!(true));

    let abort = watchdog
        .observe(&progress, 200)
        .ok_or_else(|| anyhow::anyhow!("phase-2 stall observation did not request abort"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["abort_process"], serde_json::json!(true));
    assert_eq!(abort["exit_code"], serde_json::json!(70));

    let repeat = watchdog.observe(&progress, 300);
    assert!(
        repeat.is_none(),
        "watchdog must not spam repeated abort events for the same stalled phase",
    );
    Ok(())
}

/// Regression for #297: the post-publish / post-rebuild finalize runs
/// under the "preparing" phase (phase 0) with `current == total`. A
/// wedge there used to hang forever because the abort was gated on
/// `phase == 2`. The abort now fires for a quiescent finalize wedge in
/// any phase.
#[test]
pub(crate) fn watchdog_aborts_on_quiescent_finalize_wedge() -> anyhow::Result<()> {
    use crate::indexer::IndexingProgress;
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    );
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.last_phase = 0;
    watchdog.last_current = 100;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    // Post-publish finalize: phase back to "preparing" (0), all work
    // accounted for (current == total), pipeline fully idle.
    progress.phase.store(0, Ordering::Relaxed);
    progress.total.store(100, Ordering::Relaxed);
    progress.current.store(100, Ordering::Relaxed);
    assert!(progress.rebuild_pipeline_is_quiescent());

    let report = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("finalize stall did not report"))?;
    assert_eq!(report["event"], serde_json::json!("stall_detected"));
    assert_ne!(report["abort_process"], serde_json::json!(true));

    let abort = watchdog
        .observe(&progress, 200)
        .ok_or_else(|| anyhow::anyhow!("quiescent finalize wedge did not request abort (#297)"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["abort_process"], serde_json::json!(true));
    assert_eq!(abort["exit_code"], serde_json::json!(70));
    Ok(())
}

/// Regression for #373 Variant B: the phase-2 rebuild *tail* — the terminal
/// Tantivy commit / federated publish that runs with `phase == 2`,
/// `current == total`, a quiescent pipeline, and `is_rebuilding` still set —
/// is a legitimate finalize window. It must get the larger finalize grace
/// (here: never-abort / report-only), not be killed with exit(70) at the
/// normal abort threshold. A quiescent phase-2 tail WITHOUT an active
/// rebuild still aborts, so the grace is not over-broadened.
#[test]
pub(crate) fn watchdog_grants_finalize_grace_to_phase_two_rebuild_tail() -> anyhow::Result<()> {
    use crate::indexer::IndexingProgress;
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let make_watchdog = |tmp: &TempDir| {
        let mut watchdog = IndexStallWatchdog::with_abort_policy(
            tmp.path().to_path_buf(),
            Duration::from_millis(50),
            IndexStallAbortPolicy::AbortLexicalPhases,
        );
        watchdog.threshold = Some(Duration::from_millis(1));
        watchdog.abort_threshold = Some(Duration::from_millis(2));
        // Never-abort during finalize (report-only) so the grace is visible.
        watchdog.finalize_abort_threshold = None;
        watchdog.last_phase = 2;
        watchdog.last_current = 100;
        watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);
        watchdog
    };

    // Active rebuild tail: phase 2, current == total, quiescent, is_rebuilding.
    let tmp = TempDir::new()?;
    let mut watchdog = make_watchdog(&tmp);
    let progress = Arc::new(IndexingProgress::default());
    progress.phase.store(2, Ordering::Relaxed);
    progress.total.store(100, Ordering::Relaxed);
    progress.current.store(100, Ordering::Relaxed);
    progress.is_rebuilding.store(true, Ordering::Relaxed);
    assert!(progress.rebuild_pipeline_is_quiescent());

    let report = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("rebuild tail stall did not report"))?;
    assert_eq!(report["event"], serde_json::json!("stall_detected"));
    assert!(
        watchdog.observe(&progress, 200).is_none(),
        "phase-2 rebuild tail must get the finalize grace, not exit(70) at abort_threshold"
    );

    // Control: identical quiescent phase-2 tail but NOT an active rebuild
    // still aborts at the normal threshold.
    let tmp2 = TempDir::new()?;
    let mut watchdog2 = make_watchdog(&tmp2);
    let progress2 = Arc::new(IndexingProgress::default());
    progress2.phase.store(2, Ordering::Relaxed);
    progress2.total.store(100, Ordering::Relaxed);
    progress2.current.store(100, Ordering::Relaxed);
    // is_rebuilding + finalizing both false.
    assert!(progress2.rebuild_pipeline_is_quiescent());
    assert_eq!(
        watchdog2
            .observe(&progress2, 100)
            .map(|r| r["event"].clone()),
        Some(serde_json::json!("stall_detected"))
    );
    let abort = watchdog2
        .observe(&progress2, 200)
        .ok_or_else(|| anyhow::anyhow!("non-rebuild phase-2 tail should still abort"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["exit_code"], serde_json::json!(70));
    Ok(())
}

/// Regression for gh373 third variant (bead oeu5a): a MID-run phase-2
/// stall — `current < total`, pipeline not necessarily quiescent — while
/// one giant conversation is inside batched persistence
/// (`persist_in_progress` set). That window legitimately runs for
/// minutes (allocator-heavy redaction of a 60-90MB conversation before
/// the first WAL write), so the watchdog must grant it the
/// finalize-class grace instead of exit(70) at the ordinary abort
/// threshold; the identical shape WITHOUT an active persist is a genuine
/// wedge and must still abort. The stall report during the grace must
/// carry the persist-specific hint, not the finalize-WAL decoy.
#[test]
pub(crate) fn watchdog_grants_finalize_grace_during_active_giant_batch_persist()
-> anyhow::Result<()> {
    use crate::indexer::IndexingProgress;
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let make_watchdog = |tmp: &TempDir| {
        let mut watchdog = IndexStallWatchdog::with_abort_policy(
            tmp.path().to_path_buf(),
            Duration::from_millis(50),
            IndexStallAbortPolicy::AbortLexicalPhases,
        );
        watchdog.threshold = Some(Duration::from_millis(1));
        watchdog.abort_threshold = Some(Duration::from_millis(2));
        // Never-abort during the grace (report-only) so it is visible.
        watchdog.finalize_abort_threshold = None;
        watchdog.last_phase = 2;
        watchdog.last_current = 557;
        watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);
        watchdog
    };

    // Active persist mid-run: phase 2, current < total, persist flagged.
    let tmp = TempDir::new()?;
    let mut watchdog = make_watchdog(&tmp);
    let progress = Arc::new(IndexingProgress::default());
    progress.phase.store(2, Ordering::Relaxed);
    progress.total.store(684, Ordering::Relaxed);
    progress.current.store(557, Ordering::Relaxed);
    progress.persist_in_progress.store(true, Ordering::Relaxed);

    let report = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("active-persist stall did not report"))?;
    assert_eq!(report["event"], serde_json::json!("stall_detected"));
    assert_eq!(report["persist_in_progress"], serde_json::json!(true));
    let hint = report["hint"].as_str().unwrap_or_default();
    assert!(
        hint.contains("persist_in_progress: true"),
        "phase-2 persist stall must carry the persist hint, got: {hint}"
    );
    assert!(
        !hint.contains("WAL_AUTOCHECKPOINT"),
        "the finalize-WAL decoy guidance must not appear in the persist hint"
    );
    assert!(
        watchdog.observe(&progress, 200).is_none(),
        "an active giant-batch persist must get the finalize grace, not exit(70)"
    );

    // Bounded liveness: with a finite grace, a genuinely wedged persist
    // still aborts once the grace elapses (first observe consumes the
    // one-shot stall_detected report; the second aborts).
    let tmp2 = TempDir::new()?;
    let mut watchdog2 = make_watchdog(&tmp2);
    watchdog2.finalize_abort_threshold = Some(Duration::from_millis(50));
    watchdog2.last_progress_advance = std::time::Instant::now() - Duration::from_millis(200);
    let detected = watchdog2
        .observe(&progress, 250)
        .ok_or_else(|| anyhow::anyhow!("wedged persist did not report before aborting"))?;
    assert_eq!(detected["event"], serde_json::json!("stall_detected"));
    let abort = watchdog2
        .observe(&progress, 300)
        .ok_or_else(|| anyhow::anyhow!("wedged persist past the grace did not abort"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["exit_code"], serde_json::json!(70));

    // Control: the identical mid-run shape WITHOUT an active persist is a
    // genuine wedge and aborts at the ordinary threshold.
    let tmp3 = TempDir::new()?;
    let mut watchdog3 = make_watchdog(&tmp3);
    let progress3 = Arc::new(IndexingProgress::default());
    progress3.phase.store(2, Ordering::Relaxed);
    progress3.total.store(684, Ordering::Relaxed);
    progress3.current.store(557, Ordering::Relaxed);
    assert_eq!(
        watchdog3
            .observe(&progress3, 100)
            .map(|r| r["event"].clone()),
        Some(serde_json::json!("stall_detected"))
    );
    let abort = watchdog3
        .observe(&progress3, 200)
        .ok_or_else(|| anyhow::anyhow!("mid-run wedge without persist should abort"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["exit_code"], serde_json::json!(70));
    Ok(())
}

/// Regression for #311: in `cass index --watch` mode the post-publish
/// quiescent resting state (phase 0, `current == total`, pipeline idle) is
/// NORMAL idle between rescans, not a wedge. A watch-aware watchdog must
/// neither report a stall nor abort the long-lived daemon with exit(70)
/// (the bug: a deterministic exit-70 crash-loop, systemd restart, repeat).
/// A genuine phase-2 wedge must still abort even in watch mode.
#[test]
pub(crate) fn watchdog_does_not_abort_watch_idle_resting_state() -> anyhow::Result<()> {
    use crate::indexer::IndexingProgress;
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    )
    .watch_aware(true);
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.last_phase = 0;
    watchdog.last_current = 100;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    // The owner has explicitly entered its event wait. Counts alone
    // cannot distinguish this from an active phase-zero filesystem call.
    progress.phase.store(0, Ordering::Relaxed);
    progress.total.store(100, Ordering::Relaxed);
    progress.current.store(100, Ordering::Relaxed);
    progress.watch_idle.store(true, Ordering::Release);
    assert!(progress.rebuild_pipeline_is_quiescent());

    // Well past both detect and abort thresholds: a non-watch watchdog
    // would emit stall_detected then stall_aborting(exit 70) here (see
    // `watchdog_aborts_on_quiescent_finalize_wedge`). Watch-aware: silent.
    assert!(
        watchdog.observe(&progress, 100).is_none(),
        "watch idle resting state must not be reported as a stall (#311)"
    );
    assert!(
        watchdog.observe(&progress, 200).is_none(),
        "watch idle resting state must never trigger exit(70) abort (#311)"
    );

    // A genuine phase-2 wedge in watch mode is still aborted.
    progress.watch_idle.store(false, Ordering::Release);
    progress.phase.store(2, Ordering::Relaxed);
    // The phase change resets the watchdog's progress clock; re-arm it.
    let _ = watchdog.observe(&progress, 250);
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);
    let detected = watchdog
        .observe(&progress, 300)
        .ok_or_else(|| anyhow::anyhow!("phase-2 watch stall did not report"))?;
    assert_eq!(detected["event"], serde_json::json!("stall_detected"));
    let abort = watchdog
        .observe(&progress, 400)
        .ok_or_else(|| anyhow::anyhow!("phase-2 watch stall did not abort"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["exit_code"], serde_json::json!(70));
    Ok(())
}

#[test]
pub(crate) fn gh538_watchdog_idle_preserves_counts_and_rearms_for_active_work() -> anyhow::Result<()>
{
    use crate::indexer::IndexingProgress;
    use crate::indexer::stall_watchdog::{
        IndexStallAbortPolicy, IndexStallFilesystemProbe, IndexStallWatchdog,
    };
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    for (current, total) in [(0, 0), (1, 2), (100, 100)] {
        let tmp = TempDir::new()?;
        let mut watchdog = IndexStallWatchdog::with_abort_policy(
            tmp.path().to_path_buf(),
            Duration::from_millis(50),
            IndexStallAbortPolicy::AbortLexicalPhases,
        )
        .watch_aware(true);
        watchdog.io_probe = || None;
        watchdog.threshold = Some(Duration::from_secs(1));
        watchdog.abort_threshold = Some(Duration::from_secs(2));
        watchdog.last_phase = 0;
        watchdog.last_current = current;
        watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_secs(600);
        let progress = Arc::new(IndexingProgress::default());
        progress.current.store(current, Ordering::Relaxed);
        progress.total.store(total, Ordering::Relaxed);
        progress.watch_idle.store(true, Ordering::Release);

        watchdog.filesystem_probe = IndexStallFilesystemProbe::Complete {
            diagnostics: serde_json::json!({"stale_checkpoint": true}),
            started_at_ms: 1,
            completed_at_ms: 2,
        };
        assert!(watchdog.observe(&progress, 600_000).is_none());
        assert!(matches!(
            watchdog.filesystem_probe,
            IndexStallFilesystemProbe::NotStarted
        ));

        // Idle alone must retire old diagnostics, even with unchanged
        // phase/current/activity. An unresolved worker must instead stay
        // owned, so repeated idle observations cannot spawn duplicates.
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        watchdog.filesystem_probe = IndexStallFilesystemProbe::Pending {
            receiver,
            started: std::time::Instant::now(),
            started_at_ms: 3,
        };
        for _ in 0..2 {
            assert!(watchdog.observe(&progress, 600_000).is_none());
            assert!(matches!(
                watchdog.filesystem_probe,
                IndexStallFilesystemProbe::Pending {
                    started_at_ms: 3,
                    ..
                }
            ));
            let pending = watchdog.filesystem_probe.snapshot(tmp.path());
            assert_eq!(pending["filesystem_probe"]["status"], "pending");
            assert_eq!(pending["filesystem_probe"]["started_at_ms"], 3);
        }
        sender.send((serde_json::json!({"stale_checkpoint": true}), 4))?;
        assert!(watchdog.observe(&progress, 600_000).is_none());
        assert!(matches!(
            watchdog.filesystem_probe,
            IndexStallFilesystemProbe::NotStarted
        ));
        assert_eq!(progress.current.load(Ordering::Relaxed), current);
        assert_eq!(progress.total.load(Ordering::Relaxed), total);
        assert_eq!(progress.activity.load(Ordering::Relaxed), 0);

        // Returning from a long event wait has not used the new callback's
        // stall budget. Then park a phase-zero syscall without any work
        // progress: it must report and abort, even with completed counts.
        progress.watch_idle.store(false, Ordering::Release);
        assert!(watchdog.observe(&progress, 600_001).is_none());
        watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_secs(3);
        let report = watchdog
            .observe(&progress, 603_000)
            .ok_or_else(|| anyhow::anyhow!("active watch syscall did not report"))?;
        assert_eq!(report["event"], serde_json::json!("stall_detected"));
        assert!(report["diagnostics"].get("stale_checkpoint").is_none());
        let abort = watchdog
            .observe(&progress, 603_001)
            .ok_or_else(|| anyhow::anyhow!("active watch syscall did not abort"))?;
        assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
        assert_eq!(abort["exit_code"], serde_json::json!(70));
    }
    Ok(())
}

/// Regression for #315: a one-shot `cass index --semantic` build finishes
/// the lexical rebuild (parking progress at phase 0, `current == total`,
/// pipeline quiescent) and then runs the native embed + HNSW build + vector
/// publish for minutes WITHOUT advancing the progress atomics. That work is
/// shaped exactly like a #297 finalize wedge, so a non-semantic watchdog
/// used to abort mid-embed with exit(70) before publishing any vector index
/// — leaving semantic search permanently unavailable. A semantic-aware
/// watchdog must treat that quiescent finalize state as healthy active work
/// (no exit-70 abort), while a genuine phase-2 lexical wedge still aborts.
#[test]
pub(crate) fn watchdog_does_not_abort_semantic_finalize_build() -> anyhow::Result<()> {
    use crate::indexer::IndexingProgress;
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    )
    .semantic_aware(true);
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.last_phase = 0;
    watchdog.last_current = 100;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    // Identical state to the #297 finalize-wedge test, but this is the
    // semantic embed/publish window: the lexical rebuild published and the
    // native embedder is now grinding through vectors without touching the
    // atomics. Well past both detect and abort thresholds.
    progress.phase.store(0, Ordering::Relaxed);
    progress.total.store(100, Ordering::Relaxed);
    progress.current.store(100, Ordering::Relaxed);
    assert!(progress.rebuild_pipeline_is_quiescent());

    assert!(
        watchdog.observe(&progress, 100).is_none(),
        "semantic finalize build must not be reported as a stall (#315)"
    );
    assert!(
        watchdog.observe(&progress, 200).is_none(),
        "semantic finalize build must never trigger exit(70) before publish (#315)"
    );

    // A genuine phase-2 lexical wedge during a semantic run is still aborted.
    progress.phase.store(2, Ordering::Relaxed);
    // The phase change resets the watchdog's progress clock; re-arm it.
    let _ = watchdog.observe(&progress, 250);
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);
    let detected = watchdog
        .observe(&progress, 300)
        .ok_or_else(|| anyhow::anyhow!("phase-2 semantic stall did not report"))?;
    assert_eq!(detected["event"], serde_json::json!("stall_detected"));
    let abort = watchdog
        .observe(&progress, 400)
        .ok_or_else(|| anyhow::anyhow!("phase-2 semantic stall did not abort"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["exit_code"], serde_json::json!(70));
    Ok(())
}

#[test]
pub(crate) fn issue_342_watchdog_reports_semantic_embedding_stall_without_aborting()
-> anyhow::Result<()> {
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use crate::indexer::{INDEX_PHASE_SEMANTIC_EMBEDDING, IndexingProgress};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    )
    .semantic_aware(true);
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.last_phase = INDEX_PHASE_SEMANTIC_EMBEDDING;
    watchdog.last_current = 7;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    progress
        .phase
        .store(INDEX_PHASE_SEMANTIC_EMBEDDING, Ordering::Relaxed);
    progress.current.store(7, Ordering::Relaxed);
    progress.total.store(100, Ordering::Relaxed);

    let report = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("semantic embedding stall did not report"))?;
    assert_eq!(report["event"], serde_json::json!("stall_detected"));
    assert_eq!(report["phase"], serde_json::json!("semantic_embedding"));
    assert_ne!(report["abort_process"], serde_json::json!(true));
    assert!(
        watchdog.observe(&progress, 200).is_none(),
        "semantic phases must remain report-only past the abort threshold"
    );
    Ok(())
}

/// #437: `CASS_INDEX_STALL_ABORT_ALL_PHASES=1` must promote the existing
/// report-only stall detection to a hard abort in a report-only phase.
/// The exact same setup as
/// `issue_342_watchdog_reports_semantic_embedding_stall_without_aborting`
/// — a frozen semantic-embedding phase past both thresholds — but with the
/// all-phases opt-in armed: the second observation must now carry the
/// `stall_aborting` / `abort_process` / exit-70 contract that the lexical
/// aborts use (and therefore the same finalize path:
/// `abort_after_index_stall_if_requested` checkpoints the WAL best-effort
/// before exiting, identical to lexical-phase aborts).
#[test]
pub(crate) fn gh437_all_phases_optin_promotes_report_only_stall_to_abort() -> anyhow::Result<()> {
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use crate::indexer::{INDEX_PHASE_SEMANTIC_EMBEDDING, IndexingProgress};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    )
    .semantic_aware(true);
    // The opt-in under test (env-armed in production via
    // CASS_INDEX_STALL_ABORT_ALL_PHASES; set directly here so parallel
    // tests never race on process-global env state).
    watchdog.abort_all_phases = true;
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.last_phase = INDEX_PHASE_SEMANTIC_EMBEDDING;
    watchdog.last_current = 7;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    progress
        .phase
        .store(INDEX_PHASE_SEMANTIC_EMBEDDING, Ordering::Relaxed);
    progress.current.store(7, Ordering::Relaxed);
    progress.total.store(100, Ordering::Relaxed);

    let report = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("semantic embedding stall did not report"))?;
    assert_eq!(report["event"], serde_json::json!("stall_detected"));
    assert_ne!(report["abort_process"], serde_json::json!(true));

    let abort = watchdog
        .observe(&progress, 200)
        .ok_or_else(|| anyhow::anyhow!("all-phases opt-in did not promote the stall"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["abort_process"], serde_json::json!(true));
    assert_eq!(abort["exit_code"], serde_json::json!(70));
    Ok(())
}

/// #437 default-off contract: with the opt-in NOT set, the identical
/// report-only stall must keep today's behavior (warn, never abort). This
/// pins that `with_abort_policy` leaves `abort_all_phases` disarmed when
/// the env is absent.
#[test]
pub(crate) fn gh437_without_optin_report_only_stall_is_not_promoted() -> anyhow::Result<()> {
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use crate::indexer::{INDEX_PHASE_SEMANTIC_EMBEDDING, IndexingProgress};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    )
    .semantic_aware(true);
    assert!(
        !watchdog.abort_all_phases,
        "abort_all_phases must default off when CASS_INDEX_STALL_ABORT_ALL_PHASES is unset"
    );
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.last_phase = INDEX_PHASE_SEMANTIC_EMBEDDING;
    watchdog.last_current = 7;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    progress
        .phase
        .store(INDEX_PHASE_SEMANTIC_EMBEDDING, Ordering::Relaxed);
    progress.current.store(7, Ordering::Relaxed);
    progress.total.store(100, Ordering::Relaxed);

    let report = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("semantic embedding stall did not report"))?;
    assert_eq!(report["event"], serde_json::json!("stall_detected"));
    assert!(
        watchdog.observe(&progress, 200).is_none(),
        "without the opt-in, report-only phases must never abort"
    );
    Ok(())
}

/// #437: truthiness contract for CASS_INDEX_STALL_ABORT_ALL_PHASES.
#[test]
pub(crate) fn gh437_all_phases_flag_parses_expected_truthy_values() {
    use crate::indexer::stall_watchdog::stall_abort_all_phases_flag;
    for truthy in ["1", "true", "TRUE", "yes", "on", " 1 "] {
        assert!(
            stall_abort_all_phases_flag(Some(truthy)),
            "{truthy:?} must arm the all-phases abort"
        );
    }
    for falsy in [
        None,
        Some(""),
        Some("0"),
        Some("false"),
        Some("off"),
        Some("2"),
    ] {
        assert!(
            !stall_abort_all_phases_flag(falsy),
            "{falsy:?} must leave the all-phases abort disarmed"
        );
    }
}

#[test]
pub(crate) fn issue_342_watchdog_suppresses_false_stall_for_opaque_semantic_hnsw()
-> anyhow::Result<()> {
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use crate::indexer::{INDEX_PHASE_SEMANTIC_HNSW, IndexingProgress};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    )
    .semantic_aware(true);
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.last_phase = INDEX_PHASE_SEMANTIC_HNSW;
    watchdog.last_current = 0;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    progress
        .phase
        .store(INDEX_PHASE_SEMANTIC_HNSW, Ordering::Relaxed);
    progress.total.store(100, Ordering::Relaxed);

    assert!(watchdog.observe(&progress, 100).is_none());
    assert!(watchdog.observe(&progress, 200).is_none());
    Ok(())
}

/// Regression for #319/#321: the post-publish finalize WAL checkpoint of a
/// large deferred bulk-ingest WAL runs as a synchronous, `!Send`
/// frankensqlite call on the indexer thread (inside
/// `close_storage_after_index`). It cannot advance the progress atomics, so
/// it looks exactly like a #297 finalize wedge — phase 0, `current == total`,
/// pipeline quiescent — while the process is actually alive and checkpointing
/// (minutes on macOS). Before this fix the watchdog aborted it at the normal
/// 300 s threshold with `exit(70)`, killing the checkpoint mid-write and
/// stranding the un-truncated ~1.1 GB WAL (leaving the DB malformed to stock
/// SQLite). The indexer now sets `IndexingProgress::finalizing`; the watchdog
/// must (a) NOT abort while finalizing until the larger
/// `finalize_abort_threshold` elapses, yet (b) still abort a genuinely stuck
/// finalize once that bound passes, and (c) still abort a normal phase-2
/// wedge at the ordinary threshold.
#[test]
pub(crate) fn watchdog_defers_abort_during_final_wal_checkpoint_finalize() -> anyhow::Result<()> {
    use crate::indexer::IndexingProgress;
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    );
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    // Generous finalize grace: much larger than the ordinary abort threshold.
    watchdog.finalize_abort_threshold = Some(Duration::from_millis(500));
    watchdog.last_phase = 0;
    watchdog.last_current = 100;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    // The exact #297 finalize-wedge shape, but the indexer has signalled it
    // is inside the final WAL checkpoint.
    progress.phase.store(0, Ordering::Relaxed);
    progress.total.store(100, Ordering::Relaxed);
    progress.current.store(100, Ordering::Relaxed);
    progress.finalizing.store(true, Ordering::Relaxed);
    assert!(progress.rebuild_pipeline_is_quiescent());

    // Well past the ordinary abort threshold (2 ms) but under the finalize
    // grace (500 ms): the first observe reports the stall, but the watchdog
    // must NOT abort — the checkpoint is legitimate active work.
    let detected = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("finalize stall did not report"))?;
    assert_eq!(detected["event"], serde_json::json!("stall_detected"));
    assert!(
        detected.get("exit_code").is_none(),
        "the stall_detected report must not carry an abort during finalize"
    );
    assert_eq!(detected["finalizing"], serde_json::json!(true));
    for elapsed in [200_u128, 300, 400] {
        assert!(
            watchdog.observe(&progress, elapsed).is_none(),
            "the finalize WAL checkpoint must not be aborted before the finalize grace elapses (#319/#321)"
        );
    }

    // Liveness bound: once the finalize grace itself elapses, a genuinely
    // stuck finalize is still aborted (the checkpoint blocks one thread and
    // cannot be heartbeated, so this bound is the only guard).
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(600);
    let abort = watchdog
        .observe(&progress, 500)
        .ok_or_else(|| anyhow::anyhow!("stuck finalize past the grace did not abort"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["exit_code"], serde_json::json!(70));

    // Control: with `finalizing` cleared, the identical quiescent shape is a
    // #297 wedge again and aborts at the ordinary threshold.
    let mut wedge_watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    );
    wedge_watchdog.threshold = Some(Duration::from_millis(1));
    wedge_watchdog.abort_threshold = Some(Duration::from_millis(2));
    wedge_watchdog.finalize_abort_threshold = Some(Duration::from_millis(500));
    wedge_watchdog.last_phase = 0;
    wedge_watchdog.last_current = 100;
    wedge_watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);
    progress.finalizing.store(false, Ordering::Relaxed);
    let detected = wedge_watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("non-finalizing wedge did not report"))?;
    assert_eq!(detected["event"], serde_json::json!("stall_detected"));
    let abort = wedge_watchdog.observe(&progress, 200).ok_or_else(|| {
        anyhow::anyhow!("non-finalizing wedge did not abort at the ordinary threshold")
    })?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["exit_code"], serde_json::json!(70));
    Ok(())
}

/// Regression for #366: `cass index --full --force-rebuild` on a large
/// corpus was aborted (exit 70) in the gap between ingest completion and
/// the first staged-shard-build progress event. That hand-off matches the
/// #297 finalize-wedge shape (phase 0, `current >= total`, quiescent
/// pipeline) but is legitimate work — e.g. the producer's read-only open
/// replaying a multi-GB WAL — so while `is_rebuilding` is set the
/// watchdog must (a) apply the larger finalize grace, (b) treat an
/// activity tick (pipeline runtime movement) as liveness, and (c) still
/// abort the same shape at the ordinary threshold once the rebuild flag
/// is clear.
#[test]
pub(crate) fn issue_366_watchdog_grants_rebuild_handoff_finalize_grace() -> anyhow::Result<()> {
    use crate::indexer::IndexingProgress;
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    );
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.finalize_abort_threshold = Some(Duration::from_secs(3600));
    watchdog.last_phase = 0;
    watchdog.last_current = 1207;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    // The exact #366 shape: preparing complete, pipeline quiescent, staged
    // rebuild active but no shard-build progress yet.
    let progress = Arc::new(IndexingProgress::default());
    progress.phase.store(0, Ordering::Relaxed);
    progress.total.store(1207, Ordering::Relaxed);
    progress.current.store(1207, Ordering::Relaxed);
    progress.is_rebuilding.store(true, Ordering::Relaxed);
    assert!(progress.rebuild_pipeline_is_quiescent());

    let detected = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("hand-off stall did not report"))?;
    assert_eq!(detected["event"], serde_json::json!("stall_detected"));
    assert!(
        watchdog.observe(&progress, 200).is_none(),
        "an active rebuild's hand-off window must get the finalize grace, \
             not the ordinary abort threshold (#366)"
    );

    // Pipeline runtime movement ticks the activity channel and resets the
    // stall clock entirely.
    progress.tick_activity();
    assert!(watchdog.observe(&progress, 300).is_none());

    // Control: without the rebuild flag the identical quiescent shape is a
    // genuine #297 wedge again — report, then abort, at the ordinary
    // threshold.
    progress.is_rebuilding.store(false, Ordering::Relaxed);
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);
    let detected = watchdog
        .observe(&progress, 400)
        .ok_or_else(|| anyhow::anyhow!("non-rebuild wedge did not report"))?;
    assert_eq!(detected["event"], serde_json::json!("stall_detected"));
    let abort = watchdog
        .observe(&progress, 500)
        .ok_or_else(|| anyhow::anyhow!("non-rebuild wedge did not abort"))?;
    assert_eq!(abort["event"], serde_json::json!("stall_aborting"));
    assert_eq!(abort["exit_code"], serde_json::json!(70));
    Ok(())
}

/// A legitimately slow but *active* rebuild (#294) must never be
/// aborted outside phase 2, even when `current == total` and the stall
/// threshold has elapsed: as long as the pipeline reports in-flight
/// work, the watchdog only reports, never aborts.
#[test]
pub(crate) fn watchdog_does_not_abort_active_rebuild_outside_phase_two() -> anyhow::Result<()> {
    use crate::indexer::IndexingProgress;
    use crate::indexer::stall_watchdog::{IndexStallAbortPolicy, IndexStallWatchdog};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let tmp = TempDir::new()?;
    let mut watchdog = IndexStallWatchdog::with_abort_policy(
        tmp.path().to_path_buf(),
        Duration::from_millis(50),
        IndexStallAbortPolicy::AbortLexicalPhases,
    );
    watchdog.threshold = Some(Duration::from_millis(1));
    watchdog.abort_threshold = Some(Duration::from_millis(2));
    watchdog.last_phase = 0;
    watchdog.last_current = 100;
    watchdog.last_progress_advance = std::time::Instant::now() - Duration::from_millis(100);

    let progress = Arc::new(IndexingProgress::default());
    progress.phase.store(0, Ordering::Relaxed);
    progress.total.store(100, Ordering::Relaxed);
    progress.current.store(100, Ordering::Relaxed);
    // Still one staged shard build in flight: the rebuild is slow, not
    // wedged.
    progress
        .rebuild_pipeline_staged_shard_build_active_jobs
        .store(1, Ordering::Relaxed);
    assert!(!progress.rebuild_pipeline_is_quiescent());

    let report = watchdog
        .observe(&progress, 100)
        .ok_or_else(|| anyhow::anyhow!("finalize stall did not report"))?;
    assert_eq!(report["event"], serde_json::json!("stall_detected"));

    let again = watchdog.observe(&progress, 200);
    assert!(
        again.is_none(),
        "an active (non-quiescent) rebuild must not be aborted outside phase 2 (#294)",
    );
    Ok(())
}
