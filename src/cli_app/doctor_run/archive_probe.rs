#[allow(unused_imports)]
use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
use crate::indexer::IndexOptions;
#[allow(unused_imports)]
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
use crate::robot_budget_envelope::BudgetSnapshot;
#[allow(unused_imports)]
use crate::{cli_app::*, cli_root::*, lib_doctor::*, *};
#[allow(unused_imports)]
use anyhow::Result;
#[allow(unused_imports)]
use base64::prelude::*;
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[allow(unused_imports)]
use std::path::{Path, PathBuf};
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{info, warn};
#[allow(unused_imports)]
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Data gathered by the bounded archive-DB doctor probe (#287): row counts,
/// the PRAGMA integrity verdict, and (when the database is healthy) the FTS
/// visibility state — everything `run_doctor_impl` needs to reconstruct the
/// `database`/`fts_table` checks without touching the connection on the main
/// thread.
pub(crate) struct DoctorBoundedArchiveDbProbe {
    pub(crate) conv_count: Option<i64>,
    pub(crate) msg_count: Option<i64>,
    pub(crate) integrity: Option<Result<DoctorDatabaseIntegrityProbe, String>>,
    pub(crate) integrity_skipped_reason: Option<String>,
    pub(crate) fts_state: Option<DoctorFtsTableState>,
    pub(crate) legacy_omp_analytics_pending: Result<bool, String>,
}

pub(crate) enum DoctorBoundedArchiveDbProbeOutcome {
    Completed(DoctorBoundedArchiveDbProbe),
    TimedOut { phase: &'static str },
}

/// Hard deadline for the bounded archive-DB doctor probe (#287).
/// `CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS` overrides it; otherwise the budget
/// scales with the archive bundle size (ub29t / gh#374 issue 5: a fixed 30s
/// was unmeetable on a healthy 1.4GB archive, so the probe effectively never
/// ran at normal scale — every run reported `timeout_or_busy_spin_guard`).
pub(crate) fn doctor_archive_db_probe_hard_timeout(bundle_bytes: u64) -> Duration {
    dotenvy::var("CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|&secs| secs > 0)
        .map_or_else(
            || doctor_probe_timeout_for_bundle(bundle_bytes),
            Duration::from_secs,
        )
}

/// Pure size-scaled budget: 30s base plus 20s per started GiB of archive
/// bundle (main file + WAL), capped at 10 minutes so the doctor stays
/// bounded on multi-GB archives.
pub(crate) fn doctor_probe_timeout_for_bundle(bundle_bytes: u64) -> Duration {
    const BASE_SECS: u64 = 30;
    const SECS_PER_GIB: u64 = 20;
    const CAP_SECS: u64 = 600;
    const GIB: u64 = 1024 * 1024 * 1024;
    let started_gib = bundle_bytes.div_ceil(GIB);
    let secs = BASE_SECS
        .saturating_add(started_gib.saturating_mul(SECS_PER_GIB))
        .min(CAP_SECS);
    Duration::from_secs(secs)
}

/// Archive bundle size the probe budget scales with: the main database file
/// plus its `-wal` sidecar when present. Unreadable metadata counts as zero
/// (the base budget still applies).
pub(crate) fn doctor_archive_bundle_bytes(db_path: &Path) -> u64 {
    let main = std::fs::metadata(db_path)
        .ok()
        .filter(|m| m.file_type().is_file())
        .map_or(0, |m| m.len());
    let wal_path = {
        let mut name = db_path
            .file_name()
            .map(|n| n.to_os_string())
            .unwrap_or_default();
        name.push("-wal");
        db_path.with_file_name(name)
    };
    let wal = std::fs::metadata(wal_path)
        .ok()
        .filter(|m| m.file_type().is_file())
        .map_or(0, |m| m.len());
    main.saturating_add(wal)
}

// `PRAGMA quick_check(1)` limits the number of diagnostics returned; it does
// not limit the number of database pages visited.  FrankenSQLite's synchronous
// PRAGMA executor also does not currently expose a cancellation handle, so a
// thread-level `recv_timeout` can stop the doctor from waiting but cannot stop
// the abandoned worker from continuing to consume CPU and emit trace output.
// Keep routine doctor checks away from that known-unbounded path once the
// archive bundle is large enough for a runaway full-page walk to be material.
// A skipped check is reported as unchecked (or satisfied by a still-current
// cached attestation), never silently promoted to a pass.
pub(crate) const DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Opt-in override for the size gate above (GH #390): operators on long-lived
/// archives need a supported way to run the full-page frankensqlite
/// `integrity_check` through doctor instead of being told to leave the
/// process. The probe still runs under the hard `recv_timeout` deadline
/// (`CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS`), so lifting the byte cap without
/// raising the deadline yields a timed-out probe, not an unbounded wait on the
/// doctor thread.
pub(crate) const CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE: &str =
    "CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE";

/// Wall-clock budget for the `archive_wal` `--fix` checkpoint (GH #382 /
/// g3zyo). `CASS_DOCTOR_WAL_CHECKPOINT_TIMEOUT_SECS` overrides the 120 s
/// default; `0` falls back to the default rather than disabling the bound.
pub(crate) fn doctor_wal_checkpoint_deadline() -> std::time::Duration {
    const DEFAULT_SECS: u64 = 120;
    let secs = dotenvy::var("CASS_DOCTOR_WAL_CHECKPOINT_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_SECS);
    std::time::Duration::from_secs(secs)
}

pub(crate) fn doctor_full_page_integrity_probe_requested() -> bool {
    dotenvy::var(CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE)
        .ok()
        .is_some_and(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

pub(crate) fn doctor_franken_deep_integrity_skip_reason_for_bytes(
    bundle_bytes: u64,
    full_probe_requested: bool,
) -> Option<String> {
    if full_probe_requested {
        return None;
    }
    (bundle_bytes > DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES).then(|| {
        format!(
            "frankensqlite_full_page_integrity_probe_deferred: archive bundle is {bundle_bytes} bytes, above the bounded doctor limit of {DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES} bytes; set {CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE}=1 (and raise CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS) to run the full-page integrity probe anyway"
        )
    })
}

pub(crate) fn doctor_franken_deep_integrity_skip_reason(db_path: &Path) -> Option<String> {
    let main = match std::fs::symlink_metadata(db_path) {
        Ok(metadata) if metadata.file_type().is_file() => metadata.len(),
        Ok(_) => {
            return Some(
                "frankensqlite_full_page_integrity_probe_deferred: archive path is not a regular file"
                    .to_string(),
            );
        }
        Err(err) => {
            return Some(format!(
                "frankensqlite_full_page_integrity_probe_deferred: archive size is unavailable ({err})"
            ));
        }
    };
    let mut bundle_bytes = main;
    let mut wal_name = db_path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    wal_name.push("-wal");
    let wal_path = db_path.with_file_name(wal_name);
    match std::fs::symlink_metadata(&wal_path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            let Some(total) = bundle_bytes.checked_add(metadata.len()) else {
                return Some(
                    "frankensqlite_full_page_integrity_probe_deferred: archive bundle size overflowed"
                        .to_string(),
                );
            };
            bundle_bytes = total;
        }
        Ok(_) => {
            return Some(
                "frankensqlite_full_page_integrity_probe_deferred: WAL sidecar is not a regular file"
                    .to_string(),
            );
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Some(format!(
                "frankensqlite_full_page_integrity_probe_deferred: WAL size is unavailable ({err})"
            ));
        }
    }
    doctor_franken_deep_integrity_skip_reason_for_bytes(
        bundle_bytes,
        doctor_full_page_integrity_probe_requested(),
    )
}

pub(crate) fn doctor_archive_queryable_for_non_destructive_derived_rebuild(
    db_ok: bool,
    row_counts_complete: bool,
    deep_integrity_deferred: bool,
    cached_integrity_failed: bool,
) -> bool {
    db_ok || (row_counts_complete && deep_integrity_deferred && !cached_integrity_failed)
}

pub(crate) fn doctor_canonical_candidate_replacement_authorized(
    db_exists: bool,
    not_initialized: bool,
    affirmative_integrity_failure: bool,
) -> bool {
    affirmative_integrity_failure || (!db_exists && !not_initialized)
}

/// Classify only deterministic corruption-class open failures as affirmative
/// replacement evidence. Timeouts, locks, permissions, and generic engine
/// failures remain unknown and must never authorize canonical replacement.
pub(crate) fn doctor_db_open_error_is_affirmative_integrity_failure(error: &CliError) -> bool {
    error.kind.eq(CliErrorKind::DbOpen.kind_str())
        && doctor_open_error_message_is_affirmative_corruption(&error.message)
}

/// An open failure that proves the file is damaged, as opposed to busy,
/// locked or unreadable for another reason.
pub(crate) fn doctor_open_error_message_is_affirmative_corruption(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("file is too small to contain a sqlite header")
        || message.contains("file header is not sqlite format 3")
        || message.contains("not a database")
        || message.contains("database disk image is malformed")
        || message.contains("databasecorrupt")
        || message.contains("walcorrupt")
}

pub(crate) fn doctor_archive_wide_collectors_deferred_reason(
    deep_integrity_skip_reason: Option<&str>,
    probe_timed_out: bool,
    db_open_failed: bool,
    integrity_unverified: bool,
    integrity_failed: bool,
    busy_or_locked: bool,
) -> Option<String> {
    let cause = if let Some(reason) = deep_integrity_skip_reason {
        reason.to_string()
    } else if probe_timed_out {
        "bounded archive database probe timed out".to_string()
    } else if busy_or_locked {
        "archive database is busy or locked".to_string()
    } else if integrity_failed {
        "archive database has an affirmative integrity failure".to_string()
    } else if db_open_failed {
        "archive database open did not complete successfully".to_string()
    } else if integrity_unverified {
        "archive database health is unverified".to_string()
    } else {
        return None;
    };
    Some(format!(
        "archive_wide_collectors_deferred: {cause}; source inventory, message-level raw-mirror backfill, coverage, and repair authority remain unchecked"
    ))
}

/// zp0fp: while archive-wide collectors are deferred, the recommended action
/// names the command that moves recovery forward for the cause doctor
/// observed. A staged reconstruct candidate goes to the repair dry-run, a
/// buildable one to `doctor --fix`, a held archive waits, and a timed-out or
/// size-deferred probe reruns with the setting that lets it finish.
pub(crate) fn doctor_deferred_archive_recommended_action(
    reason: &str,
    candidate_staged: bool,
    candidate_buildable: bool,
    busy_or_locked: bool,
    probe_timed_out: bool,
    full_page_probe_deferred: bool,
) -> String {
    let next = if candidate_staged {
        "An isolated reconstruct candidate is staged: review its promotion plan with 'cass doctor repair --dry-run --json', then run the apply command that plan prints."
    } else if candidate_buildable {
        "Run 'cass doctor --fix --json' to build an isolated reconstruct candidate from the checksum-verified raw mirror (the live archive is not changed), then review its promotion plan with 'cass doctor repair --dry-run --json'."
    } else if busy_or_locked {
        "Another process holds the archive (an index run, the daemon, or another doctor): wait for it to finish, then rerun 'cass doctor check --json'."
    } else if probe_timed_out {
        "Rerun 'cass doctor check --json' when the machine is idle, with a larger CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS."
    } else if full_page_probe_deferred {
        "Rerun 'cass doctor check --json' with CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE=1 and a larger CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS."
    } else {
        "Run process-isolated archive coverage probes before archive repair."
    };
    format!("{reason}. {next}")
}

/// #287: the doctor's `SELECT COUNT(*)` row counts and
/// `PRAGMA quick_check`/`integrity_check` probes used to run unbounded on the
/// main thread, so a wedged or busy-spinning database made
/// `cass doctor --json` emit zero stdout bytes until an external timeout
/// killed it. Run the connection-consuming probe work on a worker thread with
/// a hard deadline instead: on timeout the doctor marks the `database` check
/// `status:"timeout"` (with the phase that was in flight) and continues to a
/// final JSON verdict. A Rust thread cannot be force-cancelled, so the size
/// gate above prevents known-large full-page PRAGMAs from entering this worker
/// at all. An unexpected timeout on a smaller archive still leaves a detached
/// worker until process exit; process isolation or an upstream FrankenSQLite
/// cancellation handle is required to remove that residual limitation.
pub(crate) fn run_bounded_doctor_archive_db_probe(
    conn: crate::storage::sqlite::FrankenOwnerConnection,
    db_path: &Path,
    timeout: Duration,
) -> DoctorBoundedArchiveDbProbeOutcome {
    let phase = std::sync::Arc::new(std::sync::Mutex::new("row_count_conversations"));
    let (tx, rx) = std::sync::mpsc::channel();
    let worker_phase = std::sync::Arc::clone(&phase);
    let worker_db_path = db_path.to_path_buf();
    let deep_integrity_skip_reason = doctor_franken_deep_integrity_skip_reason(db_path);
    let _probe_worker = std::thread::spawn(move || {
        use crate::franken_sync::compat::RowExt as _;

        let set_phase = |value: &'static str| {
            if let Ok(mut current) = worker_phase.lock() {
                *current = value;
            }
        };
        let conv_count: Option<i64> = conn
            .query_row_map(
                "SELECT COUNT(*) FROM conversations",
                &[],
                |r: &crate::franken_sync::Row| r.get_typed(0),
            )
            .ok();
        set_phase("row_count_messages");
        let msg_count: Option<i64> = conn
            .query_row_map(
                "SELECT COUNT(*) FROM messages",
                &[],
                |r: &crate::franken_sync::Row| r.get_typed(0),
            )
            .ok();
        let mut integrity = None;
        let mut integrity_skipped_reason = None;
        let mut fts_state = None;
        set_phase("legacy_omp_analytics_authority");
        let legacy_omp_analytics_pending =
            crate::storage::sqlite::legacy_omp_analytics_pending_with_query(|sql| conn.query(sql))
                .map_err(|error| format!("{error:#}"));
        if conv_count.is_some() && msg_count.is_some() {
            if let Some(reason) = deep_integrity_skip_reason {
                set_phase("integrity_probe_deferred_large_archive");
                integrity_skipped_reason = Some(reason);
            } else {
                let probe = doctor_database_integrity_probe(&conn, set_phase);
                let healthy = matches!(&probe, Ok(result) if result.is_ok());
                integrity = Some(probe);
                if healthy {
                    set_phase("fts_probe");
                    fts_state = Some(probe_doctor_fts_table(&conn));
                }
            }
        }
        set_phase("connection_close");
        let _ = close_franken_cli_owner_read_db(conn, &worker_db_path, "doctor database health");
        let _ = tx.send(DoctorBoundedArchiveDbProbe {
            conv_count,
            msg_count,
            integrity,
            integrity_skipped_reason,
            fts_state,
            legacy_omp_analytics_pending,
        });
    });

    match rx.recv_timeout(timeout) {
        Ok(probe) => DoctorBoundedArchiveDbProbeOutcome::Completed(probe),
        Err(_) => DoctorBoundedArchiveDbProbeOutcome::TimedOut {
            phase: phase.lock().map(|current| *current).unwrap_or("unknown"),
        },
    }
}

/// GH #457: `doctor` coverage floor for the served lexical document count as
/// a percentage of the archive's `messages` rows. Not every message becomes a
/// document (empty and diverted payloads are skipped; real archives sit
/// between ~45% and ~100%), so this floor is deliberately loose and only
/// catches the hollow shape (3 documents against a million messages).
pub(crate) const DOCTOR_LEXICAL_COVERAGE_FLOOR_PERCENT: u64 = 10;
