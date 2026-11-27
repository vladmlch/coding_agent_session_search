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

/// Compute lightweight state snapshot (index/db freshness) for robot meta and state command reuse
pub(crate) const STATE_DB_OPEN_TIMEOUT: Duration = Duration::from_secs(5);
/// k2k20: one end-to-end wall-clock ceiling for the status-class state probe
/// (strict open + watermark reads + bounded integrity probe + row counts).
/// Opening and even single-row lookups can scan a large part of a
/// multi-GB archive, so a busy timeout alone never bounded `cass status`.
pub(crate) const STATE_DB_PROBE_HARD_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const STATUS_COUNT_SCAN_MAX_DB_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const CLI_DB_QUERY_RETRY_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const CLI_DIAG_DB_OPEN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Default)]
pub(crate) struct StateDbSnapshot {
    pub(crate) conversation_count: i64,
    pub(crate) message_count: i64,
    pub(crate) last_scan_ts: Option<i64>,
    pub(crate) last_indexed_at: Option<i64>,
    pub(crate) opened: bool,
    pub(crate) open_error: Option<String>,
    pub(crate) open_retryable: bool,
    pub(crate) counts_skipped: bool,
    /// `coding_agent_session_search-gi4oy`: true when state_meta_json
    /// elided the FrankenStorage open. `opened` is set to true based on
    /// regular-file metadata alone in that case; callers needing the actual
    /// open-success signal use `cass diag` / `cass doctor`.
    pub(crate) open_skipped: bool,
    /// zn1xn: failure detail when the last full index run left the canonical
    /// `fts_messages` shadow unrepaired (skipped/failed optional derived
    /// fallback-FTS repair). `None` when the shadow is not known half-built.
    pub(crate) fallback_repair_pending: Option<String>,
    /// zn1xn F4: consecutive count of non-watch runs that deferred inline
    /// lexical updates (rerouting through a full authoritative rebuild), and
    /// the last reason. `None` streak when the last run did not defer.
    pub(crate) lexical_repair_deferred_runs: Option<i64>,
    pub(crate) lexical_repair_deferred_reason: Option<String>,
    /// GH #538: a durable watch retry can outlive the process that created
    /// it, even when last_indexed_at is recent and the live watcher holds a
    /// healthy lease. This is source-root debt, not a known session count.
    /// None means the bounded metadata probe could not establish its absence.
    pub(crate) watch_lexical_replay_pending: Option<bool>,
    /// i6upe: result of the bounded real-column integrity probe. `None` when
    /// the probe did not run (watermarks-only health path, unopened DB) or
    /// could not classify (busy/transient error). A probe, not a proof:
    /// corruption on unprobed pages still needs `cass doctor --check`.
    pub(crate) integrity: Option<StateDbIntegrity>,
    /// The corruption error text when `integrity` is `Corrupt`.
    pub(crate) integrity_detail: Option<String>,
}

#[allow(unused_imports)]
pub(crate) use crate::storage::sqlite::open_and_locks::{
    StateDbIntegrity, bounded_canonical_db_corruption_probe, corruption_class_franken_error,
    probe_state_db_integrity,
};

/// Dedicated-owner twin of [`franken_query_row_map_retry`]: bounded retry on
/// engine contention only, never replaying a query whose row-mapping
/// callback failed (negs9).
pub(crate) fn owner_query_row_map_retry<T, F>(
    conn: &crate::storage::sqlite::FrankenOwnerConnection,
    sql: &str,
    map: F,
) -> Result<T, crate::franken_sync::FrankenError>
where
    F: Copy + Fn(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>,
{
    let deadline = std::time::Instant::now() + CLI_DB_QUERY_RETRY_TIMEOUT;
    let mut backoff = Duration::from_millis(4);
    let callback_failed = std::cell::Cell::new(false);
    loop {
        callback_failed.set(false);
        let mapped = conn.query_row_map(sql, &[], |row| {
            map(row).inspect_err(|_| callback_failed.set(true))
        });
        match mapped {
            Ok(value) => return Ok(value),
            Err(err) if callback_failed.get() => return Err(err),
            Err(err) if crate::storage::sqlite::retryable_franken_error(&err) => {
                let now = std::time::Instant::now();
                if now >= deadline {
                    return Err(err);
                }
                let remaining = deadline.saturating_duration_since(now);
                crate::storage::sqlite::sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(64),
                );
            }
            Err(err) => return Err(err),
        }
    }
}

/// Size of a non-empty (frames present) `-wal` sidecar next to `db_path`, or
/// `None` when there is nothing to recover. Mirrors the shape test the
/// ordinary opener's dirty-WAL recovery uses, without performing it.
pub(crate) fn unpublished_wal_sidecar_bytes(db_path: &Path) -> Option<u64> {
    let wal_path = doctor_sqlite_sidecar_path(db_path, "-wal")?;
    let meta = std::fs::symlink_metadata(wal_path).ok()?;
    (meta.file_type().is_file() && meta.len() > 32).then_some(meta.len())
}

/// Message for a strict readonly open failure on the status lane. A hard
/// failure next to an unpublished WAL sidecar is the unclean-shutdown shape
/// the ordinary opener would silently checkpoint; a readiness read reports
/// it and names the command that performs the recovery write instead.
pub(crate) fn state_db_strict_open_error_message(
    db_path: &Path,
    reason: &str,
    err: &anyhow::Error,
    retryable: bool,
) -> String {
    let base = format!(
        "Failed to open {reason} database at {}: strict readonly open failed ({err:#})",
        db_path.display()
    );
    if err.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<crate::franken_sync::FrankenError>(),
            Some(crate::franken_sync::FrankenError::BusyRecovery)
        )
    }) {
        return format!(
            "{base}; WAL-index recovery is required or another connection currently owns recovery. This strict read-only probe leaves the archive unchanged; retry after active work finishes, or run 'cass index' to attempt recovery"
        );
    }
    if retryable {
        return base;
    }
    // GH #503: the #434 shape. The strict open rejects only the derived
    // fts_messages shadow's catalog (a legacy shape older cass wrote); the
    // canonical rows are readable. Name the shadow repair only when the
    // pinned engine can open this catalog for it.
    if !doctor_recover::ENGINE_REPAIRS_MISSING_FTS5_SHADOW_AUTOINDEX
        && doctor_recover::is_missing_fts5_shadow_autoindex_failure(err)
    {
        return format!(
            "{base}; the open rejects only the derived fts_messages FTS5 shadow's legacy catalog (written by cass before the #434 fix), not the conversations or messages. This read-only probe leaves the archive unchanged. This build cannot rebuild that shadow in place (GH #503): keep the archive as it is and do not run 'cass doctor --fix'"
        );
    }
    if doctor_recover::is_fts_shadow_schema_level_open_failure(err) {
        return format!(
            "{base}; the open rejects only the derived fts_messages FTS5 shadow's catalog (a legacy shape written by older cass), not the conversations or messages. This read-only probe leaves the archive unchanged; 'cass doctor --rebuild-canonical-fts --dry-run --json' inspects it and '--yes' drops and rebuilds the derived shadow without modifying canonical rows"
        );
    }
    match unpublished_wal_sidecar_bytes(db_path) {
        Some(wal_bytes) => format!(
            "{base}; an unpublished {wal_bytes}-byte WAL sidecar from an unclean shutdown is present and this read-only probe never modifies the archive, so it was not recovered here — run 'cass index' (or 'cass doctor') to checkpoint it"
        ),
        None => base,
    }
}

/// k2k20 ask #2: the status-class state probe (every non-watermark caller of
/// [`probe_state_db_modes`]) — a readiness READ that must neither modify the
/// archive nor block for minutes on it.
///
/// - Mutation-free: the strict opener never creates the doctor lock
///   hierarchy, deduplicates schema rows, or checkpoints a dirty WAL. An
///   archive that needs a recovery write is reported with the command that
///   performs it (`state_db_strict_open_error_message`).
/// - Bounded: open + watermarks + integrity probe + counts run on a dedicated
///   owner thread; overrunning [`STATE_DB_PROBE_HARD_TIMEOUT`] abandons the
///   worker and reports a retryable open failure naming the last phase, so
///   `cass status` answers instead of sitting silent (GH #413 showed reads
///   can hydrate gigabytes even for an ORDER BY … LIMIT 1 edge lookup).
/// - The real-column integrity probe honors the same physical bundle
///   ceiling as [`bounded_canonical_db_corruption_probe`]; above it the
///   integrity stays `unknown` rather than paying an archive-sized read.
///
/// `watermarks_only` (the `cass health` scope) reads just the single-row
/// `meta` watermarks and skips
/// the integrity probe and row counts. It exists so `cass health` (k2k20 ask
/// #2) shares the strict, mutation-free, hard-deadline opener with `status`
/// instead of the inline read opener, whose sanctioned recovery writes
/// (duplicate-FTS schema repair, dirty-WAL `wal_checkpoint(TRUNCATE)`) made a
/// documented <50 ms observation surface pay a multi-GB recovery bill on the
/// archive it was only supposed to describe.
pub(crate) fn probe_state_db_strict_bounded_scoped(
    db_path: &Path,
    reason: &str,
    busy_timeout: Duration,
    include_counts: bool,
    watermarks_only: bool,
) -> StateDbSnapshot {
    let display_path = db_path.display().to_string();
    // SQLite treats a zero-byte file as a valid, empty database; only a
    // non-empty file that cannot carry a header is a corruption signal.
    let zero_byte_db =
        std::fs::metadata(db_path).is_ok_and(|meta| meta.is_file() && matches!(meta.len(), 0));
    if !zero_byte_db
        && let Some(err) = sqlite_header_preflight_error(db_path, &display_path, reason)
    {
        // A truncated or non-SQLite header is a durable fault, never a
        // "temporarily busy" one, whatever the CLI error class says.
        return StateDbSnapshot {
            counts_skipped: !include_counts,
            open_error: Some(err.message),
            open_retryable: false,
            ..StateDbSnapshot::default()
        };
    }
    // The watermark scope never pays for the integrity probe or the row
    // counts: health reads two meta rows and nothing else.
    let include_counts = include_counts && !watermarks_only;
    let integrity_probe_allowed =
        !watermarks_only && doctor_archive_bundle_bytes(db_path) <= STATUS_COUNT_SCAN_MAX_DB_BYTES;
    let phase = std::sync::Arc::new(std::sync::Mutex::new("open"));
    let (tx, rx) = std::sync::mpsc::channel();
    let worker_phase = std::sync::Arc::clone(&phase);
    let worker_path = db_path.to_path_buf();
    let worker_reason = reason.to_string();
    let _probe_worker = std::thread::spawn(move || {
        let snapshot = probe_state_db_strict_worker(
            &worker_path,
            &worker_reason,
            busy_timeout,
            include_counts,
            integrity_probe_allowed,
            &worker_phase,
        );
        let _ = tx.send(snapshot);
    });
    let (snapshot, timed_out) = match rx.recv_timeout(STATE_DB_PROBE_HARD_TIMEOUT) {
        Ok(snapshot) => (snapshot, false),
        Err(_) => {
            let last_phase = phase.lock().map(|current| *current).unwrap_or("unknown");
            (
                StateDbSnapshot {
                    counts_skipped: true,
                    open_error: Some(format!(
                        "state probe of {reason} database at {display_path} exceeded {}s (last phase: {last_phase}); the archive is very large or busy and this read-only probe did not modify it — run 'cass doctor check --json' for a bounded diagnosis",
                        STATE_DB_PROBE_HARD_TIMEOUT.as_secs()
                    )),
                    open_retryable: true,
                    ..StateDbSnapshot::default()
                },
                true,
            )
        }
    };
    if watermarks_only {
        finalize_health_watermark_snapshot(snapshot, timed_out)
    } else {
        snapshot
    }
}

/// Apply the `cass health` watermark-lane contract to a strict probe result.
///
/// The gi4oy/d0rmo health contract reports `open_skipped=true`,
/// `counts_skipped=true` and null counts whatever happened underneath, and
/// treats a lock-class (retryable) open failure as "a concurrent writer holds
/// the archive", which is not a degraded-state signal: it is elided to an
/// assumed-good open exactly as the pre-#301 fast lane did. Two things are
/// deliberately NOT elided: a hard open failure (GH #396: masking CANTOPEN /
/// corrupt-header made health say `db=available` while every search failed),
/// and the probe's own hard deadline — a 30 s silent open IS the k2k20 defect
/// class, so it surfaces as a retryable open error with the last phase named.
pub(crate) fn finalize_health_watermark_snapshot(
    mut snapshot: StateDbSnapshot,
    timed_out: bool,
) -> StateDbSnapshot {
    snapshot.counts_skipped = true;
    if !timed_out && snapshot.open_error.is_some() && snapshot.open_retryable {
        snapshot.open_error = None;
        snapshot.opened = true;
    }
    // An open we ATTEMPTED and watched fail hard (or time out) must surface
    // as such on every surface; only the assumed-good/successful shapes keep
    // the historical `open_skipped=true` marker.
    snapshot.open_skipped = snapshot.open_error.is_none();
    snapshot
}

#[cfg(test)]
mod health_watermark_probe_tests {
    use super::*;

    /// Write real rows, then close WITHOUT the final checkpoint so the WAL
    /// keeps unreplayed frames — the unclean-shutdown artifact the ordinary
    /// read opener would "recover" with a `wal_checkpoint(TRUNCATE)`.
    fn dirty_wal_db(dir: &Path) -> (PathBuf, u64) {
        let db_path = dir.join("watermarks.db");
        {
            let mut conn = crate::franken_sync::Connection::open(db_path.to_string_lossy())
                .expect("open fixture db");
            conn.execute("PRAGMA application_id = 0x43415353;")
                .expect("stamp application_id");
            conn.execute("PRAGMA user_version = 1;")
                .expect("stamp user_version");
            conn.execute("PRAGMA journal_mode = WAL;")
                .expect("enable WAL journal_mode");
            conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);")
                .expect("create meta");
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('last_indexed_at', '1700000000000'), ('last_scan_ts', '1700000001000');",
            )
            .expect("seed watermarks");
            crate::storage::sqlite::close_franken_in_place_with_busy_retry(&mut conn, false)
                .expect("close without checkpoint");
        }
        let wal_path = crate::storage::sqlite::database_sidecar_path(&db_path, "-wal");
        let wal_len = std::fs::symlink_metadata(&wal_path)
            .map(|meta| meta.len())
            .unwrap_or(0);
        assert!(
            wal_len > 32,
            "precondition: close-without-checkpoint must leave WAL frames (len={wal_len})"
        );
        (db_path, wal_len)
    }

    /// k2k20 (health half): the watermark lane reads its two meta rows
    /// through the strict opener and leaves a dirty WAL exactly as it found
    /// it. Positive observable: watermarks are read. Planted negative: the
    /// WAL byte length is unchanged, i.e. no recovery checkpoint ran.
    /// No-claim: this does not bound wall-clock on a multi-GB archive; the
    /// 30 s hard deadline is exercised by the status-probe tests.
    #[test]
    fn health_watermark_probe_reads_meta_without_checkpointing_a_dirty_wal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (db_path, dirty_len) = dirty_wal_db(dir.path());

        let snapshot = probe_state_db_modes(
            &db_path,
            "state-meta-watermarks",
            Duration::from_secs(2),
            false,
            true,
        );

        assert!(
            snapshot.opened,
            "watermark probe must open the db: {snapshot:?}"
        );
        assert!(snapshot.open_skipped && snapshot.counts_skipped);
        assert_eq!(snapshot.open_error, None);
        assert_eq!(snapshot.last_indexed_at, Some(1_700_000_000_000));
        assert_eq!(snapshot.last_scan_ts, Some(1_700_000_001_000));
        let wal_path = crate::storage::sqlite::database_sidecar_path(&db_path, "-wal");
        let after_len = std::fs::symlink_metadata(&wal_path)
            .map(|meta| meta.len())
            .unwrap_or(0);
        assert_eq!(
            after_len, dirty_len,
            "health must never checkpoint the archive it is describing (WAL {dirty_len} -> {after_len})"
        );
    }

    /// A hard open failure (here: a non-SQLite file) must surface on the
    /// health lane rather than being elided to assumed-good (GH #396).
    #[test]
    fn health_watermark_probe_surfaces_hard_open_failures() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("not-a-db.db");
        std::fs::write(&db_path, b"this is definitely not a SQLite database header").unwrap();

        let snapshot = probe_state_db_modes(
            &db_path,
            "state-meta-watermarks",
            Duration::from_secs(2),
            false,
            true,
        );

        assert!(!snapshot.opened);
        assert!(
            !snapshot.open_skipped,
            "an attempted-and-failed open is never 'skipped'"
        );
        assert!(!snapshot.open_retryable);
        assert!(snapshot.open_error.is_some(), "{snapshot:?}");
    }
}

pub(crate) fn probe_state_db_strict_worker(
    db_path: &Path,
    reason: &str,
    busy_timeout: Duration,
    include_counts: bool,
    integrity_probe_allowed: bool,
    phase: &std::sync::Mutex<&'static str>,
) -> StateDbSnapshot {
    use crate::franken_sync::compat::RowExt;

    let set_phase = |value: &'static str| {
        if let Ok(mut current) = phase.lock() {
            *current = value;
        }
    };
    let mut snapshot = StateDbSnapshot {
        counts_skipped: !include_counts,
        ..StateDbSnapshot::default()
    };
    let conn =
        match crate::storage::sqlite::open_franken_owner_strict_readonly_connection_with_timeout(
            db_path,
            busy_timeout,
        ) {
            Ok(conn) => conn,
            Err(err) => {
                let retryable = crate::storage::sqlite::retryable_franken_anyhow(&err);
                snapshot.open_error = Some(state_db_strict_open_error_message(
                    db_path, reason, &err, retryable,
                ));
                snapshot.open_retryable = retryable;
                return snapshot;
            }
        };
    let busy_ms = busy_timeout.as_millis().clamp(1, u128::from(u32::MAX));
    let _ = conn.execute(&format!("PRAGMA busy_timeout = {busy_ms};"));
    let _ = conn.execute("PRAGMA query_only = 1;");
    snapshot.opened = true;

    set_phase("meta_watermarks");
    let meta_value = |key: &str| {
        owner_query_row_map_retry(
            &conn,
            &format!("SELECT value FROM meta WHERE key = '{key}'"),
            |row| row.get_typed::<String>(0),
        )
        .ok()
    };
    snapshot.last_indexed_at = meta_value("last_indexed_at").and_then(|s| s.parse::<i64>().ok());
    snapshot.last_scan_ts = meta_value("last_scan_ts").and_then(|s| s.parse::<i64>().ok());
    snapshot.fallback_repair_pending =
        meta_value("fts_fallback_repair_pending").filter(|detail| !detail.is_empty());
    snapshot.lexical_repair_deferred_runs = meta_value("lexical_repair_deferred_consecutive_runs")
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|runs| *runs > 0);
    snapshot.lexical_repair_deferred_reason =
        meta_value("lexical_repair_deferred_reason").filter(|detail| !detail.is_empty());

    // The primary-key range and LIMIT keep this independent of archive size
    // and of the number/size of debt payloads. Even malformed debt is pending:
    // a readiness probe must not silently turn unreadable recovery work into
    // success. The existing owner-thread hard deadline bounds this query too.
    set_phase("watch_lexical_replay");
    snapshot.watch_lexical_replay_pending = match owner_query_row_map_retry(
        &conn,
        "SELECT 1 FROM meta WHERE key >= 'watch_lexical_replay_v1:' \
         AND key < 'watch_lexical_replay_v1;' LIMIT 1",
        |row| row.get_typed::<i64>(0),
    ) {
        Ok(_) => Some(true),
        Err(crate::franken_sync::FrankenError::QueryReturnedNoRows) => Some(false),
        Err(error) => {
            tracing::warn!(
                db_path = %db_path.display(),
                %error,
                "could not determine whether durable watch replay is pending"
            );
            None
        }
    };

    // i6upe: real-column corruption probe on the status surface, gated by the
    // physical bundle ceiling (GH #413) — the exact configuration in which
    // corruption previously had NO fast signal is still covered below it.
    if integrity_probe_allowed {
        set_phase("integrity_probe");
        let (integrity, integrity_detail) = probe_state_db_integrity(|sql| {
            owner_query_row_map_retry(&conn, sql, |row| row.get_typed::<i64>(0).map(|_| ()))
        });
        snapshot.integrity = integrity;
        snapshot.integrity_detail = integrity_detail;
    }
    if include_counts {
        set_phase("row_count_conversations");
        snapshot.conversation_count =
            owner_query_row_map_retry(&conn, "SELECT COUNT(*) FROM conversations", |r| {
                r.get_typed(0)
            })
            .unwrap_or(0);
        set_phase("row_count_messages");
        snapshot.message_count =
            owner_query_row_map_retry(&conn, "SELECT COUNT(*) FROM messages", |r| r.get_typed(0))
                .unwrap_or(0);
    }
    set_phase("connection_close");
    let _ = close_franken_cli_owner_read_db(conn, db_path, reason);
    if include_counts {
        // Mirror the ordinary lane's belt-and-braces re-read: a zero count is
        // re-checked once on a fresh strict connection (a stale snapshot must
        // not make `cass status` report an empty archive).
        set_phase("row_count_recheck");
        if snapshot.conversation_count <= 0 {
            snapshot.conversation_count = strict_owner_fresh_count(
                db_path,
                busy_timeout,
                "SELECT COUNT(*) FROM conversations",
            )
            .unwrap_or(snapshot.conversation_count);
        }
        if snapshot.message_count <= 0 {
            snapshot.message_count =
                strict_owner_fresh_count(db_path, busy_timeout, "SELECT COUNT(*) FROM messages")
                    .unwrap_or(snapshot.message_count);
        }
    }
    snapshot
}

/// One COUNT read on a fresh strict dedicated-owner connection.
pub(crate) fn strict_owner_fresh_count(
    db_path: &Path,
    busy_timeout: Duration,
    sql: &str,
) -> Option<i64> {
    use crate::franken_sync::compat::RowExt;

    let mut conn =
        crate::storage::sqlite::open_franken_owner_strict_readonly_connection_with_timeout(
            db_path,
            busy_timeout,
        )
        .ok()?;
    let _ = conn.execute("PRAGMA query_only = 1;");
    let count = owner_query_row_map_retry(&conn, sql, |row| row.get_typed::<i64>(0)).ok();
    if let Err(err) = conn.close_without_checkpoint_sync() {
        tracing::debug!(error = %err, db = %db_path.display(), "strict fresh-count close failed");
    }
    count
}

pub(crate) fn probe_state_db(
    db_path: &Path,
    reason: &str,
    timeout: Duration,
    include_counts: bool,
) -> StateDbSnapshot {
    probe_state_db_modes(db_path, reason, timeout, include_counts, false)
}

/// `coding_agent_session_search` (#301 truthfulness reconciliation):
/// `watermarks_only` opens the canonical DB but reads ONLY the two
/// cheap single-row `meta` watermarks (`last_scan_ts`,
/// `last_indexed_at`) — never the COUNT(*) full-table scans. It is the
/// bridge that lets `cass health` consume the SAME staleness inputs as
/// `cass status` (so the two truth surfaces can no longer disagree)
/// while still honoring health's <50ms budget. The returned snapshot
/// keeps `open_skipped=true` + `counts_skipped=true` + zero counts so
/// the gi4oy/d0rmo health contract (null counts, assumed-good open
/// signal) is preserved byte-for-byte — only the watermark fields are
/// populated. When the open fails with a RETRYABLE (busy/lock-class)
/// error, the watermark-only path degrades to the assumed-good elision
/// (open_skipped=true) rather than surfacing the open error, matching
/// the prior skip-open semantics on the fast readiness surface. Hard
/// (non-retryable) open failures surface as open_error even here — see
/// GH #396: masking them made health report db=available while every
/// search failed on an unopenable archive.
pub(crate) fn probe_state_db_modes(
    db_path: &Path,
    reason: &str,
    timeout: Duration,
    include_counts: bool,
    watermarks_only: bool,
) -> StateDbSnapshot {
    if !db_path.exists() {
        return StateDbSnapshot::default();
    }
    // k2k20 ask #2: every non-watermark caller (status / diag / index --json /
    // the count refresh) is a readiness READ. It opens strictly read-only on
    // a dedicated owner thread under one hard deadline; the health
    // watermark lane below keeps its own fast contract.
    // k2k20 ask #2 (health half): the watermark lane used to be the ONE
    // observation surface still on the inline read opener, whose sanctioned
    // recovery writes (duplicate-FTS schema repair, dirty-WAL
    // `wal_checkpoint(TRUNCATE)`) ran unbounded on the caller's thread — the
    // 13-minute silent `cass health` on a 9.3 GB archive. It now shares the
    // strict, mutation-free, hard-deadline owner-thread probe with `status`,
    // scoped to the two single-row meta watermarks it always read.
    probe_state_db_strict_bounded_scoped(db_path, reason, timeout, include_counts, watermarks_only)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActiveIndexRunDetails {
    pub(crate) pid: Option<u32>,
    pub(crate) started_at_ms: Option<i64>,
    pub(crate) data_dir: PathBuf,
    pub(crate) db_path: PathBuf,
    pub(crate) job_id: Option<String>,
    pub(crate) job_kind: Option<crate::search::asset_state::SearchMaintenanceJobKind>,
    pub(crate) phase: Option<String>,
}

impl ActiveIndexRunDetails {
    pub(crate) fn from_snapshot(
        data_dir: &Path,
        db_path: &Path,
        snapshot: crate::search::asset_state::SearchMaintenanceSnapshot,
    ) -> Option<Self> {
        snapshot.active.then(|| Self {
            pid: snapshot.pid,
            started_at_ms: snapshot.started_at_ms,
            data_dir: data_dir.to_path_buf(),
            db_path: snapshot.db_path.unwrap_or_else(|| db_path.to_path_buf()),
            job_id: snapshot.job_id,
            job_kind: snapshot.job_kind,
            phase: snapshot.phase,
        })
    }

    pub(crate) fn without_owner(data_dir: &Path, db_path: &Path) -> Self {
        Self {
            pid: None,
            started_at_ms: None,
            data_dir: data_dir.to_path_buf(),
            db_path: db_path.to_path_buf(),
            job_id: None,
            job_kind: None,
            phase: None,
        }
    }

    pub(crate) fn started_at_rfc3339(&self) -> Option<String> {
        self.started_at_ms.and_then(format_timestamp_millis_rfc3339)
    }

    pub(crate) fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "data_dir": self.data_dir.display().to_string(),
            "db_path": self.db_path.display().to_string(),
            "pid": self.pid,
            "started_at": self.started_at_rfc3339(),
            "job_id": self.job_id,
            "job_kind": self.job_kind.map(|kind| kind.as_lock_value()),
            "phase": self.phase,
        })
    }

    pub(crate) fn to_cli_error(&self) -> CliError {
        let mut details = Vec::new();
        if let Some(pid) = self.pid {
            details.push(format!("pid {pid}"));
        }
        if let Some(job_id) = &self.job_id {
            details.push(format!("job {job_id}"));
        }
        if let Some(job_kind) = self.job_kind {
            details.push(format!("kind {}", job_kind.as_lock_value()));
        }
        if let Some(phase) = &self.phase {
            details.push(format!("phase {phase}"));
        }
        if let Some(started_at) = self.started_at_rfc3339() {
            details.push(format!("started {started_at}"));
        }
        details.push(format!("db {}", self.db_path.display()));

        let mut message = format!(
            "another 'cass index' run is already active for data dir {}",
            self.data_dir.display()
        );
        if !details.is_empty() {
            message.push_str(&format!(" ({})", details.join(", ")));
        }

        CliError {
            code: 7,
            kind: CliErrorKind::IndexBusy.kind_str(),
            message,
            hint: Some(
                "Wait for the active run to finish or point --data-dir/--db at a different cass dataset."
                    .to_string(),
            ),
            retryable: true,
        }
    }
}

pub(crate) fn format_timestamp_millis_rfc3339(ts: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ts).map(|dt| dt.to_rfc3339())
}

#[allow(unused_imports)]
pub(crate) use crate::model::paths::{
    normalize_path_identity, path_identities_match, stored_path_identity_matches,
};

pub(crate) fn read_index_run_lock_snapshot(
    data_dir: &Path,
) -> crate::search::asset_state::SearchMaintenanceSnapshot {
    // Observation surfaces (status/health/triage/doctor/TUI) use the reaping
    // probe: stale metadata from a dead owner is truncated in place so later
    // readers see a clean lock (#176 / bd-k9jb9). Search-side callers reach
    // the pure-read variant directly (gh#422).
    crate::search::asset_state::read_search_maintenance_snapshot_reaping(data_dir)
}

pub(crate) fn probe_index_run_lock(
    data_dir: &Path,
    db_path: &Path,
    reap_stale: bool,
) -> crate::search::asset_state::SearchMaintenanceSnapshot {
    let snapshot = if reap_stale {
        read_index_run_lock_snapshot(data_dir)
    } else {
        crate::search::asset_state::read_search_maintenance_snapshot(data_dir)
    };
    // An active advisory lock is data-dir scoped. Legacy or interrupted
    // writers may have incomplete db_path metadata, but the held flock is
    // still the authoritative signal that maintenance is in progress.
    if snapshot.active {
        return snapshot;
    }
    if snapshot
        .db_path
        .as_deref()
        .is_none_or(|lock_db_path| !path_identities_match(lock_db_path, db_path))
    {
        return crate::search::asset_state::SearchMaintenanceSnapshot::default();
    }
    snapshot
}

pub(crate) fn active_index_run_details(
    data_dir: &Path,
    db_path: &Path,
) -> Option<ActiveIndexRunDetails> {
    ActiveIndexRunDetails::from_snapshot(data_dir, db_path, read_index_run_lock_snapshot(data_dir))
}

pub(crate) fn error_chain_indicates_active_cass_index(chain: &str) -> bool {
    chain.contains("another cass index process already holds")
}

pub(crate) fn error_chain_indicates_retryable_storage_contention(chain: &str) -> bool {
    crate::storage::sqlite::retryable_storage_error_message(chain)
}

pub(crate) fn error_chain_indicates_index_disk_headroom_blocked(chain: &str) -> bool {
    chain.contains("canonical archive disk headroom check failed")
}

pub(crate) fn error_chain_indicates_lexical_rebuild_disk_stop(chain: &str) -> bool {
    chain.contains(crate::indexer::LEXICAL_REBUILD_DISK_STOP_MARKER)
}

pub(crate) fn error_chain_indicates_orphan_fk_cleanup_blocked(chain: &str) -> bool {
    chain.contains("orphan FK self-heal failed for canonical cass archive")
}

pub(crate) fn error_chain_indicates_archive_health_blocked(chain: &str) -> bool {
    chain.contains("will not replace or truncate the SQLite source of truth")
}

pub(crate) fn error_chain_indicates_unsupported_quill_writer_admission(chain: &str) -> bool {
    chain.contains("cross-process Quill writer admission requires flock semantics")
}

pub(crate) fn index_storage_contention_cli_error(chain: &str) -> CliError {
    CliError {
        code: 7,
        kind: CliErrorKind::IndexBusy.kind_str(),
        message: format!("index failed because the canonical database is busy/locked: {chain}"),
        hint: Some(
            "Retry after the current database writer or indexer finishes; cass refused to replace the canonical database for this transient condition."
                .to_string(),
        ),
        retryable: true,
    }
}

pub(crate) fn index_archive_health_cli_error(chain: &str) -> CliError {
    CliError {
        code: 5,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!("index refused to modify an unhealthy canonical archive: {chain}"),
        hint: Some(
            "Run 'cass doctor check --json' for a read-only diagnosis; do not use index --full/--force-rebuild as a corruption repair path."
                .to_string(),
        ),
        retryable: false,
    }
}

pub(crate) fn index_disk_headroom_cli_error(chain: &str) -> CliError {
    CliError {
        code: 14,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!("index refused to start because disk headroom is too low: {chain}"),
        hint: Some(
            "Free space on the filesystem that holds the cass data directory, then retry indexing. Run 'cass doctor check --json' for a read-only health report."
                .to_string(),
        ),
        retryable: true,
    }
}

/// nw61n: a lexical rebuild that started and then stopped at the free-space
/// floor. Same exit and kind as the start refusal, its own wording: the
/// rebuild ran, kept what it committed, and the rerun resumes it.
pub(crate) fn lexical_rebuild_disk_stop_cli_error(chain: &str) -> CliError {
    CliError {
        code: 14,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!("index stopped the lexical rebuild because free disk space ran low: {chain}"),
        hint: Some(
            "Free space on the filesystem that holds the cass data directory, then rerun the same cass index command; it resumes from what this run committed. Run 'cass doctor check --json' for the headroom a rebuild needs."
                .to_string(),
        ),
        retryable: true,
    }
}

pub(crate) fn index_orphan_fk_cleanup_cli_error(chain: &str) -> CliError {
    CliError {
        code: 5,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!("index stopped before writing because orphan-FK cleanup failed: {chain}"),
        hint: Some(
            "Run 'cass doctor check --json', free disk/memory pressure if reported, then retry indexing after the canonical archive is readable."
                .to_string(),
        ),
        retryable: true,
    }
}

pub(crate) fn index_unsupported_quill_writer_admission_cli_error(chain: &str) -> CliError {
    CliError {
        code: 9,
        kind: CliErrorKind::Index.kind_str(),
        message: format!(
            "indexing is unsupported on this platform because Quill cannot acquire its cross-process writer lock: {chain}"
        ),
        hint: Some(
            "This build cannot create or update the Quill lexical index on this platform. Use a supported Unix or Windows build; Windows support requires Frankensearch 0.4.1 or later. Retrying the same command will not help."
                .to_string(),
        ),
        retryable: false,
    }
}

#[cfg(test)]
mod index_error_mapping_tests {
    use super::*;

    fn idempotency_test_bundle_bytes(db_path: &Path) -> Vec<Option<Vec<u8>>> {
        ["", "-wal", "-shm"]
            .into_iter()
            .map(|suffix| {
                let mut path = db_path.as_os_str().to_owned();
                path.push(suffix);
                match std::fs::read(Path::new(&path)) {
                    Ok(bytes) => Some(bytes),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                    Err(error) => panic!("reading cache fixture {path:?}: {error}"),
                }
            })
            .collect()
    }

    #[test]
    fn gh515_idempotency_lookup_never_creates_or_expires_archive_state() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let data_dir = temp.path().join("data");
        let db_path = temp.path().join("archive-parent/archive.db");
        assert_eq!(read_index_idempotency_cache(&db_path, "missing", 0), None);
        assert!(!db_path.parent().unwrap().exists());

        std::fs::create_dir_all(db_path.parent().unwrap())?;
        let conn = crate::franken_sync::Connection::open(db_path.to_string_lossy())?;
        conn.execute("PRAGMA application_id = 0x43415353;")?;
        conn.execute("PRAGMA user_version = 1;")?;
        conn.execute("CREATE TABLE archive_identity (content TEXT)")?;
        conn.execute("INSERT INTO archive_identity VALUES ('keep this archive')")?;
        conn.close()?;
        let before = idempotency_test_bundle_bytes(&db_path);
        assert_eq!(read_index_idempotency_cache(&db_path, "missing", 0), None);
        assert_eq!(idempotency_test_bundle_bytes(&db_path), before);
        assert!(!data_dir.exists());
        assert!(!db_path.parent().unwrap().join("doctor").exists());

        let payload = serde_json::json!({"success": true, "messages": 1});
        store_index_idempotency_result(&data_dir, &db_path, "expired", 7, &payload, 0)?;
        store_index_idempotency_result(&data_dir, &db_path, "live", 7, &payload, 100)?;
        let before = idempotency_test_bundle_bytes(&db_path);
        let now_ms = 24 * 60 * 60 * 1000;
        assert_eq!(
            read_index_idempotency_cache(&db_path, "expired", now_ms),
            None
        );
        assert_eq!(
            read_index_idempotency_cache(&db_path, "unknown", now_ms),
            None
        );
        assert_eq!(
            read_index_idempotency_cache(&db_path, "live", now_ms),
            Some(("7".to_string(), payload.to_string()))
        );
        assert_eq!(idempotency_test_bundle_bytes(&db_path), before);
        let storage = crate::storage::sqlite::FrankenStorage::open_strict_readonly(&db_path)?;
        assert_eq!(
            storage
                .raw()
                .query_row("SELECT COUNT(*) FROM idempotency_keys")?
                .get_typed::<i64>(0)?,
            2,
            "TTL lookup must leave expired rows for an admitted cache writer"
        );
        assert_eq!(
            storage
                .raw()
                .query_row("SELECT content FROM archive_identity")?
                .get_typed::<String>(0)?,
            "keep this archive"
        );
        storage.close_without_checkpoint()?;
        Ok(())
    }

    #[test]
    fn gh515_idempotency_persistence_obeys_archive_owner_and_recovers() -> Result<()> {
        use crate::search::asset_state::SearchMaintenanceJobKind;

        let temp = tempfile::tempdir()?;
        let owner_data = temp.path().join("owner-data");
        let cache_data = temp.path().join("cache-data");
        let db_path = temp.path().join("archive.db");
        let conn = crate::franken_sync::Connection::open(db_path.to_string_lossy())?;
        conn.execute("PRAGMA application_id = 0x43415353;")?;
        conn.execute("PRAGMA user_version = 1;")?;
        conn.execute("CREATE TABLE archive_identity (content TEXT)")?;
        conn.execute("INSERT INTO archive_identity VALUES ('keep this archive')")?;
        conn.close()?;
        let payload = serde_json::json!({"success": true, "messages": 1});
        let owner = crate::indexer::acquire_search_maintenance_mutation_lock(
            &owner_data,
            &db_path,
            SearchMaintenanceJobKind::SemanticRebuild,
        )?;
        let before = idempotency_test_bundle_bytes(&db_path);
        assert!(
            store_index_idempotency_result(&cache_data, &db_path, "expired", 7, &payload, 0)
                .is_err()
        );
        assert_eq!(read_index_idempotency_cache(&db_path, "expired", 0), None);
        assert_eq!(idempotency_test_bundle_bytes(&db_path), before);
        assert!(std::fs::read(cache_data.join("index-run.lock"))?.is_empty());
        drop(owner);

        store_index_idempotency_result(&cache_data, &db_path, "expired", 7, &payload, 0)?;
        store_index_idempotency_result(&cache_data, &db_path, "live", 7, &payload, 100)?;
        let owner = crate::indexer::acquire_search_maintenance_mutation_lock(
            &owner_data,
            &db_path,
            SearchMaintenanceJobKind::SemanticRebuild,
        )?;
        let now_ms = 24 * 60 * 60 * 1000;
        let before = idempotency_test_bundle_bytes(&db_path);
        assert!(
            store_index_idempotency_result(&cache_data, &db_path, "retry", 7, &payload, now_ms)
                .is_err()
        );
        assert_eq!(idempotency_test_bundle_bytes(&db_path), before);
        drop(owner);

        store_index_idempotency_result(&cache_data, &db_path, "retry", 7, &payload, now_ms)?;
        assert_eq!(
            read_index_idempotency_cache(&db_path, "retry", now_ms),
            Some(("7".to_string(), payload.to_string()))
        );
        assert!(std::fs::read(cache_data.join("index-run.lock"))?.is_empty());
        let storage = crate::storage::sqlite::FrankenStorage::open_strict_readonly(&db_path)?;
        let keys = storage.raw().query_map_collect(
            "SELECT key FROM idempotency_keys ORDER BY key",
            &[],
            |row| row.get_typed::<String>(0),
        )?;
        assert_eq!(keys, ["live", "retry"]);
        assert_eq!(
            storage
                .raw()
                .query_row("SELECT content FROM archive_identity")?
                .get_typed::<String>(0)?,
            "keep this archive"
        );
        for row in storage.raw().query("PRAGMA integrity_check")? {
            assert_eq!(row.get_typed::<String>(0)?, "ok");
        }
        storage.close_without_checkpoint()?;
        Ok(())
    }

    #[test]
    fn index_idempotency_cache_rejects_non_objects_and_preserves_valid_results() {
        for invalid in [
            "",
            "{",
            "null",
            "true",
            "false",
            "42",
            "1.5",
            "\"cached\"",
            "[]",
            "[{}]",
        ] {
            assert_eq!(cached_index_payload(invalid, "retry-key"), None);
        }
        assert_eq!(
            cached_index_payload(
                r#"{"ok":true,"messages":3,"nested":{"value":[1,2]},"cached":false,"idempotency_key":"old-key"}"#,
                "retry-key",
            ),
            Some(serde_json::json!({
                "ok": true,
                "messages": 3,
                "nested": {"value": [1, 2]},
                "cached": true,
                "idempotency_key": "retry-key",
            }))
        );
    }

    #[test]
    fn orphan_fk_cleanup_failure_is_retryable_after_operator_remediation() {
        let err = index_orphan_fk_cleanup_cli_error(
            "orphan FK self-heal failed for canonical cass archive at /tmp/cass.db: out of memory",
        );

        assert_eq!(err.code, 5);
        assert_eq!(err.kind, CliErrorKind::Storage.kind_str());
        assert!(err.retryable);
        assert!(
            err.hint
                .as_deref()
                .is_some_and(|hint| hint.contains("free disk/memory pressure")),
            "hint should name the resource-pressure retry path: {err:?}"
        );
    }

    #[test]
    fn archive_health_failure_remains_non_retryable_index_repair() {
        let err = index_archive_health_cli_error(
            "canonical cass archive at /tmp/cass.db is not safe for indexing: malformed. cass index will not replace or truncate the SQLite source of truth.",
        );

        assert_eq!(err.code, 5);
        assert_eq!(err.kind, CliErrorKind::Storage.kind_str());
        assert!(!err.retryable);
    }

    #[test]
    fn disk_headroom_failure_is_retryable_after_space_is_freed() {
        let err = index_disk_headroom_cli_error(
            "canonical archive disk headroom check failed for /data: available=1 bytes, required=2 bytes",
        );

        assert_eq!(err.code, 14);
        assert_eq!(err.kind, CliErrorKind::Storage.kind_str());
        assert!(err.retryable);
    }

    /// nw61n: the indexer's own mid-rebuild stop maps to the same retryable
    /// exit 14 / `storage` as the start refusal, with resume wording, and is
    /// not mistaken for the start refusal (or the reverse).
    #[test]
    fn lexical_rebuild_disk_stop_is_retryable_and_names_the_resume() {
        let stop = crate::indexer::lexical_rebuild_free_space_verdict(
            1,
            512 * 1024 * 1024,
            Path::new("/data/cass/index"),
        )
        .expect_err("below the floor");
        let chain = format!("lexical rebuild failed | {stop:#}");
        assert!(error_chain_indicates_lexical_rebuild_disk_stop(&chain));
        assert!(!error_chain_indicates_index_disk_headroom_blocked(&chain));
        assert!(!error_chain_indicates_lexical_rebuild_disk_stop(
            "canonical archive disk headroom check failed for /data: available=1 bytes, required=2 bytes"
        ));
        let err = lexical_rebuild_disk_stop_cli_error(&chain);
        assert_eq!(err.code, 14);
        assert_eq!(err.kind, CliErrorKind::Storage.kind_str());
        assert!(err.retryable);
        assert!(err.message.contains("/data/cass/index"), "{err:?}");
        assert!(
            err.hint
                .as_deref()
                .is_some_and(|hint| hint.contains("resumes")),
            "{err:?}"
        );
    }

    #[test]
    fn unsupported_quill_writer_admission_is_non_retryable_and_actionable() {
        let chain = "open Quill writer | verify writer-lock support at C:\\cass\\index: cross-process Quill writer admission requires flock semantics";

        assert!(error_chain_indicates_unsupported_quill_writer_admission(
            chain
        ));
        let err = index_unsupported_quill_writer_admission_cli_error(chain);

        assert_eq!(err.code, 9);
        assert_eq!(err.kind, CliErrorKind::Index.kind_str());
        assert!(!err.retryable);
        assert!(
            err.hint
                .as_deref()
                .is_some_and(|hint| hint.contains("Retrying the same command will not help")),
            "hint must prevent an infinite retry loop: {err:?}"
        );
    }

    #[test]
    fn unrelated_quill_errors_are_not_misclassified_as_platform_gaps() {
        assert!(!error_chain_indicates_unsupported_quill_writer_admission(
            "open Quill writer | another writer currently holds the admission lock"
        ));
        assert!(!error_chain_indicates_unsupported_quill_writer_admission(
            "open Quill reader | unsupported manifest version 99"
        ));
    }
}

pub(crate) fn cli_error_json_payload(err: &CliError, elapsed_ms: u128) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "success": false,
        "error": err.message,
        "elapsed_ms": elapsed_ms,
        "code": err.code,
        "kind": err.kind,
        "retryable": err.retryable,
    });
    if let Some(hint) = &err.hint {
        payload["hint"] = serde_json::json!(hint);
        payload["recommended_action"] = serde_json::json!(hint);
    }
    payload
}
