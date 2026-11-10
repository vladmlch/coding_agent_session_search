use super::probe_state_db_strict_worker::probe_state_db_strict_worker;
use crate::cli::commands::analytics::run_analytics::{
    close_franken_cli_read_db_without_checkpoint, franken_query_row_map_retry,
    sqlite_header_preflight_error,
};
use crate::doctor::forensic::doctor_sqlite_sidecar_path;
use crate::doctor::probes::doctor_archive_bundle_bytes;
use crate::doctor_recover;
use anyhow::Result;
use std::path::Path;
use std::time::Duration;

/// Compute lightweight state snapshot (index/db freshness) for robot meta and state command reuse
pub(crate) const STATE_DB_OPEN_TIMEOUT: Duration = Duration::from_secs(5);

/// k2k20: one end-to-end wall-clock ceiling for the status-class state probe
/// (strict open + watermark reads + bounded integrity probe + row counts).
/// fsqlite's open and even single-row lookups can hydrate a large part of a
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

/// i6upe: verdict of the bounded real-column integrity probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateDbIntegrity {
    Ok,
    Corrupt,
}

/// True for FrankenError classes that mean the archive's stored pages are
/// malformed — never for busy/locked/transient classes.
pub(crate) fn corruption_class_franken_error(err: &crate::franken_sync::FrankenError) -> bool {
    use crate::franken_sync::FrankenError as E;
    if matches!(
        err,
        E::DatabaseCorrupt { .. } | E::NotADatabase { .. } | E::WalCorrupt { .. }
    ) {
        return true;
    }
    let lower = err.to_string().to_ascii_lowercase();
    lower.contains("malformed") || lower.contains("disk image")
}

/// i6upe: real-column corruption probe used only below a physical archive
/// bundle-size ceiling.
///
/// `SELECT COUNT(*)` is answered from index B-trees without touching table
/// pages, so a DB whose table pages are malformed counts perfectly and only
/// fails when real columns are read (proven on ts2/csd 2026-07-26, where
/// counts matched while every projection failed). Each probe here projects
/// real columns from a single row at both ends of the two hot tables. The SQL
/// shape is logically bounded, but GH #413 proved that fsqlite may still read
/// or hydrate a large fraction of a huge archive while executing it. Callers
/// must therefore enforce a physical main+sidecar byte ceiling before using
/// this probe.
///
/// Returns `(Some(Ok), None)` when every probe reads (or the table is
/// empty), `(Some(Corrupt), Some(detail))` on a corruption-class error, and
/// `(None, None)` when a transient error prevents classification — never a
/// false-positive `Corrupt` from a busy archive.
///
/// `run_probe` executes one probe statement and reads its first column, so
/// the classification is shared by the ordinary-connection lane and the
/// dedicated-owner strict lane (`probe_state_db_strict_bounded`).
pub(crate) fn probe_state_db_integrity(
    run_probe: impl Fn(&str) -> Result<(), crate::franken_sync::FrankenError>,
) -> (Option<StateDbIntegrity>, Option<String>) {
    const PROBES: [&str; 4] = [
        "SELECT id, title, source_path FROM conversations ORDER BY id LIMIT 1",
        "SELECT id, title, source_path FROM conversations ORDER BY id DESC LIMIT 1",
        "SELECT id, conversation_id, role FROM messages ORDER BY id LIMIT 1",
        "SELECT id, conversation_id, role FROM messages ORDER BY id DESC LIMIT 1",
    ];
    for sql in PROBES {
        match run_probe(sql) {
            Ok(()) => {}
            Err(crate::franken_sync::FrankenError::QueryReturnedNoRows) => {}
            Err(err) if corruption_class_franken_error(&err) => {
                return (Some(StateDbIntegrity::Corrupt), Some(err.to_string()));
            }
            Err(_) => return (None, None),
        }
    }
    (Some(StateDbIntegrity::Ok), None)
}

/// Read the canonical database's fixed-size SQLite header, then run the
/// stronger real-column probe only when the complete main+sidecar bundle is
/// below [`STATUS_COUNT_SCAN_MAX_DB_BYTES`]. `Some(detail)` means corruption
/// was classified; `None` means the bounded checks passed or could not safely
/// classify the archive.
///
/// This is deliberately not a complete page-integrity check. Large archives
/// receive only the constant-size header check; deeper corruption still needs
/// a current doctor integrity attestation or an explicit full integrity scan.
pub(crate) fn bounded_canonical_db_corruption_probe(
    db_path: &Path,
    reason: &str,
) -> Option<String> {
    if !db_path.is_file() {
        return None;
    }
    // GH #413: this preflight must have a physical-I/O bound, not merely a
    // SQL row-count bound. fsqlite can currently hydrate a large part of the
    // B-tree while executing an ORDER BY ... LIMIT 1 edge lookup; the old
    // four-query "bounded" probe read 8.4 GiB from a 35.7 GB archive before
    // indexing had even entered its preparing phase. A query timeout would
    // not repair that contract because the synchronous query cannot be
    // cancelled safely from this call site.
    //
    // Reading the fixed 100-byte SQLite database header is genuinely bounded
    // regardless of archive size. The stronger real-column probe below is
    // retained for small bundles, where its worst-case physical work is also
    // bounded, so the earlier i6upe smashed-page guard does not regress.
    let mut header = [0_u8; 100];
    let file_len = std::fs::metadata(db_path).ok()?.len();
    if file_len == 0 {
        // SQLite permits a zero-byte file as a new, empty database.
        return None;
    }
    if file_len < header.len() as u64 {
        return Some(format!(
            "{reason}: truncated SQLite database header ({} bytes; expected at least {})",
            file_len,
            header.len()
        ));
    }
    let mut file = std::fs::File::open(db_path).ok()?;
    std::io::Read::read_exact(&mut file, &mut header).ok()?;

    const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
    if &header[..SQLITE_MAGIC.len()] != SQLITE_MAGIC {
        return Some(format!("{reason}: invalid SQLite database header magic"));
    }

    let encoded_page_size = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if encoded_page_size == 1 {
        65_536_u32
    } else {
        u32::from(encoded_page_size)
    };
    if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
        return Some(format!(
            "{reason}: invalid SQLite database page size {page_size}"
        ));
    }
    if file_len % u64::from(page_size) != 0 {
        return Some(format!(
            "{reason}: SQLite database length {file_len} is not a multiple of page size {page_size}"
        ));
    }
    let reserved_bytes = u32::from(header[20]);
    if page_size.saturating_sub(reserved_bytes) < 480 {
        return Some(format!(
            "{reason}: invalid SQLite reserved-byte count {reserved_bytes} for page size {page_size}"
        ));
    }
    if !matches!(header[18], 1 | 2) || !matches!(header[19], 1 | 2) {
        return Some(format!(
            "{reason}: invalid SQLite file format versions write={} read={}",
            header[18], header[19]
        ));
    }
    if header[21..24] != [64, 32, 32] {
        return Some(format!(
            "{reason}: invalid SQLite payload fractions {},{},{}",
            header[21], header[22], header[23]
        ));
    }

    let mut bundle_bytes = file_len;
    for suffix in ["-wal", "-shm"] {
        let sidecar_path = doctor_sqlite_sidecar_path(db_path, suffix)?;
        match std::fs::symlink_metadata(&sidecar_path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                bundle_bytes = bundle_bytes.saturating_add(metadata.len());
            }
            Ok(_) => return None,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return None,
        }
    }
    if bundle_bytes > STATUS_COUNT_SCAN_MAX_DB_BYTES {
        return None;
    }

    // This observation also runs before a read-only force rebuild has archive
    // writer admission. It must never repair WAL or schema state on open.
    match crate::storage::sqlite::FrankenStorage::open_strict_readonly_with_timeout(
        db_path,
        STATE_DB_OPEN_TIMEOUT,
    ) {
        Ok(storage) => {
            let conn = storage.into_raw();
            let (integrity, detail) = probe_state_db_integrity(|sql| {
                use crate::franken_sync::compat::RowExt;
                franken_query_row_map_retry(&conn, sql, &[], |row| {
                    row.get_typed::<i64>(0).map(|_| ())
                })
            });
            let _ = close_franken_cli_read_db_without_checkpoint(conn, db_path, reason);
            match integrity {
                Some(StateDbIntegrity::Corrupt) => {
                    Some(detail.unwrap_or_else(|| "corruption-class read failure".to_owned()))
                }
                _ => None,
            }
        }
        Err(err) if !crate::storage::sqlite::retryable_franken_anyhow(&err) => {
            let message = format!("{err:#}");
            let lower = message.to_ascii_lowercase();
            // Duplicate `fts_messages` rows in sqlite_master make the strict
            // open report a malformed image, but they describe the derived
            // FTS catalog, not the archive's data. The writable open that
            // follows deduplicates them (dedupe_conflicting_fts_schema_rows_
            // via_sqlite3), so refusing here would block the repair.
            if lower.contains("conflicting virtual-table entries") && lower.contains("fts_messages")
            {
                return None;
            }
            (lower.contains("malformed")
                || lower.contains("disk image")
                || lower.contains("not a database"))
            .then_some(message)
        }
        Err(_) => None,
    }
}

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
///   `cass status` answers instead of sitting silent (GH #413 showed fsqlite
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
