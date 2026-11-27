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

use super::{DoctorCheck, DoctorDbWalProbeResult, DoctorLexicalWorkspaceReport, DoctorRunState};

#[allow(clippy::collapsible_if, clippy::collapsible_else_if)]
pub(crate) fn probe_doctor_database_and_wal(
    state: DoctorRunState,
) -> (DoctorRunState, DoctorDbWalProbeResult) {
    #[allow(unused_imports)]
    use colored::*;

    let DoctorRunState {
        output_format,
        fix,
        verbose,
        force_rebuild,
        allow_repeated_repair,
        command_surface,
        execution_mode,
        requested_plan_fingerprint,
        wrap,
        start,
        data_dir,
        db_path,
        index_path,
        mut timing_spans,
        maintenance_snapshot,
        rebuild_active,
        cleanup_apply_requested,
        safe_auto_run_requested,
        repair_class,
        initial_failure_marker,
        repeat_refusal_reason,
        repeat_repair_refused,
        override_available,
        override_used,
        _doctor_lock_guard,
        _doctor_db_open_bypass_guard,
        operation_state,
        mutating_lock_acquired,
        fix_can_mutate,
        not_initialized,
        explanation,
        recommended_action,
        mut checks,
        mut needs_rebuild,
        mut db_ok,
        mut db_conversations,
        mut db_messages,
        mut storage_db_open_failed,
        mut storage_integrity_failed,
        mut storage_integrity_unverified,
        mut storage_integrity_skipped_reason,
        mut storage_probe_timed_out,
        storage_lexical_index_drifted,
        mut storage_attestation_check_depth,
        mut storage_attestation_detail,
        dedicated_storage_probe,
        matching_cached_integrity_attestation,
        mut auto_fix_actions,
        mut auto_fix_applied,
        fs_mutation_receipts,
        candidate_promotion_report,
        storage_pressure,
    } = state;

    macro_rules! add_check {
        ($name:expr, $status:expr, $message:expr, $fix_available:expr) => {
            checks.push(DoctorCheck {
                name: $name.to_string(),
                status: $status.to_string(),
                message: $message.to_string(),
                fix_available: $fix_available,
                fix_applied: false,
            });
        };
    }

    // Fix #128: use the CLI read-only opener with a timeout to prevent hanging on degraded
    // databases without dirtying precious archive evidence during a read-only doctor check.
    let archive_db_probe_started = Instant::now();
    if db_path.exists() {
        if dedicated_storage_probe.busy_or_locked {
            add_check!(
                "database",
                "warn",
                "Database is busy or locked by another operation; bounded typed contention probe stopped before the deep integrity check",
                false
            );
        } else {
            let db_open_result = open_franken_cli_read_db_with_hard_timeout(
                db_path.to_path_buf(),
                "doctor database health",
                Duration::from_secs(30),
            );
            match db_open_result {
                Ok(conn) => {
                    // #287: row counts and eligible small-archive PRAGMAs run on a
                    // deadline-bounded worker. Known-large full-page integrity
                    // PRAGMAs are deferred before execution because a thread timeout
                    // cannot cancel the synchronous SQLite PRAGMA executor.
                    let probe_timeout =
                        doctor_archive_db_probe_hard_timeout(doctor_archive_bundle_bytes(&db_path));
                    match run_bounded_doctor_archive_db_probe(conn, &db_path, probe_timeout) {
                        DoctorBoundedArchiveDbProbeOutcome::Completed(probe) => {
                            match &probe.legacy_omp_analytics_pending {
                                Ok(false) => {}
                                Ok(true) => {
                                    add_check!(
                                        "legacy_omp_analytics",
                                        "warn",
                                        format!(
                                            "OMP analytics identity repair is pending even if table counts match; lexical publication is independent. Run {} without --since/--days.",
                                            cass_dataset_command(
                                                &data_dir,
                                                &db_path,
                                                &[
                                                    "analytics",
                                                    "rebuild",
                                                    "--track",
                                                    "all",
                                                    "--json"
                                                ]
                                            )
                                        ),
                                        false
                                    );
                                }
                                Err(error) => {
                                    add_check!(
                                        "legacy_omp_analytics",
                                        "warn",
                                        format!(
                                            "OMP analytics completion authority could not be read: {error}; inspect the archive before maintenance."
                                        ),
                                        false
                                    );
                                }
                            }
                            if let (Some(conv_count), Some(msg_count)) =
                                (probe.conv_count, probe.msg_count)
                            {
                                db_conversations = Some(conv_count.max(0) as usize);
                                db_messages = Some(msg_count.max(0) as usize);
                                if let Some(reason) = probe.integrity_skipped_reason {
                                    storage_integrity_skipped_reason = Some(reason.clone());
                                    add_check!(
                                        "fts_table",
                                        "warn",
                                        format!(
                                            "Database-resident FTS parity probe was intentionally deferred because it performs additional archive-wide counts; {reason}. The Tantivy lexical index is checked separately."
                                        ),
                                        false
                                    );
                                    match matching_cached_integrity_attestation.as_ref() {
                                        Some(attestation)
                                            if attestation.verdict
                                                == crate::search::storage_integrity::IntegrityAttestationVerdict::Pass =>
                                        {
                                            db_ok = true;
                                            add_check!(
                                                "database",
                                                "pass",
                                                format!(
                                                    "Database opened and bounded row counts succeeded ({conv_count} conversations, {msg_count} messages); {reason}; structural health is supplied by a current fingerprint-matching cached {} attestation",
                                                    attestation.check_depth
                                                ),
                                                false
                                            );
                                        }
                                        Some(attestation) => {
                                            storage_integrity_failed = true;
                                            let leaked_pages_remedy =
                                                doctor_leaked_pages_remedy_suffix(
                                                    doctor_recover::attested_integrity_is_leaked_pages_only(
                                                        &attestation.check_depth,
                                                        attestation.detail.as_deref(),
                                                    ),
                                                );
                                            add_check!(
                                                "database",
                                                "fail",
                                                format!(
                                                    "Database opened and bounded row counts succeeded ({conv_count} conversations, {msg_count} messages), but a current fingerprint-matching cached {} attestation records structural failure; {reason}{leaked_pages_remedy}",
                                                    attestation.check_depth
                                                ),
                                                true
                                            );
                                            needs_rebuild = true;
                                        }
                                        None => {
                                            add_check!(
                                                "database",
                                                "warn",
                                                format!(
                                                    "Database opened and bounded row counts succeeded ({conv_count} conversations, {msg_count} messages), but structural integrity is unchecked; {reason}. Run a process-isolated SQLite integrity check with its own hard timeout before any archive repair."
                                                ),
                                                false
                                            );
                                        }
                                    }
                                } else if let Some(integrity_probe) = probe.integrity {
                                    match integrity_probe {
                                        Ok(integrity) if integrity.is_ok() => {
                                            storage_attestation_check_depth =
                                                Some("integrity_check");
                                            db_ok = true;
                                            add_check!(
                                                "database",
                                                "pass",
                                                format!(
                                                    "Database OK ({} conversations, {} messages)",
                                                    conv_count, msg_count
                                                ),
                                                false
                                            );

                                            // Check whether the FTS table is visible through
                                            // SQLite on this connection. Do not auto-register
                                            // it here: on migrated databases with legacy rootpage=0
                                            // FTS schema entries, CREATE VIRTUAL TABLE IF NOT EXISTS
                                            // can persist duplicate sqlite_master rows.
                                            match probe.fts_state {
                                                Some(
                                                    DoctorFtsTableState::QueryableViaFrankensqlite,
                                                ) => {
                                                    // #438: keep queryability distinct from
                                                    // structural validation; never dismiss
                                                    // an independent integrity failure.
                                                    add_check!(
                                                        "fts_table",
                                                        "pass",
                                                        DOCTOR_FTS_TABLE_QUERYABLE_MESSAGE,
                                                        false
                                                    );
                                                }
                                                Some(DoctorFtsTableState::PartialParity {
                                                    indexable_messages,
                                                    indexed_messages,
                                                }) => {
                                                    // #355: a queryable-but-partial shadow previously
                                                    // reported `pass` here while `index --full` failed
                                                    // on it. Lexical search still works via Tantivy,
                                                    // so this warns rather than fails.
                                                    add_check!(
                                                        "fts_table",
                                                        "warn",
                                                        format!(
                                                            "Database-resident FTS shadow is queryable but out of parity with the canonical archive ({indexed_messages} indexed vs {indexable_messages} indexable messages); run `cass doctor --rebuild-canonical-fts --yes` to catch it up. Small drift during an active index run is transient — re-check after it completes"
                                                        ),
                                                        false
                                                    );
                                                }
                                                Some(DoctorFtsTableState::Missing {
                                                    sqlite_error,
                                                    not_viable,
                                                }) => {
                                                    // An absent in-DB FTS shadow is benign
                                                    // here (lexical search falls back to
                                                    // Tantivy), so it does NOT feed the
                                                    // storage_state derivation — doctor
                                                    // reports it as a `pass` below.
                                                    let message = match not_viable {
                                                        Some(detail) => format!(
                                                            "Database-resident FTS shadow was dropped on purpose: {detail}"
                                                        ),
                                                        None => format!(
                                                            "Database-resident FTS table is absent or not queryable via SQLite ({sqlite_error}); lexical search relies on the Tantivy index instead"
                                                        ),
                                                    };
                                                    add_check!("fts_table", "pass", message, false);
                                                }
                                                Some(DoctorFtsTableState::ShadowCorrupt {
                                                    sqlite_error,
                                                }) => {
                                                    // A queryable table whose docsize shadow
                                                    // cannot be counted is the #362/#368
                                                    // corruption class — previously buried
                                                    // inside a `pass` message
                                                    // (adversarial-review F6). Lexical
                                                    // search still works via Tantivy, so
                                                    // warn rather than fail.
                                                    add_check!(
                                                        "fts_table",
                                                        "warn",
                                                        format!(
                                                            "Database-resident FTS shadow is queryable but its docsize shadow table failed to count ({sqlite_error}); the shadow structure is likely corrupt — run `cass doctor --rebuild-canonical-fts --yes` to rebuild it. Lexical search continues via the Tantivy index"
                                                        ),
                                                        false
                                                    );
                                                }
                                                None => {}
                                            }
                                        }
                                        Ok(integrity) => {
                                            storage_integrity_failed = true;
                                            let failed_pragma = integrity.failed_pragma_name();
                                            let diagnostic_summary = integrity.diagnostic_summary();
                                            storage_attestation_check_depth = Some(failed_pragma);
                                            storage_attestation_detail =
                                                Some(diagnostic_summary.clone());
                                            let leaked_pages_remedy =
                                                doctor_leaked_pages_remedy_suffix(
                                                    doctor_recover::integrity_is_leaked_pages_only(
                                                        &integrity.quick_check_status,
                                                        &integrity.integrity_check_diagnostics,
                                                    ),
                                                );
                                            add_check!(
                                                "database",
                                                "fail",
                                                format!(
                                                    "Database failed SQLite {failed_pragma}: {} ({} conversations, {} messages){leaked_pages_remedy}",
                                                    diagnostic_summary, conv_count, msg_count
                                                ),
                                                true
                                            );
                                            needs_rebuild = true;
                                        }
                                        Err(err) => {
                                            storage_integrity_unverified = true;
                                            add_check!(
                                                "database",
                                                "fail",
                                                format!(
                                                    "Database health probe failed via SQLite: {err}"
                                                ),
                                                true
                                            );
                                            needs_rebuild = true;
                                        }
                                    }
                                } else {
                                    storage_integrity_unverified = true;
                                    add_check!(
                                        "database",
                                        "fail",
                                        "Database row counts completed but no integrity verdict was returned",
                                        true
                                    );
                                    needs_rebuild = true;
                                }
                            } else {
                                storage_integrity_unverified = true;
                                add_check!("database", "fail", "Database query failed", true);
                                needs_rebuild = true;
                            }
                        }
                        DoctorBoundedArchiveDbProbeOutcome::TimedOut { phase } => {
                            storage_probe_timed_out = true;
                            // Deliberately NOT a `fail`: a busy or wedged database is
                            // not proof of corruption, so the doctor must not steer
                            // operators toward a rebuild. The `timeout` status keeps
                            // the run bounded and is surfaced through the
                            // `timeout_or_busy_spin_guard` reason code (#287).
                            add_check!(
                                "database",
                                "timeout",
                                format!(
                                    "Database health probe timed out after {}s during {phase}; archive health is unverified (busy or wedged database). Retry later or inspect with `cass status --json`.",
                                    probe_timeout.as_secs()
                                ),
                                false
                            );
                        }
                    }
                }
                Err(e) => {
                    storage_db_open_failed = true;
                    if doctor_db_open_error_is_affirmative_integrity_failure(&e) {
                        storage_integrity_failed = true;
                        storage_attestation_check_depth = Some("archive_open");
                        storage_attestation_detail = Some(e.message.clone());
                    }
                    add_check!(
                        "database",
                        "fail",
                        format!("Cannot open database: {}", e.message),
                        true
                    );
                    needs_rebuild = true;
                }
            }
        }
    } else if not_initialized {
        add_check!(
            "database",
            "warn",
            "Database not initialized yet - no archive has been created in this data dir",
            false
        );
    } else {
        add_check!("database", "fail", "Database not found", true);
        needs_rebuild = true;
    }
    doctor_push_timing_span(
        &mut timing_spans,
        "archive_db_open_integrity",
        "database",
        "archive_db_open_and_integrity",
        archive_db_probe_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["archive DB open, row counts, and integrity-style checks completed or were skipped by state".to_string()],
    );

    // GH #413 follow-up (iify0): a background catch-up that keeps dying looks
    // like nothing more than "stale" from a read path. The breaker state says
    // what happened and where the child's output is.
    if let Some(auto) = crate::indexer::background_refresh::load_state(&data_dir)
        && auto.consecutive_failures > 0
    {
        let tripped =
            auto.consecutive_failures >= crate::indexer::background_refresh::FAILURE_TRIP_THRESHOLD;
        add_check!(
            "auto_refresh",
            "warn",
            format!(
                "{} background catch-up{} ended without advancing the index; auto-refresh is {}. \
                 {}. Run `cass index` in the foreground to see why; the breaker resets when a run \
                 completes",
                auto.consecutive_failures,
                if auto.consecutive_failures == 1 {
                    ""
                } else {
                    "s in a row"
                },
                if tripped {
                    "tripped (no auto-spawn until a run completes)"
                } else {
                    "backing off"
                },
                auto.last_failure.as_deref().unwrap_or("no detail recorded")
            ),
            false
        );
    }

    // WS-B.5 (z2uon): an untruncated WAL is a tax every opener pays (the
    // owner's archive carried 200 MB for 18 days; every default search
    // replayed it). Observe from metadata only; repair only under --fix, never
    // while an index run owns the archive, and never claim a checkpoint that
    // the engine reported as blocked.
    {
        const DOCTOR_WAL_OVERSIZED_BYTES: u64 = 64 * 1024 * 1024;
        let wal_path = crate::storage::sqlite::database_sidecar_path(&db_path, "-wal");
        let wal_bytes = std::fs::metadata(&wal_path)
            .ok()
            .filter(|meta| meta.is_file())
            .map_or(0, |meta| meta.len());
        if wal_bytes <= DOCTOR_WAL_OVERSIZED_BYTES {
            add_check!(
                "archive_wal",
                "pass",
                format!("WAL sidecar is {wal_bytes} bytes (bounded)"),
                false
            );
        } else if rebuild_active {
            add_check!(
                "archive_wal",
                "warn",
                format!(
                    "WAL sidecar is {wal_bytes} bytes while an index run owns the archive; \
                     the run truncates it at finalize"
                ),
                false
            );
        } else if fix_can_mutate {
            // GH #382 / g3zyo: bounded, so an archive whose writable open
            // loops turns into a truthful `fail` with the remedy instead of a
            // doctor that never returns.
            match crate::indexer::checkpoint_wal_truncate_with_deadline(
                &db_path,
                "doctor --fix",
                doctor_wal_checkpoint_deadline(),
            ) {
                Ok(true) => {
                    let after = std::fs::metadata(&wal_path).map_or(0, |meta| meta.len());
                    checks.push(DoctorCheck {
                        name: "archive_wal".to_string(),
                        status: "pass".to_string(),
                        message: format!(
                            "WAL sidecar checkpointed and truncated: {wal_bytes} -> {after} bytes"
                        ),
                        fix_available: true,
                        fix_applied: true,
                    });
                    auto_fix_actions.push(format!(
                        "Checkpointed the archive WAL ({wal_bytes} -> {after} bytes)"
                    ));
                    auto_fix_applied = true;
                }
                Ok(false) => {
                    add_check!(
                        "archive_wal",
                        "warn",
                        format!(
                            "WAL sidecar is {wal_bytes} bytes and the checkpoint was blocked by \
                             a concurrent reader; retry when no other cass process is open"
                        ),
                        true
                    );
                }
                Err(err) => {
                    add_check!(
                        "archive_wal",
                        "fail",
                        format!(
                            "WAL sidecar is {wal_bytes} bytes and the checkpoint failed: {err:#}"
                        ),
                        true
                    );
                }
            }
        } else {
            add_check!(
                "archive_wal",
                "warn",
                format!(
                    "WAL sidecar is {wal_bytes} bytes (> {DOCTOR_WAL_OVERSIZED_BYTES}); every \
                     opener replays it. Run `cass doctor --fix` or `cass index` to checkpoint it. \
                     If either sits in `preparing` with no progress, the writable open is \
                     looping on this WAL (GH #382): back up the archive and its sidecars, then \
                     checkpoint it with stock sqlite3 (`PRAGMA wal_checkpoint(TRUNCATE)`)"
                ),
                true
            );
        }
    }
    // This predicate means only that canonical rows may be consumed without
    // modifying the archive.  It deliberately includes a large archive whose
    // bounded counts succeeded while deep PRAGMAs were deferred, but excludes
    // any current cached FAIL attestation.  Keep `db_ok` for claims that
    // structural health itself was actually attested.
    let archive_queryable_for_non_destructive_derived_rebuild =
        doctor_archive_queryable_for_non_destructive_derived_rebuild(
            db_ok,
            db_conversations.is_some() && db_messages.is_some(),
            storage_integrity_skipped_reason.is_some(),
            matching_cached_integrity_attestation
                .as_ref()
                .is_some_and(|attestation| {
                    attestation.verdict
                        == crate::search::storage_integrity::IntegrityAttestationVerdict::Fail
                }),
        );
    // Canonical replacement needs affirmative evidence; negating
    // `archive_queryable...` would incorrectly turn timeout/busy/unknown into
    // permission to replace precious bytes.
    let canonical_candidate_replacement_authorized =
        doctor_canonical_candidate_replacement_authorized(
            db_path.exists(),
            not_initialized,
            storage_integrity_failed,
        );
    // Kept for the deferred-archive recommendation; the probe itself is
    // consumed by the storage report below.
    let storage_busy_or_locked = dedicated_storage_probe.busy_or_locked;
    let archive_wide_collectors_deferred_reason = doctor_archive_wide_collectors_deferred_reason(
        storage_integrity_skipped_reason.as_deref(),
        storage_probe_timed_out,
        storage_db_open_failed,
        storage_integrity_unverified,
        storage_integrity_failed,
        storage_busy_or_locked,
    );
    let archive_wide_collectors_complete = archive_wide_collectors_deferred_reason.is_none();

    let state = DoctorRunState {
        output_format,
        fix,
        verbose,
        force_rebuild,
        allow_repeated_repair,
        command_surface,
        execution_mode,
        requested_plan_fingerprint,
        wrap,
        start,
        data_dir,
        db_path,
        index_path,
        timing_spans,
        maintenance_snapshot,
        rebuild_active,
        cleanup_apply_requested,
        safe_auto_run_requested,
        repair_class,
        initial_failure_marker,
        repeat_refusal_reason,
        repeat_repair_refused,
        override_available,
        override_used,
        _doctor_lock_guard,
        _doctor_db_open_bypass_guard,
        operation_state,
        mutating_lock_acquired,
        fix_can_mutate,
        not_initialized,
        explanation,
        recommended_action,
        checks,
        needs_rebuild,
        db_ok,
        db_conversations,
        db_messages,
        storage_db_open_failed,
        storage_integrity_failed,
        storage_integrity_unverified,
        storage_integrity_skipped_reason,
        storage_probe_timed_out,
        storage_lexical_index_drifted,
        storage_attestation_check_depth,
        storage_attestation_detail,
        dedicated_storage_probe,
        matching_cached_integrity_attestation,
        auto_fix_actions,
        auto_fix_applied,
        fs_mutation_receipts,
        candidate_promotion_report,
        storage_pressure,
    };
    let db_wal = DoctorDbWalProbeResult {
        archive_db_probe_started,
        archive_queryable_for_non_destructive_derived_rebuild,
        canonical_candidate_replacement_authorized,
        storage_busy_or_locked,
        archive_wide_collectors_deferred_reason,
        archive_wide_collectors_complete,
    };
    (state, db_wal)
}

#[allow(clippy::collapsible_if, clippy::collapsible_else_if)]
pub(crate) fn probe_doctor_lexical_and_workspace(
    state: DoctorRunState,
    db_wal: DoctorDbWalProbeResult,
) -> (DoctorRunState, DoctorLexicalWorkspaceReport) {
    #[allow(unused_imports)]
    use colored::*;

    let DoctorRunState {
        output_format,
        fix,
        verbose,
        force_rebuild,
        allow_repeated_repair,
        command_surface,
        execution_mode,
        requested_plan_fingerprint,
        wrap,
        start,
        data_dir,
        db_path,
        index_path,
        mut timing_spans,
        maintenance_snapshot,
        rebuild_active,
        cleanup_apply_requested,
        safe_auto_run_requested,
        repair_class,
        initial_failure_marker,
        repeat_refusal_reason,
        repeat_repair_refused,
        override_available,
        override_used,
        _doctor_lock_guard,
        _doctor_db_open_bypass_guard,
        operation_state,
        mutating_lock_acquired,
        fix_can_mutate,
        not_initialized,
        explanation,
        recommended_action,
        mut checks,
        mut needs_rebuild,
        db_ok,
        db_conversations,
        db_messages,
        storage_db_open_failed,
        storage_integrity_failed,
        storage_integrity_unverified,
        storage_integrity_skipped_reason,
        storage_probe_timed_out,
        mut storage_lexical_index_drifted,
        storage_attestation_check_depth,
        storage_attestation_detail,
        dedicated_storage_probe,
        matching_cached_integrity_attestation,
        auto_fix_actions,
        auto_fix_applied,
        fs_mutation_receipts,
        candidate_promotion_report,
        storage_pressure,
    } = state;

    let DoctorDbWalProbeResult {
        archive_db_probe_started,
        archive_queryable_for_non_destructive_derived_rebuild,
        canonical_candidate_replacement_authorized,
        storage_busy_or_locked,
        archive_wide_collectors_deferred_reason,
        archive_wide_collectors_complete,
    } = db_wal;

    macro_rules! add_check {
        ($name:expr, $status:expr, $message:expr, $fix_available:expr) => {
            checks.push(DoctorCheck {
                name: $name.to_string(),
                status: $status.to_string(),
                message: $message.to_string(),
                fix_available: $fix_available,
                fix_applied: false,
            });
        };
    }

    // 4. Check Tantivy index exists and is readable
    let lexical_probe_started = Instant::now();
    if crate::search::tantivy::searchable_index_exists(&index_path) {
        match crate::search::tantivy::searchable_index_summary(&index_path) {
            Ok(Some(summary)) => {
                let num_docs = summary.docs;
                add_check!(
                    "index",
                    "pass",
                    format!("Search index OK ({} documents)", num_docs),
                    false
                );

                // #441 / WS-B.1a: a generation that fragmented into hundreds
                // of segments makes every query pay `segments × terms`
                // dictionary walks and exhausts the engine's per-query fuel
                // on ordinary stopword-heavy queries. Doctor may spend an
                // engine open, so it reports the LIVE segment count (what a
                // query pays for); folded inputs linger on disk after a merge,
                // so the file count would keep warning after a successful
                // consolidation. Files are the fallback when the reader fails.
                let live_segments = crate::search::quill_bridge::live_segment_count(&index_path);
                let counted = live_segments.map(|n| (n, "segments")).or_else(|| {
                    crate::search::quill_bridge::segment_file_count(&index_path)
                        .map(|n| (n, "segment files"))
                });
                if let Some((segments, unit)) = counted {
                    let pressure = crate::search::quill_bridge::CASS_SEGMENT_PRESSURE_FILES;
                    if segments > pressure {
                        add_check!(
                            "index_segments",
                            "warn",
                            format!(
                                "Search index has {segments} {unit} (> {pressure}); queries pay \
                                 per segment and long queries can exhaust the engine's fuel \
                                 budget. Run `cass index`: its maintenance pass folds the \
                                 fragmented generation in place (`--full` rebuilds from scratch \
                                 and needs the headroom full_rebuild_readiness reports)"
                            ),
                            true
                        );
                    } else {
                        add_check!(
                            "index_segments",
                            "pass",
                            format!("Search index has {segments} {unit} (bounded)"),
                            false
                        );
                    }
                }

                // Check if index is empty but database has data. #287: reuse the
                // message count from the bounded archive-DB probe above instead
                // of re-opening the database and running another unbounded
                // COUNT on the main thread.
                // GH #457: `num_docs` above came from the engine's own
                // reader, so it is what queries are served from. Compare it
                // with what the completed checkpoint certified (the exact
                // signal readiness uses) and, as a second floor, with the
                // archive's message count: a generation that serves less
                // than a tenth of the messages is hollow whatever the
                // checkpoint says.
                let served_docs = u64::try_from(num_docs).unwrap_or(u64::MAX);
                let hollow_vs_checkpoint =
                    crate::search::asset_state::lexical_generation_hollow_verdict(
                        crate::search::asset_state::completed_lexical_checkpoint_indexed_docs(
                            &index_path,
                            &db_path,
                        ),
                        Some(served_docs),
                    );
                let hollow_vs_messages = db_messages
                    .filter(|&count| count > 0)
                    .map(|count| u64::try_from(count).unwrap_or(u64::MAX))
                    .filter(|&messages| {
                        served_docs.saturating_mul(100)
                            < messages.saturating_mul(DOCTOR_LEXICAL_COVERAGE_FLOOR_PERCENT)
                    });
                if let Some(verdict) = hollow_vs_checkpoint {
                    storage_lexical_index_drifted = true;
                    add_check!("index_sync", "warn", verdict.reason(), true);
                    needs_rebuild = true;
                } else if num_docs == 0
                    && archive_queryable_for_non_destructive_derived_rebuild
                    && let Some(msg_count) = db_messages.filter(|&count| count > 0)
                {
                    storage_lexical_index_drifted = true;
                    add_check!(
                        "index_sync",
                        "warn",
                        format!("Index is empty but database has {} messages", msg_count),
                        true
                    );
                    needs_rebuild = true;
                } else if let Some(messages) = hollow_vs_messages
                    && archive_queryable_for_non_destructive_derived_rebuild
                {
                    storage_lexical_index_drifted = true;
                    add_check!(
                        "index_sync",
                        "warn",
                        format!(
                            "Index serves {num_docs} document(s) but the database has {messages} \
                             messages (below {DOCTOR_LEXICAL_COVERAGE_FLOOR_PERCENT}% coverage); \
                             the published lexical generation is hollow — run `cass index` to \
                             rebuild it from the canonical database"
                        ),
                        true
                    );
                    needs_rebuild = true;
                }
            }
            Ok(None) => {
                // A broken/absent derived index over an intact, populated DB is
                // derived-only drift (the `.14.1` `DerivedOnlyDrift` signal); an
                // empty DB with no index is consistent, not drift.
                storage_lexical_index_drifted |= db_messages.is_some_and(|m| m > 0);
                add_check!("index", "fail", "Search index metadata is missing", true);
                needs_rebuild = true;
            }
            Err(e) => {
                storage_lexical_index_drifted |= db_messages.is_some_and(|m| m > 0);
                add_check!("index", "fail", format!("Cannot open index: {}", e), true);
                needs_rebuild = true;
            }
        }
    } else if not_initialized {
        add_check!(
            "index",
            "warn",
            "Search index not initialized yet - run the first index to build lexical metadata",
            false
        );
    } else {
        storage_lexical_index_drifted |= db_messages.is_some_and(|m| m > 0);
        add_check!("index", "fail", "Search index not found", true);
        needs_rebuild = true;
    }
    doctor_push_timing_span(
        &mut timing_spans,
        "lexical_probe",
        "index",
        "lexical_probe",
        lexical_probe_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["derived lexical index presence and metadata probe completed".to_string()],
    );

    // `.14.1` storage-integrity projection (bead vl1cj): fold the read-only
    // database + lexical-index signals gathered above into the canonical
    // `StorageState` / `SourceOfTruthRisk` / `ArchiveReadability` contract so
    // doctor robot JSON projects the same vocabulary status/search-meta will.
    // The common signals derive ok/openread/integrity/derived/deferred first;
    // the dedicated probe overlays typed contention, schema drift, legacy
    // layout, or structurally suspect WAL/SHM evidence below. FTS metadata
    // remains separate because a missing in-DB shadow is a benign fallback.
    let storage_integrity_report = {
        use crate::search::storage_integrity::{
            ArchiveReadability, DoctorStorageSignals, IntegrityAttestationVerdict, StorageCheck,
            StorageCheckNotAttempted, StorageIntegrityReport, StorageState,
            apply_dedicated_storage_probe, build_doctor_storage_integrity,
        };
        let db_file_present = db_path.exists();
        let db_elapsed_ms = archive_db_probe_started.elapsed().as_millis() as i64;
        let index_elapsed_ms = lexical_probe_started.elapsed().as_millis() as i64;
        let mut storage_checks: Vec<StorageCheck> = Vec::new();
        if !db_file_present {
            let reason = if not_initialized {
                "database not initialized"
            } else {
                "no archive present to probe"
            };
            storage_checks.push(StorageCheck::skipped("archive_integrity", reason));
        } else if storage_probe_timed_out {
            storage_checks.push(StorageCheck::timed_out("archive_integrity", db_elapsed_ms));
        } else if let Some(reason) = storage_integrity_skipped_reason.as_deref() {
            storage_checks.push(StorageCheck::skipped("archive_integrity", reason));
        } else {
            storage_checks.push(StorageCheck::ran("archive_integrity", db_elapsed_ms));
        }
        if not_initialized {
            storage_checks.push(StorageCheck::skipped(
                "lexical_index",
                "index not initialized",
            ));
        } else {
            storage_checks.push(StorageCheck::ran("lexical_index", index_elapsed_ms));
        }
        let signals = DoctorStorageSignals {
            db_file_present,
            not_initialized,
            db_open_failed: storage_db_open_failed,
            probe_timed_out: storage_probe_timed_out,
            integrity_failed: storage_integrity_failed,
            integrity_unverified: storage_integrity_unverified,
            lexical_index_drifted: storage_lexical_index_drifted,
        };
        // #331: capture completed deep-probe evidence against the current
        // db/WAL fingerprint. The ordinary `doctor check` surface is strictly
        // read-only, so it projects this as live evidence without persisting
        // anything. An explicitly mutating doctor surface may refresh the
        // cache that lightweight status/search later reuse.
        let live_attestation = if db_file_present
            && !storage_db_open_failed
            && !storage_probe_timed_out
            && !storage_integrity_unverified
            && let Some(check_depth) = storage_attestation_check_depth
        {
            crate::search::storage_integrity::capture_integrity_attestation(
                &db_path,
                if storage_integrity_failed {
                    crate::search::storage_integrity::IntegrityAttestationVerdict::Fail
                } else {
                    crate::search::storage_integrity::IntegrityAttestationVerdict::Pass
                },
                check_depth,
                storage_attestation_detail.clone(),
            )
        } else {
            None
        };
        if fix_can_mutate && let Some(attestation) = live_attestation.as_ref() {
            crate::search::storage_integrity::persist_integrity_attestation(&data_dir, attestation);
        }
        let base_report = if let Some(reason) = storage_integrity_skipped_reason.as_deref() {
            let (state, readability) = match matching_cached_integrity_attestation
                .as_ref()
                .map(|attestation| attestation.verdict)
            {
                Some(IntegrityAttestationVerdict::Pass) if storage_lexical_index_drifted => {
                    (StorageState::DerivedOnlyDrift, ArchiveReadability::Readable)
                }
                Some(IntegrityAttestationVerdict::Pass) => {
                    (StorageState::Ok, ArchiveReadability::Readable)
                }
                Some(IntegrityAttestationVerdict::Fail) => (
                    StorageState::IntegrityFailed,
                    ArchiveReadability::PartiallyReadable,
                ),
                None => (StorageState::Unchecked, ArchiveReadability::Readable),
            };
            let mut report = StorageIntegrityReport::derive(state, readability, storage_checks);
            if let Some(attestation) = matching_cached_integrity_attestation.as_ref() {
                report.attestation_source = Some("cached".to_string());
                report.attestation_check_depth = Some(attestation.check_depth.clone());
                report.attested_at_ms = Some(attestation.checked_at_ms);
                report.attested_db_fingerprint = Some(
                    crate::search::storage_integrity::integrity_attestation_fingerprint(
                        attestation,
                    ),
                );
            } else {
                report.attestation_source = Some("none".to_string());
                report.checks_not_attempted.push(StorageCheckNotAttempted {
                    name: "quick_check".to_string(),
                    reason: reason.to_string(),
                });
            }
            report
        } else {
            build_doctor_storage_integrity(signals, storage_checks)
        };
        let mut report =
            apply_dedicated_storage_probe(base_report, dedicated_storage_probe.clone());
        if let Some(attestation) = live_attestation {
            report.attestation_source = Some("live".to_string());
            report.attestation_check_depth = Some(attestation.check_depth.clone());
            report.attested_at_ms = Some(attestation.checked_at_ms);
            report.attested_db_fingerprint = Some(
                crate::search::storage_integrity::integrity_attestation_fingerprint(&attestation),
            );
        }
        report
    };

    // 5. Check config file
    let config_path = data_dir.join("config.toml");
    if config_path.exists() {
        match std::fs::read_to_string(&config_path) {
            Ok(content) => match toml::from_str::<toml::Value>(&content) {
                Ok(_) => {
                    add_check!("config", "pass", "Config file valid", false);
                }
                Err(e) => {
                    add_check!(
                        "config",
                        "warn",
                        format!("Config parse error: {}", e),
                        false
                    );
                }
            },
            Err(e) => {
                add_check!(
                    "config",
                    "warn",
                    format!("Cannot read config: {}", e),
                    false
                );
            }
        }
    } else {
        add_check!("config", "pass", "No config file (using defaults)", false);
    }

    // 6. Check sources.toml
    let sources_path = doctor_sources_config_path(&data_dir);
    let sources_config_check = doctor_sources_config_check(&sources_path);
    add_check!(
        "sources_config",
        sources_config_check.status,
        sources_config_check.message,
        false
    );

    let config_exclusion_risks =
        collect_doctor_config_exclusion_risks(&data_dir, &db_path, &config_path, &sources_path);
    if config_exclusion_risks.is_empty() {
        add_check!(
            "config_exclusion_risks",
            "pass",
            "No cass archive preservation exclusions found in the narrow config and ignore-file scan",
            false
        );
    } else {
        add_check!(
            "config_exclusion_risks",
            "warn",
            format!(
                "Preservation warning: {} backup/sync/ignore exclusion risk(s) may leave cass archive evidence unprotected; this is not proof backups are broken",
                config_exclusion_risks.len()
            ),
            false
        );
    }

    // 7. Check common session directories exist
    let mut session_dirs_found = 0usize;
    let home = dirs::home_dir().unwrap_or_default();
    let session_paths = [
        home.join(".claude"),        // Claude Code
        home.join(".codex"),         // Codex
        home.join(".cursor"),        // Cursor
        home.join(".chatgpt"),       // ChatGPT
        home.join(".config/gemini"), // Gemini
    ];
    for path in &session_paths {
        if path.exists() {
            session_dirs_found += 1;
        }
    }
    if session_dirs_found > 0 {
        add_check!(
            "sessions",
            "pass",
            format!("Found {} agent session directories", session_dirs_found),
            false
        );
    } else {
        add_check!(
            "sessions",
            "warn",
            "No agent session directories found",
            false
        );
    }

    let source_inventory_started = Instant::now();
    let source_inventory = if let Some(reason) = archive_wide_collectors_deferred_reason.as_deref()
    {
        doctor_source_inventory_deferred(reason)
    } else {
        collect_doctor_source_inventory(&data_dir, &db_path)
    };
    doctor_push_timing_span(
        &mut timing_spans,
        "source_ledger_scan",
        "source_inventory",
        "source_ledger_scan",
        source_inventory_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec![if archive_wide_collectors_complete {
            "source ledger and visible provider roots were scanned with bounded reporting"
                .to_string()
        } else {
            "archive-wide source ledger scan was explicitly deferred".to_string()
        }],
    );
    if let Some(error) = source_inventory.db_query_error.as_deref() {
        add_check!(
            "source_inventory",
            "warn",
            format!("Source inventory could not query archive coverage: {error}"),
            false
        );
    } else if !source_inventory.db_available {
        add_check!(
            "source_inventory",
            "pass",
            "Source inventory skipped until the cass archive database exists",
            false
        );
    } else if source_inventory.missing_current_source_count > 0
        || source_inventory.unknown_mapping_count > 0
    {
        add_check!(
            "source_inventory",
            "warn",
            format!(
                "Source coverage risk: {} indexed local conversation(s) no longer have a visible upstream file; {} indexed conversation(s) have incomplete source mapping",
                source_inventory.missing_current_source_count,
                source_inventory.unknown_mapping_count
            ),
            false
        );
    } else {
        add_check!(
            "source_inventory",
            "pass",
            format!(
                "Source inventory OK ({} indexed conversation(s), {} provider root(s) detected)",
                source_inventory.total_indexed_conversations,
                source_inventory.detected_provider_root_count
            ),
            false
        );
    }

    let state = DoctorRunState {
        output_format,
        fix,
        verbose,
        force_rebuild,
        allow_repeated_repair,
        command_surface,
        execution_mode,
        requested_plan_fingerprint,
        wrap,
        start,
        data_dir,
        db_path,
        index_path,
        timing_spans,
        maintenance_snapshot,
        rebuild_active,
        cleanup_apply_requested,
        safe_auto_run_requested,
        repair_class,
        initial_failure_marker,
        repeat_refusal_reason,
        repeat_repair_refused,
        override_available,
        override_used,
        _doctor_lock_guard,
        _doctor_db_open_bypass_guard,
        operation_state,
        mutating_lock_acquired,
        fix_can_mutate,
        not_initialized,
        explanation,
        recommended_action,
        checks,
        needs_rebuild,
        db_ok,
        db_conversations,
        db_messages,
        storage_db_open_failed,
        storage_integrity_failed,
        storage_integrity_unverified,
        storage_integrity_skipped_reason,
        storage_probe_timed_out,
        storage_lexical_index_drifted,
        storage_attestation_check_depth,
        storage_attestation_detail,
        dedicated_storage_probe,
        matching_cached_integrity_attestation,
        auto_fix_actions,
        auto_fix_applied,
        fs_mutation_receipts,
        candidate_promotion_report,
        storage_pressure,
    };
    let lexical_ws = DoctorLexicalWorkspaceReport {
        archive_queryable_for_non_destructive_derived_rebuild,
        canonical_candidate_replacement_authorized,
        storage_busy_or_locked,
        archive_wide_collectors_deferred_reason,
        archive_wide_collectors_complete,
        storage_integrity_report,
        config_exclusion_risks,
        source_inventory,
    };
    (state, lexical_ws)
}
