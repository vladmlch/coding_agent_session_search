use std::path::{Path, PathBuf};

use crate::{CliError, CliResult, RobotFormat, default_data_dir};

use super::{io_error, print_json, resolve_db_path};

/// The engine's integrity verdict for one canonical archive, classified for the
/// in-place leaked-page repair (2l1b0.73).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LeakedPagesVerdict {
    /// quick_check and integrity_check both report `ok`.
    Clean,
    /// The only failure is unowned page(s). SQLite's integrity_check
    /// scans for unowned pages only after the freelist and every table and
    /// index b-tree were walked with no doubly owned, out-of-range, or
    /// malformed page. Only "Page N: never used" (or legacy "page N is never
    /// used") lines therefore prove the damage is leaked pages alone: pages
    /// that left the freelist without entering a tree.
    LeakedPagesOnly { first_leaked_page: u64 },
    /// Any other failure: a doubled reference, a malformed page or freelist,
    /// an index that disagrees with its table. Freeing leaked pages cannot
    /// repair these, and running the repair over them is refused.
    OtherDamage { diagnostics: Vec<String> },
}

impl LeakedPagesVerdict {
    fn status(&self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::LeakedPagesOnly { .. } => "leaked_pages",
            Self::OtherDamage { .. } => "other_damage",
        }
    }

    fn first_leaked_page(&self) -> Option<u64> {
        match self {
            Self::LeakedPagesOnly { first_leaked_page } => Some(*first_leaked_page),
            _ => None,
        }
    }

    fn diagnostics(&self) -> &[String] {
        match self {
            Self::OtherDamage { diagnostics } => diagnostics,
            _ => &[],
        }
    }
}

/// The page number of a leaked-page diagnostic, supporting both C SQLite's
/// `Page N: never used` (case-insensitive to `Page`/`page`) and legacy
/// `page N is never used` (with or without `database disk image is malformed: `).
fn leaked_page_number(diagnostic: &str) -> Option<u64> {
    let detail = diagnostic.trim();
    let detail = detail
        .strip_prefix("database disk image is malformed: ")
        .unwrap_or(detail);
    let number = if let Some(rest) = detail
        .strip_prefix("Page ")
        .or_else(|| detail.strip_prefix("page "))
        .and_then(|rest| rest.strip_suffix(": never used"))
    {
        rest
    } else {
        detail
            .strip_prefix("page ")?
            .strip_suffix(" is never used")?
    };
    if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    number.parse().ok()
}

fn is_sqlite_database_header(line: &str) -> bool {
    line.starts_with("*** in database ") && line.ends_with(" ***")
}

/// Extract non-`ok`, non-`note:` diagnostic lines from `quick_check` /
/// `integrity_check` result cells, splitting multi-line C SQLite cells.
/// `*** in database ... ***` headers are skipped only when accompanied by
/// diagnostic lines so a lone header never collapses to `Clean`.
fn diagnostic_lines(rows: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for row in rows {
        let mut headers_in_row = Vec::new();
        let mut details_in_row = Vec::new();
        for raw_line in row.lines() {
            let line = raw_line.trim();
            if line.is_empty()
                || line.eq_ignore_ascii_case("ok")
                || line.starts_with("note:")
            {
                continue;
            }
            if is_sqlite_database_header(line) {
                headers_in_row.push(line.to_string());
            } else {
                details_in_row.push(line.to_string());
            }
        }
        if details_in_row.is_empty() {
            out.extend(headers_in_row);
        } else {
            out.extend(details_in_row);
        }
    }
    out
}

/// Classify quick_check and integrity_check output rows. `ok` rows and the
/// engine's informational `note:` rows are not failures.
pub(super) fn classify_leaked_pages(
    quick_check: &[String],
    integrity_check: &[String],
) -> LeakedPagesVerdict {
    let quick = diagnostic_lines(quick_check);
    let full = diagnostic_lines(integrity_check);
    if quick.is_empty() && full.is_empty() {
        return LeakedPagesVerdict::Clean;
    }
    let all_leaked = quick
        .iter()
        .chain(full.iter())
        .all(|line| leaked_page_number(line).is_some());
    if all_leaked
        && let Some(first_leaked_page) = full
            .iter()
            .chain(quick.iter())
            .find_map(|line| leaked_page_number(line))
    {
        return LeakedPagesVerdict::LeakedPagesOnly { first_leaked_page };
    }
    LeakedPagesVerdict::OtherDamage {
        diagnostics: quick.into_iter().chain(full).collect(),
    }
}

/// The command doctor's read-only guidance names for a leak-only integrity
/// failure; the database check message carries it verbatim.
pub const LEAKED_PAGES_DRY_RUN_COMMAND: &str = "cass doctor --repair-leaked-pages --dry-run --json";

/// True when a live quick_check status plus integrity_check diagnostics are the
/// leak-only class the in-place repair accepts.
pub fn integrity_is_leaked_pages_only(
    quick_check_status: &str,
    integrity_diagnostics: &[String],
) -> bool {
    matches!(
        classify_leaked_pages(&[quick_check_status.to_string()], integrity_diagnostics),
        LeakedPagesVerdict::LeakedPagesOnly { .. }
    )
}

/// True when a cached failing attestation recorded an `integrity_check` or
/// `quick_check` whose diagnostics are solely leaked pages.
pub fn attested_integrity_is_leaked_pages_only(check_depth: &str, detail: Option<&str>) -> bool {
    if !matches!(check_depth, "integrity_check" | "quick_check") {
        return false;
    }
    let Some(detail) = detail else {
        return false;
    };
    let lines = diagnostic_lines(&[detail.to_string()]);
    !lines.is_empty() && lines.iter().all(|line| leaked_page_number(line).is_some())
}

/// What one read-only inspection of the canonical archive observed.
struct LeakedPagesInspection {
    verdict: LeakedPagesVerdict,
    /// `None` when damage kept the value from being read.
    page_count: Option<i64>,
    freelist_count: Option<i64>,
    conversations: Option<i64>,
    messages: Option<i64>,
    elapsed_ms: u64,
}

impl LeakedPagesInspection {
    fn other_damage(detail: String, started: std::time::Instant) -> Self {
        Self {
            verdict: LeakedPagesVerdict::OtherDamage {
                diagnostics: vec![detail],
            },
            page_count: None,
            freelist_count: None,
            conversations: None,
            messages: None,
            elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        }
    }
}

const LEAKED_PAGES_OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn leaked_pages_single_integer(
    conn: &crate::franken_sync::Connection,
    sql: &str,
) -> Result<i64, String> {
    use crate::franken_sync::compat::RowExt as _;
    let row = conn.query_row(sql).map_err(|err| format!("{sql}: {err}"))?;
    row.get_typed::<i64>(0)
        .map_err(|err| format!("{sql}: reading the result: {err}"))
}

fn leaked_pages_text_rows(
    conn: &crate::franken_sync::Connection,
    sql: &str,
) -> Result<Vec<String>, String> {
    use crate::franken_sync::compat::RowExt as _;
    conn.query(sql)
        .map_err(|err| format!("{sql}: {err}"))?
        .iter()
        .map(|row| {
            row.get_typed::<String>(0)
                .map_err(|err| format!("{sql}: reading a diagnostic row: {err}"))
        })
        .collect()
}

/// Run the engine's own quick_check and integrity_check on a read-only open of
/// the canonical archive and record the counts the repair must preserve.
fn inspect_leaked_pages(db_path: &Path) -> CliResult<LeakedPagesInspection> {
    let started = std::time::Instant::now();
    let mut conn = match crate::storage::sqlite::open_franken_raw_readonly_connection_with_timeout(
        db_path,
        LEAKED_PAGES_OPEN_TIMEOUT,
    ) {
        Ok(conn) => conn,
        // Damage can fail the open itself (a freelist entry naming a live
        // page, say). That is other damage, not a busy archive to retry.
        Err(err)
            if crate::doctor_open_error_message_is_affirmative_corruption(&format!("{err:#}")) =>
        {
            return Ok(LeakedPagesInspection::other_damage(
                format!("read-only open failed: {err:#}"),
                started,
            ));
        }
        Err(err) => {
            return Err(io_error(
                format!(
                    "opening canonical archive {} read-only for the leaked-page inspection: {err:#}",
                    db_path.display()
                ),
                Some("Retry once no cass index or watch process holds the archive."),
            ));
        }
    };
    let result = (|| -> Result<LeakedPagesInspection, String> {
        let checks = leaked_pages_text_rows(&conn, "PRAGMA quick_check(1);").and_then(|quick| {
            leaked_pages_text_rows(&conn, "PRAGMA integrity_check(8);").map(|full| (quick, full))
        });
        let (quick, full) = match checks {
            Ok(rows) => rows,
            Err(err) if crate::doctor_open_error_message_is_affirmative_corruption(&err) => {
                return Ok(LeakedPagesInspection::other_damage(err, started));
            }
            Err(err) => return Err(err),
        };
        let verdict = classify_leaked_pages(&quick, &full);
        // Counts must be readable for the verdicts the repair acts on; damage
        // elsewhere may leave them unreadable, which the verdict already says.
        let count = |sql: &str| match &verdict {
            LeakedPagesVerdict::OtherDamage { .. } => {
                Ok(leaked_pages_single_integer(&conn, sql).ok())
            }
            _ => leaked_pages_single_integer(&conn, sql).map(Some),
        };
        Ok(LeakedPagesInspection {
            page_count: count("PRAGMA page_count;")?,
            freelist_count: count("PRAGMA freelist_count;")?,
            conversations: count("SELECT COUNT(*) FROM conversations;")?,
            messages: count("SELECT COUNT(*) FROM messages;")?,
            verdict,
            elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        })
    })();
    if conn.close_without_checkpoint_in_place().is_err() {
        conn.close_best_effort_in_place();
    }
    result.map_err(|err| {
        io_error(
            format!(
                "inspecting canonical archive {} for leaked pages: {err}",
                db_path.display()
            ),
            Some("Preserve the archive bundle and run 'cass doctor check --json'."),
        )
    })
}

fn leaked_pages_count_text(count: Option<i64>) -> String {
    count.map_or_else(|| "unknown".to_string(), |count| count.to_string())
}

fn leaked_pages_inspection_json(inspection: &LeakedPagesInspection) -> serde_json::Value {
    serde_json::json!({
        "status": inspection.verdict.status(),
        "first_leaked_page": inspection.verdict.first_leaked_page(),
        "diagnostics": inspection.verdict.diagnostics(),
        "page_count": inspection.page_count,
        "freelist_count": inspection.freelist_count,
        "conversations": inspection.conversations,
        "messages": inspection.messages,
        "elapsed_ms": inspection.elapsed_ms,
    })
}

/// Files of the live SQLite bundle: the DB, its WAL when present, and its SHM
/// only beside a WAL. `doctor backups verify` refuses to restore an SHM
/// without its WAL, and an SHM alone carries no committed data.
pub(super) fn leaked_pages_bundle_components(db_path: &Path) -> Vec<(&'static str, PathBuf)> {
    let mut components = vec![("", db_path.to_path_buf())];
    for suffix in ["-wal", "-shm"] {
        match crate::doctor_sqlite_sidecar_path(db_path, suffix) {
            Some(path) if path.is_file() => components.push((suffix, path)),
            // Without a WAL there is no SHM worth keeping.
            _ if suffix == "-wal" => break,
            _ => {}
        }
    }
    components
}

/// Copy the quiescent live bundle into a backup that `cass doctor backups
/// verify|restore` accept, and return the backup id and its manifest.
fn backup_bundle_before_leaked_pages_repair(
    data_dir: &Path,
    db_path: &Path,
    inspection: &LeakedPagesInspection,
) -> CliResult<(String, PathBuf, serde_json::Value)> {
    let components = leaked_pages_bundle_components(db_path);
    let required_bytes: u64 = components
        .iter()
        .filter_map(|(_, path)| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum();
    let backup_root = crate::doctor_candidate_promotion_root(data_dir);
    crate::doctor_forensic_create_private_dir_all(&backup_root).map_err(|err| {
        io_error(
            format!(
                "creating the doctor backup root {}: {err}",
                backup_root.display()
            ),
            None,
        )
    })?;
    // Keep 64 MiB spare so the backup cannot fill the disk the repair then
    // needs for its own commit.
    let available = fs2::available_space(&backup_root).map_err(|err| {
        io_error(
            format!(
                "measuring free space under {}: {err}",
                backup_root.display()
            ),
            None,
        )
    })?;
    let margin = 64 * 1024 * 1024;
    if available < required_bytes.saturating_add(margin) {
        return Err(io_error(
            format!(
                "the pre-repair backup needs {required_bytes} bytes plus a 64 MiB margin, but only {available} bytes are free under {}",
                backup_root.display()
            ),
            Some(
                "Free space on the data dir's filesystem, then re-run the repair. The archive was not modified.",
            ),
        ));
    }

    let backup_id = crate::doctor_candidate_id("leaked-pages-repair", crate::doctor_now_ms());
    let backup_dir = backup_root.join(&backup_id).join("backup");
    let file_name = db_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "agent_search.db".to_string());
    let mut artifacts = Vec::new();
    for (suffix, source) in &components {
        let artifact_kind = match *suffix {
            "" => "prior_live_archive_db_backup",
            "-wal" => "prior_live_archive_wal_backup",
            _ => "prior_live_archive_shm_backup",
        };
        let target = backup_dir.join(format!("{file_name}{suffix}"));
        let (size_bytes, blake3) = crate::doctor_copy_regular_file_to_private_target(
            source,
            &target,
            None,
            "leaked-page repair backup",
        )
        .map_err(|err| {
            io_error(
                err,
                Some("The archive was not modified; free space or fix permissions and retry."),
            )
        })?;
        artifacts.push(serde_json::json!({
            "artifact_kind": artifact_kind,
            "asset_class": "backup_bundle",
            "source_path": source.display().to_string(),
            "redacted_source_path": crate::doctor_redacted_path(&source.display().to_string(), data_dir),
            "backup_path": target.display().to_string(),
            "redacted_backup_path": crate::doctor_redacted_path(&target.display().to_string(), data_dir),
            "target_path": source.display().to_string(),
            "redacted_target_path": crate::doctor_redacted_path(&source.display().to_string(), data_dir),
            "size_bytes": size_bytes,
            "checksum_blake3": blake3,
            "copied_to_backup": true,
            "promoted_to_live": false,
        }));
    }
    let manifest_path = backup_dir.join("manifest.json");
    let manifest = serde_json::json!({
        "schema_version": 1,
        "manifest_kind": crate::DOCTOR_LEAKED_PAGES_REPAIR_BACKUP_MANIFEST_KIND,
        "promotion_id": &backup_id,
        "backup_dir": backup_dir.display().to_string(),
        "redacted_backup_dir": crate::doctor_redacted_path(&backup_dir.display().to_string(), data_dir),
        "inspection_before": leaked_pages_inspection_json(inspection),
        "artifacts": artifacts,
    });
    crate::doctor_write_private_json_artifact(
        &manifest_path,
        &manifest,
        "leaked-page repair backup manifest",
    )
    .map_err(|err| {
        io_error(
            err,
            Some("The archive was not modified; fix the doctor backup dir and retry."),
        )
    })?;
    Ok((backup_id, manifest_path, manifest))
}

/// `cass doctor --repair-leaked-pages`: reclaim leaked pages (integrity_check's
/// "Page N: never used" / legacy "page N is never used" class) via `VACUUM;`
/// after proving they are the only damage and backing up the live bundle
/// (2l1b0.73).
///
/// Reclaiming them changes no row: `VACUUM;` rewrites only live tables and
/// indexes, dropping unowned pages, and the counts and `integrity_check` are
/// re-verified afterward.
pub fn run_doctor_repair_leaked_pages(
    data_dir_override: Option<PathBuf>,
    db_override: Option<PathBuf>,
    dry_run: bool,
    yes: bool,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let db_path = resolve_db_path(&data_dir, db_override.as_deref());

    if !dry_run && !yes {
        return Err(CliError {
            code: 4,
            kind: "refused-unsafe",
            message: "`cass doctor --repair-leaked-pages` rewrites the canonical archive via VACUUM and requires `--yes`".to_string(),
            hint: Some(
                "Inspect first with `cass doctor --repair-leaked-pages --dry-run --json`; apply with `--yes` only when it reports status=leaked_pages. The live bundle is backed up first and restorable with `cass doctor backups restore <id>`.".to_string(),
            ),
            retryable: false,
        });
    }
    if !db_path.exists() {
        return Err(CliError {
            code: 13,
            kind: "not-found",
            message: format!("canonical archive {} does not exist", db_path.display()),
            hint: Some("Run `cass index --full` to create the archive.".to_string()),
            retryable: false,
        });
    }

    // Same exclusive index-run lock as every other canonical-archive mutation,
    // held across inspection, backup, repair and re-verification so the
    // classified image is the one repaired.
    let _mutation_guard = if dry_run {
        None
    } else {
        Some(
            crate::indexer::acquire_search_maintenance_mutation_lock(
                &data_dir,
                &db_path,
                crate::search::asset_state::SearchMaintenanceJobKind::LexicalRefresh,
            )
            .map_err(|err| CliError {
                code: 7,
                kind: "index-busy",
                message: format!(
                    "acquiring the exclusive index-run lock for the leaked-page repair: {err:#}"
                ),
                hint: Some("Stop the active cass index/watch process, then retry.".to_string()),
                retryable: true,
            })?,
        )
    };

    let before = inspect_leaked_pages(&db_path)?;
    if dry_run {
        let (planned_action, next_command) = match &before.verdict {
            LeakedPagesVerdict::Clean => ("none", None),
            LeakedPagesVerdict::LeakedPagesOnly { .. } => (
                "free_leaked_pages_in_place",
                Some("cass doctor --repair-leaked-pages --yes --json"),
            ),
            LeakedPagesVerdict::OtherDamage { .. } => {
                ("refuse", Some("cass doctor repair --dry-run --json"))
            }
        };
        let backup_required_bytes: u64 = leaked_pages_bundle_components(&db_path)
            .iter()
            .filter_map(|(_, path)| std::fs::metadata(path).ok())
            .map(|metadata| metadata.len())
            .sum();
        let envelope = serde_json::json!({
            "schema_version": 1,
            "doctor_contract_version": 1,
            "kind": "repair_leaked_pages",
            "mode": "dry_run",
            "db_path": db_path.display().to_string(),
            "inspection": leaked_pages_inspection_json(&before),
            "planned_action": planned_action,
            "backup_required_bytes": backup_required_bytes,
            "next_command": next_command,
            "note": "Read-only. The apply path re-inspects under the index-run lock, backs up the live bundle, runs VACUUM to reclaim leaked pages, and re-runs integrity_check.",
        });
        if structured_format.is_some() {
            print_json(&envelope)?;
        } else {
            println!(
                "Leaked-page dry-run: status={}, planned_action={planned_action}, page_count={}, freelist_count={}{}",
                before.verdict.status(),
                leaked_pages_count_text(before.page_count),
                leaked_pages_count_text(before.freelist_count),
                next_command
                    .map(|command| format!("; next: {command}"))
                    .unwrap_or_default()
            );
        }
        return Ok(());
    }

    match &before.verdict {
        LeakedPagesVerdict::Clean => {
            let envelope = serde_json::json!({
                "schema_version": 1,
                "doctor_contract_version": 1,
                "kind": "repair_leaked_pages",
                "mode": "apply",
                "db_path": db_path.display().to_string(),
                "status": "already_clean",
                "inspection": leaked_pages_inspection_json(&before),
                "canonical_rows_modified": false,
            });
            if structured_format.is_some() {
                print_json(&envelope)?;
            } else {
                println!(
                    "Canonical archive {} passes integrity_check; nothing to repair.",
                    db_path.display()
                );
            }
            return Ok(());
        }
        LeakedPagesVerdict::OtherDamage { diagnostics } => {
            return Err(CliError {
                code: 5,
                kind: "data-corruption",
                message: format!(
                    "canonical archive {} has damage beyond leaked pages ({}); an in-place freelist repair cannot fix it and was not attempted",
                    db_path.display(),
                    diagnostics.join("; ")
                ),
                hint: Some(
                    "Preserve the archive bundle (db + -wal + -shm) and follow `cass doctor repair --dry-run --json`, which ranks the reconstruction authorities.".to_string(),
                ),
                retryable: false,
            });
        }
        LeakedPagesVerdict::LeakedPagesOnly { .. } => {}
    }

    let (backup_id, backup_manifest_path, backup_manifest) =
        backup_bundle_before_leaked_pages_repair(&data_dir, &db_path, &before)?;
    let restore_hint = format!(
        "The pre-repair bundle is backed up as {backup_id}: verify with `cass doctor backups verify {backup_id} --json` and restore with `cass doctor backups restore {backup_id} --json`."
    );

    let repair_started = std::time::Instant::now();
    let freed_pages = (|| -> Result<usize, String> {
        let conn = crate::storage::sqlite::open_franken_raw_connection_with_timeout(
            &db_path,
            LEAKED_PAGES_OPEN_TIMEOUT,
        )
        .map_err(|err| format!("opening the archive writable: {err:#}"))?;
        let page_count_before = leaked_pages_single_integer(&conn, "PRAGMA page_count;")?;
        let freelist_count_before = leaked_pages_single_integer(&conn, "PRAGMA freelist_count;")?;
        conn.execute("VACUUM;")
            .map_err(|err| format!("vacuuming leaked pages: {err}"))?;
        let page_count_after = leaked_pages_single_integer(&conn, "PRAGMA page_count;")?;
        let freelist_count_after = leaked_pages_single_integer(&conn, "PRAGMA freelist_count;")?;
        conn.close()
            .map_err(|err| format!("closing the repaired archive: {err}"))?;
        let non_freelist_before = page_count_before.saturating_sub(freelist_count_before);
        let non_freelist_after = page_count_after.saturating_sub(freelist_count_after);
        Ok(usize::try_from(non_freelist_before.saturating_sub(non_freelist_after))
            .unwrap_or(usize::MAX))
    })()
    .map_err(|err| CliError {
        code: 3,
        kind: "repair-failure",
        message: format!("leaked-page repair of {} failed: {err}", db_path.display()),
        hint: Some(restore_hint.clone()),
        retryable: false,
    })?;
    let repair_ms = u64::try_from(repair_started.elapsed().as_millis()).unwrap_or(u64::MAX);

    let after = inspect_leaked_pages(&db_path)?;
    let rows_unchanged = after.conversations.is_some()
        && after.messages.is_some()
        && after.conversations == before.conversations
        && after.messages == before.messages;
    if after.verdict != LeakedPagesVerdict::Clean || !rows_unchanged {
        return Err(CliError {
            code: 3,
            kind: "repair-failure",
            message: format!(
                "after reclaiming {freed_pages} leaked page(s), {} is {} with {} conversations / {} messages (before: {} / {}){}",
                db_path.display(),
                after.verdict.status(),
                leaked_pages_count_text(after.conversations),
                leaked_pages_count_text(after.messages),
                leaked_pages_count_text(before.conversations),
                leaked_pages_count_text(before.messages),
                if after.verdict.diagnostics().is_empty() {
                    String::new()
                } else {
                    format!(": {}", after.verdict.diagnostics().join("; "))
                }
            ),
            hint: Some(restore_hint),
            retryable: false,
        });
    }

    let envelope = serde_json::json!({
        "schema_version": 1,
        "doctor_contract_version": 1,
        "kind": "repair_leaked_pages",
        "mode": "apply",
        "db_path": db_path.display().to_string(),
        "status": "repaired",
        "freed_pages": freed_pages,
        "repair_ms": repair_ms,
        "inspection_before": leaked_pages_inspection_json(&before),
        "inspection_after": leaked_pages_inspection_json(&after),
        "canonical_rows_modified": false,
        "backup_id": &backup_id,
        "backup_manifest_path": backup_manifest_path.display().to_string(),
        "backup_artifacts": backup_manifest["artifacts"].clone(),
        "verify_command": format!("cass doctor backups verify {backup_id} --json"),
        "restore_rehearsal_command": format!("cass doctor backups restore {backup_id} --json"),
        "note": "Leaked pages were reclaimed via VACUUM; integrity_check passes and canonical rows are unchanged.",
    });
    if structured_format.is_some() {
        print_json(&envelope)?;
    } else {
        println!(
            "Freed {freed_pages} leaked page(s) in {}; integrity_check now passes with {} conversations and {} messages unchanged. Pre-repair backup: {backup_id}.",
            db_path.display(),
            leaked_pages_count_text(after.conversations),
            leaked_pages_count_text(after.messages)
        );
    }
    Ok(())
}
