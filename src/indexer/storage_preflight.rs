use super::*;

/// Open SQLite storage for indexing without replacing canonical data.
///
/// Returns `(storage, rebuilt, opened_fresh_for_full)`. `rebuilt` and
/// `opened_fresh_for_full` are retained for progress metadata compatibility;
/// this path now fails closed on non-empty unreadable/incompatible archives
/// instead of backing them up and creating an empty replacement. An existing
/// zero-byte main file is an interrupted fresh initialization, not an archive:
/// resume schema creation in place without deleting or replacing the path.
pub(in crate::indexer) fn open_storage_for_index(
    db_path: &Path,
    full_index: bool,
) -> Result<(FrankenStorage, bool, bool)> {
    let existing_archive_bytes = match fs::metadata(db_path) {
        Ok(metadata) => Some(metadata.len()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "inspecting canonical archive before index: {}",
                    db_path.display()
                )
            });
        }
    };

    if existing_archive_bytes == Some(0) {
        return match FrankenStorage::open(db_path) {
            Ok(storage) => Ok((storage, false, full_index)),
            Err(err) if anyhow_chain_indicates_retryable_storage_contention(&err) => {
                Err(anyhow::anyhow!(
                    "canonical db is busy/locked while resuming interrupted initialization; refusing to replace it: {err}"
                ))
            }
            Err(err) => Err(canonical_archive_unhealthy_for_index_error(
                db_path,
                &format!("resuming interrupted zero-byte initialization failed: {err:#}"),
            )),
        };
    }

    if existing_archive_bytes.is_some() {
        match non_destructive_meta_schema_version(db_path) {
            Ok(Some(version)) if version > crate::storage::sqlite::CURRENT_SCHEMA_VERSION => {
                return Err(canonical_archive_unhealthy_for_index_error(
                    db_path,
                    &format!(
                        "schema_version {version} is newer than supported version {}",
                        crate::storage::sqlite::CURRENT_SCHEMA_VERSION
                    ),
                ));
            }
            Ok(_) => {}
            Err(err) if anyhow_chain_indicates_retryable_storage_contention(&err) => {
                return Err(anyhow::anyhow!(
                    "canonical db is busy/locked during index open; refusing to replace it: {err:#}"
                ));
            }
            Err(err) => {
                return Err(canonical_archive_unhealthy_for_index_error(
                    db_path,
                    &index_storage_open_error_reason(&err),
                ));
            }
        }
    }

    if existing_archive_bytes.is_some() {
        match crate::storage::sqlite::open_current_schema_storage_with_timeout(
            db_path,
            Duration::from_secs(10),
        ) {
            Ok(Some(storage)) => return Ok((storage, false, false)),
            Ok(None) => {}
            Err(err) => tracing::warn!(
                db_path = %db_path.display(),
                error = ?err,
                "single-open current-schema storage path failed; attempting non-destructive compatibility open"
            ),
        }
    }

    if existing_archive_bytes.is_some() {
        match FrankenStorage::open(db_path) {
            Ok(storage) => Ok((storage, false, false)),
            Err(err) if anyhow_chain_indicates_retryable_storage_contention(&err) => {
                Err(anyhow::anyhow!(
                    "canonical db is busy/locked during index open; refusing to replace it: {err}"
                ))
            }
            Err(err) => Err(canonical_archive_unhealthy_for_index_error(
                db_path,
                &index_storage_open_error_reason(&err),
            )),
        }
    } else {
        FrankenStorage::open(db_path)
            .map(|storage| (storage, false, full_index))
            .with_context(|| format!("creating SQLite storage at {}", db_path.display()))
    }
}

pub(in crate::indexer) fn index_storage_open_error_reason(err: &anyhow::Error) -> String {
    let message = format!("{err:#}");
    crate::storage::sqlite::fts_messages_integrity_error_from_message(&message)
        .map(|fts_err| fts_err.to_string())
        .unwrap_or(message)
}

pub(in crate::indexer) fn non_destructive_meta_schema_version(
    db_path: &Path,
) -> Result<Option<i64>> {
    let mut conn = match crate::storage::sqlite::open_franken_raw_readonly_connection_with_timeout(
        db_path,
        Duration::from_secs(10),
    ) {
        Ok(conn) => conn,
        // Duplicate `fts_messages` schema rows are derived-FTS debris, not
        // archive damage: the storage open that follows deduplicates them
        // (FrankenStorage::open) and checks the schema version itself.
        Err(err)
            if err.chain().any(|cause| {
                let cause = cause.to_string();
                cause.contains("conflicting virtual-table entries")
                    && cause.contains("fts_messages")
            }) =>
        {
            return Ok(None);
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "opening canonical archive read-only before index: {}",
                    db_path.display()
                )
            });
        }
    };

    let result = match conn.query("SELECT value FROM meta WHERE key = 'schema_version';") {
        Ok(rows) => Ok(rows
            .first()
            .and_then(|row| row.get_typed::<String>(0).ok())
            .and_then(|raw| raw.parse::<i64>().ok())),
        Err(err) if storage_error_mentions_missing_table_or_column(&err) => Ok(None),
        Err(err) => Err(anyhow::anyhow!(
            "reading canonical archive schema_version before index: {err}"
        )),
    };

    if let Err(close_err) = conn.close_without_checkpoint_in_place() {
        tracing::debug!(
            error = %close_err,
            db_path = %db_path.display(),
            "non_destructive_meta_schema_version: close_without_checkpoint_in_place failed"
        );
        conn.close_best_effort_in_place();
    }

    result
}

pub(in crate::indexer) fn storage_error_mentions_missing_table_or_column(
    err: &impl std::fmt::Display,
) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("no such table") || message.contains("no such column")
}

pub(in crate::indexer) fn anyhow_chain_indicates_retryable_storage_contention(
    err: &anyhow::Error,
) -> bool {
    use crate::franken_sync::FrankenError;

    if let Some(engine_error) = err
        .chain()
        .find_map(|cause| cause.downcast_ref::<FrankenError>())
    {
        return matches!(
            engine_error,
            FrankenError::Busy
                | FrankenError::BusyRecovery
                | FrankenError::BusySnapshot { .. }
                | FrankenError::WriteConflict { .. }
                | FrankenError::SerializationFailure { .. }
        );
    }

    err.chain()
        .any(|cause| crate::storage::sqlite::retryable_storage_error_message(&cause.to_string()))
}

pub(in crate::indexer) const DEFAULT_INDEX_INTEGRITY_PREFLIGHT_MAX_BYTES: u64 =
    2 * 1024 * 1024 * 1024;

pub(in crate::indexer) fn index_integrity_preflight_max_bytes() -> u64 {
    dotenvy::var("CASS_INDEX_INTEGRITY_PREFLIGHT_MAX_BYTES")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_INDEX_INTEGRITY_PREFLIGHT_MAX_BYTES)
}

/// GH #450: the archive bundle size when a background index run (stale-on-read
/// refresh or scheduled job) must not start the engine's one-time first-open
/// migration repair, or `None` when it may proceed. That repair copies and
/// rewrites the whole archive; on a 2.6 GiB archive it ran for 25 minutes and
/// saturated host IO even at idle IO priority, so only a foreground
/// `cass index` performs it on archives above the same size guard that already
/// defers the full-rebuild integrity preflight.
pub fn background_migration_repair_pending_bytes(db_path: &Path) -> Option<u64> {
    background_migration_repair_pending_bytes_with_max_bytes(
        db_path,
        index_integrity_preflight_max_bytes(),
    )
}

pub(in crate::indexer) fn background_migration_repair_pending_bytes_with_max_bytes(
    db_path: &Path,
    max_bytes: u64,
) -> Option<u64> {
    if !db_path.is_file() {
        return None;
    }
    let bundle_bytes = database_bundle_size_bytes(db_path);
    if should_run_engine_backed_archive_integrity_preflight(Some(bundle_bytes), max_bytes) {
        return None;
    }
    (!crate::storage::sqlite::index_engine_migration_is_complete(db_path)).then_some(bundle_bytes)
}

pub(in crate::indexer) fn should_run_engine_backed_archive_integrity_preflight(
    bundle_bytes: Option<u64>,
    max_bytes: u64,
) -> bool {
    if max_bytes == 0 {
        return true;
    }
    match bundle_bytes {
        Some(bytes) => bytes <= max_bytes,
        None => true,
    }
}

pub(in crate::indexer) fn archive_bundle_size_for_integrity_preflight(
    storage: &FrankenStorage,
) -> Option<u64> {
    let db_path = storage.database_path().ok()?;
    // A missing or unreadable main file must fail safe into the integrity walk.
    // Sidecars are optional, and database_bundle_size_bytes already treats an
    // absent WAL/SHM as zero while preserving filename bytes.
    fs::metadata(&db_path).ok()?;
    Some(database_bundle_size_bytes(&db_path))
}

/// Detect the exact legacy FTS5 shadow DDL behind GH #374 without walking the
/// archive. Legacy storage-engine versions before the canonical-shadow fix created
/// `%_config`/`%_idx` as ordinary rowid tables; stock SQLite then reported
/// `wrong # of entries in index sqlite_autoindex_fts_messages_config_1`, while
/// legacy `quick_check` could still return `ok`. The two sqlite_master
/// rows are tiny, stable metadata and therefore remain suitable for a bounded
/// full-index preflight.
pub(in crate::indexer) fn incompatible_legacy_fts_shadow_problem(
    storage: &FrankenStorage,
) -> Result<Option<String>> {
    let rows = storage
        .raw()
        .query(
            "SELECT name, sql
             FROM sqlite_master
             WHERE type = 'table'
               AND name IN ('fts_messages_config', 'fts_messages_idx')
             ORDER BY name",
        )
        .context("reading canonical FTS shadow DDL for full rebuild preflight")?;
    for row in rows {
        let table = row
            .get_typed::<String>(0)
            .context("reading canonical FTS shadow table name")?;
        let ddl = row
            .get_typed::<String>(1)
            .context("reading canonical FTS shadow table DDL")?;
        if let Some(problem) = incompatible_legacy_fts_shadow_ddl(&table, &ddl) {
            return Ok(Some(problem));
        }
    }
    Ok(None)
}

/// Ask SQLite's FTS5 reader to decode the persisted structure,
/// averages, and every segment leaf. `PRAGMA quick_check` validates SQLite's
/// b-trees, but it does not understand the payload stored inside `%_data`, so
/// it can report `ok` for the exact corrupt-segment shape seen in GH #374.
/// The special command is read-only despite its INSERT syntax.
pub(in crate::indexer) fn canonical_fts_segment_integrity_problem(
    storage: &FrankenStorage,
) -> Result<Option<String>> {
    let fts_ddl_rows = storage
        .raw()
        .query_map_collect(
            "SELECT sql FROM sqlite_master
             WHERE type = 'table' AND name = 'fts_messages'",
            &[] as &[ParamValue],
            |row| row.get_typed::<Option<String>>(0),
        )
        .context("checking for canonical FTS before segment integrity preflight")?;
    // GH #495: non-canonical legacy `fts_messages` registrations (such as
    // internal-content 7-column or `content=''`-without-`contentless_delete=1`)
    // are disposable residue that `rebuild_fts_native` drops and recreates.
    // Only probe FTS5 segment integrity on canonical contentless shadows.
    let is_canonical_contentless = !fts_ddl_rows.is_empty()
        && fts_ddl_rows.iter().all(|sql| {
            let normalized: String = sql
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .split_whitespace()
                .collect();
            (normalized.contains("content=''") || normalized.contains("content=\"\""))
                && normalized.contains("contentless_delete=1")
        });
    if !is_canonical_contentless {
        return Ok(None);
    }

    match storage.raw().query_map_collect(
        "PRAGMA integrity_check(fts_messages)",
        &[] as &[ParamValue],
        |row| row.get_typed::<String>(0),
    ) {
        Ok(rows) if rows.len() == 1 && rows[0].trim().eq_ignore_ascii_case("ok") => Ok(None),
        Ok(rows) => Ok(Some(format!(
            "FTS5 segment integrity-check failed: {}",
            rows.join("; ")
        ))),
        Err(err) if crate::storage::sqlite::retryable_franken_error(&err) => Err(anyhow::anyhow!(
            "full rebuild FTS segment integrity preflight hit transient storage contention: {err}"
        )),
        Err(err) => Ok(Some(format!("FTS5 segment integrity-check failed: {err}"))),
    }
}

pub(in crate::indexer) fn full_rebuild_existing_storage_integrity_problem(
    storage: &FrankenStorage,
) -> Result<Option<String>> {
    let bundle_bytes = archive_bundle_size_for_integrity_preflight(storage);
    let max_bytes = index_integrity_preflight_max_bytes();
    if !should_run_engine_backed_archive_integrity_preflight(bundle_bytes, max_bytes) {
        tracing::warn!(
            bundle_bytes = bundle_bytes.unwrap_or_default(),
            max_bytes,
            "skipping every SQLite-backed integrity query before a large full rebuild; the fixed-size SQLite header guard already ran, and later rebuild phases provide named progress. Use 'cass doctor check --json' for the exhaustive scan, or set CASS_INDEX_INTEGRITY_PREFLIGHT_MAX_BYTES=0 to force it here"
        );
        return Ok(None);
    }

    match incompatible_legacy_fts_shadow_problem(storage) {
        Ok(Some(problem)) => return Ok(Some(problem)),
        Ok(None) => {}
        Err(err) if anyhow_chain_indicates_retryable_storage_contention(&err) => {
            return Err(anyhow::anyhow!(
                "full rebuild FTS shadow-schema preflight hit transient storage contention: {err:#}"
            ));
        }
        Err(err) => {
            return Ok(Some(format!("FTS shadow-schema preflight failed: {err:#}")));
        }
    }

    match canonical_fts_segment_integrity_problem(storage) {
        Ok(Some(problem)) => return Ok(Some(problem)),
        Ok(None) => {}
        Err(err) => return Err(err),
    }

    let quick_check = match storage.raw().query_row_map(
        "PRAGMA quick_check(1)",
        &[] as &[ParamValue],
        |row| row.get_typed::<String>(0),
    ) {
        Ok(status) => status,
        Err(err) if crate::storage::sqlite::retryable_franken_error(&err) => {
            return Err(anyhow::anyhow!(
                "full rebuild archive integrity preflight hit transient storage contention: {err}"
            ));
        }
        Err(err) => return Ok(Some(format!("quick_check failed: {err}"))),
    };

    if !quick_check.trim().eq_ignore_ascii_case("ok") {
        return Ok(Some(format!("quick_check reported {quick_check:?}")));
    }

    // A first index can be killed after SQLite creates the file, or after
    // MigrationRunner creates its empty tracking table, but before the atomic
    // V13 schema transaction commits. Neither state contains an archive yet.
    // Admit only those exact catalogs after integrity checks; missing canonical
    // tables in any other schema must retain the archive-damage refusal below.
    let schema_objects = storage
        .raw()
        .query("SELECT type, name FROM sqlite_master LIMIT 2")
        .context("classifying an interrupted canonical schema bootstrap")?;
    let unfinished_bootstrap = match schema_objects.as_slice() {
        [] => true,
        [object]
            if object.get_typed::<String>(0)? == "table"
                && object.get_typed::<String>(1)? == "_schema_migrations" =>
        {
            storage
                .raw()
                .query("SELECT version FROM _schema_migrations LIMIT 1")
                .context("checking for a committed canonical schema migration")?
                .is_empty()
        }
        _ => false,
    };
    if unfinished_bootstrap {
        return Ok(None);
    }

    for (table, sql) in [
        ("conversations", "SELECT COUNT(*) FROM conversations"),
        ("messages", "SELECT COUNT(*) FROM messages"),
        ("sources", "SELECT COUNT(*) FROM sources"),
    ] {
        match storage
            .raw()
            .query_row_map(sql, &[] as &[ParamValue], |row| row.get_typed::<i64>(0))
        {
            Ok(_) => {}
            Err(err) if crate::storage::sqlite::retryable_franken_error(&err) => {
                return Err(anyhow::anyhow!(
                    "full rebuild archive integrity canary for {table} hit transient storage contention: {err}"
                ));
            }
            Err(err) => {
                return Ok(Some(format!(
                    "canonical table canary failed for {table}: {err}"
                )));
            }
        }
    }

    Ok(None)
}

pub(in crate::indexer) fn full_rebuild_existing_archive_integrity_preflight(
    db_path: &Path,
) -> Result<Option<String>> {
    full_rebuild_existing_archive_integrity_preflight_with_max_bytes(
        db_path,
        index_integrity_preflight_max_bytes(),
    )
}

/// Injected-cap half of the read-only full-rebuild preflight (bet45/qu81y):
/// tests exercise the size gate through this parameter instead of mutating
/// process-global CASS_INDEX_INTEGRITY_PREFLIGHT_MAX_BYTES, which under
/// parallel scheduling leaked into sibling preflight tests and made their
/// detection silently skip.
pub(in crate::indexer) fn full_rebuild_existing_archive_integrity_preflight_with_max_bytes(
    db_path: &Path,
    max_bytes: u64,
) -> Result<Option<String>> {
    let archive_bytes = match fs::metadata(db_path) {
        Ok(metadata) => metadata.len(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "inspecting canonical archive before read-only full-rebuild preflight: {}",
                    db_path.display()
                )
            });
        }
    };
    if archive_bytes == 0 {
        return Ok(None);
    }

    // GH #413: decide from filesystem metadata before opening SQLite. Even a
    // syntactically tiny sqlite_master or COUNT query can hydrate gigabytes in
    // a large archive, so an after-open/after-query cap is not a bound.
    let bundle_bytes = database_bundle_size_bytes(db_path);
    if !should_run_engine_backed_archive_integrity_preflight(Some(bundle_bytes), max_bytes) {
        tracing::warn!(
            db_path = %db_path.display(),
            bundle_bytes,
            max_bytes,
            "large full-rebuild archive passed the fixed SQLite header guard; deferring all SQLite-backed integrity work to named rebuild/doctor phases"
        );
        return Ok(None);
    }

    let storage = match FrankenStorage::open_strict_readonly_with_timeout(
        db_path,
        Duration::from_secs(10),
    ) {
        Ok(storage) => storage,
        Err(err)
            if err.chain().any(|cause| {
                matches!(
                    cause.downcast_ref::<crate::franken_sync::FrankenError>(),
                    Some(crate::franken_sync::FrankenError::BusyRecovery)
                )
            }) =>
        {
            tracing::debug!(
                db_path = %db_path.display(),
                error = %err,
                "strict read-only full-rebuild preflight encountered unrecovered WAL/SHM; falling back to private read-only snapshot"
            );
            match FrankenStorage::open_readonly_with_doctor_lock_timeout(
                db_path,
                Duration::from_secs(10),
            ) {
                Ok(storage) => storage,
                Err(snapshot_err)
                    if anyhow_chain_indicates_retryable_storage_contention(&snapshot_err) =>
                {
                    return Err(anyhow::anyhow!(
                        "full rebuild read-only archive preflight hit transient storage contention: {snapshot_err:#}"
                    ));
                }
                Err(snapshot_err) => {
                    return Ok(Some(format!(
                        "read-only full-rebuild integrity preflight could not open the canonical archive: {snapshot_err:#}"
                    )));
                }
            }
        }
        Err(err) if anyhow_chain_indicates_retryable_storage_contention(&err) => {
            return Err(anyhow::anyhow!(
                "full rebuild read-only archive preflight hit transient storage contention: {err:#}"
            ));
        }
        Err(err) => {
            return Ok(Some(format!(
                "read-only full-rebuild integrity preflight could not open the canonical archive: {err:#}"
            )));
        }
    };
    let result = full_rebuild_existing_storage_integrity_problem(&storage);
    if let Err(close_err) = storage.close_without_checkpoint() {
        if result.is_ok() {
            return Err(close_err).with_context(|| {
                format!(
                    "closing strict read-only full-rebuild preflight for {}",
                    db_path.display()
                )
            });
        }
        tracing::debug!(
            db_path = %db_path.display(),
            error = %close_err,
            "strict read-only full-rebuild preflight also failed to close after its integrity check failed"
        );
    }
    result
}

pub(in crate::indexer) fn targeted_watch_once_fts_parity_problem(
    storage: &FrankenStorage,
    opts: &IndexOptions,
) -> Result<Option<String>> {
    let is_targeted_watch_once = opts
        .watch_once_paths
        .as_ref()
        .is_some_and(|paths| !paths.is_empty())
        && !opts.watch
        && !opts.full
        && !opts.force_rebuild;
    if !is_targeted_watch_once {
        return Ok(None);
    }

    let parity = storage.inspect_search_fallback_fts_parity()?;
    match parity.status {
        // A completely absent fallback shadow is the normal lazy state for a
        // fresh/Tantivy-authoritative archive. It has no large partial table
        // to re-encode, so targeted ingestion remains safe. Only a published
        // shadow with bad or unprovable parity must stop canonical mutation.
        crate::storage::sqlite::FtsShadowParityStatus::Healthy
        | crate::storage::sqlite::FtsShadowParityStatus::Absent => Ok(None),
        crate::storage::sqlite::FtsShadowParityStatus::Residue
        | crate::storage::sqlite::FtsShadowParityStatus::Partial
        | crate::storage::sqlite::FtsShadowParityStatus::Excess
        | crate::storage::sqlite::FtsShadowParityStatus::Divergent
        | crate::storage::sqlite::FtsShadowParityStatus::Unqueryable => Ok(Some(format!(
            "derived canonical FTS shadow is {} before targeted watch-once mutation \
             (canonical_messages={}, indexable_messages={}, indexed_messages={:?}, detail={:?})",
            parity.status.as_str(),
            parity.canonical_messages,
            parity.indexable_messages,
            parity.indexed_messages,
            parity.detail
        ))),
    }
}

pub(in crate::indexer) fn targeted_watch_once_unhealthy_fts_error(
    db_path: &Path,
    reason: &str,
) -> anyhow::Error {
    anyhow::anyhow!(
        "targeted watch-once stopped before canonical mutation because {reason}. \
         Run 'cass doctor --rebuild-canonical-fts --dry-run --json' to inspect the \
         bounded repair plan, then apply it with \
         'cass doctor --rebuild-canonical-fts --yes --json'. Retry watch-once only \
         after the doctor reports exact canonical/FTS parity."
    )
    .context(format!("canonical archive: {}", db_path.display()))
}

pub(in crate::indexer) fn canonical_archive_unhealthy_for_index_error(
    db_path: &Path,
    reason: &str,
) -> anyhow::Error {
    anyhow::anyhow!(
        "canonical cass archive at {} is not safe for indexing: {reason}. \
         cass index will not replace or truncate the SQLite source of truth. \
         Run 'cass doctor check --json' first — it ranks the recovery authorities \
         (canonical DB vs a checksum-verified raw mirror vs backups) and names the \
         exact next command. If the canonical rows are readable but a derived/FTS5 \
         structure is corrupt, run 'cass doctor --rebuild-canonical-fts --dry-run \
         --json' and apply its safe parity repair with '--yes' (if the FTS \
         corruption blocks the open itself so even that repair cannot read the \
         archive — GH #368 — treat it as unopenable and use reconstruct below). \
         If the archive cannot be opened at all, run 'cass doctor --fix --json' \
         to fully verify the raw mirror and build an isolated reconstruct candidate \
         under the doctor staging directory; that step does not replace the live \
         archive. Then inspect 'cass doctor repair --dry-run --json' and apply only \
         its exact fingerprint-approved candidate-promotion command. If instead the \
         archive opens read-only but is \
         malformed, 'cass doctor --recover-from-archive <DIR>' rebuilds the source \
         JSONL from cass's preserved extra_json/extra_bin envelopes for re-ingest. \
         An explicit backup restore via 'cass doctor backups list --json' followed \
         by 'cass doctor backups restore <id> --json' also remains available.",
        db_path.display()
    )
}

pub(in crate::indexer) fn orphan_fk_cleanup_failed_index_error(
    db_path: &Path,
    err: &anyhow::Error,
) -> anyhow::Error {
    anyhow::anyhow!(
        "orphan FK self-heal failed for canonical cass archive at {}: {err:#}. \
         cass stopped before counting or writing because the archive connection may be OOM-poisoned or corrupt. \
         Run 'cass doctor check --json' and free disk/memory pressure before retrying.",
        db_path.display()
    )
}

pub(in crate::indexer) const INDEX_MIN_FREE_SPACE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) enum IndexStorageHeadroomRequirement {
    IncrementalStartup,
    AuthoritativeLexicalRebuild,
}

pub(in crate::indexer) fn index_startup_storage_headroom_requirement(
    full: bool,
    force_rebuild: bool,
) -> IndexStorageHeadroomRequirement {
    if full || force_rebuild {
        IndexStorageHeadroomRequirement::AuthoritativeLexicalRebuild
    } else {
        IndexStorageHeadroomRequirement::IncrementalStartup
    }
}

pub(in crate::indexer) fn ensure_index_startup_storage_headroom(opts: &IndexOptions) -> Result<()> {
    ensure_index_storage_headroom(
        &opts.data_dir,
        &opts.db_path,
        index_startup_storage_headroom_requirement(opts.full, opts.force_rebuild),
    )
}

pub(in crate::indexer) fn ensure_authoritative_lexical_rebuild_storage_headroom(
    data_dir: &Path,
    db_path: &Path,
) -> Result<()> {
    ensure_index_storage_headroom(
        data_dir,
        db_path,
        IndexStorageHeadroomRequirement::AuthoritativeLexicalRebuild,
    )
}

pub(in crate::indexer) fn ensure_index_storage_headroom(
    data_dir: &Path,
    db_path: &Path,
    requirement: IndexStorageHeadroomRequirement,
) -> Result<()> {
    if index_disk_headroom_check_disabled() {
        return Ok(());
    }

    let projection = index_headroom_projection(data_dir, db_path, requirement);
    let required = projection.map_or(INDEX_MIN_FREE_SPACE_BYTES, |projection| {
        projection.required_bytes
    });
    for probe_path in existing_headroom_probe_paths(data_dir, db_path) {
        let available = fs2::available_space(&probe_path).with_context(|| {
            format!(
                "checking free disk space for cass index at {}",
                probe_path.display()
            )
        })?;
        if available >= required {
            continue;
        }

        return Err(anyhow::anyhow!(
            "canonical archive disk headroom check failed for {}: available={} bytes, required={} bytes. \
             Free space before running cass index so SQLite, WAL, and lexical scratch writes cannot fail mid-commit.{}{} \
             Run 'cass doctor check --json' for a read-only health report.",
            probe_path.display(),
            available,
            required,
            projection
                .map(|projection| full_rebuild_refusal_terms(data_dir, &probe_path, &projection))
                .unwrap_or_default(),
            raw_mirror_headroom_note(data_dir, &probe_path)
        ));
    }

    Ok(())
}

/// What a full-rebuild refusal says beyond the two numbers (nw61n): the rule
/// and the terms it charged, and, on the data dir's filesystem, the space cass
/// can give back without touching the live index. Merge-retired segments,
/// retained publish backups and failed-rebuild quarantine bundles are not
/// charged, so freeing them raises the available side only. The quarantine
/// inventory is a bounded metadata scan, run only on this refusal path.
pub(in crate::indexer) fn full_rebuild_refusal_terms(
    data_dir: &Path,
    probe_path: &Path,
    projection: &FullRebuildHeadroomProjection,
) -> String {
    let mut terms = format!(
        " The requirement is {FULL_REBUILD_HEADROOM_FORMULA}: lexical index {} bytes, \
         staged generation credit {} bytes, db bundle {} bytes (growth allowance {} bytes), \
         floor {} bytes.",
        projection.lexical_index_bytes,
        projection.rebuild_staging_bytes,
        projection.db_bundle_bytes,
        projection.db_growth_allowance_bytes,
        projection.floor_bytes
    );
    if projection.lexical_index_bytes == 0 && projection.lexical_rebuild_basis_bytes > 0 {
        terms.push_str(&format!(
            " There is no live lexical generation to size the rebuild by, so it is sized as \
             the db bundle ({} bytes).",
            projection.lexical_rebuild_basis_bytes
        ));
    }
    if projection.migration_repair_pending {
        terms.push_str(&format!(
            " It includes {} bytes for the engine's one-time migration repair, which is still \
             pending for this archive and keeps a copy of the whole db bundle while it runs.",
            projection.migration_copy_bytes
        ));
    }
    let probes_data_dir = nearest_existing_path(data_dir)
        .is_some_and(|data_dir_probe| same_headroom_probe_path(&data_dir_probe, probe_path));
    if !probes_data_dir {
        return terms;
    }
    if projection.retired_segment_bytes > 0 {
        terms.push_str(&format!(
            " {} bytes of merge-retired lexical segments are reclaimable: 'cass index --gc' \
             removes them once no published generation has referenced them for the engine's \
             grace period.",
            projection.retired_segment_bytes
        ));
    }
    if projection.retained_backup_bytes > 0 {
        terms.push_str(&format!(
            " {} bytes of prior lexical generations under index/.lexical-publish-backups/ are \
             reclaimable with 'cass doctor cleanup'.",
            projection.retained_backup_bytes
        ));
    }
    if let Ok(inventory) = crate::search::lexical_quarantine::inspect(data_dir)
        && inventory.total_bytes > 0
    {
        terms.push_str(&format!(
            " Failed-rebuild quarantine bundles hold {} bytes, {} of them eligible under the \
             retention policy; 'cass lexical-quarantine --data-dir {} --plan --json' previews \
             reclaiming them.",
            inventory.total_bytes,
            inventory.eligible_bytes,
            data_dir.display()
        ));
    }
    terms
}

/// Marker the CLI matches to report a rebuild that stopped for disk space
/// (exit 14, kind `storage`, retryable).
pub(crate) const LEXICAL_REBUILD_DISK_STOP_MARKER: &str =
    "lexical rebuild stopped: free disk space fell below the floor";

/// Stop an authoritative lexical rebuild, keeping what it has committed, once
/// free space where it writes falls below the preflight floor (nw61n). The
/// preflight sizes the whole run from measurements; this catches a short
/// projection before the filesystem fills, which is how the July trj rebuild
/// failed (`disk I/O error (10)` with the root filesystem at 0 bytes). It runs
/// before each commit, at the progress heartbeat and before the terminal
/// commit, so one commit can still write past the floor; it is a backstop, not
/// a reservation. `CASS_INDEX_SKIP_DISK_HEADROOM_CHECK` disables it with the
/// preflight.
pub(in crate::indexer) fn ensure_lexical_rebuild_free_space(build_path: &Path) -> Result<()> {
    if index_disk_headroom_check_disabled() {
        return Ok(());
    }
    let Some(probe_path) = nearest_existing_path(build_path) else {
        return Ok(());
    };
    let available = fs2::available_space(&probe_path).with_context(|| {
        format!(
            "checking free disk space for the lexical rebuild at {}",
            probe_path.display()
        )
    })?;
    lexical_rebuild_free_space_verdict(available, INDEX_MIN_FREE_SPACE_BYTES, &probe_path)
}

pub(crate) fn lexical_rebuild_free_space_verdict(
    available: u64,
    floor: u64,
    probe_path: &Path,
) -> Result<()> {
    if available >= floor {
        return Ok(());
    }
    Err(anyhow::anyhow!(
        "{LEXICAL_REBUILD_DISK_STOP_MARKER} at {}: available={available} bytes, floor={floor} bytes. \
         The live lexical index is unchanged, and what this rebuild committed stays in its staged \
         generation; free space, then rerun the same cass index command to resume. Run \
         'cass doctor check --json' for the headroom a rebuild needs.",
        probe_path.display()
    ))
}

/// Whether the post-rebuild fold (one `force_merge` over the staged
/// generation) fits: it writes about as much as the generation holds while the
/// inputs stay on disk. When it does not, the rebuild publishes the unmerged
/// generation, which is what a failed fold already does (#441).
pub(in crate::indexer) fn lexical_rebuild_fold_fits(
    available: u64,
    generation_bytes: u64,
    floor: u64,
) -> bool {
    available >= generation_bytes.saturating_add(floor)
}

/// Why the post-rebuild fold of `build_path` does not fit, or `None` when it
/// does (or the headroom checks are disabled, or space cannot be read).
pub(in crate::indexer) fn lexical_rebuild_fold_shortfall(build_path: &Path) -> Option<String> {
    if index_disk_headroom_check_disabled() {
        return None;
    }
    let probe_path = nearest_existing_path(build_path)?;
    let available = fs2::available_space(&probe_path).ok()?;
    let generation_bytes = directory_size_bytes_best_effort(build_path);
    (!lexical_rebuild_fold_fits(available, generation_bytes, INDEX_MIN_FREE_SPACE_BYTES)).then(
        || {
            format!(
                "available={available} bytes at {}, generation={generation_bytes} bytes, floor={INDEX_MIN_FREE_SPACE_BYTES} bytes",
                probe_path.display()
            )
        },
    )
}

/// The raw mirror sits in the data dir and is the one large store there that
/// cass shrinks on request, so a refusal on the data dir's filesystem names
/// its size and the prune that retires superseded captures (#522: a 69 GB
/// mirror left a timer-driven index refusing to start).
pub(in crate::indexer) fn raw_mirror_headroom_note(data_dir: &Path, probe_path: &Path) -> String {
    let probes_data_dir = nearest_existing_path(data_dir)
        .is_some_and(|data_dir_probe| same_headroom_probe_path(&data_dir_probe, probe_path));
    if !probes_data_dir {
        return String::new();
    }
    let mirror_bytes = crate::raw_mirror::physical_storage_bytes(data_dir);
    if mirror_bytes == 0 {
        return String::new();
    }
    format!(
        " The raw mirror in {} holds {mirror_bytes} bytes; \
         'cass mirror prune --keep-latest 1 --safety-hold-down 6h --json' previews retiring \
         every capture older than 6 hours except the newest of each source file, and \
         --apply reclaims them.",
        data_dir.display()
    )
}

pub(crate) fn index_disk_headroom_check_disabled() -> bool {
    dotenvy::var("CASS_INDEX_SKIP_DISK_HEADROOM_CHECK")
        .map(|value| env_value_truthy(&value))
        .unwrap_or(false)
}

pub(in crate::indexer) fn env_value_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// The filesystem locations whose free space gates an index run: the data
/// dir and the database's directory (deduplicated; nearest existing
/// ancestor when the path does not exist yet). Shared with `doctor` so its
/// full-rebuild readiness answer probes exactly what `index --full` probes.
pub(crate) fn existing_headroom_probe_paths(data_dir: &Path, db_path: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::with_capacity(2);
    for candidate in [data_dir, db_path.parent().unwrap_or(db_path)] {
        if let Some(existing) = nearest_existing_path(candidate) {
            push_unique_headroom_probe_path(&mut paths, existing);
        }
    }
    if paths.is_empty() {
        paths.push(std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")));
    }
    paths
}

pub(in crate::indexer) fn push_unique_headroom_probe_path(
    paths: &mut Vec<PathBuf>,
    candidate: PathBuf,
) {
    if paths
        .iter()
        .any(|existing| same_headroom_probe_path(existing, &candidate))
    {
        return;
    }
    paths.push(candidate);
}

pub(in crate::indexer) fn same_headroom_probe_path(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    match (fs::canonicalize(left), fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

pub(in crate::indexer) fn nearest_existing_path(path: &Path) -> Option<PathBuf> {
    for ancestor in path.ancestors() {
        if ancestor.as_os_str().is_empty() {
            let current = Path::new(".");
            if current.exists() {
                return Some(current.to_path_buf());
            }
            continue;
        }
        if ancestor.exists() {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

/// Directory holding the lexical (Tantivy) index, relative to the data dir.
pub(in crate::indexer) const LEXICAL_INDEX_ROOT_DIR: &str = "index";

/// The full-rebuild projection a requirement is checked against; the
/// incremental requirement is the bare floor and has none.
pub(in crate::indexer) fn index_headroom_projection(
    data_dir: &Path,
    db_path: &Path,
    requirement: IndexStorageHeadroomRequirement,
) -> Option<FullRebuildHeadroomProjection> {
    match requirement {
        IndexStorageHeadroomRequirement::IncrementalStartup => None,
        IndexStorageHeadroomRequirement::AuthoritativeLexicalRebuild => {
            Some(full_rebuild_headroom_projection(data_dir, db_path))
        }
    }
}

#[cfg(test)]
pub(in crate::indexer) fn required_index_headroom_bytes(
    data_dir: &Path,
    db_path: &Path,
    requirement: IndexStorageHeadroomRequirement,
) -> u64 {
    index_headroom_projection(data_dir, db_path, requirement)
        .map_or(INDEX_MIN_FREE_SPACE_BYTES, |projection| {
            projection.required_bytes
        })
}

/// Human-readable statement of the full-rebuild headroom rule, reported by
/// `doctor` next to the numbers so an agent can see *why* a rebuild is
/// blocked. Keep in sync with [`full_rebuild_headroom_projection`].
/// `lexical_rebuild_basis_bytes` is the LIVE lexical footprint (see
/// [`LexicalIndexFootprint::live_bytes`]), or the db bundle when there is no
/// live generation; `migration_copy_bytes` is the db bundle while the engine's
/// one-time migration repair is pending, else 0.
pub(crate) const FULL_REBUILD_HEADROOM_FORMULA: &str = "max(512 MiB, lexical_rebuild_basis_bytes * 9 / 4 - rebuild_staging_bytes + db_bundle_bytes / 16 + migration_copy_bytes), where lexical_rebuild_basis_bytes is lexical_index_bytes, or db_bundle_bytes when no live lexical generation exists";

/// What a lexical rebuild writes per live lexical byte, as a fraction: the new
/// generation's segments plus the post-run fold that rewrites them into one
/// segment while the inputs are still on disk. Measured 2,821 MiB written for
/// 1,329 MiB live (2.12x) rebuilding a 256k-message archive (nw61n,
/// 2026-10-23); 9/4 keeps a margin over that.
pub(in crate::indexer) const LEXICAL_REBUILD_BYTES_PER_LIVE_BYTE: (u64, u64) = (9, 4);

/// The db bundle's growth a rebuild or full rescan is charged for (WAL and the
/// rows a rescan inserts), as a divisor of the bundle. The same measurement
/// saw no growth for `--force-rebuild` or for a `--full` rescan of an
/// unchanged archive; the July trj rebuild (25 GB db) wrote about 1 GB beyond
/// twice its lexical index.
pub(in crate::indexer) const REBUILD_DB_GROWTH_DIVISOR: u64 = 16;

/// The inputs and result of the full-rebuild headroom rule, so `doctor` can
/// report the same requirement `index --full` / `--force-rebuild` enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FullRebuildHeadroomProjection {
    /// Bytes that must be free at every probe path for the rebuild to start.
    pub(crate) required_bytes: u64,
    /// The absolute floor (`INDEX_MIN_FREE_SPACE_BYTES`).
    pub(crate) floor_bytes: u64,
    /// `agent_search.db` plus its `-wal`/`-shm` sidecars.
    pub(crate) db_bundle_bytes: u64,
    /// Live on-disk size of the lexical index tree: the bytes a rebuild has
    /// to write a second copy of ([`LexicalIndexFootprint::live_bytes`]).
    pub(crate) lexical_index_bytes: u64,
    /// The generation size the rule charges for: `lexical_index_bytes`, or
    /// `db_bundle_bytes` when no live generation exists to size it by.
    pub(crate) lexical_rebuild_basis_bytes: u64,
    /// Merge-retired segment files still on disk under the lexical index
    /// ([`LexicalIndexFootprint::retired_segment_bytes`]). Already consumed,
    /// reclaimed by the engine, never rewritten by a rebuild: excluded from
    /// `required_bytes`.
    pub(crate) retired_segment_bytes: u64,
    /// Number of files behind `retired_segment_bytes`.
    pub(crate) retired_segment_files: usize,
    /// Prior generations parked under `index/.lexical-publish-backups/`
    /// ([`LexicalIndexFootprint::retained_backup_bytes`]). A rebuild replaces
    /// them (retention keeps the newest, `cass doctor cleanup` reclaims the
    /// rest); excluded from `required_bytes`.
    pub(crate) retained_backup_bytes: u64,
    /// A previous rebuild's staged scratch generation
    /// ([`LexicalIndexFootprint::rebuild_staging_bytes`]). The next rebuild
    /// resumes into it or clears it first, so those bytes are already paid
    /// for: they are subtracted from `required_bytes` (GH #496: doubling them
    /// locked a failed large rebuild out of its retry; GH #498 left 32 GB).
    pub(crate) rebuild_staging_bytes: u64,
    /// Failed-rebuild quarantine bundles under `index/`
    /// ([`LexicalIndexFootprint::quarantine_bytes`]): evidence a rebuild
    /// never rewrites; excluded from `required_bytes`.
    pub(crate) quarantine_bytes: u64,
    /// The db growth allowance charged (`db_bundle_bytes / 16`).
    pub(crate) db_growth_allowance_bytes: u64,
    /// Whether the archive's one-time migration repair is still pending
    /// (`index_engine_migration_is_complete` is false). While pending, the
    /// headroom rule charges a second copy of the whole db bundle.
    pub(crate) migration_repair_pending: bool,
    /// `db_bundle_bytes` while `migration_repair_pending`, else 0.
    pub(crate) migration_copy_bytes: u64,
}

/// Single source of truth for the authoritative (full) rebuild headroom rule.
///
/// An authoritative rebuild writes a *second* full lexical index (plus the
/// fold that rewrites it into one segment) while the existing one is still on
/// disk, so the db bundle alone badly under-projects the requirement.
///
/// Sizing this as `db_bundle * 2` (the pre-2026-07 formula) let a real
/// rebuild run a filesystem to 0 bytes: on a host with a 25 GB db and a
/// 57 GB lexical index the check required 50 GB, passed against 113 GB
/// free, then consumed ~115 GB and died on `disk I/O error (10)`.
/// Counting the lexical index makes that case fail the preflight
/// instead of failing mid-commit.
///
/// The db is charged for what touches it (nw61n). A lexical rebuild of a
/// migrated archive reads it and writes almost nothing there: rebuilding a
/// 3.0 GB db / 1.3 GiB-live archive used 2.8 GB, all lexical, while
/// `db * 2 + lexical * 2` demanded 9.1 GB and refused a `--full` that then
/// wrote nothing. So the bundle is charged a growth allowance, plus one
/// whole copy only while the archive's one-time migration repair is pending.
/// The rebuild itself stops at the
/// free-space floor rather than filling the disk if the projection is short
/// ([`ensure_lexical_rebuild_free_space`]).
///
/// Only the LIVE lexical bytes count (#453). Merge-retired segment files the
/// MANIFEST no longer references and prior generations parked under
/// `.lexical-publish-backups/` are already on disk and are not rewritten by
/// the rebuild -- the engine sweep and backup retention reclaim them -- so
/// doubling them would refuse a rebuild the disk can hold. On the archive
/// that motivated this, 4.2 GB of folded inputs plus a 6.4 GB retained
/// backup sat next to a 6.8 GB live index; the recursive size demanded
/// 48 GB and refused, while the rebuild itself needed 37 GB against 43 GB
/// free.
pub(crate) fn full_rebuild_headroom_projection(
    data_dir: &Path,
    db_path: &Path,
) -> FullRebuildHeadroomProjection {
    let db_bundle_bytes = database_bundle_size_bytes(db_path);
    let migration_repair_pending =
        db_path.is_file() && !crate::storage::sqlite::index_engine_migration_is_complete(db_path);
    full_rebuild_headroom_projection_from(
        db_bundle_bytes,
        lexical_index_footprint(data_dir),
        migration_repair_pending,
    )
}

/// The rule of [`full_rebuild_headroom_projection`] over measured inputs.
pub(in crate::indexer) fn full_rebuild_headroom_projection_from(
    db_bundle_bytes: u64,
    footprint: LexicalIndexFootprint,
    migration_repair_pending: bool,
) -> FullRebuildHeadroomProjection {
    let (per_live_num, per_live_den) = LEXICAL_REBUILD_BYTES_PER_LIVE_BYTE;
    // With no live generation to size it by (the index was deleted, or only
    // backups, staging or quarantine are left), a repair still writes a whole
    // generation: size it as the db bundle. Measured generations were 0.44x
    // (nw61n fixture) and 0.96x (ts1) of their bundle, so this over-provisions
    // rather than projecting the floor for a full rebuild.
    let lexical_rebuild_basis_bytes = if footprint.live_bytes > 0 {
        footprint.live_bytes
    } else {
        db_bundle_bytes
    };
    let lexical_rebuild_bytes =
        lexical_rebuild_basis_bytes.saturating_mul(per_live_num) / per_live_den;
    let db_growth_allowance_bytes = db_bundle_bytes / REBUILD_DB_GROWTH_DIVISOR;
    let migration_copy_bytes = if migration_repair_pending {
        db_bundle_bytes
    } else {
        0
    };
    // A leftover staged generation occupies disk the rebuild gets back: a
    // resume continues writing into it, and a from-zero restart clears it
    // before writing. Either way the new generation's bytes already on disk
    // (or about to be freed) are not needed a second time, so they are
    // credited against the lexical charge rather than merely left out of it.
    let projected = lexical_rebuild_bytes
        .saturating_sub(footprint.rebuild_staging_bytes)
        .saturating_add(db_growth_allowance_bytes)
        .saturating_add(migration_copy_bytes);
    FullRebuildHeadroomProjection {
        required_bytes: INDEX_MIN_FREE_SPACE_BYTES.max(projected),
        floor_bytes: INDEX_MIN_FREE_SPACE_BYTES,
        db_bundle_bytes,
        lexical_index_bytes: footprint.live_bytes,
        lexical_rebuild_basis_bytes,
        retired_segment_bytes: footprint.retired_segment_bytes,
        retired_segment_files: footprint.retired_segment_files,
        retained_backup_bytes: footprint.retained_backup_bytes,
        rebuild_staging_bytes: footprint.rebuild_staging_bytes,
        quarantine_bytes: footprint.quarantine_bytes,
        db_growth_allowance_bytes,
        migration_repair_pending,
        migration_copy_bytes,
    }
}

/// On-disk bytes under `<data_dir>/index/`, split by whether a full rebuild
/// has to reproduce them (#453).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LexicalIndexFootprint {
    /// Everything a rebuild rewrites: segment files the current MANIFEST
    /// references, manifests and sidecars, schema markers, and any file whose
    /// classification is uncertain.
    pub(crate) live_bytes: u64,
    /// `seg-*.fslx` files (and `.retired` receipts) inside a readable Quill
    /// index directory that its MANIFEST no longer references: merge-folded
    /// inputs the engine unlinks once they have been unreferenced by both
    /// durable slots for its grace period.
    pub(crate) retired_segment_bytes: u64,
    /// Number of unreferenced `seg-*.fslx` files behind `retired_segment_bytes`.
    pub(crate) retired_segment_files: usize,
    /// Everything under `index/.lexical-publish-backups/`: prior live
    /// generations kept for rollback, pruned to the retention cap on the next
    /// staged publish and reclaimable through `cass doctor cleanup`.
    pub(crate) retained_backup_bytes: u64,
    /// Everything under a staged rebuild's scratch generation
    /// (`.<name>.rebuild-staging`, see `staged_lexical_rebuild_scratch_path`):
    /// what an interrupted or failed rebuild left behind. The next rebuild
    /// resumes into it or clears it before starting over, so a rebuild never
    /// writes it a second time (GH #496).
    pub(crate) rebuild_staging_bytes: u64,
    /// Everything under a failed rebuild's quarantine bundle (or an
    /// interrupted reclaim of one) beneath `index/`, GH #523: evidence a
    /// rebuild never rewrites, retired by retention and `cass
    /// lexical-quarantine` (nw61n; ts1 held 34.6 GB of it, which the rule
    /// had been charging as live).
    pub(crate) quarantine_bytes: u64,
}

impl LexicalIndexFootprint {
    /// Recursive size of the tree, the figure the pre-#453 preflight doubled.
    #[cfg(test)]
    pub(crate) fn total_bytes(&self) -> u64 {
        self.live_bytes
            .saturating_add(self.retired_segment_bytes)
            .saturating_add(self.retained_backup_bytes)
            .saturating_add(self.rebuild_staging_bytes)
            .saturating_add(self.quarantine_bytes)
    }
}

/// Directory under `index/` where staged publishes park the prior live
/// generation (see `lexical_publish_backups_dir`).
pub(in crate::indexer) const LEXICAL_PUBLISH_BACKUPS_DIR_NAME: &str = ".lexical-publish-backups";

/// Suffix of a staged rebuild's scratch generation directory,
/// `.<name>.rebuild-staging` (see `staged_lexical_rebuild_scratch_path`).
pub(in crate::indexer) const LEXICAL_REBUILD_STAGING_SUFFIX: &str = ".rebuild-staging";

pub(in crate::indexer) fn is_lexical_rebuild_staging_dir_name(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.starts_with('.') && name.ends_with(LEXICAL_REBUILD_STAGING_SUFFIX))
}

/// Outcome of one explicit lexical garbage sweep (`cass index --gc`, #453).
#[derive(Debug, Clone, serde::Serialize)]
pub struct LexicalSegmentGcReport {
    /// The Quill index directory that was swept.
    pub index_path: PathBuf,
    /// Segments the published MANIFEST references after the sweep: the count
    /// a query pays for.
    pub live_segments: usize,
    /// `seg-*.fslx` files on disk before and after the sweep.
    pub segment_files_before: usize,
    pub segment_files_after: usize,
    /// Bytes of MANIFEST-unreferenced segment files (and their receipts)
    /// before and after the sweep.
    pub retired_bytes_before: u64,
    pub retired_bytes_after: u64,
    /// What the sweep unlinked.
    pub reclaimed_files: usize,
    pub reclaimed_bytes: u64,
    /// The engine's grace period: a retired segment survives at least this
    /// long after the publication that dropped it from every MANIFEST slot,
    /// so a reader that opened the previous generation can still finish
    /// opening its segments.
    pub grace_secs: u64,
}

/// Reclaim merge-retired lexical segment files without indexing anything
/// (`cass index --gc`, #453).
///
/// The engine owns the reader-safety rule for unlinking a segment (a file is
/// removed only once no durable MANIFEST slot has referenced it for the
/// grace period), so cass never deletes segment files itself: it opens the
/// lexical writer, which runs that sweep under the writer admission, and
/// reports the before/after footprint. This is the same sweep every
/// incremental `cass index` performs at open; the flag exists so an operator
/// can run it on its own and see what it reclaimed.
///
/// # Errors
///
/// Fails when no published lexical index exists, when the writer admission
/// cannot be acquired (another `cass index` holds it), or when the engine
/// refuses to open the index.
pub fn run_lexical_segment_gc(data_dir: &Path) -> Result<LexicalSegmentGcReport> {
    use crate::search::quill_bridge::{
        QUILL_INDEX_MARKER, QuillCassIndex, quill_directory_footprint, segment_file_count,
    };

    let index_path = crate::search::tantivy::expected_index_dir(data_dir);
    if !index_path.join(QUILL_INDEX_MARKER).is_file() {
        anyhow::bail!(
            "no published lexical index at {} (run `cass index` first)",
            index_path.display()
        );
    }
    let before = quill_directory_footprint(&index_path).with_context(|| {
        format!(
            "reading the lexical MANIFEST at {} before the sweep",
            index_path.display()
        )
    })?;
    let segment_files_before = segment_file_count(&index_path).unwrap_or(0);

    // Opening the writer is the sweep: `KeeperWriter::open` witnesses
    // orphans and collects aged garbage before it hands the writer back.
    let index = QuillCassIndex::open_or_create(&index_path)
        .with_context(|| format!("opening the lexical writer at {}", index_path.display()))?;
    let live_segments = index.segment_count();
    drop(index);

    let after = quill_directory_footprint(&index_path).with_context(|| {
        format!(
            "reading the lexical MANIFEST at {} after the sweep",
            index_path.display()
        )
    })?;
    let segment_files_after = segment_file_count(&index_path).unwrap_or(0);
    Ok(LexicalSegmentGcReport {
        index_path,
        live_segments,
        segment_files_before,
        segment_files_after,
        retired_bytes_before: before.retired_bytes,
        retired_bytes_after: after.retired_bytes,
        reclaimed_files: segment_files_before.saturating_sub(segment_files_after),
        reclaimed_bytes: before.retired_bytes.saturating_sub(after.retired_bytes),
        grace_secs: frankensearch::quill::DEFAULT_GARBAGE_GRACE.as_secs(),
    })
}

/// Classify every byte under `<data_dir>/index/`, or all zeros when it does
/// not exist yet.
///
/// Walks explicitly (rather than following symlinks) so a symlinked index dir
/// cannot make the projection wander outside the data dir. A directory that
/// holds a readable Quill MANIFEST is split through
/// [`crate::search::quill_bridge::quill_directory_footprint`]; anything the
/// manifest cannot vouch for is counted live, because an unreadable manifest
/// is a reason to over-provision, never to call bytes reclaimable.
pub(crate) fn lexical_index_footprint(data_dir: &Path) -> LexicalIndexFootprint {
    let mut footprint = LexicalIndexFootprint::default();
    // (path, whether an ancestor is the retained-backups directory, whether
    // an ancestor is a staged rebuild's scratch generation, whether an
    // ancestor is a failed rebuild's quarantine bundle)
    let mut stack = vec![(data_dir.join(LEXICAL_INDEX_ROOT_DIR), false, false, false)];
    while let Some((path, in_backups, in_staging, in_quarantine)) = stack.pop() {
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_file() {
            if in_backups {
                footprint.retained_backup_bytes = footprint
                    .retained_backup_bytes
                    .saturating_add(metadata.len());
            } else if in_staging {
                footprint.rebuild_staging_bytes = footprint
                    .rebuild_staging_bytes
                    .saturating_add(metadata.len());
            } else if in_quarantine {
                footprint.quarantine_bytes =
                    footprint.quarantine_bytes.saturating_add(metadata.len());
            } else {
                footprint.live_bytes = footprint.live_bytes.saturating_add(metadata.len());
            }
            continue;
        }
        if !metadata.is_dir() {
            continue;
        }
        let in_backups = in_backups
            || path
                .file_name()
                .is_some_and(|name| name == LEXICAL_PUBLISH_BACKUPS_DIR_NAME);
        let in_staging = in_staging
            || path
                .file_name()
                .is_some_and(is_lexical_rebuild_staging_dir_name);
        let in_quarantine = in_quarantine
            || path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(crate::search::lexical_quarantine::is_bundle_name);
        // Backups, staging and quarantine are counted whole, never split by a
        // MANIFEST.
        let quill = if in_backups || in_staging || in_quarantine {
            None
        } else {
            crate::search::quill_bridge::quill_directory_footprint(&path)
        };
        if let Some(quill) = quill {
            footprint.live_bytes = footprint.live_bytes.saturating_add(quill.live_bytes);
            footprint.retired_segment_bytes = footprint
                .retired_segment_bytes
                .saturating_add(quill.retired_bytes);
            footprint.retired_segment_files += quill.retired_segment_files;
        }
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            // A Quill directory's regular children were already accounted
            // for through the manifest split; only descend into
            // subdirectories there.
            if quill.is_some() && !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                continue;
            }
            stack.push((entry.path(), in_backups, in_staging, in_quarantine));
        }
    }
    footprint
}

pub(in crate::indexer) fn database_bundle_size_bytes(db_path: &Path) -> u64 {
    let mut total = file_size_bytes(db_path);
    for sidecar in database_sidecar_paths(db_path) {
        total = total.saturating_add(file_size_bytes(&sidecar));
    }
    total
}

pub(in crate::indexer) fn database_sidecar_paths(db_path: &Path) -> [PathBuf; 2] {
    [
        database_path_with_suffix(db_path, "-wal"),
        database_path_with_suffix(db_path, "-shm"),
    ]
}

/// GH #450: how long a writable canonical open may take before it is worth an
/// operator-visible warning naming the phase (schema migration / one-time
/// migration repair). Well above a healthy open on a multi-gigabyte archive.
pub(in crate::indexer) const SLOW_STORAGE_OPEN_WARN_THRESHOLD: Duration = Duration::from_secs(30);

/// GH #450: the files whose byte footprint proves a writable storage open is
/// still doing work: the canonical archive and its WAL/SHM sidecars.
pub(in crate::indexer) fn storage_open_liveness_targets(db_path: &Path) -> Vec<PathBuf> {
    let mut targets = Vec::with_capacity(3);
    targets.push(db_path.to_path_buf());
    targets.extend(database_sidecar_paths(db_path));
    targets
}

pub(in crate::indexer) fn database_path_with_suffix(db_path: &Path, suffix: &str) -> PathBuf {
    let Some(file_name) = db_path.file_name() else {
        return PathBuf::from(format!("{}{}", db_path.display(), suffix));
    };
    let mut sidecar_name = file_name.to_os_string();
    sidecar_name.push(suffix);
    db_path.with_file_name(sidecar_name)
}

pub(in crate::indexer) fn file_size_bytes(path: &Path) -> u64 {
    fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

#[cfg(test)]
pub(in crate::indexer) fn current_schema_fast_probe(db_path: &Path) -> Result<bool> {
    let mut storage = FrankenStorage::open_readonly(db_path)
        .with_context(|| format!("opening SQLite db readonly at {}", db_path.display()))?;

    let version = storage
        .raw()
        .query("SELECT value FROM meta WHERE key = 'schema_version';")
        .ok()
        .and_then(|rows| rows.first().cloned())
        .and_then(|row| row.get_typed::<String>(0).ok())
        .and_then(|raw| raw.parse::<i64>().ok());

    if let Err(close_err) = storage.close_without_checkpoint_in_place() {
        tracing::warn!(
            error = %close_err,
            db_path = %db_path.display(),
            "current_schema_fast_probe: close_without_checkpoint_in_place failed; falling back to best-effort close"
        );
        storage.close_best_effort_in_place();
    }

    Ok(version == Some(crate::storage::sqlite::CURRENT_SCHEMA_VERSION))
}

pub(in crate::indexer) fn quarantine_failed_seed_bundle(db_path: &Path) -> Result<Option<PathBuf>> {
    if !db_path.exists() {
        return Ok(None);
    }

    let Some(parent) = db_path.parent() else {
        return Ok(None);
    };
    let db_name = db_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("agent_search.db");
    let backups_dir = parent.join("backups");
    fs::create_dir_all(&backups_dir).with_context(|| {
        format!(
            "creating backups directory for failed baseline seed bundle: {}",
            backups_dir.display()
        )
    })?;
    sync_parent_directory(&backups_dir)?;
    let backup_root = unique_failed_seed_backup_root(&backups_dir, db_name);

    for suffix in ["", "-wal", "-shm"] {
        let src = if suffix.is_empty() {
            db_path.to_path_buf()
        } else {
            db_path.with_file_name(format!("{db_name}{suffix}"))
        };
        if !src.exists() {
            continue;
        }
        let dest = if suffix.is_empty() {
            backup_root.clone()
        } else {
            backup_root.with_file_name(format!(
                "{}{}",
                backup_root
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("agent_search.db.failed-baseline-seed.bak"),
                suffix
            ))
        };
        fs::rename(&src, &dest).with_context(|| {
            format!(
                "moving failed baseline seed bundle component {} -> {}",
                src.display(),
                dest.display()
            )
        })?;
    }
    sync_parent_directory(db_path)?;
    sync_parent_directory(&backup_root)?;

    Ok(Some(backup_root))
}

pub(in crate::indexer) fn maybe_seed_empty_canonical_from_historical_bundle(
    storage: FrankenStorage,
    db_path: &Path,
) -> Result<(FrankenStorage, Option<HistoricalSalvageOutcome>)> {
    let conversation_count = count_total_conversations_exact(&storage)?;
    if conversation_count > 0 {
        return Ok((storage, None));
    }

    storage.close().with_context(|| {
        format!(
            "closing canonical db before baseline historical seed attempt: {}",
            db_path.display()
        )
    })?;
    match seed_canonical_from_best_historical_bundle(db_path) {
        Ok(result) => {
            let reopened = if result.is_some() {
                FrankenStorage::open_writer(db_path).with_context(|| {
                    format!(
                        "reopening canonical database after baseline historical seed attempt without rerunning migrations: {}",
                        db_path.display()
                    )
                })?
            } else {
                FrankenStorage::open(db_path).with_context(|| {
                    format!(
                        "reopening canonical database after baseline historical seed attempt: {}",
                        db_path.display()
                    )
                })?
            };
            Ok((reopened, result))
        }
        Err(err) => {
            tracing::warn!(
                db_path = %db_path.display(),
                error = %err,
                "baseline historical seed import failed; falling back to incremental salvage"
            );
            match FrankenStorage::open(db_path) {
                Ok(reopened) => Ok((reopened, None)),
                Err(reopen_err) => {
                    tracing::warn!(
                        db_path = %db_path.display(),
                        error = %reopen_err,
                        "canonical database could not be reopened after failed baseline seed; quarantining partial bundle"
                    );
                    let failed_seed_backup =
                        quarantine_failed_seed_bundle(db_path).with_context(|| {
                            format!(
                                "quarantining failed baseline seed bundle before incremental salvage: {}",
                                db_path.display()
                            )
                        })?;
                    if let Some(path) = failed_seed_backup {
                        tracing::info!(
                            db_path = %db_path.display(),
                            backup_path = %path.display(),
                            "moved failed baseline seed bundle aside before incremental salvage fallback"
                        );
                    }
                    let reopened = FrankenStorage::open(db_path).with_context(|| {
                        format!(
                            "recreating fresh canonical database after failed baseline seed import: {}",
                            db_path.display()
                        )
                    })?;
                    Ok((reopened, None))
                }
            }
        }
    }
}
