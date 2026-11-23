#[cfg(test)]
use super::backup_and_integrity::SqliteDatabaseHealthProbe;
use super::backup_and_integrity::{
    FtsConsistencyRepair, HistoricalBundleProbe, HistoricalDatabaseBundle, copy_database_bundle,
    database_sidecar_path, ensure_fts_consistency_via_rusqlite,
    fts_messages_integrity_error_from_message, move_database_bundle, sqlite3_cli_launch_error,
};
use super::schema_migrations::CURRENT_SCHEMA_VERSION;
use super::storage_core::FrankenStorage;
use crate::franken_sync::{
    Connection as FrankenConnection,
    compat::{
        ConnectionExt as FrankenConnectionExt, OpenFlags as FrankenOpenFlags,
        OptionalExtension as FrankenOptionalExtension, RowExt as FrankenRowExt,
        open_with_flags as open_franken_with_flags,
    },
};
use crate::model::types::{Conversation, Message};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::UNIX_EPOCH;
use tracing::info;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HistoricalSalvageOutcome {
    pub bundles_considered: usize,
    pub bundles_imported: usize,
    pub conversations_imported: usize,
    pub messages_imported: usize,
}

impl HistoricalSalvageOutcome {
    pub(crate) fn accumulate(&mut self, other: Self) {
        self.bundles_considered += other.bundles_considered;
        self.bundles_imported += other.bundles_imported;
        self.conversations_imported += other.conversations_imported;
        self.messages_imported += other.messages_imported;
    }
}

#[derive(Debug)]
pub(super) struct HistoricalReadConnection {
    pub(super) conn: FrankenConnection,
    pub(super) method: &'static str,
    root_path: PathBuf,
    _tempdir: Option<tempfile::TempDir>,
}

pub(super) const HISTORICAL_RECOVERY_CORE_SCHEMA: &str = r"
CREATE TABLE sources (
    id TEXT PRIMARY KEY,
    kind TEXT,
    host_label TEXT,
    machine_id TEXT,
    platform TEXT,
    config_json TEXT,
    created_at INTEGER,
    updated_at INTEGER
);
CREATE TABLE agents (
    id INTEGER PRIMARY KEY,
    slug TEXT,
    name TEXT,
    version TEXT,
    kind TEXT,
    created_at INTEGER,
    updated_at INTEGER
);
CREATE TABLE workspaces (
    id INTEGER PRIMARY KEY,
    path TEXT,
    display_name TEXT
);
CREATE TABLE conversations (
    id INTEGER PRIMARY KEY,
    agent_id INTEGER,
    workspace_id INTEGER,
    source_id TEXT,
    external_id TEXT,
    title TEXT,
    source_path TEXT,
    started_at INTEGER,
    ended_at INTEGER,
    approx_tokens INTEGER,
    metadata_json TEXT,
    origin_host TEXT,
    metadata_bin BLOB,
    total_input_tokens INTEGER,
    total_output_tokens INTEGER,
    total_cache_read_tokens INTEGER,
    total_cache_creation_tokens INTEGER,
    grand_total_tokens INTEGER,
    estimated_cost_usd REAL,
    primary_model TEXT,
    api_call_count INTEGER,
    tool_call_count INTEGER,
    user_message_count INTEGER,
    assistant_message_count INTEGER,
    last_message_idx INTEGER,
    last_message_created_at INTEGER
);
CREATE TABLE messages (
    id INTEGER PRIMARY KEY,
    conversation_id INTEGER,
    idx INTEGER,
    role TEXT,
    author TEXT,
    created_at INTEGER,
    content TEXT,
    extra_json TEXT,
    extra_bin BLOB
);
CREATE TABLE snippets (
    id INTEGER PRIMARY KEY,
    message_id INTEGER,
    file_path TEXT,
    start_line INTEGER,
    end_line INTEGER,
    language TEXT,
    snippet_text TEXT
);
";
pub(super) const HISTORICAL_SALVAGE_LEDGER_VERSION: u32 = 2;
pub(super) const HISTORICAL_SALVAGE_PROGRESS_VERSION: u32 = 1;
pub(super) const SOURCE_PATH_MERGE_START_TOLERANCE_MS: i64 = 5 * 60 * 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct HistoricalBundleProgress {
    pub(super) progress_version: u32,
    pub(super) path: String,
    pub(super) bytes: u64,
    pub(super) modified_at_ms: i64,
    method: String,
    pub(super) last_completed_source_row_id: i64,
    pub(super) conversations_imported: usize,
    pub(super) messages_imported: usize,
    pub(super) updated_at_ms: i64,
}

#[derive(Debug, Clone)]
pub(super) struct HistoricalBatchEntry {
    pub(super) source_row_id: i64,
    pub(super) agent_id: i64,
    pub(super) workspace_id: Option<i64>,
    pub(super) conversation: Conversation,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct HistoricalBatchImportTotals {
    pub(super) inserted_source_rows: usize,
    pub(super) inserted_messages: usize,
}

fn historical_bundle_root_paths(db_path: &Path) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    let Some(parent) = db_path.parent() else {
        return roots;
    };
    let db_name = db_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("agent_search.db");
    let db_stem = db_path
        .file_stem()
        .and_then(|n| n.to_str())
        .unwrap_or("agent_search");

    let mut push_root = |path: PathBuf| {
        if path == db_path {
            return;
        }
        if !roots.iter().any(|existing| existing == &path) {
            roots.push(path);
        }
    };

    if let Ok(entries) = fs::read_dir(parent) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if has_db_sidecar_suffix(name) {
                continue;
            }
            if name.starts_with(&format!("{db_name}.backup."))
                || name.starts_with(&format!("{db_stem}.corrupt."))
            {
                push_root(path);
            }
        }
    }

    let backups_dir = parent.join("backups");
    if let Ok(entries) = fs::read_dir(backups_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if has_db_sidecar_suffix(name) {
                continue;
            }
            if name.starts_with(&format!("{db_name}.")) && name.ends_with(".bak") {
                push_root(path);
            }
        }
    }

    push_named_database_children(&mut roots, db_path, &parent.join("repair-lab"), db_name);
    push_named_database_children(&mut roots, db_path, &parent.join("snapshots"), db_name);

    roots
}

fn push_named_database_children(
    roots: &mut Vec<PathBuf>,
    canonical_db_path: &Path,
    dir: &Path,
    db_name: &str,
) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let candidate = entry.path().join(db_name);
            if candidate == canonical_db_path {
                continue;
            }
            if candidate.exists() && !roots.iter().any(|existing| existing == &candidate) {
                roots.push(candidate);
            }
        }
    }
}

fn file_mtime_ms(path: &Path) -> i64 {
    fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|ts| ts.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn bundle_total_bytes(root_path: &Path) -> u64 {
    let mut total = fs::metadata(root_path).map(|meta| meta.len()).unwrap_or(0);
    for suffix in ["-wal", "-shm"] {
        let sidecar = database_sidecar_path(root_path, suffix);
        total = total.saturating_add(fs::metadata(sidecar).map(|meta| meta.len()).unwrap_or(0));
    }
    total
}

pub(crate) fn discover_historical_database_bundles(
    db_path: &Path,
) -> Vec<HistoricalDatabaseBundle> {
    let mut bundles: Vec<_> = historical_bundle_root_paths(db_path)
        .into_iter()
        .filter(|root| root.exists())
        .map(|root_path| {
            let modified_at_ms = file_mtime_ms(&root_path);
            let total_bytes = bundle_total_bytes(&root_path);
            let supports_direct_readonly = historical_bundle_supports_direct_readonly(&root_path);
            let probe = probe_historical_bundle(&root_path);
            HistoricalDatabaseBundle {
                modified_at_ms,
                total_bytes,
                supports_direct_readonly,
                root_path,
                probe,
            }
        })
        .filter(|bundle| bundle.total_bytes > 0)
        .collect();

    fn bundle_priority(path: &Path) -> i32 {
        let path_str = path.to_string_lossy();
        if path_str.contains("/repair-lab/replay-") {
            return 5;
        }
        if path_str.contains("/repair-lab/") {
            return 4;
        }
        if path_str.contains("/snapshots/") {
            return 3;
        }
        if path_str.contains(".corrupt.") || path_str.contains("failed-baseline-seed") {
            return 0;
        }
        1
    }

    fn bundle_health_rank(bundle: &HistoricalDatabaseBundle) -> i32 {
        // Classify FTS health. The probe only sets `fts_queryable = true`
        // when `fts_schema_rows == Some(1)` (see
        // `historical_bundle_fts_queryable_via_frankensqlite`), so we have
        // two legitimate "clean" shapes for a bundle:
        //
        //   * `fts_schema_rows == Some(1) && fts_queryable` — a pre-V14
        //     bundle where the FTS virtual table was eagerly created by
        //     migration and is queryable right now.
        //
        //   * `fts_schema_rows == Some(0) && schema_version == Some(V14+)` —
        //     a modern bundle where `MIGRATION_V14` dropped fts_messages on
        //     purpose and cass recreates it lazily via
        //     `ensure_search_fallback_fts_consistency` on the first open.
        //     Gating on `schema_version == CURRENT_SCHEMA_VERSION` is critical
        //     so an incomplete pre-V14 bundle with 0 fts rows is not promoted
        //     alongside real lazy-V14+ bundles. A `None` schema_version
        //     (schema marker unreadable) is excluded for the same reason.
        //
        // Everything else — `Some(1)` without queryability, `Some(n)` for
        // n >= 2 (duplicated CREATE VIRTUAL TABLE rows from a broken legacy
        // rebuild), `None` entirely, or `Some(0)` on a non-current schema —
        // is not "fts clean".
        let fts_clean = match bundle.probe.fts_schema_rows {
            Some(1) => bundle.probe.fts_queryable,
            Some(0) => bundle.probe.schema_version == Some(CURRENT_SCHEMA_VERSION),
            _ => false,
        };

        let clean_schema14_fts =
            bundle.probe.schema_version == Some(CURRENT_SCHEMA_VERSION) && fts_clean;
        if clean_schema14_fts {
            return 5;
        }

        if fts_clean {
            return 4;
        }

        if bundle.probe.schema_version == Some(CURRENT_SCHEMA_VERSION)
            && bundle.supports_direct_readonly
        {
            return 3;
        }

        if bundle.supports_direct_readonly {
            return 2;
        }

        1
    }

    bundles.sort_by(|left, right| {
        bundle_health_rank(right)
            .cmp(&bundle_health_rank(left))
            .then_with(|| right.probe.max_message_id.cmp(&left.probe.max_message_id))
            .then_with(|| bundle_priority(&right.root_path).cmp(&bundle_priority(&left.root_path)))
            .then_with(|| {
                right
                    .supports_direct_readonly
                    .cmp(&left.supports_direct_readonly)
            })
            .then_with(|| right.total_bytes.cmp(&left.total_bytes))
            .then_with(|| right.modified_at_ms.cmp(&left.modified_at_ms))
            .then_with(|| right.root_path.cmp(&left.root_path))
    });
    bundles
}

fn probe_historical_bundle(root_path: &Path) -> HistoricalBundleProbe {
    let Ok(conn) = open_historical_bundle_readonly(root_path) else {
        return probe_historical_bundle_via_sqlite3_metadata(root_path).unwrap_or_default();
    };

    let schema_version = read_meta_schema_version(&conn).ok().flatten();
    let fts_schema_rows: Option<i64> = conn
        .query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
            fparams![],
            |row| row.get_typed(0),
        )
        .ok();
    let fts_queryable =
        historical_bundle_fts_queryable_via_frankensqlite(root_path, fts_schema_rows);
    let max_message_id: i64 = conn
        .query_row_map(
            "SELECT COALESCE(MAX(id), 0) FROM messages",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap_or(0);

    let probe = HistoricalBundleProbe {
        schema_version,
        fts_schema_rows,
        fts_queryable,
        max_message_id,
    };

    if probe.schema_version.is_none()
        && probe.fts_schema_rows.is_none()
        && probe.max_message_id == 0
    {
        return probe_historical_bundle_via_sqlite3_metadata(root_path).unwrap_or(probe);
    }

    probe
}

fn probe_historical_bundle_via_sqlite3_metadata(root_path: &Path) -> Option<HistoricalBundleProbe> {
    let bundle_uri = format!("file:{}?immutable=1", root_path.to_string_lossy());
    let output = Command::new("sqlite3")
        .arg("-batch")
        .arg("-noheader")
        .arg(&bundle_uri)
        // `writable_schema=ON` is deliberate: it relaxes schema-integrity
        // enforcement so this probe can still read `schema_version` out of a
        // bundle whose sqlite_master is damaged. Nothing here writes, and the
        // connection is opened `?immutable=1`, so the relaxed setting cannot
        // corrupt anything — but it is bracketed with an explicit OFF so a
        // future edit that adds a statement here does not silently inherit an
        // unprotected connection.
        .arg(
            "PRAGMA writable_schema=ON;
             SELECT COALESCE((SELECT value FROM meta WHERE key = 'schema_version'), '');
             SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages';
             SELECT COALESCE(MAX(id), 0) FROM messages;
             PRAGMA writable_schema=OFF;",
        )
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8(output.stdout).ok()?;
    let mut lines = stdout.lines();
    let schema_version = lines.next().and_then(|raw| raw.trim().parse::<i64>().ok());
    let fts_schema_rows = lines.next().and_then(|raw| raw.trim().parse::<i64>().ok());
    let max_message_id = lines
        .next()
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .unwrap_or(0);

    Some(HistoricalBundleProbe {
        schema_version,
        fts_schema_rows,
        fts_queryable: false,
        max_message_id,
    })
}

fn historical_bundle_fts_queryable_via_frankensqlite(
    root_path: &Path,
    fts_schema_rows: Option<i64>,
) -> bool {
    matches!(fts_schema_rows, Some(1))
        && FrankenStorage::open_readonly(root_path)
            .map(|storage| {
                storage
                    .raw()
                    .query("SELECT COUNT(*) FROM fts_messages")
                    .is_ok()
            })
            .unwrap_or(false)
}

fn historical_bundle_supports_direct_readonly(root_path: &Path) -> bool {
    open_historical_bundle_readonly(root_path)
        .and_then(|conn| historical_bundle_has_queryable_core_tables(&conn))
        .is_ok()
}

/// `PRAGMA table_info` answers from the connection's schema. A `sqlite_master`
/// query instead materializes a virtual table, and fsqlite 0.4.4's clean-root
/// probe for it reads every page of the file: the token rollup stage's check
/// for its stage table held 17 GB on a 16 GB archive (xcqqa). Callers pass
/// internal table names; views also answer, which none of them name.
pub(super) fn historical_table_exists(conn: &FrankenConnection, table: &str) -> Result<bool> {
    let quoted = table.replace('"', "\"\"");
    let columns = conn
        .query(&format!("PRAGMA table_info(\"{quoted}\")"))
        .with_context(|| format!("checking for historical table {table}"))?;
    Ok(!columns.is_empty())
}

fn probe_historical_table_reads(conn: &FrankenConnection, table: &str) -> Result<()> {
    if !historical_table_exists(conn, table)? {
        return Err(anyhow!(
            "historical database missing required table {table}"
        ));
    }

    let sql = format!("SELECT rowid FROM {table} LIMIT 1");
    let _: Option<i64> = conn
        .query_row_map(&sql, fparams![], |row| row.get_typed(0))
        .optional()
        .with_context(|| format!("probing rows from historical table {table}"))?;
    Ok(())
}

fn historical_bundle_has_queryable_core_tables(conn: &FrankenConnection) -> Result<()> {
    probe_historical_table_reads(conn, "conversations")?;
    probe_historical_table_reads(conn, "messages")?;
    Ok(())
}

fn open_historical_bundle_readonly(root_path: &Path) -> Result<FrankenConnection> {
    let path_str = root_path.to_string_lossy();
    let flags = FrankenOpenFlags::SQLITE_OPEN_READ_ONLY;
    let conn = open_franken_with_flags(&path_str, flags)
        .with_context(|| format!("opening historical database {}", root_path.display()))?;
    Ok(conn)
}

fn is_recoverable_insert_line(line: &str) -> bool {
    [
        "sources",
        "agents",
        "workspaces",
        "conversations",
        "messages",
        "snippets",
    ]
    .iter()
    .any(|table| {
        line.starts_with(&format!("INSERT INTO '{table}'"))
            || line.starts_with(&format!("INSERT OR IGNORE INTO '{table}'"))
            || line.starts_with(&format!("INSERT INTO \"{table}\""))
            || line.starts_with(&format!("INSERT OR IGNORE INTO \"{table}\""))
    })
}

fn recover_historical_bundle_via_sqlite3(
    bundle: &HistoricalDatabaseBundle,
) -> Result<HistoricalReadConnection> {
    let tempdir = tempfile::TempDir::new().context("creating temporary salvage directory")?;
    let recovered_db = tempdir.path().join("historical-recovered.db");
    let temp_conn = FrankenConnection::open(recovered_db.to_string_lossy().as_ref())
        .with_context(|| format!("creating recovered database {}", recovered_db.display()))?;
    temp_conn
        .execute_batch(HISTORICAL_RECOVERY_CORE_SCHEMA)
        .with_context(|| format!("initializing recovered schema {}", recovered_db.display()))?;
    drop(temp_conn);

    let bundle_uri = format!("file:{}?immutable=1", bundle.root_path.to_string_lossy());
    let mut recover = Command::new("sqlite3")
        .arg(&bundle_uri)
        .arg(".recover")
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|error| {
            sqlite3_cli_launch_error(
                error,
                &format!(
                    "recovering historical bundle {}",
                    bundle.root_path.display()
                ),
            )
        })?;
    let recover_stdout = recover
        .stdout
        .take()
        .context("capturing sqlite3 .recover stdout")?;

    let mut importer = Command::new("sqlite3")
        .arg(&recovered_db)
        .stdin(Stdio::piped())
        .spawn()
        .map_err(|error| {
            sqlite3_cli_launch_error(
                error,
                &format!("importing recovered bundle {}", recovered_db.display()),
            )
        })?;

    {
        let importer_stdin = importer
            .stdin
            .as_mut()
            .context("opening sqlite3 importer stdin")?;
        importer_stdin
            .write_all(b"BEGIN;\n")
            .context("starting recovery import transaction")?;

        let reader = BufReader::new(recover_stdout);
        for line in reader.lines() {
            let line = line.context("reading sqlite3 .recover output")?;
            if is_recoverable_insert_line(&line) {
                importer_stdin
                    .write_all(line.as_bytes())
                    .context("writing recovered INSERT")?;
                importer_stdin
                    .write_all(b"\n")
                    .context("writing recovered INSERT newline")?;
            }
        }

        importer_stdin
            .write_all(b"COMMIT;\n")
            .context("committing recovery import transaction")?;
    }

    let importer_status = importer
        .wait()
        .context("waiting for sqlite3 recovery importer")?;
    let recover_status = recover
        .wait()
        .context("waiting for sqlite3 .recover process")?;
    if !importer_status.success() {
        anyhow::bail!(
            "sqlite3 recovery importer exited with status {} for {} after sqlite3 .recover exited with status {}",
            importer_status,
            recovered_db.display(),
            recover_status
        );
    }

    let conn = open_historical_bundle_readonly(&recovered_db)?;
    historical_bundle_has_queryable_core_tables(&conn)?;
    if !recover_status.success() {
        let (conversations, messages) = historical_bundle_counts(&conn)?;
        if conversations == 0 && messages == 0 {
            anyhow::bail!(
                "sqlite3 .recover exited with status {} for {} and recovered no core rows",
                recover_status,
                bundle.root_path.display()
            );
        }
        tracing::warn!(
            path = %bundle.root_path.display(),
            status = %recover_status,
            conversations,
            messages,
            "sqlite3 .recover exited nonzero after emitting recoverable core rows; continuing with recovered subset"
        );
    }
    Ok(HistoricalReadConnection {
        conn,
        method: "sqlite3-recover",
        root_path: recovered_db,
        _tempdir: Some(tempdir),
    })
}

pub(super) fn open_historical_bundle_for_salvage(
    bundle: &HistoricalDatabaseBundle,
) -> Result<HistoricalReadConnection> {
    match open_historical_bundle_readonly(&bundle.root_path) {
        Ok(conn) => {
            if historical_bundle_has_queryable_core_tables(&conn).is_ok() {
                return Ok(HistoricalReadConnection {
                    conn,
                    method: "direct-readonly",
                    root_path: bundle.root_path.clone(),
                    _tempdir: None,
                });
            }
        }
        Err(err) => {
            tracing::warn!(
                path = %bundle.root_path.display(),
                error = %err,
                "historical bundle direct open failed; falling back to sqlite3 .recover"
            );
        }
    }

    recover_historical_bundle_via_sqlite3(bundle)
}

fn historical_bundle_counts(conn: &FrankenConnection) -> Result<(usize, usize)> {
    let conversations: i64 =
        conn.query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
            row.get_typed(0)
        })?;
    let messages: i64 = conn.query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
        row.get_typed(0)
    })?;
    Ok((
        usize::try_from(conversations.max(0)).unwrap_or(usize::MAX),
        usize::try_from(messages.max(0)).unwrap_or(usize::MAX),
    ))
}

fn clear_seeded_runtime_meta(conn: &FrankenConnection) -> Result<()> {
    conn.execute(
        "DELETE FROM meta
         WHERE key LIKE 'historical_bundle_salvaged:%'
            OR key IN ('last_scan_ts', 'last_indexed_at', 'last_embedded_message_id')",
    )?;
    Ok(())
}

pub(super) fn record_historical_bundle_import(
    conn: &FrankenConnection,
    bundle: &HistoricalDatabaseBundle,
    method: &str,
    conversations_imported: usize,
    messages_imported: usize,
) -> Result<()> {
    let key = FrankenStorage::historical_bundle_meta_key(bundle);
    let value = serde_json::json!({
        "salvage_version": HISTORICAL_SALVAGE_LEDGER_VERSION,
        "path": bundle.root_path.display().to_string(),
        "bytes": bundle.total_bytes,
        "modified_at_ms": bundle.modified_at_ms,
        "method": method,
        "conversations_imported": conversations_imported,
        "messages_imported": messages_imported,
        "recorded_at_ms": FrankenStorage::now_millis(),
    });
    let value_str = serde_json::to_string(&value)?;
    conn.execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
        fparams![key, value_str],
    )?;
    Ok(())
}

pub(super) fn scrub_staged_derived_fts_metadata_via_sqlite3(staged_db_path: &Path) -> Result<()> {
    let scrub_sql = "PRAGMA writable_schema = ON;
         DELETE FROM sqlite_master
          WHERE name = 'fts_messages'
             OR tbl_name = 'fts_messages'
             OR name IN (
                'fts_messages_config',
                'fts_messages_content',
                'fts_messages_data',
                'fts_messages_docsize',
                'fts_messages_idx'
             )
             OR tbl_name IN (
                'fts_messages_config',
                'fts_messages_content',
                'fts_messages_data',
                'fts_messages_docsize',
                'fts_messages_idx'
             );
         PRAGMA writable_schema = OFF;";

    let run_scrub = |disable_defensive: bool| -> Result<std::process::Output> {
        let mut command = Command::new("sqlite3");
        command.arg("-batch").arg(staged_db_path);
        if disable_defensive {
            command.arg(".dbconfig defensive off");
        }
        command.arg(scrub_sql).output().map_err(|error| {
            sqlite3_cli_launch_error(
                error,
                &format!(
                    "the staged FTS metadata scrub of {}",
                    staged_db_path.display()
                ),
            )
        })
    };
    let render_output = |output: &std::process::Output| -> String {
        format!(
            "status {}; stdout: {}; stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
    };

    let defensive_off_output = run_scrub(true)?;
    if defensive_off_output.status.success() {
        return Ok(());
    }

    let fallback_output = run_scrub(false)?;
    if !fallback_output.status.success() {
        anyhow::bail!(
            "sqlite3 staged FTS metadata scrub failed for {}; defensive-off attempt {}; fallback without .dbconfig {}",
            staged_db_path.display(),
            render_output(&defensive_off_output),
            render_output(&fallback_output)
        );
    }
    Ok(())
}

fn ensure_seeded_canonical_fts_consistency(staged_db_path: &Path) -> Result<FtsConsistencyRepair> {
    match ensure_fts_consistency_via_rusqlite(staged_db_path) {
        Ok(repair) => Ok(repair),
        Err(err) => {
            if fts_messages_integrity_error_from_message(format!("{err:#}")).is_none() {
                return Err(err).with_context(|| {
                    format!(
                        "repairing staged canonical FTS consistency before finalization: {}",
                        staged_db_path.display()
                    )
                });
            }

            tracing::warn!(
                path = %staged_db_path.display(),
                error = %err,
                "staged historical seed has malformed derived FTS metadata; scrubbing and rebuilding FTS on staged copy"
            );
            scrub_staged_derived_fts_metadata_via_sqlite3(staged_db_path).with_context(|| {
                format!(
                    "scrubbing malformed staged FTS metadata before finalization: {}",
                    staged_db_path.display()
                )
            })?;
            ensure_fts_consistency_via_rusqlite(staged_db_path).with_context(|| {
                format!(
                    "repairing staged canonical FTS consistency after metadata scrub: {}",
                    staged_db_path.display()
                )
            })
        }
    }
}

fn finalize_seeded_canonical_bundle_via_rusqlite(
    canonical_db_path: &Path,
    bundle: &HistoricalDatabaseBundle,
) -> Result<(usize, usize)> {
    let _fts_repair = ensure_seeded_canonical_fts_consistency(canonical_db_path)?;

    let path_str = canonical_db_path.to_string_lossy();
    let conn = FrankenConnection::open(path_str.as_ref()).with_context(|| {
        format!(
            "opening seeded canonical database for post-seed finalization: {}",
            canonical_db_path.display()
        )
    })?;
    conn.execute("PRAGMA busy_timeout = 30000;")
        .with_context(|| {
            format!(
                "configuring busy timeout for seeded canonical database {}",
                canonical_db_path.display()
            )
        })?;
    let schema_version = read_meta_schema_version(&conn)?;

    if let Some(version) = schema_version
        && version < CURRENT_SCHEMA_VERSION
        && version != 13
    {
        anyhow::bail!(
            "seeded canonical bundle schema_version {version} is too old for baseline import and cannot be finalized automatically"
        );
    }

    clear_seeded_runtime_meta(&conn)?;
    let (conversations_imported, messages_imported) = historical_bundle_counts(&conn)?;

    conn.execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?1)",
        fparams![CURRENT_SCHEMA_VERSION.to_string()],
    )?;

    conn.execute_compat(
        "INSERT OR IGNORE INTO _schema_migrations(version, name) VALUES(?1, 'fts_contentless')",
        fparams![CURRENT_SCHEMA_VERSION],
    )?;
    record_historical_bundle_import(
        &conn,
        bundle,
        "baseline-bulk-sql-copy",
        conversations_imported,
        messages_imported,
    )?;
    Ok((conversations_imported, messages_imported))
}

fn read_meta_schema_version(conn: &FrankenConnection) -> Result<Option<i64>> {
    let version: Option<String> = conn
        .query_row_map(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            fparams![],
            |row| row.get_typed(0),
        )
        .optional()?;
    Ok(version.and_then(|raw| raw.parse::<i64>().ok()))
}

#[cfg(test)]
pub(super) fn franken_fts_schema_rows(conn: &FrankenConnection) -> Result<i64> {
    conn.query_row_map(
        "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
        fparams![],
        |row| row.get_typed(0),
    )
    .context("counting sqlite_master rows for fts_messages via frankensqlite")
}

#[cfg(test)]
fn franken_fts_limit_probe(conn: &FrankenConnection) -> bool {
    conn.query("SELECT COUNT(*) FROM fts_messages").is_ok()
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn probe_database_health_via_frankensqlite(
    db_path: &Path,
) -> Result<SqliteDatabaseHealthProbe> {
    let path_str = db_path.to_string_lossy();
    let conn = FrankenConnection::open(path_str.as_ref()).with_context(|| {
        format!(
            "opening frankensqlite db at {} for database health probe",
            db_path.display()
        )
    })?;
    conn.execute_batch("PRAGMA busy_timeout = 30000;")
        .with_context(|| {
            format!(
                "configuring busy timeout for database health probe at {}",
                db_path.display()
            )
        })?;

    let schema_version = read_meta_schema_version(&conn)?;
    let quick_check_status: String = conn
        .query_row_map("PRAGMA quick_check(1)", fparams![], |row| row.get_typed(0))
        .with_context(|| format!("running PRAGMA quick_check(1) for {}", db_path.display()))?;
    let quick_check_ok = quick_check_status.trim().eq_ignore_ascii_case("ok");
    let fts_schema_rows = franken_fts_schema_rows(&conn)?;
    let fts_queryable = fts_schema_rows == 1 && franken_fts_limit_probe(&conn);

    if !quick_check_ok {
        return Ok(SqliteDatabaseHealthProbe {
            schema_version,
            quick_check_ok,
            fts_schema_rows,
            fts_queryable,
            message_count: 0,
            max_message_id: 0,
        });
    }

    let message_count: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .context("counting messages during frankensqlite database health probe")?;
    let max_message_id: i64 = conn
        .query_row_map(
            "SELECT COALESCE(MAX(id), 0) FROM messages",
            fparams![],
            |row| row.get_typed(0),
        )
        .context("reading max message id during frankensqlite database health probe")?;

    Ok(SqliteDatabaseHealthProbe {
        schema_version,
        quick_check_ok,
        fts_schema_rows,
        fts_queryable,
        message_count,
        max_message_id,
    })
}

struct StagedHistoricalSeed {
    tempdir: tempfile::TempDir,
    db_path: PathBuf,
}

fn stage_historical_bundle_for_seed(
    canonical_db_path: &Path,
    source_root_path: &Path,
) -> Result<StagedHistoricalSeed> {
    let canonical_parent = canonical_db_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(canonical_parent).with_context(|| {
        format!(
            "creating canonical database directory before bulk historical seed import: {}",
            canonical_parent.display()
        )
    })?;
    let tempdir = tempfile::TempDir::new_in(canonical_parent)
        .context("creating temporary baseline seed directory")?;
    let staged_seed_db = tempdir.path().join("baseline-seed-output.db");
    copy_database_bundle(source_root_path, &staged_seed_db)?;

    Ok(StagedHistoricalSeed {
        tempdir,
        db_path: staged_seed_db,
    })
}

fn stage_and_finalize_historical_seed(
    canonical_db_path: &Path,
    bundle: &HistoricalDatabaseBundle,
    source_root_path: &Path,
) -> Result<(StagedHistoricalSeed, usize, usize)> {
    let staged_seed = stage_historical_bundle_for_seed(canonical_db_path, source_root_path)?;
    let (conversations_imported, messages_imported) =
        finalize_seeded_canonical_bundle_via_rusqlite(&staged_seed.db_path, bundle)?;
    Ok((staged_seed, conversations_imported, messages_imported))
}

fn promote_staged_historical_seed(
    canonical_db_path: &Path,
    staged_seed: &StagedHistoricalSeed,
) -> Result<()> {
    let canonical_backup = staged_seed
        .tempdir
        .path()
        .join("pre-seed-canonical-backup.db");
    let had_canonical = canonical_db_path.exists()
        || database_sidecar_path(canonical_db_path, "-wal").exists()
        || database_sidecar_path(canonical_db_path, "-shm").exists();

    if had_canonical {
        move_database_bundle(canonical_db_path, &canonical_backup).with_context(|| {
            format!(
                "backing up canonical database before promoting staged historical seed import: {}",
                canonical_db_path.display()
            )
        })?;
    }

    if let Err(err) =
        move_database_bundle(&staged_seed.db_path, canonical_db_path).with_context(|| {
            format!(
                "promoting staged historical seed database bundle {} into canonical path {}",
                staged_seed.db_path.display(),
                canonical_db_path.display()
            )
        })
    {
        if had_canonical {
            let _ = move_database_bundle(&canonical_backup, canonical_db_path);
        }
        return Err(err);
    }

    // Re-bind the pathname namespace to the promoted file identity. The
    // bundle move deliberately carries only db/-wal/-shm: the
    // `-fsqlite-ns-use`/`-fsqlite-ns-gate` sidecars are identity records
    // for the REPLACED file, so after promotion the first read-only open
    // would fail closed under `ReadOnlyExisting` semantics ("missing or
    // malformed records fail closed"). One writable open takes the
    // quiescent namespace exclusively and republishes the identity record
    // for the promoted bytes; on failure the pre-seed canonical is
    // restored exactly like a failed move.
    if let Err(err) = FrankenConnection::open(canonical_db_path.to_string_lossy().into_owned())
        .map(drop)
        .with_context(|| {
            format!(
                "re-binding database namespace after promoting staged historical seed into {}",
                canonical_db_path.display()
            )
        })
    {
        let _ = move_database_bundle(canonical_db_path, &staged_seed.db_path);
        if had_canonical {
            let _ = move_database_bundle(&canonical_backup, canonical_db_path);
        }
        return Err(err);
    }

    Ok(())
}

pub(crate) fn seed_canonical_from_best_historical_bundle(
    canonical_db_path: &Path,
) -> Result<Option<HistoricalSalvageOutcome>> {
    let ordered_bundles = discover_historical_database_bundles(canonical_db_path);
    let mut last_seed_error: Option<anyhow::Error> = None;
    for bundle in ordered_bundles {
        if let Some(version) = bundle.probe.schema_version
            && version < 13
        {
            let err = anyhow!(
                "historical bundle {} schema_version {version} is too old for baseline import",
                bundle.root_path.display()
            );
            tracing::warn!(
                path = %bundle.root_path.display(),
                schema_version = version,
                "historical bundle is too old for baseline seed import"
            );
            last_seed_error = Some(err);
            continue;
        }

        let (staged_seed, conversations_imported, messages_imported) =
            match stage_and_finalize_historical_seed(canonical_db_path, &bundle, &bundle.root_path)
            {
                Ok(result) => result,
                Err(primary_err) => {
                    tracing::warn!(
                        path = %bundle.root_path.display(),
                        error = %primary_err,
                        "direct bulk baseline seed from historical bundle failed; trying sqlite3 salvage copy"
                    );
                    let source = match open_historical_bundle_for_salvage(&bundle).with_context(
                        || {
                            format!(
                                "opening historical seed bundle {} for baseline import",
                                bundle.root_path.display()
                            )
                        },
                    ) {
                        Ok(source) => source,
                        Err(salvage_err) => {
                            last_seed_error = Some(anyhow!(
                                "direct baseline seed from {} failed: {primary_err:#}; sqlite3 salvage open also failed: {salvage_err:#}",
                                bundle.root_path.display()
                            ));
                            continue;
                        }
                    };
                    match stage_and_finalize_historical_seed(
                        canonical_db_path,
                        &bundle,
                        &source.root_path,
                    ) {
                        Ok(result) => result,
                        Err(err) => {
                            tracing::warn!(
                                path = %bundle.root_path.display(),
                                source_path = %source.root_path.display(),
                                error = %err,
                                "bulk baseline seed staging from sqlite3-salvaged historical bundle failed; trying next candidate"
                            );
                            last_seed_error = Some(err);
                            continue;
                        }
                    }
                }
            };

        if conversations_imported == 0 && messages_imported == 0 {
            let err = anyhow!(
                "historical bundle {} has no core rows for baseline import",
                bundle.root_path.display()
            );
            tracing::warn!(
                path = %bundle.root_path.display(),
                "historical bundle has no core rows for baseline seed import"
            );
            last_seed_error = Some(err);
            continue;
        }

        if let Err(err) = promote_staged_historical_seed(canonical_db_path, &staged_seed) {
            tracing::warn!(
                path = %bundle.root_path.display(),
                error = %err,
                "promoting staged historical seed import failed; trying next candidate"
            );
            last_seed_error = Some(err);
            continue;
        }

        tracing::info!(
            path = %bundle.root_path.display(),
            conversations_imported,
            messages_imported,
            "seeded empty canonical database from largest healthy historical bundle"
        );

        return Ok(Some(HistoricalSalvageOutcome {
            bundles_considered: 0,
            bundles_imported: 1,
            conversations_imported,
            messages_imported,
        }));
    }
    if let Some(err) = last_seed_error {
        return Err(err);
    }
    Ok(None)
}

pub(super) fn parse_json_column(value: Option<String>) -> serde_json::Value {
    value
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or(serde_json::Value::Null)
}

const HISTORICAL_RAW_JSON_SENTINEL_KEY: &str = "__cass_historical_raw_json__";

pub(super) fn wrap_historical_raw_json(raw: String) -> serde_json::Value {
    serde_json::json!({ HISTORICAL_RAW_JSON_SENTINEL_KEY: raw })
}

pub(super) fn historical_raw_json(value: &serde_json::Value) -> Option<&str> {
    match value {
        serde_json::Value::Object(map) if map.len() == 1 => map
            .get(HISTORICAL_RAW_JSON_SENTINEL_KEY)
            .and_then(serde_json::Value::as_str),
        _ => None,
    }
}

/// Render a single preserved message envelope back into one source JSONL line.
///
/// `extra_json` here is the value already decoded by
/// [`franken_read_message_extra_compat`] (msgpack `extra_bin` takes precedence
/// over the `extra_json` text column). When the envelope is the historical
/// raw-line wrapper, the verbatim original line is returned unchanged; when it
/// is a decoded event object, it is re-serialized as compact JSON; a `null`
/// envelope (no preserved event) yields `None`.
pub(crate) fn message_extra_to_source_jsonl_line(extra_json: &serde_json::Value) -> Option<String> {
    if let Some(raw) = historical_raw_json(extra_json) {
        return Some(raw.to_string());
    }
    if extra_json.is_null() {
        return None;
    }
    serde_json::to_string(extra_json).ok()
}

pub(super) fn parse_historical_json_column(value: Option<String>) -> serde_json::Value {
    match value {
        Some(raw) if raw.trim().is_empty() => serde_json::Value::Null,
        Some(raw) => wrap_historical_raw_json(raw),
        None => serde_json::Value::Null,
    }
}

pub(super) fn historical_salvage_debug_enabled() -> bool {
    std::env::var_os("CASS_DEBUG_HISTORICAL_SALVAGE").is_some()
}

#[derive(Debug, Clone, Copy)]
pub(super) struct HistoricalImportBatchLimits {
    pub(super) conversations: usize,
    pub(super) messages: usize,
    pub(super) payload_chars: usize,
}

fn env_positive_usize(key: &str) -> Option<usize> {
    dotenvy::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

pub(super) fn historical_import_batch_limits() -> HistoricalImportBatchLimits {
    let cpu_count = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);

    let default_limits = if cpu_count >= 32 {
        HistoricalImportBatchLimits {
            conversations: 128,
            messages: 16_384,
            payload_chars: 12_000_000,
        }
    } else {
        HistoricalImportBatchLimits {
            conversations: 32,
            messages: 4_096,
            payload_chars: 3_000_000,
        }
    };

    HistoricalImportBatchLimits {
        conversations: env_positive_usize("CASS_HISTORICAL_IMPORT_BATCH_CONVERSATIONS")
            .unwrap_or(default_limits.conversations),
        messages: env_positive_usize("CASS_HISTORICAL_IMPORT_BATCH_MESSAGES")
            .unwrap_or(default_limits.messages),
        payload_chars: env_positive_usize("CASS_HISTORICAL_IMPORT_BATCH_CHARS")
            .unwrap_or(default_limits.payload_chars),
    }
}

pub(super) fn json_value_size_hint(value: &serde_json::Value) -> usize {
    if let Some(raw) = historical_raw_json(value) {
        return raw.len();
    }
    match value {
        serde_json::Value::Null => 0,
        other => serde_json::to_string(other)
            .map(|raw| raw.len())
            .unwrap_or(0),
    }
}

pub(super) fn message_payload_size_hint(message: &Message) -> usize {
    message
        .content
        .len()
        .saturating_add(json_value_size_hint(&message.extra_json))
}

pub(super) fn is_backup_root_name(name: &str, prefix: &str) -> bool {
    name.starts_with(prefix) && !name.ends_with("-wal") && !name.ends_with("-shm")
}

// Suffixes that mark sqlite sidecar files we must never re-open as a DB root.
// Includes the standard -wal/-shm pair plus frankensqlite's Windows advisory-
// lock sidecars (-lock-shared/-lock-reserved/-lock-pending). Used by directory
// enumeration paths in `historical_bundle_root_paths`; deliberately NOT used
// by `is_backup_root_name`, because the existing backup-rotation cleanup must
// continue to sweep up any pre-existing orphan lock sidecars.
fn has_db_sidecar_suffix(name: &str) -> bool {
    const SIDECAR_SUFFIXES: &[&str] = &[
        "-wal",
        "-shm",
        "-lock-shared",
        "-lock-reserved",
        "-lock-pending",
        // frankensqlite VFS namespace sidecars (fsqlite-vfs namespace.rs).
        // The pinned family creates these next to every opened database;
        // without this exclusion a `<stem>.corrupt.<ts>-fsqlite-ns-use`
        // lock file (40 bytes, non-empty) is discovered as a historical
        // bundle candidate and can even be seeded into the canonical path.
        "-fsqlite-ns-gate",
        "-fsqlite-ns-use",
        // fsqlite 0.2.x WAL certification sidecars. Created next to any
        // database with WAL content; advisory at open (a copied bundle
        // without them recovers normally, probe-verified), but they are
        // runtime artifacts, never independent archives.
        "-wal-cert",
        "-wal-cert-head",
        // fsqlite 0.3.x stamps a migration-state marker at birth on every
        // database it creates. Without this exclusion a
        // `<stem>.corrupt.<ts>.fsqlite-migration-state` marker matches the
        // corrupt-bundle prefix and salvage counts a phantom third bundle.
        ".fsqlite-migration-state",
        ".fsqlite-migration-state.tmp",
    ];
    SIDECAR_SUFFIXES.iter().any(|suffix| name.ends_with(suffix))
}
