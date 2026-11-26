use super::batch_ingest::normalized_storage_source_parts;
use super::connection_pool::FrankenOwnerConnection;
use super::omp_and_semantic_meta::CURRENT_SCHEMA_VERSION;
use super::open_and_locks::retryable_franken_error;
use super::source_and_historical_import::historical_table_exists;
use super::storage_core::FrankenStorage;
use crate::franken_sync::{
    Connection as FrankenConnection, Row as FrankenRow, SqliteValue,
    compat::{
        ConnectionExt as FrankenConnectionExt, OptionalExtension as FrankenOptionalExtension,
        RowExt as FrankenRowExt,
    },
};
use crate::model::types::Conversation;
use anyhow::{Context, Result, anyhow, bail};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

// -------------------------------------------------------------------------
// Binary Metadata Serialization (Opt 3.1)
// -------------------------------------------------------------------------
// MessagePack provides 50-70% storage reduction vs JSON and faster parsing.
// New rows use binary columns; existing JSON is read on fallback.

/// Serialize a JSON value to MessagePack bytes.
/// Returns None for null/empty values to save storage.
pub(super) fn serialize_json_to_msgpack(value: &serde_json::Value) -> Option<Vec<u8>> {
    if value.is_null() || value.as_object().is_some_and(|o| o.is_empty()) {
        return None;
    }
    rmp_serde::to_vec(value).ok()
}

/// Deserialize MessagePack bytes to a JSON value.
/// Returns default Value::Object({}) on error or empty input.
pub(super) fn deserialize_msgpack_to_json(bytes: &[u8]) -> serde_json::Value {
    if bytes.is_empty() {
        return serde_json::Value::Object(serde_json::Map::new());
    }
    rmp_serde::from_slice(bytes).unwrap_or_else(|e| {
        tracing::debug!(
            error = %e,
            bytes_len = bytes.len(),
            "Failed to deserialize metadata - returning empty object"
        );
        serde_json::Value::Object(serde_json::Map::new())
    })
}

/// Read metadata from a SQLite Row, preferring binary (msgpack) over JSON.
pub(super) fn franken_read_metadata_compat(
    row: &FrankenRow,
    json_idx: usize,
    bin_idx: usize,
) -> serde_json::Value {
    // Try binary column first (new format)
    if let Ok(Some(bytes)) = row.get_typed::<Option<Vec<u8>>>(bin_idx)
        && !bytes.is_empty()
    {
        return deserialize_msgpack_to_json(&bytes);
    }

    // Fall back to JSON column (old format or migration in progress)
    if let Ok(Some(json_str)) = row.get_typed::<Option<String>>(json_idx) {
        return serde_json::from_str(&json_str)
            .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()));
    }

    serde_json::Value::Object(serde_json::Map::new())
}

/// One canonical `conversations` row as read by
/// [`FrankenStorage::list_conversations_for_recovery`].
#[derive(Debug)]
pub enum RecoveryConversationRow {
    /// The row was readable, possibly after coercing content columns (title,
    /// timestamps, token count) whose stored type disagreed with the schema;
    /// `coercions` names each such column. Identity columns are never
    /// coerced here — see [`Self::Quarantined`].
    Readable {
        conversation: Box<Conversation>,
        coercions: Vec<String>,
    },
    /// The row's `id` is an integer, but an identity column (`agent_slug`,
    /// `workspace`, `external_id`, `source_path`, `source_id`, `origin_host`)
    /// held a non-text value, or `source_path` was NULL/empty in its NOT NULL
    /// column. These values violate CASS's identity contract. They can occur
    /// when a damaged page decodes a foreign record (#391), but the types
    /// alone do not establish that cause: SQLite also permits BLOB values
    /// in a TEXT-affinity column. Skip and count the row rather than risk
    /// exporting messages under an invented identity. `coercions` lists
    /// every coerced column, identity columns first.
    Quarantined { id: i64, coercions: Vec<String> },
    /// The row's `id` itself was not an integer, so nothing downstream can
    /// address it; `stored_id` is the offending value's SQLite type name
    /// (`text`, `blob`, ...) or `<missing>` — never its content.
    Unreadable { stored_id: String, reason: String },
}

/// Read a schema-`TEXT` column leniently: NULL stays `None`, text is taken
/// as-is, and numeric or blob values are coerced to text and recorded in
/// `coercions`. TEXT affinity does not prevent a stored BLOB value.
fn lenient_text_column(
    row: &FrankenRow,
    idx: usize,
    column: &str,
    coercions: &mut Vec<String>,
) -> Option<String> {
    match row.get(idx)? {
        SqliteValue::Null => None,
        SqliteValue::Text(text) => Some(text.to_string()),
        SqliteValue::Integer(n) => {
            coercions.push(format!("{column}: integer {n} stored in TEXT column"));
            Some(n.to_string())
        }
        SqliteValue::Float(f) => {
            coercions.push(format!("{column}: real {f} stored in TEXT column"));
            Some(f.to_string())
        }
        SqliteValue::Blob(bytes) => {
            coercions.push(format!(
                "{column}: {}-byte blob stored in TEXT column",
                bytes.len()
            ));
            Some(String::from_utf8_lossy(bytes).into_owned())
        }
    }
}

/// Read a schema-`INTEGER` column leniently: integers are taken as-is, a
/// real or a numeric string is coerced (and recorded), anything else drops to
/// `None` with a note rather than failing the row.
fn lenient_integer_column(
    row: &FrankenRow,
    idx: usize,
    column: &str,
    coercions: &mut Vec<String>,
) -> Option<i64> {
    match row.get(idx)? {
        SqliteValue::Null => None,
        SqliteValue::Integer(n) => Some(*n),
        SqliteValue::Float(f) => {
            coercions.push(format!("{column}: real {f} stored in INTEGER column"));
            // Truncation toward zero is the intended lenient reading of a
            // real that landed in an integer column.
            Some(*f as i64)
        }
        SqliteValue::Text(text) => {
            let text = text.to_string();
            let parsed = text.trim().parse::<i64>().ok();
            coercions.push(format!(
                "{column}: text {text:?} stored in INTEGER column{}",
                if parsed.is_some() { "" } else { " (dropped)" }
            ));
            parsed
        }
        SqliteValue::Blob(bytes) => {
            coercions.push(format!(
                "{column}: {}-byte blob stored in INTEGER column (dropped)",
                bytes.len()
            ));
            None
        }
    }
}

/// Map one row of the recovery listing (column order as in
/// [`FrankenStorage::list_conversations_for_recovery`]) without letting a
/// schema/type disagreement fail the page.
pub(crate) fn recovery_conversation_row_from_lenient_columns(
    row: &FrankenRow,
) -> RecoveryConversationRow {
    let id = match row.get(0) {
        Some(SqliteValue::Integer(id)) => *id,
        other => {
            let stored_id = other.map_or_else(
                || "<missing>".to_string(),
                |value| value.typeof_str().to_string(),
            );
            return RecoveryConversationRow::Unreadable {
                stored_id,
                reason: "conversation id is not an integer; the row cannot be addressed for \
                         message reconstruction"
                    .to_string(),
            };
        }
    };
    // Identity columns are tracked apart from content columns: a coercion on
    // any of them quarantines the row (see `RecoveryConversationRow::
    // Quarantined`), whereas a coerced title or timestamp still leaves an
    // addressable conversation worth exporting.
    let mut identity_coercions = Vec::new();
    let mut coercions = Vec::new();
    let agent_slug = lenient_text_column(row, 1, "agent_slug", &mut identity_coercions)
        .filter(|slug| !slug.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    let workspace = lenient_text_column(row, 2, "workspace", &mut identity_coercions)
        .map(|p| Path::new(&p).to_path_buf());
    let external_id = lenient_text_column(row, 3, "external_id", &mut identity_coercions);
    let title = lenient_text_column(row, 4, "title", &mut coercions);
    let source_path = lenient_text_column(row, 5, "source_path", &mut identity_coercions)
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| {
            identity_coercions.push("source_path: NULL or empty in NOT NULL column".to_string());
            // Never exported: the identity coercion just recorded quarantines
            // the row below.
            String::new()
        });
    let started_at = lenient_integer_column(row, 6, "started_at", &mut coercions);
    let ended_at = lenient_integer_column(row, 7, "ended_at", &mut coercions);
    let approx_tokens = lenient_integer_column(row, 8, "approx_tokens", &mut coercions);
    let raw_source_id = lenient_text_column(row, 10, "source_id", &mut identity_coercions);
    let raw_origin_host = lenient_text_column(row, 11, "origin_host", &mut identity_coercions);
    if !identity_coercions.is_empty() {
        identity_coercions.extend(coercions);
        return RecoveryConversationRow::Quarantined {
            id,
            coercions: identity_coercions,
        };
    }
    let (source_id, _, origin_host) =
        normalized_storage_source_parts(raw_source_id.as_deref(), None, raw_origin_host.as_deref());
    RecoveryConversationRow::Readable {
        conversation: Box::new(Conversation {
            id: Some(id),
            agent_slug,
            workspace,
            external_id,
            title,
            source_path: Path::new(&source_path).to_path_buf(),
            started_at,
            ended_at,
            approx_tokens,
            // Already tolerant: falls back to an empty object on any decode error.
            metadata_json: franken_read_metadata_compat(row, 9, 12),
            messages: Vec::new(),
            source_id,
            origin_host,
        }),
        coercions,
    }
}

pub(super) fn franken_read_message_extra_compat(
    row: &FrankenRow,
    json_idx: usize,
    bin_idx: usize,
) -> serde_json::Value {
    if let Ok(Some(bytes)) = row.get_typed::<Option<Vec<u8>>>(bin_idx)
        && !bytes.is_empty()
    {
        return deserialize_msgpack_to_json(&bytes);
    }

    if let Ok(Some(json_str)) = row.get_typed::<Option<String>>(json_idx) {
        return serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null);
    }

    serde_json::Value::Null
}

// -------------------------------------------------------------------------
// Migration Error Types (P1.5)
// -------------------------------------------------------------------------

/// Error type for schema migration operations.
#[derive(Debug, Error)]
pub enum MigrationError {
    /// The schema requires a full rebuild. The database has been backed up.
    #[error("Rebuild required: {reason}")]
    RebuildRequired {
        reason: String,
        backup_path: Option<std::path::PathBuf>,
    },

    /// A database error occurred during migration.
    #[error("Database error: {0}")]
    Database(#[from] crate::franken_sync::FrankenError),

    /// An I/O error occurred during backup.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Other migration error.
    #[error("{0}")]
    Other(String),
}

impl From<anyhow::Error> for MigrationError {
    fn from(e: anyhow::Error) -> Self {
        MigrationError::Other(e.to_string())
    }
}

/// Maximum number of backup files to retain.
pub(super) const MAX_BACKUPS: usize = 3;
const BACKUP_VACUUM_BUSY_TIMEOUT_PRAGMA: &str = "PRAGMA busy_timeout = 30000;";

/// Files that contain user-authored state and must NEVER be deleted during rebuild.
const USER_DATA_FILES: &[&str] = &["bookmarks.db", "tui_state.json", "sources.toml", ".env"];

/// Check if a file is user-authored data that must be preserved during rebuild.
pub fn is_user_data_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|name| USER_DATA_FILES.contains(&name))
        .unwrap_or(false)
}

/// Connectors whose current build must re-read sources an incremental scan
/// would skip, either because the reader learned a source form that can
/// already be on disk with an old modification time, or because the persist
/// layer changed which records a re-parse keeps. Until a scan of the
/// connector under its current contract succeeds, the connector is scanned
/// without a cutoff; durable source reuse still skips unchanged files.
/// codex: a rollout Codex compresses to `.jsonl.zst` keeps the plain
/// rollout's modification time (GH #513). devin: the replay dedupe only
/// learned devin's top-level `tool_call_id` record identity with the
/// `tool-call-id-native-identity` contract — archives ingested before it
/// collapsed duplicate-text tool results at ingest, so every devin session
/// must re-parse from source once for the dropped rows to refill.
const CONNECTOR_SCAN_CONTRACTS: &[(&str, &str)] = &[
    ("codex", "rollout-jsonl-zst"),
    ("devin", "tool-call-id-native-identity"),
];

/// The scan contract a connector's watermark must be recorded under.
pub fn connector_scan_contract(connector_name: &str) -> Option<&'static str> {
    let name = connector_name.trim();
    CONNECTOR_SCAN_CONTRACTS
        .iter()
        .find(|(connector, _)| connector.eq_ignore_ascii_case(name))
        .map(|(_, contract)| *contract)
}

/// What the indexer needs to choose a connector's incremental cutoff.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConnectorScanState {
    pub last_scan_ts: Option<i64>,
    pub has_conversations: bool,
    /// The connector has a scan contract this archive has not recorded.
    pub scan_contract_pending: bool,
}

/// SQL to register the derived FTS5 shadow on a connection. Canonical
/// schema-v1 shape: two indexed columns plus the implicit rowid
/// (`contentless_delete=1` lets row removal propagate through the
/// `messages_fts_delete` trigger's `DELETE FROM fts_messages`).
pub const FTS5_REGISTER_SQL: &str = "\
    CREATE VIRTUAL TABLE IF NOT EXISTS fts_messages USING fts5(\
        content, title, \
        content='', contentless_delete=1, tokenize='porter unicode61'\
    )";

pub(super) const FTS_FRANKEN_REBUILD_META_KEY: &str = "fts_frankensqlite_rebuild_generation";
pub(super) const FTS_FRANKEN_REBUILD_FINGERPRINT_META_KEY: &str =
    "fts_frankensqlite_archive_fingerprint";
/// zn1xn (adversarial-review F5): set to the failure detail when a full index
/// run's optional derived fallback-FTS repair was skipped/failed and the
/// canonical SQLite `fts_messages` shadow may be half-rebuilt; deleted when a
/// later run repairs it. Surfaced additively in `state_meta_json` so `cass
/// index --json` / `status` expose the half-built shadow immediately, rather
/// than only when doctor's next PartialParity/ShadowCorrupt check catches it.
pub(super) const FTS_FALLBACK_REPAIR_PENDING_META_KEY: &str = "fts_fallback_repair_pending";
/// GH #413 follow-up (iify0): set when the derived fts5 shadow was dropped
/// because the engine cannot materialize a corpus this large (see
/// `fts_shadow_max_messages`). Gates recreation.
pub(super) const FTS_SHADOW_NOT_VIABLE_META_KEY: &str = "fts_shadow_not_viable";
/// Prefix of the error the shadow repair raises when it refuses to recreate a
/// dropped, still-oversized shadow; the index run maps it to a nonfatal outcome.
pub(crate) const FTS_SHADOW_NOT_VIABLE_ERROR_PREFIX: &str =
    "fallback FTS shadow not viable on this engine: ";
/// Bound on the persisted failure detail so a pathological error chain cannot
/// bloat the `meta` row.
pub(super) const FTS_FALLBACK_REPAIR_PENDING_DETAIL_MAX_BYTES: usize = 400;
/// zn1xn F4: consecutive count of non-watch index runs that deferred inline
/// lexical updates (an OOM under ample memory reroutes EVERY plain run through
/// the full authoritative lexical rebuild — a multi-minute/hour amplification
/// that was previously only a stderr warn). Surfaced in `status`/`index --json`
/// so operators/agents can see the persistently-deferred degradation and its
/// streak length, and reset once a run completes without deferring.
pub(super) const LEXICAL_REPAIR_DEFERRED_COUNT_META_KEY: &str =
    "lexical_repair_deferred_consecutive_runs";
pub(super) const LEXICAL_REPAIR_DEFERRED_REASON_META_KEY: &str = "lexical_repair_deferred_reason";
pub(super) const FTS_FRANKEN_REBUILD_GENERATION: i64 = 1;
/// Rowid parity cannot certify content after an in-place canonical revision.
pub(super) const FTS_FRANKEN_CONTENT_REVISION_PENDING: i64 = -1;
/// Exact canonical cardinality driven by the compact parent-rowid domain.
/// SQLite lowers this shape to a covering index scan on `(conversation_id, idx)`
/// without opening the message table, crossing the Rust row handler per message,
/// or admitting orphan parent IDs.
pub(crate) const FTS_INDEXABLE_MESSAGE_COUNT_SQL: &str = "SELECT COUNT(*) \
     FROM messages INDEXED BY idx_messages_conv_idx \
     WHERE conversation_id IN (SELECT rowid FROM conversations)";
/// Exact intersection stream driven by the (potentially much smaller) derived
/// shadow. The supported two-table join scans `fts_messages_docsize` once and
/// point-seeks canonical messages by integer primary key. Only the compact
/// parent ID crosses into Rust, where a bounded parent-ID set excludes orphan
/// messages exactly. No TEMP writes are required, which is essential for
/// read-only doctor dry-runs.
pub(super) const FTS_INDEXED_CANONICAL_INTERSECTION_SQL: &str = "SELECT m.conversation_id \
     FROM fts_messages_docsize f NOT INDEXED \
     INNER JOIN messages m NOT INDEXED ON m.id = f.id";
pub(super) const DAILY_STATS_HEALTH_META_KEY: &str = "daily_stats_archive_fingerprint";
pub(super) const DAILY_STATS_HEALTH_GENERATION_META_KEY: &str = "daily_stats_health_generation";
pub(super) const DAILY_STATS_HEALTH_GENERATION: i64 = 1;
pub(super) const DAILY_STATS_CONTENT_REVISION_PENDING: i64 = -1;

/// SQL to clear all rows from the contentless `fts_messages` table using
/// FTS5's native `'delete-all'` command.
pub const FTS5_DELETE_ALL_SQL: &str =
    "INSERT INTO fts_messages(fts_messages) VALUES('delete-all');";

pub const FTS_MESSAGES_REQUIRED_SHADOW_TABLES: [&str; 4] = [
    "fts_messages_config",
    "fts_messages_data",
    "fts_messages_docsize",
    "fts_messages_idx",
];

pub const FTS_MESSAGES_INTEGRITY_PROBE_SQL: &str = "SELECT * FROM fts_messages LIMIT 0";

pub const FTS_MESSAGES_CORRUPTION_RECOVERY_HINT: &str = "Stop all cass index/watch processes, back up the current database, then run \
     'cass doctor check --json' for a read-only diagnosis before using a supported \
     repair/rebuild path.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsMessagesIntegrityError {
    missing_shadow_tables: Vec<&'static str>,
    failed_sql: Option<&'static str>,
    source_error: Option<String>,
}

impl FtsMessagesIntegrityError {
    fn new(
        missing_shadow_tables: Vec<&'static str>,
        failed_sql: Option<&'static str>,
        source_error: Option<String>,
    ) -> Self {
        Self {
            missing_shadow_tables,
            failed_sql,
            source_error,
        }
    }

    pub fn missing_shadow_tables(&self) -> &[&'static str] {
        &self.missing_shadow_tables
    }
}

impl std::fmt::Display for FtsMessagesIntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CASS database FTS5 index is corrupt: fts_messages exists, but required FTS5 shadow tables are missing or unreadable"
        )?;
        if !self.missing_shadow_tables.is_empty() {
            write!(
                f,
                "; missing shadow tables: {}",
                self.missing_shadow_tables.join(", ")
            )?;
        }
        if let Some(sql) = self.failed_sql {
            write!(f, "; failed SQL: {sql}")?;
        }
        if let Some(source_error) = &self.source_error {
            write!(f, "; error: {source_error}")?;
        }
        write!(
            f,
            ". Suggested recovery: {FTS_MESSAGES_CORRUPTION_RECOVERY_HINT}"
        )
    }
}

impl std::error::Error for FtsMessagesIntegrityError {}

pub fn fts_messages_integrity_error_from_message(
    source_error: impl Into<String>,
) -> Option<FtsMessagesIntegrityError> {
    let source_error = source_error.into();
    let lower = source_error.to_ascii_lowercase();
    if !lower.contains("fts_messages") {
        return None;
    }

    let mentions_required_shadow_table = FTS_MESSAGES_REQUIRED_SHADOW_TABLES
        .iter()
        .any(|table| lower.contains(&table.to_ascii_lowercase()));
    let mentions_structural_fts_failure = lower.contains("shadow table")
        || lower.contains("vtable constructor failed")
        || lower.contains("sqlite_corrupt")
        || lower.contains("databasecorrupt")
        || lower.contains("database corrupt")
        || lower.contains("missing required")
        || (mentions_required_shadow_table
            && (lower.contains("table not found") || lower.contains("no such table")));
    if !mentions_structural_fts_failure {
        return None;
    }

    let missing_shadow_tables = FTS_MESSAGES_REQUIRED_SHADOW_TABLES
        .iter()
        .copied()
        .filter(|table| lower.contains(&table.to_ascii_lowercase()))
        .collect::<Vec<_>>();

    Some(FtsMessagesIntegrityError::new(
        missing_shadow_tables,
        Some(FTS_MESSAGES_INTEGRITY_PROBE_SQL),
        Some(source_error),
    ))
}

fn fts_schema_tolerates_missing_shadow_metadata(sql: &str) -> bool {
    let normalized = sql
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    normalized.contains("usingfts5(")
        && normalized.contains("content=''")
        && !normalized.contains("message_id")
}

fn validate_fts_messages_integrity_with_queries(
    query_strings: impl Fn(&str) -> std::result::Result<Vec<String>, crate::franken_sync::FrankenError>,
    probe: impl Fn(&str) -> std::result::Result<(), crate::franken_sync::FrankenError>,
) -> Result<()> {
    let fts_schema_sql = query_strings(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'fts_messages'",
    )
    .with_context(|| "checking for fts_messages in sqlite_master")?;
    if fts_schema_sql.is_empty() {
        return Ok(());
    }

    let probe_error = probe(FTS_MESSAGES_INTEGRITY_PROBE_SQL).err();
    if let Some(ref err) = probe_error
        && (retryable_franken_error(err) || matches!(err, crate::franken_sync::FrankenError::Abort))
    {
        return Err(anyhow::Error::new(probe_error.unwrap()));
    }
    if probe_error.is_none()
        && fts_schema_sql
            .iter()
            .all(|sql| fts_schema_tolerates_missing_shadow_metadata(sql))
    {
        return Ok(());
    }

    let present_shadow_tables: HashSet<String> = query_strings(
        "SELECT name FROM sqlite_master
             WHERE type = 'table'
               AND name IN (
                 'fts_messages_config',
                 'fts_messages_data',
                 'fts_messages_docsize',
                 'fts_messages_idx'
               )",
    )
        .map(|rows| rows.into_iter().collect())
        .map_err(|err| {
            FtsMessagesIntegrityError::new(
                Vec::new(),
                Some(
                    "SELECT name FROM sqlite_master WHERE name IN \
                     ('fts_messages_config','fts_messages_data','fts_messages_docsize','fts_messages_idx')",
                ),
                Some(err.to_string()),
            )
        })?;
    let missing_shadow_tables = FTS_MESSAGES_REQUIRED_SHADOW_TABLES
        .iter()
        .copied()
        .filter(|table| !present_shadow_tables.contains(*table))
        .collect::<Vec<_>>();

    if probe_error.is_none() && missing_shadow_tables.is_empty() {
        return Ok(());
    }

    Err(FtsMessagesIntegrityError::new(
        missing_shadow_tables,
        probe_error
            .as_ref()
            .map(|_| FTS_MESSAGES_INTEGRITY_PROBE_SQL),
        probe_error.map(|err| err.to_string()),
    )
    .into())
}

pub fn validate_fts_messages_integrity_for_connection(conn: &FrankenConnection) -> Result<()> {
    validate_fts_messages_integrity_with_queries(
        |sql| {
            conn.query_map_collect(sql, fparams![], |row: &FrankenRow| {
                row.get_typed::<String>(0)
            })
        },
        |sql| conn.query(sql).map(|_| ()),
    )
}

pub(crate) fn validate_fts_messages_integrity_for_async_connection(
    conn: &FrankenOwnerConnection,
) -> Result<()> {
    validate_fts_messages_integrity_with_queries(
        |sql| {
            let rows = conn.query_sync(sql)?;
            rows.iter()
                .map(|row: &FrankenRow| row.get_typed::<String>(0))
                .collect()
        },
        |sql| conn.query_sync(sql).map(|_| ()),
    )
}

pub(super) fn sqlite3_cli_launch_error(error: std::io::Error, action: &str) -> anyhow::Error {
    if error.kind() == std::io::ErrorKind::NotFound {
        anyhow::anyhow!(
            "{action} needs the sqlite3 command-line tool, which is not installed or not on \
             PATH; install sqlite3 and retry"
        )
    } else {
        anyhow::Error::new(error).context(format!("launching sqlite3 for {action}"))
    }
}

#[cfg(test)]
pub(crate) fn materialize_fresh_fts_schema_via_rusqlite(db_path: &Path) -> Result<()> {
    let storage = FrankenStorage::open(db_path).with_context(|| {
        format!(
            "opening sqlite db at {} for FTS materialization",
            db_path.display()
        )
    })?;
    storage.rebuild_fts_via_frankensqlite().map(|_| ())
}

#[cfg(test)]
pub(crate) fn rebuild_fts_via_rusqlite(db_path: &Path) -> Result<usize> {
    let storage = FrankenStorage::open(db_path)
        .with_context(|| format!("opening sqlite db at {} for FTS rebuild", db_path.display()))?;
    let inserted = storage.rebuild_fts_via_frankensqlite()?;
    storage.record_fts_franken_rebuild_generation()?;
    Ok(inserted)
}

pub(crate) fn ensure_fts_consistency_via_rusqlite(db_path: &Path) -> Result<FtsConsistencyRepair> {
    // Delegates to the FrankenStorage-native path; retains the historical
    // function name for test-site compatibility.
    let storage = FrankenStorage::open(db_path).with_context(|| {
        format!(
            "opening sqlite db at {} for FTS consistency check",
            db_path.display()
        )
    })?;
    storage.ensure_search_fallback_fts_consistency()
}

/// Create a uniquely named backup of the database file.
///
/// Returns the path to the backup file, or None if the source doesn't exist.
pub fn create_backup(db_path: &Path) -> Result<Option<std::path::PathBuf>, MigrationError> {
    if !bundle_path_exists(db_path)? {
        return Ok(None);
    }

    if !copyable_bundle_file_exists(db_path)? {
        return Ok(None);
    }
    let _ = copyable_bundle_sidecar_sources(db_path)?;

    let backup_path = unique_backup_path(db_path);
    let vacuum_stage_path = vacuum_stage_backup_path(&backup_path);

    // Try to use SQLite's VACUUM INTO command first, which safely handles WAL files
    // and produces a clean, minimized backup.
    match vacuum_into_backup_stage(db_path, &vacuum_stage_path) {
        Ok(()) => {
            fs::rename(&vacuum_stage_path, &backup_path)?;
        }
        Err(err) if backup_vacuum_error_requires_consistent_retry(&err) => {
            tracing::warn!(
                db_path = %db_path.display(),
                error = %err,
                "create_backup: VACUUM INTO hit transient contention; refusing raw WAL bundle copy"
            );
            return Err(MigrationError::Database(err));
        }
        Err(err) => {
            tracing::warn!(
                db_path = %db_path.display(),
                error = %err,
                "create_backup: VACUUM INTO failed; falling back to raw evidence copy"
            );
        }
    }

    if backup_path.exists() {
        sync_file_if_exists(&backup_path)?;
        if let Some(parent) = backup_path.parent() {
            sync_parent_directory(parent)?;
        }
        return Ok(Some(backup_path));
    }

    // Fallback to a raw evidence copy if VACUUM INTO failed (e.g., older SQLite
    // or corruption). Keep this on the same symlink-safe bundle path as
    // historical seeding so a malformed archive root cannot make us copy an
    // arbitrary symlink target or publish a partial sidecar backup.
    copy_database_bundle(db_path, &backup_path)?;

    Ok(Some(backup_path))
}

fn vacuum_into_backup_stage(
    db_path: &Path,
    stage_path: &Path,
) -> std::result::Result<(), crate::franken_sync::FrankenError> {
    let mut conn =
        FrankenConnection::open_schema_only_with_wal_index_recovery(&db_path.to_string_lossy())?;
    let result = (|| {
        conn.execute(BACKUP_VACUUM_BUSY_TIMEOUT_PRAGMA)?;
        let path_str = stage_path.to_string_lossy();
        conn.execute_compat("VACUUM INTO ?", fparams![path_str.as_ref()])?;
        Ok(())
    })();
    if let Err(close_err) = conn.close_in_place() {
        tracing::warn!(
            error = %close_err,
            db_path = %db_path.display(),
            "create_backup: close_in_place failed after VACUUM INTO; falling back to best-effort close"
        );
        conn.close_best_effort_in_place();
    }
    result
}

pub(super) fn backup_vacuum_error_requires_consistent_retry(
    err: &crate::franken_sync::FrankenError,
) -> bool {
    retryable_franken_error(err) || matches!(err, crate::franken_sync::FrankenError::BusyRecovery)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DatabaseBundleMoveResult {
    pub database: bool,
    pub wal: bool,
    pub shm: bool,
}

impl DatabaseBundleMoveResult {
    pub fn moved_any(&self) -> bool {
        self.database || self.wal || self.shm
    }
}

pub(crate) fn database_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{}", path.to_string_lossy(), suffix))
}

/// Move a database file and its WAL/SHM sidecars to a new basename.
///
/// This is used for non-destructive quarantine of a corrupted bundle before a
/// rebuild. If the main database file is already missing but orphaned sidecars
/// remain, those sidecars are still moved so a fresh database can be created
/// without inheriting stale WAL state.
pub(crate) fn move_database_bundle(
    source_root: &Path,
    destination_root: &Path,
) -> std::io::Result<DatabaseBundleMoveResult> {
    let mut moved = DatabaseBundleMoveResult::default();
    if let Some(parent) = destination_root.parent() {
        fs::create_dir_all(parent)?;
        sync_parent_directory(parent)?;
    }

    if bundle_path_exists(source_root)? {
        fs::rename(source_root, destination_root)?;
        moved.database = true;
    }

    let wal_source = database_sidecar_path(source_root, "-wal");
    if bundle_path_exists(&wal_source)? {
        fs::rename(&wal_source, database_sidecar_path(destination_root, "-wal"))?;
        moved.wal = true;
    }

    let shm_source = database_sidecar_path(source_root, "-shm");
    if bundle_path_exists(&shm_source)? {
        fs::rename(&shm_source, database_sidecar_path(destination_root, "-shm"))?;
        moved.shm = true;
    }

    // Legacy WAL certification sidecars travel with the WAL they certify.
    // Leaving one behind (or pairing a destination's stale cert with a
    // freshly moved WAL) is incoherent.
    for cert_suffix in ["-wal-cert", "-wal-cert-head"] {
        let cert_source = database_sidecar_path(source_root, cert_suffix);
        if bundle_path_exists(&cert_source)? {
            fs::rename(
                &cert_source,
                database_sidecar_path(destination_root, cert_suffix),
            )?;
        }
    }

    if moved.moved_any() {
        if let Some(parent) = source_root.parent() {
            sync_parent_directory(parent)?;
        }
        if let Some(parent) = destination_root.parent() {
            sync_parent_directory(parent)?;
        }
    }

    Ok(moved)
}

fn bundle_path_exists(path: &Path) -> std::io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub(super) fn copy_database_bundle(source_root: &Path, destination_root: &Path) -> Result<()> {
    if let Some(parent) = destination_root.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "creating destination directory for database bundle copy: {}",
                parent.display()
            )
        })?;
        sync_parent_directory(parent)
            .with_context(|| format!("syncing destination directory {}", parent.display()))?;
    }

    if !copyable_bundle_file_exists(source_root)? {
        bail!(
            "database bundle root is missing before copy: {}",
            source_root.display()
        );
    }

    let sidecars = copyable_bundle_sidecar_sources(source_root)?;

    fs::copy(source_root, destination_root).with_context(|| {
        format!(
            "copying database bundle {} -> {}",
            source_root.display(),
            destination_root.display()
        )
    })?;
    sync_file_if_exists(destination_root).with_context(|| {
        format!(
            "syncing copied database bundle {}",
            destination_root.display()
        )
    })?;

    for (source_sidecar, suffix) in sidecars {
        let destination_sidecar = database_sidecar_path(destination_root, suffix);
        fs::copy(&source_sidecar, &destination_sidecar).with_context(|| {
            format!(
                "copying database bundle sidecar {} -> {}",
                source_sidecar.display(),
                destination_sidecar.display()
            )
        })?;
        sync_file_if_exists(&destination_sidecar).with_context(|| {
            format!(
                "syncing copied database bundle sidecar {}",
                destination_sidecar.display()
            )
        })?;
    }

    if let Some(parent) = destination_root.parent() {
        sync_parent_directory(parent)
            .with_context(|| format!("syncing destination directory {}", parent.display()))?;
    }

    Ok(())
}

fn copyable_bundle_sidecar_sources(source_root: &Path) -> Result<Vec<(PathBuf, &'static str)>> {
    let mut sidecars = Vec::new();
    for suffix in ["-wal", "-wal-cert", "-wal-cert-head", "-shm"] {
        let source_sidecar = database_sidecar_path(source_root, suffix);
        if copyable_bundle_file_exists(&source_sidecar)? {
            sidecars.push((source_sidecar, suffix));
        }
    }
    Ok(sidecars)
}

fn copyable_bundle_file_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                bail!(
                    "refusing to copy database bundle symlink: {}",
                    path.display()
                );
            }
            if !file_type.is_file() {
                bail!(
                    "refusing to copy non-file database bundle path: {}",
                    path.display()
                );
            }
            Ok(true)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err).with_context(|| {
            format!(
                "checking database bundle path before copy: {}",
                path.display()
            )
        }),
    }
}

/// Helper to safely remove a database file and its potential WAL/SHM sidecars.
pub(crate) fn remove_database_files(path: &Path) -> std::io::Result<()> {
    let mut removed_any = false;

    match fs::remove_file(path) {
        Ok(()) => removed_any = true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }

    // Best-effort removal of sidecar files (ignore errors if they don't exist)
    for suffix in ["-wal", "-shm", "-journal"] {
        match fs::remove_file(database_sidecar_path(path, suffix)) {
            Ok(()) => removed_any = true,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }

    if removed_any && let Some(parent) = path.parent() {
        sync_parent_directory(parent)?;
    }

    Ok(())
}

#[cfg(not(windows))]
fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
    fs::File::open(path)?.sync_all()
}

#[cfg(windows)]
fn sync_parent_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(windows))]
fn sync_file_if_exists(path: &Path) -> std::io::Result<()> {
    if path.exists() {
        fs::File::open(path)?.sync_all()?;
    }
    Ok(())
}

#[cfg(windows)]
fn sync_file_if_exists(path: &Path) -> std::io::Result<()> {
    if path.exists() {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?
            .sync_all()?;
    }
    Ok(())
}

/// Remove old backup files, keeping only the most recent `keep_count`.
pub fn cleanup_old_backups(db_path: &Path, keep_count: usize) -> Result<(), std::io::Error> {
    let parent = match db_path.parent() {
        Some(p) => p,
        None => return Ok(()),
    };

    let db_name = db_path.file_name().and_then(|n| n.to_str()).unwrap_or("db");

    let prefix = format!("{}.backup.", db_name);

    // Collect backup files matching the pattern
    let mut backups: Vec<(std::path::PathBuf, SystemTime)> = Vec::new();

    if let Ok(entries) = fs::read_dir(parent) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && is_backup_root_name(name, &prefix)
                && let Ok(meta) = fs::metadata(&path)
                && meta.is_file()
                && let Ok(mtime) = meta.modified()
            {
                backups.push((path, mtime));
            }
        }
    }

    // Sort by modification time, newest first
    backups.sort_by_key(|entry| std::cmp::Reverse(entry.1));

    // Delete oldest backups beyond keep_count
    for (path, _) in backups.into_iter().skip(keep_count) {
        let _ = fs::remove_file(&path);

        // Also try to cleanup potential sidecars from fs::copy fallback
        let _ = fs::remove_file(database_sidecar_path(&path, "-wal"));
        let _ = fs::remove_file(database_sidecar_path(&path, "-shm"));
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct HistoricalDatabaseBundle {
    pub(super) root_path: PathBuf,
    pub(super) total_bytes: u64,
    pub(super) modified_at_ms: i64,
    pub(super) supports_direct_readonly: bool,
    pub(super) probe: HistoricalBundleProbe,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct HistoricalBundleProbe {
    pub(super) schema_version: Option<i64>,
    pub(super) fts_schema_rows: Option<i64>,
    pub(super) fts_queryable: bool,
    pub(super) max_message_id: i64,
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SqliteDatabaseHealthProbe {
    pub schema_version: Option<i64>,
    pub quick_check_ok: bool,
    pub fts_schema_rows: i64,
    pub fts_queryable: bool,
    pub message_count: i64,
    pub max_message_id: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FtsConsistencyRepair {
    AlreadyHealthy {
        rows: usize,
    },
    IncrementalCatchUp {
        inserted_rows: usize,
        total_rows: usize,
    },
    Rebuilt {
        inserted_rows: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FtsShadowParityStatus {
    Absent,
    /// The virtual-table registration is absent while one or more of its
    /// required shadow tables remain, or the registered virtual table uses a
    /// legacy/noncanonical DDL.
    Residue,
    Healthy,
    Partial,
    Excess,
    Divergent,
    Unqueryable,
}

impl FtsShadowParityStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Residue => "residue",
            Self::Healthy => "healthy",
            Self::Partial => "partial",
            Self::Excess => "excess",
            Self::Divergent => "divergent",
            Self::Unqueryable => "unqueryable",
        }
    }
}

/// Exact parity between canonical, indexable messages and the contentless FTS
/// shadow. `indexable_messages` excludes orphaned message rows because the FTS
/// projection intentionally inner-joins `conversations`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FtsShadowParity {
    pub(crate) status: FtsShadowParityStatus,
    pub(crate) canonical_messages: i64,
    pub(crate) indexable_messages: i64,
    pub(crate) indexed_messages: Option<i64>,
    pub(crate) detail: Option<String>,
}

/// Bounded, read-only parity evidence for the canonical FTS doctor dry-run.
///
/// `exact_status` is `None` when either row-ID domain exceeds the comparison
/// cap. Callers must surface that state as `indeterminate`: matching prefixes
/// are not proof that the complete domains match. The mutating doctor path
/// deliberately continues to use [`FtsShadowParity`] and its exact scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FtsDryRunParity {
    pub(crate) exact_status: Option<FtsShadowParityStatus>,
    pub(crate) canonical_messages: i64,
    pub(crate) indexable_messages: i64,
    pub(crate) indexed_messages: Option<i64>,
    pub(crate) inspection_complete: bool,
    pub(crate) comparison_cap: usize,
    pub(crate) canonical_ids_examined: usize,
    pub(crate) indexed_ids_examined: usize,
    pub(crate) observed_missing_canonical_rowids_at_least: usize,
    pub(crate) observed_excess_fts_rowids_at_least: usize,
    pub(crate) detail: Option<String>,
}

impl FtsDryRunParity {
    pub(crate) const fn status_as_str(&self) -> &'static str {
        match self.exact_status {
            Some(status) => status.as_str(),
            None => "indeterminate",
        }
    }

    /// #345: the divergence FLOOR observed within the comparison cap — missing
    /// canonical rowids plus excess FTS rowids. On a capped (indeterminate)
    /// inspection this is a ">= N divergent" lower bound, never an exact
    /// count; on a complete inspection it is exact.
    pub(crate) const fn divergent_rowids_at_least(&self) -> usize {
        self.observed_missing_canonical_rowids_at_least
            .saturating_add(self.observed_excess_fts_rowids_at_least)
    }
}

pub(super) fn classify_fts_shadow_parity(
    indexable_messages: i64,
    indexed_messages: i64,
    intersection_messages: i64,
    missing_messages: i64,
    excess_messages: i64,
) -> (FtsShadowParityStatus, Option<String>) {
    match indexed_messages.cmp(&indexable_messages) {
        std::cmp::Ordering::Less => {
            if excess_messages > 0 {
                (
                    FtsShadowParityStatus::Divergent,
                    Some(format!(
                        "FTS contains {excess_messages} non-canonical rowids while {missing_messages} canonical rowids are missing (intersection={intersection_messages})"
                    )),
                )
            } else {
                (FtsShadowParityStatus::Partial, None)
            }
        }
        std::cmp::Ordering::Equal => {
            if missing_messages > 0 || excess_messages > 0 {
                (
                    FtsShadowParityStatus::Divergent,
                    Some(format!(
                        "equal counts conceal rowid divergence (missing_canonical_rowids={missing_messages}, excess_fts_rowids={excess_messages}, intersection={intersection_messages})"
                    )),
                )
            } else {
                (FtsShadowParityStatus::Healthy, None)
            }
        }
        std::cmp::Ordering::Greater => {
            if missing_messages > 0 {
                (
                    FtsShadowParityStatus::Divergent,
                    Some(format!(
                        "FTS has {excess_messages} excess non-canonical rowids while {missing_messages} canonical rowids are missing (intersection={intersection_messages})"
                    )),
                )
            } else {
                (FtsShadowParityStatus::Excess, None)
            }
        }
    }
}

pub(super) fn unique_backup_path(path: &Path) -> PathBuf {
    static NEXT_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let nonce = NEXT_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("db");

    path.with_file_name(format!(
        "{file_name}.backup.{}.{}.{}",
        std::process::id(),
        timestamp,
        nonce
    ))
}

pub(super) fn vacuum_stage_backup_path(backup_path: &Path) -> PathBuf {
    let file_name = backup_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("db.backup");
    backup_path.with_file_name(format!(".{file_name}.vacuum-in-progress"))
}

pub(super) fn is_backup_root_name(name: &str, prefix: &str) -> bool {
    name.starts_with(prefix) && !name.ends_with("-wal") && !name.ends_with("-shm")
}

// Suffixes that mark sqlite sidecar files we must never re-open as a DB root.
// Includes the standard -wal/-shm pair plus legacy Windows advisory-lock and
// namespace/WAL-cert/migration-state sidecars. Used by directory enumeration
// paths in `historical_bundle_root_paths`; deliberately NOT used by
// `is_backup_root_name`, because the existing backup-rotation cleanup must
// continue to sweep up any pre-existing orphan lock sidecars.
fn has_db_sidecar_suffix(name: &str) -> bool {
    const SIDECAR_SUFFIXES: &[&str] = &[
        "-wal",
        "-shm",
        "-lock-shared",
        "-lock-reserved",
        "-lock-pending",
        // Legacy VFS namespace sidecars.
        "-fsqlite-ns-gate",
        "-fsqlite-ns-use",
        // Legacy WAL certification sidecars.
        "-wal-cert",
        "-wal-cert-head",
        // Legacy migration-state markers.
        ".fsqlite-migration-state",
        ".fsqlite-migration-state.tmp",
    ];
    SIDECAR_SUFFIXES.iter().any(|suffix| name.ends_with(suffix))
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
        // `historical_bundle_fts_queryable`), so we have two legitimate
        // "clean" shapes for a bundle:
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
        return HistoricalBundleProbe::default();
    };

    let schema_version = read_meta_schema_version(&conn).ok().flatten();
    let fts_schema_rows: Option<i64> = conn
        .query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
            fparams![],
            |row| row.get_typed(0),
        )
        .ok();
    let fts_queryable = historical_bundle_fts_queryable(root_path, fts_schema_rows);
    let max_message_id: i64 = conn
        .query_row_map(
            "SELECT COALESCE(MAX(id), 0) FROM messages",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap_or(0);

    HistoricalBundleProbe {
        schema_version,
        fts_schema_rows,
        fts_queryable,
        max_message_id,
    }
}

fn historical_bundle_fts_queryable(root_path: &Path, fts_schema_rows: Option<i64>) -> bool {
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

pub(super) fn historical_bundle_has_queryable_core_tables(conn: &FrankenConnection) -> Result<()> {
    probe_historical_table_reads(conn, "conversations")?;
    probe_historical_table_reads(conn, "messages")?;
    Ok(())
}

pub(super) fn open_historical_bundle_readonly(root_path: &Path) -> Result<FrankenConnection> {
    let path_str = root_path.to_string_lossy();
    let conn = FrankenConnection::open_schema_only_with_wal_index_recovery(&*path_str)
        .with_context(|| format!("opening historical database {}", root_path.display()))?;
    Ok(conn)
}

pub(super) fn read_meta_schema_version(conn: &FrankenConnection) -> Result<Option<i64>> {
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
    .context("counting sqlite_master rows for fts_messages")
}

#[cfg(test)]
fn franken_fts_limit_probe(conn: &FrankenConnection) -> bool {
    conn.query("SELECT COUNT(*) FROM fts_messages").is_ok()
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn probe_database_health(db_path: &Path) -> Result<SqliteDatabaseHealthProbe> {
    let path_str = db_path.to_string_lossy();
    let conn = FrankenConnection::open(path_str.as_ref()).with_context(|| {
        format!(
            "opening sqlite db at {} for database health probe",
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
        .context("counting messages during sqlite database health probe")?;
    let max_message_id: i64 = conn
        .query_row_map(
            "SELECT COALESCE(MAX(id), 0) FROM messages",
            fparams![],
            |row| row.get_typed(0),
        )
        .context("reading max message id during sqlite database health probe")?;

    Ok(SqliteDatabaseHealthProbe {
        schema_version,
        quick_check_ok,
        fts_schema_rows,
        fts_queryable,
        message_count,
        max_message_id,
    })
}
