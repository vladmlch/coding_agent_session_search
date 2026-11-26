use super::storage_core::FrankenStorage;
use crate::franken_sync::{
    Connection as FrankenConnection,
    compat::{
        ConnectionExt as FrankenConnectionExt, OptionalExtension as FrankenOptionalExtension,
        RowExt as FrankenRowExt,
    },
};
#[cfg(test)]
use crate::model::types::Conversation;
use crate::sources::provenance::{LOCAL_SOURCE_ID, Source, SourceKind};
use anyhow::{Context, Result};
use std::path::Path;

impl FrankenStorage {
    /// Get a source by ID.
    pub fn get_source(&self, id: &str) -> Result<Option<Source>> {
        let result = self.conn.query_row_map(
            "SELECT id, kind, host_label, machine_id, platform, config_json, created_at, updated_at FROM sources WHERE id = ?1",
            fparams![id],
            |row| {
                let kind_str: String = row.get_typed(1)?;
                let config_json_str: Option<String> = row.get_typed(5)?;
                Ok(Source {
                    id: row.get_typed(0)?,
                    kind: SourceKind::parse(&kind_str).unwrap_or_default(),
                    host_label: row.get_typed(2)?,
                    machine_id: row.get_typed(3)?,
                    platform: row.get_typed(4)?,
                    config_json: config_json_str.and_then(|s| serde_json::from_str(&s).ok()),
                    created_at: row.get_typed(6)?,
                    updated_at: row.get_typed(7)?,
                })
            },
        );
        Ok(result.optional()?)
    }

    /// List all sources.
    pub fn list_sources(&self) -> Result<Vec<Source>> {
        self.conn
            .query_map_collect(
                "SELECT id, kind, host_label, machine_id, platform, config_json, created_at, updated_at FROM sources ORDER BY id",
                fparams![],
                |row| {
                    let kind_str: String = row.get_typed(1)?;
                    let config_json_str: Option<String> = row.get_typed(5)?;
                    Ok(Source {
                        id: row.get_typed(0)?,
                        kind: SourceKind::parse(&kind_str).unwrap_or_default(),
                        host_label: row.get_typed(2)?,
                        machine_id: row.get_typed(3)?,
                        platform: row.get_typed(4)?,
                        config_json: config_json_str.and_then(|s| serde_json::from_str(&s).ok()),
                        created_at: row.get_typed(6)?,
                        updated_at: row.get_typed(7)?,
                    })
                },
            )
            .with_context(|| "listing sources")
    }

    /// Get IDs of all non-local sources.
    pub fn get_source_ids(&self) -> Result<Vec<String>> {
        self.conn
            .query_map_collect(
                "SELECT id
                 FROM sources
                 WHERE CASE
                           WHEN LOWER(TRIM(COALESCE(kind, ''))) = 'local' THEN 0
                           WHEN TRIM(COALESCE(kind, '')) != '' THEN 1
                           WHEN LOWER(TRIM(COALESCE(id, ''))) = 'local' THEN 0
                           ELSE 1
                       END = 1
                 ORDER BY id",
                fparams![],
                |row| row.get_typed(0),
            )
            .with_context(|| "listing source ids")
    }

    /// Create or update a source.
    pub fn upsert_source(&self, source: &Source) -> Result<()> {
        self.invalidate_conversation_source_cache(source.id.as_str());
        let now = Self::now_millis();
        let kind_str = source.kind.to_string();
        let config_json_str = source
            .config_json
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;

        // Re-indexing commonly reuses the same normalized source metadata
        // across many conversations. Skip the write entirely when the row is
        // already identical so we avoid needless WAL churn and timestamp bumps.
        self.conn.execute_compat(
            "INSERT INTO sources(id, kind, host_label, machine_id, platform, config_json, created_at, updated_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(id) DO UPDATE SET
                 kind = excluded.kind,
                 host_label = excluded.host_label,
                 machine_id = excluded.machine_id,
                 platform = excluded.platform,
                 config_json = excluded.config_json,
                 updated_at = excluded.updated_at
             WHERE NOT (
                 sources.kind IS excluded.kind
                 AND sources.host_label IS excluded.host_label
                 AND sources.machine_id IS excluded.machine_id
                 AND sources.platform IS excluded.platform
                 AND sources.config_json IS excluded.config_json
             )",
            fparams![
                source.id.as_str(),
                kind_str.as_str(),
                source.host_label.as_deref(),
                source.machine_id.as_deref(),
                source.platform.as_deref(),
                config_json_str.as_deref(),
                source.created_at.unwrap_or(now),
                now
            ],
        )?;
        Ok(())
    }

    /// Whether any historical backup bundles still await salvage.
    ///
    /// Task C3 retired the legacy-bundle salvage lane (it existed to import
    /// archives stamped with the pre-v1 legacy schema, which the current
    /// open contract refuses rather than imports). The API is kept so
    /// indexer call sites compile; it reports "nothing pending".
    pub(crate) fn has_pending_historical_bundles(&self, canonical_db_path: &Path) -> Result<bool> {
        let _ = canonical_db_path;
        Ok(false)
    }

    /// Salvage pass over historical backup bundles.
    ///
    /// No-op under the schema-v1 contract: legacy archives are refused by
    /// [`super::open::SchemaRebuildRequired`] and are never copied into the
    /// canonical archive. Returns an empty [`HistoricalSalvageOutcome`] so the
    /// indexer keeps its progress-accounting shape.
    pub fn salvage_historical_databases(
        &self,
        canonical_db_path: &Path,
    ) -> Result<HistoricalSalvageOutcome> {
        let _ = canonical_db_path;
        Ok(HistoricalSalvageOutcome::default())
    }

    /// Delete a source by ID. Returns true if a row was deleted.
    pub fn delete_source(&self, id: &str, _cascade: bool) -> Result<bool> {
        if id == LOCAL_SOURCE_ID {
            anyhow::bail!("cannot delete the local source");
        }
        let count = self
            .conn
            .execute_compat("DELETE FROM sources WHERE id = ?1", fparams![id])?;
        if count > 0 {
            self.invalidate_conversation_source_cache(id);
        }
        Ok(count > 0)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HistoricalSalvageOutcome {
    pub bundles_considered: usize,
    pub bundles_imported: usize,
    pub conversations_imported: usize,
    pub messages_imported: usize,
}

impl HistoricalSalvageOutcome {
    #[allow(dead_code)]
    pub(crate) fn accumulate(&mut self, other: Self) {
        self.bundles_considered += other.bundles_considered;
        self.bundles_imported += other.bundles_imported;
        self.conversations_imported += other.conversations_imported;
        self.messages_imported += other.messages_imported;
    }
}

#[cfg(test)]
#[allow(dead_code)]
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
pub(super) const SOURCE_PATH_MERGE_START_TOLERANCE_MS: i64 = 5 * 60 * 1000;

#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(super) struct HistoricalBatchEntry {
    pub(super) source_row_id: i64,
    pub(super) agent_id: i64,
    pub(super) workspace_id: Option<i64>,
    pub(super) conversation: Conversation,
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct HistoricalBatchImportTotals {
    pub(super) inserted_source_rows: usize,
    pub(super) inserted_messages: usize,
}

/// Check whether `table` exists in the connection's schema via
/// `PRAGMA table_info`. Callers pass internal table names; views also answer,
/// which none of them name.
pub(super) fn historical_table_exists(conn: &FrankenConnection, table: &str) -> Result<bool> {
    let quoted = table.replace('"', "\"\"");
    let columns = conn
        .query(&format!("PRAGMA table_info(\"{quoted}\")"))
        .with_context(|| format!("checking for historical table {table}"))?;
    Ok(!columns.is_empty())
}

/// Historical-bundle baseline seeding was a pre-v1 legacy migration
/// path: it copied a legacy archive into the canonical location and stamped
/// the old `_schema_migrations` ledger. Under the schema-v1 contract a
/// legacy archive is refused by [`super::open::archive_disposition`] and the
/// only remedy is a full re-index, so seeding is a no-op that reports "no
/// seed". The signature is preserved for `indexer/mod.rs` (Task C3).
pub(crate) fn seed_canonical_from_best_historical_bundle(
    canonical_db_path: &Path,
) -> Result<Option<HistoricalSalvageOutcome>> {
    let _ = canonical_db_path;
    Ok(None)
}

const HISTORICAL_RAW_JSON_SENTINEL_KEY: &str = "__cass_historical_raw_json__";

#[cfg(test)]
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
/// [`super::backup_and_integrity::franken_read_message_extra_compat`] (msgpack
/// `extra_bin` takes precedence over the `extra_json` text column). When the
/// envelope is the historical raw-line wrapper, the verbatim original line is
/// returned unchanged; when it is a decoded event object, it is re-serialized
/// as compact JSON; a `null` envelope (no preserved event) yields `None`.
pub(crate) fn message_extra_to_source_jsonl_line(extra_json: &serde_json::Value) -> Option<String> {
    if let Some(raw) = historical_raw_json(extra_json) {
        return Some(raw.to_string());
    }
    if extra_json.is_null() {
        return None;
    }
    serde_json::to_string(extra_json).ok()
}

#[cfg(test)]
pub(super) fn parse_historical_json_column(value: Option<String>) -> serde_json::Value {
    match value {
        Some(raw) if raw.trim().is_empty() => serde_json::Value::Null,
        Some(raw) => wrap_historical_raw_json(raw),
        None => serde_json::Value::Null,
    }
}

#[cfg(test)]
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
