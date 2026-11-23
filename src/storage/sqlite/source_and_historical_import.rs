use super::aggregators_and_pricing::IndexingCache;
use super::backup_and_integrity::HistoricalDatabaseBundle;
use super::batch_ingest::{normalized_source_for_conversation, normalized_storage_source_parts};
use super::historical_salvage::{
    HISTORICAL_SALVAGE_LEDGER_VERSION, HISTORICAL_SALVAGE_PROGRESS_VERSION, HistoricalBatchEntry,
    HistoricalBatchImportTotals, HistoricalBundleProgress, HistoricalSalvageOutcome,
    discover_historical_database_bundles, historical_import_batch_limits,
    historical_salvage_debug_enabled, message_payload_size_hint,
    open_historical_bundle_for_salvage, parse_historical_json_column, parse_json_column,
    record_historical_bundle_import,
};
use super::storage_core::FrankenStorage;
use crate::franken_sync::{
    Connection as FrankenConnection,
    compat::{
        ConnectionExt as FrankenConnectionExt, OptionalExtension as FrankenOptionalExtension,
        ParamValue, RowExt as FrankenRowExt,
    },
};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::sources::provenance::{LOCAL_SOURCE_ID, Source, SourceKind};
use anyhow::{Context, Result, anyhow, bail};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tracing::info;

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

    fn historical_bundle_key_hash(
        version: u32,
        bundle: &HistoricalDatabaseBundle,
        include_bundle_stats: bool,
    ) -> String {
        let signature = if include_bundle_stats {
            format!(
                "{}:{}:{}:{}",
                version,
                bundle.root_path.display(),
                bundle.total_bytes,
                bundle.modified_at_ms
            )
        } else {
            format!("{}:{}", version, bundle.root_path.display())
        };
        blake3::hash(signature.as_bytes()).to_hex().to_string()
    }

    pub(super) fn historical_bundle_meta_key(bundle: &HistoricalDatabaseBundle) -> String {
        format!(
            "historical_bundle_salvaged:{}",
            Self::historical_bundle_key_hash(HISTORICAL_SALVAGE_LEDGER_VERSION, bundle, false)
        )
    }

    fn historical_bundle_legacy_meta_key(bundle: &HistoricalDatabaseBundle) -> String {
        let signature = format!(
            "{}:{}:{}:{}",
            HISTORICAL_SALVAGE_LEDGER_VERSION,
            bundle.root_path.display(),
            bundle.total_bytes,
            bundle.modified_at_ms
        );
        format!(
            "historical_bundle_salvaged:{}",
            blake3::hash(signature.as_bytes()).to_hex()
        )
    }

    pub(super) fn historical_bundle_progress_key(bundle: &HistoricalDatabaseBundle) -> String {
        format!(
            "historical_bundle_progress:{}",
            Self::historical_bundle_key_hash(HISTORICAL_SALVAGE_PROGRESS_VERSION, bundle, false)
        )
    }

    fn historical_bundle_legacy_progress_key(bundle: &HistoricalDatabaseBundle) -> String {
        let signature = format!(
            "{}:{}:{}:{}",
            HISTORICAL_SALVAGE_PROGRESS_VERSION,
            bundle.root_path.display(),
            bundle.total_bytes,
            bundle.modified_at_ms
        );
        format!(
            "historical_bundle_progress:{}",
            blake3::hash(signature.as_bytes()).to_hex()
        )
    }

    pub(super) fn historical_bundle_already_imported(
        &self,
        bundle: &HistoricalDatabaseBundle,
    ) -> Result<bool> {
        for key in [
            Self::historical_bundle_meta_key(bundle),
            Self::historical_bundle_legacy_meta_key(bundle),
        ] {
            let existing: Option<String> = self
                .conn
                .query_row_map(
                    "SELECT value FROM meta WHERE key = ?1",
                    fparams![key.as_str()],
                    |row| row.get_typed(0),
                )
                .optional()?;
            if existing.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn has_pending_historical_bundles(&self, canonical_db_path: &Path) -> Result<bool> {
        for bundle in discover_historical_database_bundles(canonical_db_path) {
            if !self.historical_bundle_already_imported(&bundle)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn load_historical_bundle_progress(
        &self,
        bundle: &HistoricalDatabaseBundle,
    ) -> Result<Option<HistoricalBundleProgress>> {
        for key in [
            Self::historical_bundle_progress_key(bundle),
            Self::historical_bundle_legacy_progress_key(bundle),
        ] {
            let raw: Option<String> = self
                .conn
                .query_row_map(
                    "SELECT value FROM meta WHERE key = ?1",
                    fparams![key.as_str()],
                    |row| row.get_typed(0),
                )
                .optional()?;
            let Some(raw) = raw else {
                continue;
            };
            let parsed: HistoricalBundleProgress =
                serde_json::from_str(&raw).with_context(|| {
                    format!(
                        "parsing historical salvage progress checkpoint for {}",
                        bundle.root_path.display()
                    )
                })?;
            if parsed.progress_version == HISTORICAL_SALVAGE_PROGRESS_VERSION {
                return Ok(Some(parsed));
            }
        }
        Ok(None)
    }

    pub(super) fn record_historical_bundle_progress(
        &self,
        bundle: &HistoricalDatabaseBundle,
        method: &str,
        last_completed_source_row_id: i64,
        conversations_imported: usize,
        messages_imported: usize,
    ) -> Result<()> {
        let key = Self::historical_bundle_progress_key(bundle);
        let value = HistoricalBundleProgress {
            progress_version: HISTORICAL_SALVAGE_PROGRESS_VERSION,
            path: bundle.root_path.display().to_string(),
            bytes: bundle.total_bytes,
            modified_at_ms: bundle.modified_at_ms,
            method: method.to_string(),
            last_completed_source_row_id,
            conversations_imported,
            messages_imported,
            updated_at_ms: Self::now_millis(),
        };
        let value_str = serde_json::to_string(&value)?;
        self.conn.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
            fparams![key.as_str(), value_str.as_str()],
        )?;
        Ok(())
    }

    fn clear_historical_bundle_progress(&self, bundle: &HistoricalDatabaseBundle) -> Result<()> {
        for key in [
            Self::historical_bundle_progress_key(bundle),
            Self::historical_bundle_legacy_progress_key(bundle),
        ] {
            self.conn
                .execute_compat("DELETE FROM meta WHERE key = ?1", fparams![key.as_str()])?;
        }
        Ok(())
    }

    pub(super) fn record_historical_bundle_import(
        &self,
        bundle: &HistoricalDatabaseBundle,
        method: &str,
        conversations_imported: usize,
        messages_imported: usize,
    ) -> Result<()> {
        let key = Self::historical_bundle_meta_key(bundle);
        let value = serde_json::json!({
            "salvage_version": HISTORICAL_SALVAGE_LEDGER_VERSION,
            "path": bundle.root_path.display().to_string(),
            "bytes": bundle.total_bytes,
            "modified_at_ms": bundle.modified_at_ms,
            "method": method,
            "conversations_imported": conversations_imported,
            "messages_imported": messages_imported,
            "recorded_at_ms": Self::now_millis(),
        });
        let value_str = serde_json::to_string(&value)?;
        self.conn.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
            fparams![key.as_str(), value_str.as_str()],
        )?;
        Ok(())
    }

    fn historical_import_error_is_split_retryable(err: &anyhow::Error) -> bool {
        const RETRYABLE_PATTERNS: &[&str] = &[
            "out of memory",
            "string or blob too big",
            "too many sql variables",
        ];
        err.chain().any(|cause| {
            let rendered = cause.to_string().to_ascii_lowercase();
            RETRYABLE_PATTERNS
                .iter()
                .any(|pattern| rendered.contains(pattern))
        })
    }

    fn split_historical_batch_entry_messages(
        entry: &HistoricalBatchEntry,
    ) -> Option<(HistoricalBatchEntry, HistoricalBatchEntry)> {
        if entry.conversation.messages.len() < 2 {
            return None;
        }
        let split_at = entry.conversation.messages.len() / 2;
        if split_at == 0 || split_at >= entry.conversation.messages.len() {
            return None;
        }

        let mut left = entry.clone();
        left.conversation.messages = entry.conversation.messages[..split_at].to_vec();

        let mut right = entry.clone();
        right.conversation.messages = entry.conversation.messages[split_at..].to_vec();

        Some((left, right))
    }

    pub(super) fn import_historical_batch_with_retry<F>(
        entries: &[HistoricalBatchEntry],
        insert_batch: &mut F,
    ) -> Result<HistoricalBatchImportTotals>
    where
        F: FnMut(&[HistoricalBatchEntry]) -> Result<HistoricalBatchImportTotals>,
    {
        match insert_batch(entries) {
            Ok(totals) => Ok(totals),
            Err(err) if Self::historical_import_error_is_split_retryable(&err) => {
                if entries.len() > 1 {
                    let mid = entries.len() / 2;
                    tracing::warn!(
                        batch_entries = entries.len(),
                        split_left = mid,
                        split_right = entries.len() - mid,
                        error = %err,
                        "historical salvage batch failed; retrying in smaller sub-batches"
                    );
                    let left =
                        Self::import_historical_batch_with_retry(&entries[..mid], insert_batch)?;
                    let right =
                        Self::import_historical_batch_with_retry(&entries[mid..], insert_batch)?;
                    return Ok(HistoricalBatchImportTotals {
                        inserted_source_rows: left.inserted_source_rows
                            + right.inserted_source_rows,
                        inserted_messages: left.inserted_messages + right.inserted_messages,
                    });
                }

                if let Some(entry) = entries.first()
                    && let Some((left, right)) = Self::split_historical_batch_entry_messages(entry)
                {
                    tracing::warn!(
                        source_row_id = entry.source_row_id,
                        message_count = entry.conversation.messages.len(),
                        error = %err,
                        "historical salvage conversation failed; retrying in smaller message slices"
                    );
                    let left_totals = Self::import_historical_batch_with_retry(
                        std::slice::from_ref(&left),
                        insert_batch,
                    )?;
                    let right_totals = Self::import_historical_batch_with_retry(
                        std::slice::from_ref(&right),
                        insert_batch,
                    )?;
                    return Ok(HistoricalBatchImportTotals {
                        inserted_source_rows: usize::from(
                            left_totals.inserted_source_rows > 0
                                || right_totals.inserted_source_rows > 0,
                        ),
                        inserted_messages: left_totals
                            .inserted_messages
                            .saturating_add(right_totals.inserted_messages),
                    });
                }

                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    fn import_historical_sources(&self, source_conn: &FrankenConnection) -> Result<()> {
        let sources: Vec<Source> = match source_conn.query_map_collect(
            "SELECT id, kind, host_label, machine_id, platform, config_json, created_at, updated_at
             FROM sources",
            fparams![],
            |row| {
                let raw_source_id: String = row.get_typed(0)?;
                let kind_str: String = row.get_typed(1)?;
                let raw_host_label: Option<String> = row.get_typed(2)?;
                let config_json_raw: Option<String> = row.get_typed(5)?;
                let (source_id, source_kind, host_label) = normalized_storage_source_parts(
                    Some(raw_source_id.as_str()),
                    Some(kind_str.as_str()),
                    raw_host_label.as_deref(),
                );
                Ok(Source {
                    id: source_id,
                    kind: source_kind,
                    host_label,
                    machine_id: row.get_typed(3)?,
                    platform: row.get_typed(4)?,
                    config_json: config_json_raw.and_then(|raw| serde_json::from_str(&raw).ok()),
                    created_at: row.get_typed(6)?,
                    updated_at: row.get_typed(7)?,
                })
            },
        ) {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(error = %err, "historical sources table unavailable; skipping source import");
                return Ok(());
            }
        };

        for source in sources {
            self.upsert_source(&source)?;
        }
        Ok(())
    }

    fn import_historical_conversations(
        &self,
        bundle: &HistoricalDatabaseBundle,
        salvage_method: &str,
        source_conn: &FrankenConnection,
    ) -> Result<(usize, usize)> {
        let batch_limits = historical_import_batch_limits();
        let cache_enabled = IndexingCache::is_enabled();
        let mut indexing_cache = IndexingCache::new();
        let mut known_sources: HashSet<String> = self
            .list_sources()?
            .into_iter()
            .map(|source| source.id)
            .collect();
        let resume_progress = self.load_historical_bundle_progress(bundle)?;
        let resume_after_row_id = resume_progress
            .as_ref()
            .map(|progress| progress.last_completed_source_row_id)
            .filter(|row_id| *row_id > 0);

        tracing::info!(
            target: "cass::historical_salvage",
            batch_conversations = batch_limits.conversations,
            batch_messages = batch_limits.messages,
            batch_payload_chars = batch_limits.payload_chars,
            cache_enabled,
            resume_after_row_id,
            "configured historical salvage batch limits"
        );

        if let Some(progress) = &resume_progress {
            tracing::info!(
                target: "cass::historical_salvage",
                path = %bundle.root_path.display(),
                resume_after_row_id = progress.last_completed_source_row_id,
                prior_conversations_imported = progress.conversations_imported,
                prior_messages_imported = progress.messages_imported,
                "resuming historical salvage bundle from durable checkpoint"
            );
        }

        // LEFT JOIN + COALESCE on agents so legacy source databases with NULL
        // agent_id (the V1 schema did not require NOT NULL) still have their
        // conversations imported, degrading to 'unknown' slug like the other
        // rebuild paths.  Using INNER JOIN here would silently drop those
        // conversations during historical salvage, which is data loss.
        let conv_sql = if resume_after_row_id.is_some() {
            "SELECT
                c.id,
                COALESCE(a.slug, 'unknown'),
                w.path,
                c.external_id,
                c.title,
                c.source_path,
                c.started_at,
                c.ended_at,
                c.approx_tokens,
                c.metadata_json,
                c.source_id,
                c.origin_host
             FROM conversations c
             LEFT JOIN agents a ON c.agent_id = a.id
             LEFT JOIN workspaces w ON c.workspace_id = w.id
             WHERE c.id > ?1
             ORDER BY c.id"
        } else {
            "SELECT
                c.id,
                COALESCE(a.slug, 'unknown'),
                w.path,
                c.external_id,
                c.title,
                c.source_path,
                c.started_at,
                c.ended_at,
                c.approx_tokens,
                c.metadata_json,
                c.source_id,
                c.origin_host
             FROM conversations c
             LEFT JOIN agents a ON c.agent_id = a.id
             LEFT JOIN workspaces w ON c.workspace_id = w.id
             ORDER BY c.id"
        };
        let conv_params: &[ParamValue] =
            if let Some(last_completed_source_row_id) = resume_after_row_id {
                &[ParamValue::from(last_completed_source_row_id)]
            } else {
                &[]
            };

        #[allow(clippy::type_complexity)]
        let conv_rows: Vec<(
            i64,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<String>,
            Option<String>,
            Option<String>,
        )> = source_conn
            .query_map_collect(conv_sql, conv_params, |row| {
                Ok((
                    row.get_typed::<i64>(0)?,
                    row.get_typed::<String>(1)?,
                    row.get_typed::<Option<String>>(2)?,
                    row.get_typed::<Option<String>>(3)?,
                    row.get_typed::<Option<String>>(4)?,
                    row.get_typed::<String>(5)?,
                    row.get_typed::<Option<i64>>(6)?,
                    row.get_typed::<Option<i64>>(7)?,
                    row.get_typed::<Option<i64>>(8)?,
                    row.get_typed::<Option<String>>(9)?,
                    row.get_typed::<Option<String>>(10)?,
                    row.get_typed::<Option<String>>(11)?,
                ))
            })
            .context("querying historical conversations")?;

        let msg_sql = "SELECT idx, role, author, created_at, content, extra_json
             FROM messages
             WHERE conversation_id = ?1
             ORDER BY idx";

        let mut imported_conversations = resume_progress
            .as_ref()
            .map(|progress| progress.conversations_imported)
            .unwrap_or(0);
        let mut imported_messages = resume_progress
            .as_ref()
            .map(|progress| progress.messages_imported)
            .unwrap_or(0);
        let mut pending_batch: Vec<HistoricalBatchEntry> = Vec::new();
        let mut pending_batch_messages = 0usize;
        let mut pending_batch_chars = 0usize;
        let mut pending_batch_first_row_id: Option<i64> = None;
        let mut pending_batch_last_row_id: Option<i64> = None;

        let flush_batch = |storage: &FrankenStorage,
                           batch: &mut Vec<HistoricalBatchEntry>,
                           pending_messages: &mut usize,
                           pending_chars: &mut usize,
                           first_row_id: &mut Option<i64>,
                           last_row_id: &mut Option<i64>,
                           imported_conversations: &mut usize,
                           imported_messages: &mut usize|
         -> Result<()> {
            if batch.is_empty() {
                return Ok(());
            }

            let batch_first_row_id = *first_row_id;
            let batch_last_row_id = *last_row_id;
            if historical_salvage_debug_enabled() {
                eprintln!(
                    "[historical-salvage] flushing batch rows {:?}..{:?} conversations={} messages={} payload_chars={}",
                    batch_first_row_id,
                    batch_last_row_id,
                    batch.len(),
                    *pending_messages,
                    *pending_chars
                );
            }
            tracing::info!(
                target: "cass::historical_salvage",
                batch_conversations = batch.len(),
                batch_messages = *pending_messages,
                batch_payload_chars = *pending_chars,
                first_source_row_id = batch_first_row_id,
                last_source_row_id = batch_last_row_id,
                "flushing historical salvage batch"
            );

            let mut insert_batch =
                |entries: &[HistoricalBatchEntry]| -> Result<HistoricalBatchImportTotals> {
                    let borrowed_batch: Vec<(i64, Option<i64>, &Conversation)> = entries
                        .iter()
                        .map(|entry| (entry.agent_id, entry.workspace_id, &entry.conversation))
                        .collect();
                    let outcomes = storage
                        .insert_conversations_batched(&borrowed_batch)
                        .with_context(|| {
                            let first_source_row_id =
                                entries.first().map(|entry| entry.source_row_id);
                            let last_source_row_id =
                                entries.last().map(|entry| entry.source_row_id);
                            format!(
                                "inserting historical salvage batch source rows {:?}..{:?}",
                                first_source_row_id, last_source_row_id
                            )
                        })?;
                    let mut totals = HistoricalBatchImportTotals::default();
                    for outcome in outcomes {
                        if !outcome.inserted_indices.is_empty() {
                            totals.inserted_source_rows += 1;
                            totals.inserted_messages += outcome.inserted_indices.len();
                        }
                    }
                    Ok(totals)
                };
            let totals =
                Self::import_historical_batch_with_retry(batch.as_slice(), &mut insert_batch)?;
            *imported_conversations =
                (*imported_conversations).saturating_add(totals.inserted_source_rows);
            *imported_messages = (*imported_messages).saturating_add(totals.inserted_messages);
            if let Some(last_completed_row_id) = batch_last_row_id {
                storage.record_historical_bundle_progress(
                    bundle,
                    salvage_method,
                    last_completed_row_id,
                    *imported_conversations,
                    *imported_messages,
                )?;
            }
            tracing::info!(
                target: "cass::historical_salvage",
                batch_conversations = batch.len(),
                batch_messages = *pending_messages,
                imported_conversations = *imported_conversations,
                imported_messages = *imported_messages,
                first_source_row_id = batch_first_row_id,
                last_source_row_id = batch_last_row_id,
                "historical salvage batch committed"
            );
            if historical_salvage_debug_enabled() {
                eprintln!(
                    "[historical-salvage] committed batch rows {:?}..{:?} imported_conversations={} imported_messages={}",
                    batch_first_row_id,
                    batch_last_row_id,
                    *imported_conversations,
                    *imported_messages
                );
            }
            batch.clear();
            *pending_messages = 0;
            *pending_chars = 0;
            *first_row_id = None;
            *last_row_id = None;
            Ok(())
        };

        for (
            conversation_row_id,
            agent_slug,
            workspace_path,
            external_id,
            title,
            source_path,
            started_at,
            ended_at,
            approx_tokens,
            metadata_json_raw,
            raw_source_id,
            raw_origin_host,
        ) in conv_rows
        {
            let source_id = crate::search::tantivy::normalized_index_source_id(
                raw_source_id.as_deref(),
                None,
                raw_origin_host.as_deref(),
            );
            let origin_host =
                crate::search::tantivy::normalized_index_origin_host(raw_origin_host.as_deref());

            let messages: Vec<Message> = source_conn
                .query_map_collect(msg_sql, fparams![conversation_row_id], |msg_row| {
                    let role: String = msg_row.get_typed(1)?;
                    Ok(Message {
                        id: None,
                        idx: msg_row.get_typed(0)?,
                        role: match role.as_str() {
                            "user" => MessageRole::User,
                            "agent" | "assistant" => MessageRole::Agent,
                            "tool" => MessageRole::Tool,
                            "system" => MessageRole::System,
                            other => MessageRole::Other(other.to_string()),
                        },
                        author: msg_row.get_typed(2)?,
                        created_at: msg_row.get_typed(3)?,
                        content: msg_row.get_typed(4)?,
                        extra_json: parse_historical_json_column(msg_row.get_typed(5)?),
                        snippets: Vec::new(),
                    })
                })
                .context("collecting historical message rows")?;

            if messages.is_empty() {
                continue;
            }

            let conversation_message_count = messages.len();
            let conversation_chars = messages
                .iter()
                .map(message_payload_size_hint)
                .sum::<usize>();

            let conversation = Conversation {
                id: None,
                agent_slug: agent_slug.clone(),
                workspace: workspace_path.map(PathBuf::from),
                external_id,
                title,
                source_path: PathBuf::from(source_path),
                started_at,
                ended_at,
                approx_tokens,
                metadata_json: parse_json_column(metadata_json_raw),
                messages,
                source_id,
                origin_host,
            };

            if !known_sources.contains(&conversation.source_id) {
                let placeholder = normalized_source_for_conversation(&conversation);
                self.upsert_source(&placeholder)?;
                known_sources.insert(conversation.source_id.clone());
            }

            let agent = Agent {
                id: None,
                slug: agent_slug.clone(),
                name: agent_slug,
                version: None,
                kind: AgentKind::Cli,
            };
            let agent_id = if cache_enabled {
                indexing_cache.get_or_insert_agent(self, &agent)?
            } else {
                self.ensure_agent(&agent)?
            };
            let workspace_id = if let Some(workspace) = &conversation.workspace {
                if cache_enabled {
                    Some(indexing_cache.get_or_insert_workspace(self, workspace, None)?)
                } else {
                    Some(self.ensure_workspace(workspace, None)?)
                }
            } else {
                None
            };

            let exceeds_pending_limits = !pending_batch.is_empty()
                && (pending_batch.len() >= batch_limits.conversations
                    || pending_batch_messages.saturating_add(conversation_message_count)
                        > batch_limits.messages
                    || pending_batch_chars.saturating_add(conversation_chars)
                        > batch_limits.payload_chars);
            if exceeds_pending_limits {
                flush_batch(
                    self,
                    &mut pending_batch,
                    &mut pending_batch_messages,
                    &mut pending_batch_chars,
                    &mut pending_batch_first_row_id,
                    &mut pending_batch_last_row_id,
                    &mut imported_conversations,
                    &mut imported_messages,
                )?;
            }

            if pending_batch_first_row_id.is_none() {
                pending_batch_first_row_id = Some(conversation_row_id);
            }
            pending_batch_last_row_id = Some(conversation_row_id);
            pending_batch_messages =
                pending_batch_messages.saturating_add(conversation_message_count);
            pending_batch_chars = pending_batch_chars.saturating_add(conversation_chars);
            pending_batch.push(HistoricalBatchEntry {
                source_row_id: conversation_row_id,
                agent_id,
                workspace_id,
                conversation,
            });

            if pending_batch.len() >= batch_limits.conversations
                || pending_batch_messages >= batch_limits.messages
                || pending_batch_chars >= batch_limits.payload_chars
            {
                flush_batch(
                    self,
                    &mut pending_batch,
                    &mut pending_batch_messages,
                    &mut pending_batch_chars,
                    &mut pending_batch_first_row_id,
                    &mut pending_batch_last_row_id,
                    &mut imported_conversations,
                    &mut imported_messages,
                )?;
            }
        }

        flush_batch(
            self,
            &mut pending_batch,
            &mut pending_batch_messages,
            &mut pending_batch_chars,
            &mut pending_batch_first_row_id,
            &mut pending_batch_last_row_id,
            &mut imported_conversations,
            &mut imported_messages,
        )?;

        if cache_enabled {
            let (hits, misses, hit_rate) = indexing_cache.stats();
            tracing::info!(
                target: "cass::historical_salvage",
                hits,
                misses,
                hit_rate = format!("{:.1}%", hit_rate * 100.0),
                agents = indexing_cache.agent_count(),
                workspaces = indexing_cache.workspace_count(),
                sources = known_sources.len(),
                "historical salvage cache stats"
            );
        }

        Ok((imported_conversations, imported_messages))
    }

    pub fn salvage_historical_databases(
        &self,
        canonical_db_path: &Path,
    ) -> Result<HistoricalSalvageOutcome> {
        let ordered_bundles = discover_historical_database_bundles(canonical_db_path);
        let mut outcome = HistoricalSalvageOutcome {
            bundles_considered: ordered_bundles.len(),
            ..HistoricalSalvageOutcome::default()
        };

        for bundle in ordered_bundles {
            if self.historical_bundle_already_imported(&bundle)? {
                self.clear_historical_bundle_progress(&bundle)?;
                continue;
            }

            let source = match open_historical_bundle_for_salvage(&bundle).with_context(|| {
                format!(
                    "opening historical bundle {} for salvage",
                    bundle.root_path.display()
                )
            }) {
                Ok(source) => source,
                Err(err) => {
                    tracing::warn!(
                        path = %bundle.root_path.display(),
                        error = %err,
                        "skipping unreadable historical cass database bundle during salvage"
                    );
                    self.clear_historical_bundle_progress(&bundle)?;
                    continue;
                }
            };

            // #247 (coding_agent_session_search-r8pcy): if a per-bundle progress
            // checkpoint already covers the backup's entire conversation row-id
            // space, the bundle was effectively fully imported but the daemon was
            // killed (e.g. OOM) before the completion ledger marker landed.
            // Re-scanning it is a pure O(n) no-op — every batch commits
            // imported=0 while taking 5-12 min. Detect it via the high-water
            // checkpoint, write the ledger marker, drop the checkpoint, and skip.
            if let Some(progress) = self.load_historical_bundle_progress(&bundle)? {
                let backup_max_conversation_id: i64 = source
                    .conn
                    .query_row_map(
                        "SELECT COALESCE(MAX(id), 0) FROM conversations",
                        fparams![],
                        |row| row.get_typed(0),
                    )
                    .unwrap_or(0);
                if backup_max_conversation_id > 0
                    && progress.last_completed_source_row_id >= backup_max_conversation_id
                {
                    self.record_historical_bundle_import(
                        &bundle,
                        source.method,
                        progress.conversations_imported,
                        progress.messages_imported,
                    )?;
                    self.clear_historical_bundle_progress(&bundle)?;
                    tracing::info!(
                        path = %bundle.root_path.display(),
                        last_completed_source_row_id = progress.last_completed_source_row_id,
                        backup_max_conversation_id,
                        conversations_imported = progress.conversations_imported,
                        messages_imported = progress.messages_imported,
                        "historical bundle already fully imported per checkpoint; marking salvaged and skipping O(n) re-scan"
                    );
                    continue;
                }
            }

            self.import_historical_sources(&source.conn)?;
            let (imported_conversations, imported_messages) =
                self.import_historical_conversations(&bundle, source.method, &source.conn)?;
            self.record_historical_bundle_import(
                &bundle,
                source.method,
                imported_conversations,
                imported_messages,
            )?;
            self.clear_historical_bundle_progress(&bundle)?;

            outcome.bundles_imported += 1;
            outcome.conversations_imported += imported_conversations;
            outcome.messages_imported += imported_messages;

            tracing::info!(
                path = %bundle.root_path.display(),
                bytes = bundle.total_bytes,
                method = source.method,
                imported_conversations,
                imported_messages,
                "salvaged historical cass database bundle"
            );
        }

        Ok(outcome)
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
