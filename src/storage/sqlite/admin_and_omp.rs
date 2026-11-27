use super::analytics_rebuild::{AnalyticsRebuildResult, LEGACY_OMP_ANALYTICS_CHUNK_SIZE};
use super::backup_and_integrity::{
    ConnectorScanState, connector_scan_contract, franken_read_message_extra_compat,
};
use super::batch_ingest::normalized_storage_source_parts;
use super::forget_and_dedup::AgentArchivePurgeResult;
use super::fts_rebuild::path_to_string;
use super::reconcile_and_codebuff::{
    clear_semantic_embed_watermark_after_deletion, codebuff_native_message_id,
    codebuff_snippet_rows_match, provider_workspace_is_authoritative,
    replay_owns_provider_workspace, role_from_str,
};
use super::omp_and_semantic_meta::{
    LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY, LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
    LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY, LEGACY_OMP_ANALYTICS_REBUILT_META_KEY,
    LEGACY_OMP_LEXICAL_PUBLISHED_META_KEY, LEGACY_OMP_RECLASSIFICATION_META_KEY,
    LegacyOmpReclassificationResult, PREVIOUS_LEGACY_OMP_RECLASSIFICATION_META_KEY,
    SemanticIdentityTier,
};
use super::storage_core::{EnsuredAgentKey, EnsuredWorkspaceKey, FrankenStorage};
use crate::franken_sync::{
    SqliteValue, TransactionBehavior,
    compat::{
        ConnectionExt as FrankenConnectionExt, OptionalExtension as FrankenOptionalExtension,
        RowExt as FrankenRowExt,
    },
};
use crate::model::types::{Agent, AgentKind, Conversation};
use crate::sources::provenance::SourceKind;
use crate::storage::omp_paths::{PiFamilyOwner, PiFamilyOwnership};
use anyhow::{Context, Result, anyhow, bail};
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

// =========================================================================
// FrankenStorage CRUD operations
// =========================================================================

impl FrankenStorage {
    /// Compare the normalized, redacted packet before the indexer admits a
    /// canonical write. Revisions need a durable lexical repair checkpoint:
    /// they leave both message count and the rowid watermark unchanged.
    pub(crate) fn codebuff_message_revisions_needed(&self, conv: &Conversation) -> Result<bool> {
        if conv.agent_slug != "codebuff" {
            return Ok(false);
        }
        let external_id = conv
            .external_id
            .as_deref()
            .filter(|id| !id.is_empty())
            .context("Codebuff / Freebuff transcript requires its store-scoped identity")?;
        let existing: Option<i64> = self
            .conn
            .query_row_map(
                "SELECT id FROM conversations WHERE source_id = ?1 AND external_id = ?2
             AND agent_id = (SELECT id FROM agents WHERE slug = 'codebuff')",
                fparams![conv.source_id.as_str(), external_id],
                |row| row.get_typed(0),
            )
            .optional()?;
        let mut native = HashMap::new();
        if let Some(conversation_id) = existing {
            for row in self.conn.query_with_params(
                "SELECT id, role, author, created_at, content, extra_json, extra_bin
                 FROM messages WHERE conversation_id = ?1",
                &[SqliteValue::from(conversation_id)],
            )? {
                let extra = franken_read_message_extra_compat(&row, 5, 6);
                let native_id = codebuff_native_message_id(&extra)?.to_owned();
                anyhow::ensure!(
                    native.insert(native_id, (row, extra)).is_none(),
                    "Codebuff / Freebuff canonical transcript has duplicate native IDs"
                );
            }
        }
        let mut packet_ids = HashSet::new();
        let mut changed = false;
        for message in &conv.messages {
            let native_id = codebuff_native_message_id(&message.extra_json)?;
            anyhow::ensure!(
                packet_ids.insert(native_id),
                "Codebuff / Freebuff packet has duplicate native IDs"
            );
            let Some((row, extra)) = native.get(native_id) else {
                continue;
            };
            anyhow::ensure!(
                role_from_str(&row.get_typed::<String>(1)?) == message.role
                    && row.get_typed::<Option<String>>(2)? == message.author
                    && row.get_typed::<Option<i64>>(3)? == message.created_at,
                "Codebuff / Freebuff native ID changed immutable role, author or timestamp"
            );
            let snippets = self.conn.query_with_params(
                "SELECT file_path, start_line, end_line, language, snippet_text
                 FROM snippets WHERE message_id = ?1 ORDER BY id",
                &[SqliteValue::from(row.get_typed::<i64>(0)?)],
            )?;
            changed |= row.get_typed::<String>(4)? != message.content
                || extra != &message.extra_json
                || !codebuff_snippet_rows_match(&snippets, &message.snippets)?;
        }
        Ok(changed)
    }

    /// Read-only admission for the indexer's durable pre-mutation checkpoint.
    /// Cursor Agent and Codebuff external IDs are stable across workspace
    /// attribution changes.
    pub(crate) fn provider_workspace_repair_needed(
        &self,
        agent_slug: &str,
        source_id: &str,
        external_id: Option<&str>,
        source_path: &Path,
        workspace: Option<&Path>,
        metadata: &serde_json::Value,
    ) -> Result<bool> {
        if !provider_workspace_is_authoritative(agent_slug, workspace, metadata) {
            return Ok(false);
        }
        let Some(external_id) = external_id else {
            return Ok(false);
        };
        let existing: Option<(Option<String>, Option<String>)> = self
            .conn
            .query_row_map(
                "SELECT (SELECT w.path FROM workspaces w WHERE w.id = c.workspace_id), c.source_path
             FROM conversations c WHERE c.source_id = ?1 AND c.external_id = ?2
             AND c.agent_id = (SELECT id FROM agents WHERE slug = ?3)",
                fparams![source_id, external_id, agent_slug],
                |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
            )
            .optional()?;
        Ok(existing.is_some_and(|(current, canonical_source)| {
            replay_owns_provider_workspace(agent_slug, canonical_source.as_deref(), source_path)
                && current.as_deref() != workspace.map(path_to_string).as_deref()
        }))
    }

    /// Ensure an agent exists in the database, returning its ID.
    pub fn ensure_agent(&self, agent: &Agent) -> Result<i64> {
        let cache_key = EnsuredAgentKey::from_agent(agent);
        if let Some(id) = self.cached_agent_id(&cache_key) {
            return Ok(id);
        }

        let now = Self::now_millis();
        self.conn.execute_compat(
            "INSERT INTO agents(slug, name, version, kind, created_at, updated_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(slug) DO UPDATE SET
                 name = excluded.name,
                 version = excluded.version,
                 kind = excluded.kind,
                 updated_at = excluded.updated_at
             WHERE NOT (
                 agents.name IS excluded.name
                 AND agents.version IS excluded.version
                 AND agents.kind IS excluded.kind
             )",
            fparams![
                agent.slug.as_str(),
                agent.name.as_str(),
                agent.version.as_deref(),
                cache_key.kind.as_str(),
                now,
                now
            ],
        )?;

        let id = self
            .conn
            .query_row_map(
                "SELECT id FROM agents WHERE slug = ?1 LIMIT 1",
                fparams![agent.slug.as_str()],
                |row| row.get_typed(0),
            )
            .with_context(|| format!("fetching agent id for {}", agent.slug))?;
        self.mark_agent_ensured(cache_key, id);
        Ok(id)
    }

    /// Ensure a workspace exists in the database, returning its ID.
    pub fn ensure_workspace(&self, path: &Path, display_name: Option<&str>) -> Result<i64> {
        let path_str = path.to_string_lossy().to_string();
        let cache_key = EnsuredWorkspaceKey::new(path_str.clone(), display_name);
        if let Some(id) = self.cached_workspace_id(&cache_key) {
            return Ok(id);
        }

        if let Some(display_name) = display_name {
            self.conn.execute_compat(
                "INSERT INTO workspaces(path, display_name)
                 VALUES(?1, ?2)
                 ON CONFLICT(path) DO UPDATE SET
                     display_name = excluded.display_name
                 WHERE NOT (workspaces.display_name IS excluded.display_name)",
                fparams![path_str.as_str(), display_name],
            )?;
        } else {
            self.conn.execute_compat(
                "INSERT OR IGNORE INTO workspaces(path, display_name) VALUES(?1, NULL)",
                fparams![path_str.as_str()],
            )?;
        }

        let id = self
            .conn
            .query_row_map(
                "SELECT id FROM workspaces WHERE path = ?1 LIMIT 1",
                fparams![path_str.as_str()],
                |row| row.get_typed(0),
            )
            .with_context(|| format!("fetching workspace id for {path_str}"))?;
        self.mark_workspace_ensured(cache_key, id);
        Ok(id)
    }

    /// Get current time as milliseconds since epoch.
    pub fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }

    /// Convert a millisecond timestamp to a day ID (days since 2020-01-01).
    pub fn day_id_from_millis(timestamp_ms: i64) -> i64 {
        const EPOCH_2020_SECS: i64 = 1_577_836_800;
        let secs = timestamp_ms.div_euclid(1000);
        (secs - EPOCH_2020_SECS).div_euclid(86400)
    }

    /// Convert a millisecond timestamp to an hour ID (hours since 2020-01-01 00:00 UTC).
    pub fn hour_id_from_millis(timestamp_ms: i64) -> i64 {
        const EPOCH_2020_SECS: i64 = 1_577_836_800;
        let secs = timestamp_ms.div_euclid(1000);
        (secs - EPOCH_2020_SECS).div_euclid(3600)
    }

    /// Convert a day ID back to milliseconds (start of day).
    pub fn millis_from_day_id(day_id: i64) -> i64 {
        const EPOCH_2020_SECS: i64 = 1_577_836_800;
        (EPOCH_2020_SECS + day_id * 86400) * 1000
    }

    /// Convert an hour ID back to milliseconds (start of hour).
    pub fn millis_from_hour_id(hour_id: i64) -> i64 {
        const EPOCH_2020_SECS: i64 = 1_577_836_800;
        (EPOCH_2020_SECS + hour_id * 3600) * 1000
    }

    /// Get the timestamp of the last successful scan.
    pub fn get_last_scan_ts(&self) -> Result<Option<i64>> {
        let result: Result<String, _> = self.conn.query_row_map(
            "SELECT value FROM meta WHERE key = 'last_scan_ts'",
            fparams![],
            |row| row.get_typed(0),
        );
        match result.optional() {
            Ok(Some(s)) => Ok(s.parse().ok()),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Set the timestamp of the last successful scan (milliseconds since epoch).
    pub fn set_last_scan_ts(&self, ts: i64) -> Result<()> {
        self.conn.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('last_scan_ts', ?1)",
            fparams![ts.to_string()],
        )?;
        Ok(())
    }

    pub(super) fn connector_last_scan_ts_meta_key(connector_name: &str) -> String {
        format!(
            "last_scan_ts:connector:{}",
            connector_name.trim().to_ascii_lowercase()
        )
    }

    pub(super) fn connector_agent_slug_candidates(connector_name: &str) -> SmallVec<[String; 3]> {
        let normalized = connector_name.trim().to_ascii_lowercase();
        let mut candidates = SmallVec::<[String; 3]>::new();
        if normalized.is_empty() {
            return candidates;
        }

        candidates.push(normalized.clone());
        match normalized.as_str() {
            "claude" | "claude-code" | "claude_code" => {
                candidates.push("claude_code".to_string());
                candidates.push("claude-code".to_string());
            }
            _ => {}
        }
        candidates.sort();
        candidates.dedup();
        candidates
    }

    /// Get the timestamp of the last successful scan for a specific connector.
    pub fn get_connector_last_scan_ts(&self, connector_name: &str) -> Result<Option<i64>> {
        let key = Self::connector_last_scan_ts_meta_key(connector_name);
        let result: Result<String, _> = self.conn.query_row_map(
            "SELECT value FROM meta WHERE key = ?1",
            fparams![key.as_str()],
            |row| row.get_typed(0),
        );
        match result.optional() {
            Ok(Some(s)) => Ok(s.parse().ok()),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Set the timestamp of the last successful scan for a specific connector.
    /// The scan succeeded, so it also records the connector's scan contract.
    pub fn set_connector_last_scan_ts(&self, connector_name: &str, ts: i64) -> Result<()> {
        let key = Self::connector_last_scan_ts_meta_key(connector_name);
        match connector_scan_contract(connector_name) {
            Some(contract) => {
                let contract_key = Self::connector_scan_contract_meta_key(connector_name);
                self.conn.execute_compat(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2), (?3, ?4)",
                    fparams![
                        key.as_str(),
                        ts.to_string(),
                        contract_key.as_str(),
                        contract
                    ],
                )?;
            }
            None => {
                self.conn.execute_compat(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                    fparams![key.as_str(), ts.to_string()],
                )?;
            }
        }
        Ok(())
    }

    fn connector_scan_contract_meta_key(connector_name: &str) -> String {
        format!(
            "scan_contract:connector:{}",
            connector_name.trim().to_ascii_lowercase()
        )
    }

    /// Record that a scan of this connector under its current scan contract
    /// succeeded, for a scan whose watermark was withheld (path exclusions, a
    /// skipped active source): those hold back the watermark, not what the
    /// scan read. No-op for a connector without a contract.
    pub fn record_connector_scan_contract(&self, connector_name: &str) -> Result<()> {
        if let Some(contract) = connector_scan_contract(connector_name) {
            let key = Self::connector_scan_contract_meta_key(connector_name);
            self.conn.execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![key.as_str(), contract],
            )?;
        }
        Ok(())
    }

    fn mirror_scan_fingerprint_meta_key(source_id: &str) -> String {
        format!("mirror_scan_fp:{}", source_id.trim())
    }

    /// #372: read the file fingerprint recorded at the last error-free scan of a
    /// remote mirror source, used to skip re-scanning an unchanged mirror. A
    /// missing key returns `None` (never seen / never cleanly scanned).
    pub fn read_mirror_scan_fingerprint(&self, source_id: &str) -> Result<Option<String>> {
        let key = Self::mirror_scan_fingerprint_meta_key(source_id);
        let result: Result<String, _> = self.conn.query_row_map(
            "SELECT value FROM meta WHERE key = ?1",
            fparams![key.as_str()],
            |row| row.get_typed(0),
        );
        Ok(result.optional()?)
    }

    /// #372: record the current file fingerprint of a remote mirror after an
    /// error-free scan, so the next incremental run can skip it if unchanged.
    pub fn record_mirror_scan_fingerprint(&self, source_id: &str, fingerprint: &str) -> Result<()> {
        let key = Self::mirror_scan_fingerprint_meta_key(source_id);
        self.conn.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
            fparams![key.as_str(), fingerprint],
        )?;
        Ok(())
    }

    /// Load per-connector scan watermarks and archived-row presence in one
    /// consistent read-only transaction snapshot.
    ///
    /// Keeping both queries in one explicit deferred transaction avoids
    /// observing mid-scan drift across tables while preserving the same
    /// newly-enabled-connector semantics as the scalar helpers below.
    pub fn connector_scan_states(
        &self,
        connector_names: &[&str],
    ) -> Result<HashMap<String, ConnectorScanState>> {
        let requested = connector_names
            .iter()
            .map(|name| name.trim().to_ascii_lowercase())
            .filter(|name| !name.is_empty())
            .collect::<HashSet<_>>();
        let mut states = requested
            .iter()
            .map(|name| {
                let state = ConnectorScanState {
                    scan_contract_pending: connector_scan_contract(name).is_some(),
                    ..ConnectorScanState::default()
                };
                (name.clone(), state)
            })
            .collect::<HashMap<_, _>>();
        if states.is_empty() {
            return Ok(states);
        }

        let mut tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let watermark_rows: Vec<(String, String)> = tx.query_map_collect(
            "SELECT key, value FROM meta
             WHERE key LIKE 'last_scan_ts:connector:%' OR key LIKE 'scan_contract:connector:%'",
            fparams![],
            |row| {
                let key: String = row.get_typed(0)?;
                let value: String = row.get_typed(1)?;
                Ok((key, value))
            },
        )?;

        for (key, value) in watermark_rows {
            if let Some(connector_name) = key.strip_prefix("last_scan_ts:connector:") {
                if let Some(state) =
                    states.get_mut(connector_name.trim().to_ascii_lowercase().as_str())
                {
                    state.last_scan_ts = value.parse().ok();
                }
            } else if let Some(connector_name) = key.strip_prefix("scan_contract:connector:")
                && let Some(state) =
                    states.get_mut(connector_name.trim().to_ascii_lowercase().as_str())
                && connector_scan_contract(connector_name) == Some(value.as_str())
            {
                state.scan_contract_pending = false;
            }
        }

        let archived_agent_slugs = tx
            .query_map_collect(
                "SELECT DISTINCT a.slug
                 FROM agents a
                 JOIN conversations c ON c.agent_id = a.id",
                fparams![],
                |row| row.get_typed::<String>(0),
            )?
            .into_iter()
            .map(|slug| slug.trim().to_ascii_lowercase())
            .collect::<HashSet<_>>();

        for connector_name in requested {
            if Self::connector_agent_slug_candidates(&connector_name)
                .iter()
                .any(|slug| archived_agent_slugs.contains(slug))
                && let Some(state) = states.get_mut(connector_name.as_str())
            {
                state.has_conversations = true;
            }
        }

        tx.rollback()?;
        Ok(states)
    }

    /// Whether this connector already has archived conversations.
    pub fn connector_has_conversations(&self, connector_name: &str) -> Result<bool> {
        let candidate_slugs = Self::connector_agent_slug_candidates(connector_name);
        if candidate_slugs.is_empty() {
            return Ok(false);
        }

        for slug in candidate_slugs {
            let exists: i64 = self.conn.query_row_map(
                "SELECT EXISTS(
                     SELECT 1
                     FROM conversations c
                     JOIN agents a ON a.id = c.agent_id
                     WHERE a.slug = ?1
                     LIMIT 1
                 )",
                fparams![slug.as_str()],
                |row| row.get_typed(0),
            )?;
            if exists != 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Reclassify OMP sessions that pre-0.2 FAD releases indexed as `pi_agent`.
    ///
    /// Older detector releases scanned OMP config roots and configured remote
    /// mirrors through the Pi Agent connector. FAD 0.2 gives OMP a dedicated
    /// identity; this versioned upgrade snapshots the same conservative
    /// ownership policy as live discovery before connector watermarks are
    /// planned. Durable canonical/conventional-XDG/remote-mirror evidence and
    /// currently resolved provider-qualified roots therefore merge the first
    /// OMP scan into the existing row instead of creating a second copy under
    /// another agent id. Historical custom-XDG paths with no current provider
    /// evidence remain Pi-owned. Analytics are derived from canonical messages
    /// and rebuilt after the identity swap.
    pub fn reclassify_legacy_omp_conversations(&self) -> Result<LegacyOmpReclassificationResult> {
        self.reclassify_legacy_omp_conversations_inner(false)
    }

    /// Recheck for legacy OMP rows after a historical bundle import.
    ///
    /// A completed marker only describes the canonical archive as it existed
    /// when the marker was written. Historical salvage can subsequently add
    /// older Pi-labeled OMP rows, so the importer must bypass the normal fast
    /// path once after it has actually imported messages.
    pub(crate) fn reclassify_legacy_omp_conversations_after_historical_import(
        &self,
    ) -> Result<LegacyOmpReclassificationResult> {
        self.reclassify_legacy_omp_conversations_inner(true)
    }

    fn reclassify_legacy_omp_conversations_inner(
        &self,
        recheck_complete_archive: bool,
    ) -> Result<LegacyOmpReclassificationResult> {
        let ownership = PiFamilyOwnership::live();
        let ownership_context = ownership.archive_reclassification_context();
        let complete_state = format!("complete:{ownership_context}");
        let pending_state = format!("analytics_pending:{ownership_context}");
        let state: Option<String> = self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY],
                |row| row.get_typed(0),
            )
            .optional()?;
        if state.as_deref() == Some(complete_state.as_str()) && !recheck_complete_archive {
            return Ok(LegacyOmpReclassificationResult::default());
        }
        let previous_state: Option<String> = if state.is_none() {
            self.conn
                .query_row_map(
                    "SELECT value FROM meta WHERE key = ?1",
                    fparams![PREVIOUS_LEGACY_OMP_RECLASSIFICATION_META_KEY],
                    |row| row.get_typed(0),
                )
                .optional()?
        } else {
            None
        };
        let pending_state_matches_context = state.as_deref() == Some(pending_state.as_str());
        let assets_were_pending = state.as_deref().is_some_and(|value| {
            value == "analytics_pending" || value.starts_with("analytics_pending:")
        }) || previous_state.as_deref() == Some("analytics_pending");

        let legacy_agent_id: Option<i64> = self
            .conn
            .query_row_map(
                "SELECT id FROM agents WHERE slug = 'pi_agent'",
                fparams![],
                |row| row.get_typed(0),
            )
            .optional()?;
        struct LegacyOmpConversation {
            id: i64,
            source_path: String,
            external_id: Option<String>,
            normalized_source_id: String,
            source_kind: SourceKind,
            normalized_origin_host: Option<String>,
        }

        let legacy_conversations = if let Some(agent_id) = legacy_agent_id {
            self.conn
                .query_map_collect(
                    "SELECT id, source_path, source_id, origin_host, external_id
                     FROM conversations
                     WHERE agent_id = ?1",
                    fparams![agent_id],
                    |row| {
                        Ok((
                            row.get_typed::<i64>(0)?,
                            row.get_typed::<String>(1)?,
                            row.get_typed::<String>(2)?,
                            row.get_typed::<Option<String>>(3)?,
                            row.get_typed::<Option<String>>(4)?,
                        ))
                    },
                )?
                .into_iter()
                .filter_map(|(id, source_path, source_id, origin_host, external_id)| {
                    if ownership.owner(Path::new(&source_path)) != PiFamilyOwner::Omp {
                        return None;
                    }
                    let (normalized_source_id, source_kind, normalized_origin_host) =
                        normalized_storage_source_parts(
                            Some(source_id.as_str()),
                            None,
                            origin_host.as_deref(),
                        );
                    Some(LegacyOmpConversation {
                        id,
                        source_path,
                        external_id,
                        normalized_source_id,
                        source_kind,
                        normalized_origin_host,
                    })
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let legacy_count = legacy_conversations.len();

        if legacy_count > 0 {
            let omp_agent_id = self.ensure_agent(&Agent {
                id: None,
                slug: "omp".to_string(),
                name: "omp".to_string(),
                version: None,
                kind: AgentKind::Cli,
            })?;
            let legacy_agent_id = legacy_agent_id.ok_or_else(|| {
                anyhow!(
                    "legacy OMP reclassification selected {legacy_count} conversation(s), but the pi_agent row disappeared"
                )
            })?;
            let mut conflicting_rows = 0usize;
            let mut seen_legacy_paths = HashSet::new();
            let mut seen_legacy_external_ids = HashSet::new();
            for legacy in &legacy_conversations {
                let path_is_new = seen_legacy_paths.insert((
                    legacy.normalized_source_id.clone(),
                    legacy.source_path.clone(),
                ));
                let external_id_is_new = legacy.external_id.as_ref().is_none_or(|external_id| {
                    seen_legacy_external_ids
                        .insert((legacy.normalized_source_id.clone(), external_id.clone()))
                });
                let current_sources = self.conn.query_map_collect(
                    "SELECT source_id, origin_host
                     FROM conversations
                     WHERE agent_id = ?1
                       AND (
                            source_path = ?2
                            OR (?3 IS NOT NULL AND external_id = ?3)
                       )",
                    fparams![
                        omp_agent_id,
                        legacy.source_path.as_str(),
                        legacy.external_id.as_deref()
                    ],
                    |row| {
                        Ok((
                            row.get_typed::<String>(0)?,
                            row.get_typed::<Option<String>>(1)?,
                        ))
                    },
                )?;
                let conflict = current_sources.into_iter().any(|(source_id, origin_host)| {
                    normalized_storage_source_parts(
                        Some(source_id.as_str()),
                        None,
                        origin_host.as_deref(),
                    )
                    .0 == legacy.normalized_source_id
                });
                conflicting_rows += usize::from(conflict || !path_is_new || !external_id_is_new);
            }
            if conflicting_rows > 0 {
                return Err(anyhow!(
                    "cannot reclassify {conflicting_rows} legacy OMP conversation(s): matching normalized OMP identities already exist; run a source-path duplicate audit before indexing"
                ));
            }

            let mut metadata_updates = Vec::with_capacity(legacy_count);
            for legacy in &legacy_conversations {
                let (metadata_json, metadata_bin): (Option<String>, Option<Vec<u8>>) =
                    self.conn.query_row_map(
                        "SELECT metadata_json, metadata_bin
                         FROM conversations
                         WHERE id = ?1 AND agent_id = ?2",
                        fparams![legacy.id, legacy_agent_id],
                        |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
                    )?;
                let mut metadata = if let Some(bytes) =
                    metadata_bin.as_deref().filter(|bytes| !bytes.is_empty())
                {
                    rmp_serde::from_slice(bytes).with_context(|| {
                        format!(
                            "decoding MessagePack metadata for legacy OMP conversation {}",
                            legacy.id
                        )
                    })?
                } else if let Some(json) = metadata_json.as_deref() {
                    serde_json::from_str(json).with_context(|| {
                        format!(
                            "decoding JSON metadata for legacy OMP conversation {}",
                            legacy.id
                        )
                    })?
                } else {
                    serde_json::Value::Null
                };
                if metadata.is_null() {
                    metadata = serde_json::json!({});
                }
                let object = metadata.as_object_mut().ok_or_else(|| {
                    anyhow!(
                        "legacy OMP conversation {} metadata must be an object or null",
                        legacy.id
                    )
                })?;
                // Provider identity comes from the ownership proof above, not
                // from optional legacy metadata. Always make the canonical
                // metadata agree with the migrated agent row, including when
                // the old row stored NULL or omitted `source` entirely.
                object.insert("source".into(), serde_json::Value::String("omp".into()));
                // A named profile encoded in the transcript path is durable
                // provenance, including for remote mirrors. Legacy Pi-owned
                // rows predate first-class OMP profile tagging, and ordinary
                // append merges intentionally do not replace conversation
                // metadata, so recover this authoritative field during the
                // identity migration itself.
                if let Some(profile) = crate::storage::omp_paths::profile_from_session_path(
                    Path::new(&legacy.source_path),
                ) {
                    object.insert("profile".into(), serde_json::Value::String(profile));
                }
                metadata_updates.push((
                    legacy.id,
                    legacy.normalized_source_id.clone(),
                    legacy.normalized_origin_host.clone(),
                    serde_json::to_string(&metadata)?,
                ));
            }

            let mut tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY, pending_state.as_str()],
            )?;
            tx.execute_compat(
                "DELETE FROM meta WHERE key IN (?1, ?2, ?3, ?4, ?5)",
                fparams![
                    LEGACY_OMP_LEXICAL_PUBLISHED_META_KEY,
                    LEGACY_OMP_ANALYTICS_REBUILT_META_KEY,
                    LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY,
                    LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
                    LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY
                ],
            )?;
            // Semantic doc ids embed both the numeric agent id and a hash of
            // source_id. Reclassification changes those filter identities in
            // place without changing message rowids, so neither the semantic
            // watermark nor the count/max-id DB fingerprint notices. Keep a
            // durable fail-closed marker and remove the incremental watermark;
            // a successful semantic republish clears the marker later.
            for semantic_tier in [SemanticIdentityTier::Fast, SemanticIdentityTier::Quality] {
                tx.execute_compat(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                    fparams![semantic_tier.meta_key(), pending_state.as_str()],
                )?;
            }
            tx.execute("DELETE FROM meta WHERE key = 'last_embedded_message_id'")?;
            let now_ms = Self::now_millis();
            let mut normalized_sources = HashMap::new();
            for legacy in &legacy_conversations {
                normalized_sources
                    .entry(legacy.normalized_source_id.as_str())
                    .or_insert((legacy.source_kind, legacy.normalized_origin_host.as_deref()));
            }
            for (source_id, (source_kind, host_label)) in normalized_sources {
                tx.execute_compat(
                    "INSERT OR IGNORE INTO sources(
                         id, kind, host_label, created_at, updated_at
                     ) VALUES(?1, ?2, ?3, ?4, ?4)",
                    fparams![source_id, source_kind.as_str(), host_label, now_ms],
                )?;
            }
            for (conversation_id, source_id, origin_host, metadata_json) in metadata_updates {
                tx.execute_compat(
                    "UPDATE conversations
                     SET agent_id = ?1, source_id = ?2, origin_host = ?3,
                         metadata_json = ?4, metadata_bin = NULL
                     WHERE id = ?5 AND agent_id = ?6",
                    fparams![
                        omp_agent_id,
                        source_id.as_str(),
                        origin_host.as_deref(),
                        metadata_json,
                        conversation_id,
                        legacy_agent_id
                    ],
                )?;
                tx.execute_compat(
                    "UPDATE token_usage
                     SET agent_id = ?1, source_id = ?2
                     WHERE conversation_id = ?3",
                    fparams![omp_agent_id, source_id.as_str(), conversation_id],
                )?;
                tx.execute_compat(
                    "DELETE FROM conversation_external_lookup WHERE conversation_id = ?1",
                    fparams![conversation_id],
                )?;
                tx.execute_compat(
                    "DELETE FROM conversation_external_tail_lookup WHERE conversation_id = ?1",
                    fparams![conversation_id],
                )?;
            }
            tx.execute(
                "INSERT OR REPLACE INTO conversation_external_lookup (lookup_key, conversation_id)
                 SELECT
                     CAST(length(c.source_id) AS TEXT) || ':' || c.source_id || ':' ||
                     CAST(c.agent_id AS TEXT) || ':' ||
                     CAST(length(c.external_id) AS TEXT) || ':' || c.external_id,
                     c.id
                 FROM conversations c
                 WHERE c.agent_id = (SELECT id FROM agents WHERE slug = 'omp')
                   AND c.external_id IS NOT NULL",
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO conversation_external_tail_lookup (
                     lookup_key, conversation_id, ended_at, last_message_idx, last_message_created_at
                 )
                 SELECT
                     CAST(length(c.source_id) AS TEXT) || ':' || c.source_id || ':' ||
                     CAST(c.agent_id AS TEXT) || ':' ||
                     CAST(length(c.external_id) AS TEXT) || ':' || c.external_id,
                     c.id, ts.ended_at, ts.last_message_idx, ts.last_message_created_at
                 FROM conversations c
                 LEFT JOIN conversation_tail_state ts ON ts.conversation_id = c.id
                 WHERE c.agent_id = (SELECT id FROM agents WHERE slug = 'omp')
                   AND c.external_id IS NOT NULL",
            )?;
            tx.commit()?;
        } else if !assets_were_pending {
            self.conn.execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![
                    LEGACY_OMP_RECLASSIFICATION_META_KEY,
                    complete_state.as_str()
                ],
            )?;
            return Ok(LegacyOmpReclassificationResult::default());
        } else {
            // Bind a legacy/plain or older-context pending marker to the exact
            // ownership snapshot whose derived assets are about to rebuild.
            // Older builds may already have committed the identity swap before
            // crashing, so also invalidate semantic identity metadata on this
            // zero-row recovery path.
            let mut tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY, pending_state.as_str()],
            )?;
            if !pending_state_matches_context {
                tx.execute_compat(
                    "DELETE FROM meta WHERE key IN (?1, ?2, ?3, ?4, ?5)",
                    fparams![
                        LEGACY_OMP_LEXICAL_PUBLISHED_META_KEY,
                        LEGACY_OMP_ANALYTICS_REBUILT_META_KEY,
                        LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY,
                        LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
                        LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY
                    ],
                )?;
            }
            for semantic_tier in [SemanticIdentityTier::Fast, SemanticIdentityTier::Quality] {
                tx.execute_compat(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                    fparams![semantic_tier.meta_key(), pending_state.as_str()],
                )?;
            }
            tx.execute("DELETE FROM meta WHERE key = 'last_embedded_message_id'")?;
            tx.commit()?;
        }

        // #424: the identity transaction is preflight-critical; rebuilding a
        // multi-million-row derived analytics surface is not. Persist the two
        // derived phases independently so the indexer can publish lexical
        // state first, drive analytics progress through its watchdog, and
        // resume without repeating a phase that already completed.
        let lexical_published = self.legacy_omp_phase_matches_context(
            LEGACY_OMP_LEXICAL_PUBLISHED_META_KEY,
            &ownership_context,
        )?;
        let analytics_rebuilt = self.legacy_omp_phase_matches_context(
            LEGACY_OMP_ANALYTICS_REBUILT_META_KEY,
            &ownership_context,
        )?;
        if lexical_published && analytics_rebuilt {
            // Recover an older/non-atomic state where both durable phase
            // markers landed but the aggregate `complete:` marker did not.
            // Re-recording analytics performs the idempotent atomic promotion
            // and retires any stale message-metrics cursor.
            self.record_legacy_omp_phase_complete(LEGACY_OMP_ANALYTICS_REBUILT_META_KEY)?;
            return Ok(LegacyOmpReclassificationResult::default());
        }

        Ok(LegacyOmpReclassificationResult {
            conversations_reclassified: legacy_count,
            // The transaction commits the canonical identity change before
            // rebuilding derived assets. If a prior run stopped in that
            // window, `legacy_count` is already zero on retry, but the live
            // lexical generation still carries the old `pi_agent` identity.
            lexical_rebuild_required: legacy_count > 0 || !lexical_published,
            analytics_rebuild_required: legacy_count > 0 || !analytics_rebuilt,
        })
    }

    fn legacy_omp_phase_matches_context(&self, key: &str, context: &str) -> Result<bool> {
        let recorded: Option<String> = self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![key],
                |row| row.get_typed(0),
            )
            .optional()?;
        Ok(recorded.as_deref() == Some(context))
    }

    fn record_legacy_omp_phase_complete(&self, phase_key: &str) -> Result<()> {
        if phase_key != LEGACY_OMP_LEXICAL_PUBLISHED_META_KEY
            && phase_key != LEGACY_OMP_ANALYTICS_REBUILT_META_KEY
        {
            bail!("unknown legacy OMP derived phase key {phase_key:?}");
        }
        let state: Option<String> = self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY],
                |row| row.get_typed(0),
            )
            .optional()?;
        let Some(state) = state else {
            bail!("cannot complete a legacy OMP derived phase without a pending marker");
        };
        if state.starts_with("complete:") {
            return Ok(());
        }
        let Some(context) = state.strip_prefix("analytics_pending:") else {
            bail!(
                "cannot complete legacy OMP reclassification from unexpected marker state {state:?}"
            );
        };
        let lexical_published = phase_key == LEGACY_OMP_LEXICAL_PUBLISHED_META_KEY
            || self
                .legacy_omp_phase_matches_context(LEGACY_OMP_LEXICAL_PUBLISHED_META_KEY, context)?;
        let analytics_rebuilt = phase_key == LEGACY_OMP_ANALYTICS_REBUILT_META_KEY
            || self
                .legacy_omp_phase_matches_context(LEGACY_OMP_ANALYTICS_REBUILT_META_KEY, context)?;

        // The phase marker, overall completion marker, and analytics cursor
        // retirement are one durable state transition. A failure cannot leave
        // `complete:<context>` paired with a stale resume cursor, nor can it
        // advertise a phase whose own cleanup did not commit.
        let mut tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
            fparams![phase_key, context],
        )?;
        if phase_key == LEGACY_OMP_ANALYTICS_REBUILT_META_KEY {
            tx.execute_compat(
                "DELETE FROM meta WHERE key IN (?1, ?2, ?3)",
                fparams![
                    LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY,
                    LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
                    LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY
                ],
            )?;
        }
        if lexical_published && analytics_rebuilt {
            let complete_state = format!("complete:{context}");
            tx.execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![
                    LEGACY_OMP_RECLASSIFICATION_META_KEY,
                    complete_state.as_str()
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Record that the canonical SQLite-to-lexical publication for the legacy
    /// OMP identity rewrite completed. Analytics may still be pending.
    pub fn mark_legacy_omp_lexical_publish_complete(&self) -> Result<()> {
        self.record_legacy_omp_phase_complete(LEGACY_OMP_LEXICAL_PUBLISHED_META_KEY)
    }

    fn legacy_omp_pending_context(&self) -> Result<Option<String>> {
        let state: Option<String> = self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY],
                |row| row.get_typed(0),
            )
            .optional()?;
        let Some(state) = state else {
            bail!("cannot rebuild legacy OMP analytics without a migration marker");
        };
        if state.starts_with("complete:") {
            return Ok(None);
        }
        let Some(context) = state.strip_prefix("analytics_pending:") else {
            bail!("cannot rebuild legacy OMP analytics from unexpected marker state {state:?}");
        };
        Ok(Some(context.to_string()))
    }

    /// Rebuild every analytics derivative invalidated by the legacy OMP
    /// identity rewrite, reporting message-keyset progress to the owning
    /// indexer watchdog. The durable completion marker is written only after
    /// all three derived phases finish successfully. Returned values describe
    /// actual Track A work, Track B rows, and Track B elapsed milliseconds.
    pub(crate) fn rebuild_legacy_omp_analytics_with_progress(
        &self,
        progress: Option<&dyn Fn(i64, i64)>,
        heartbeat: Option<&dyn Fn()>,
        control: Option<&dyn Fn() -> Result<()>>,
    ) -> Result<Option<(AnalyticsRebuildResult, usize, u64)>> {
        if let Some(control) = control {
            control()?;
        }
        let Some(context) = self.legacy_omp_pending_context()? else {
            return Ok(None);
        };
        if let Some(heartbeat) = heartbeat {
            heartbeat();
        }
        let track_a = self
            .rebuild_analytics_since_with_chunk_size_and_progress(
                None,
                LEGACY_OMP_ANALYTICS_CHUNK_SIZE,
                progress,
                heartbeat,
                Some(context.as_str()),
                control,
            )
            .with_context(|| "rebuilding analytics after legacy OMP identity upgrade")?;
        if let Some(progress) = progress {
            progress(0, 0);
        }
        if let Some(heartbeat) = heartbeat {
            heartbeat();
        }
        let track_b_start = std::time::Instant::now();
        let track_b_rows = self
            .rebuild_token_daily_stats_with_progress(heartbeat, control)
            .with_context(|| "rebuilding token rollups after legacy OMP identity upgrade")?;
        let track_b_elapsed_ms = track_b_start.elapsed().as_millis() as u64;
        if let Some(progress) = progress {
            progress(0, 0);
        }
        if let Some(heartbeat) = heartbeat {
            heartbeat();
        }
        self.rebuild_daily_stats_with_progress(heartbeat, control)
            .with_context(|| "rebuilding daily stats after legacy OMP identity upgrade")?;
        if let Some(control) = control {
            control()?;
        }
        self.record_legacy_omp_phase_complete(LEGACY_OMP_ANALYTICS_REBUILT_META_KEY)?;
        Ok(Some((track_a, track_b_rows, track_b_elapsed_ms)))
    }

    /// Get the timestamp of the last successful index completion.
    pub fn get_last_indexed_at(&self) -> Result<Option<i64>> {
        let result: Result<String, _> = self.conn.query_row_map(
            "SELECT value FROM meta WHERE key = 'last_indexed_at'",
            fparams![],
            |row| row.get_typed(0),
        );
        match result.optional() {
            Ok(Some(s)) => Ok(s.parse().ok()),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Set the timestamp of the last successful index completion (milliseconds since epoch).
    pub fn set_last_indexed_at(&self, ts: i64) -> Result<()> {
        self.conn.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('last_indexed_at', ?1)",
            fparams![ts.to_string()],
        )?;
        Ok(())
    }

    /// List all registered agents.
    pub fn list_agents(&self) -> Result<Vec<Agent>> {
        self.conn
            .query_map_collect(
                "SELECT id, slug, name, version, kind FROM agents ORDER BY slug",
                fparams![],
                |row| {
                    let kind: String = row.get_typed(4)?;
                    Ok(Agent {
                        id: Some(row.get_typed(0)?),
                        slug: row.get_typed(1)?,
                        name: row.get_typed(2)?,
                        version: row.get_typed(3)?,
                        kind: match kind.as_str() {
                            "cli" => AgentKind::Cli,
                            "vscode" => AgentKind::VsCode,
                            _ => AgentKind::Hybrid,
                        },
                    })
                },
            )
            .with_context(|| "listing agents")
    }

    /// Count all archived conversations.
    pub fn total_conversation_count(&self) -> Result<usize> {
        let count: i64 =
            self.conn
                .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
                    row.get_typed(0)
                })?;
        Ok(count.max(0) as usize)
    }

    /// Count archived conversations grouped by `source_id`, for archive
    /// provenance display (e.g. `cass sources list` reporting how many
    /// conversations a legacy remote entry still covers).
    pub fn conversation_counts_by_source(
        &self,
    ) -> Result<std::collections::HashMap<String, usize>> {
        let rows = self.conn.query_map_collect(
            "SELECT COALESCE(source_id, 'local'), COUNT(*) FROM conversations GROUP BY COALESCE(source_id, 'local')",
            fparams![],
            |row| Ok((row.get_typed::<String>(0)?, row.get_typed::<i64>(1)?)),
        )?;
        Ok(rows
            .into_iter()
            .map(|(source_id, count)| (source_id, count.max(0) as usize))
            .collect())
    }

    /// Count all archived messages.
    pub fn total_message_count(&self) -> Result<usize> {
        let count: i64 =
            self.conn
                .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
                    row.get_typed(0)
                })?;
        Ok(count.max(0) as usize)
    }

    /// Remove all archived conversations/messages for one agent slug.
    ///
    /// This only affects cass's local archive database. Source session files on
    /// disk are untouched.
    pub fn purge_agent_archive_data(&self, agent_slug: &str) -> Result<AgentArchivePurgeResult> {
        let normalized = agent_slug.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Err(anyhow!("agent slug cannot be empty"));
        }

        let Some(agent_id) = self
            .conn
            .query_row_map(
                "SELECT id FROM agents WHERE slug = ?1",
                fparams![normalized.as_str()],
                |row| row.get_typed::<i64>(0),
            )
            .optional()?
        else {
            return Ok(AgentArchivePurgeResult::default());
        };

        let conversations_deleted: i64 = self.conn.query_row_map(
            "SELECT COUNT(*) FROM conversations WHERE agent_id = ?1",
            fparams![agent_id],
            |row| row.get_typed(0),
        )?;
        if conversations_deleted == 0 {
            return Ok(AgentArchivePurgeResult::default());
        }

        let messages_deleted: i64 = self.conn.query_row_map(
            "SELECT COUNT(*)
             FROM messages
             WHERE conversation_id IN (
                 SELECT id FROM conversations WHERE agent_id = ?1
             )",
            fparams![agent_id],
            |row| row.get_typed(0),
        )?;

        let mut tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_compat(
            "DELETE FROM conversation_external_lookup
             WHERE conversation_id IN (
                 SELECT id FROM conversations WHERE agent_id = ?1
             )",
            fparams![agent_id],
        )?;
        tx.execute_compat(
            "DELETE FROM conversation_external_tail_lookup
             WHERE conversation_id IN (
                 SELECT id FROM conversations WHERE agent_id = ?1
             )",
            fparams![agent_id],
        )?;
        tx.execute_compat(
            "DELETE FROM conversations WHERE agent_id = ?1",
            fparams![agent_id],
        )?;
        tx.execute_compat(
            "DELETE FROM agents
             WHERE id = ?1
               AND NOT EXISTS (
                   SELECT 1 FROM conversations WHERE agent_id = ?1
               )",
            fparams![agent_id],
        )?;
        clear_semantic_embed_watermark_after_deletion(&tx)?;
        tx.commit()?;

        Ok(AgentArchivePurgeResult {
            conversations_deleted: conversations_deleted.max(0) as usize,
            messages_deleted: messages_deleted.max(0) as usize,
        })
    }
}
