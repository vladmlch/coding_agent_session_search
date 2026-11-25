use super::source_and_historical_import::historical_table_exists;
use super::reconcile_and_codebuff::{
    clear_semantic_embed_watermark_after_deletion, delete_duplicate_conversation_rows,
};
use super::storage_core::FrankenStorage;
use crate::franken_sync::{
    SqliteValue,
    compat::{
        ConnectionExt as FrankenConnectionExt, OptionalExtension as FrankenOptionalExtension,
        ParamValue, RowExt as FrankenRowExt, Transaction as FrankenTransaction,
        TransactionExt as FrankenTransactionExt,
    },
};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

impl FrankenStorage {
    /// Tombstones written by `cass forget --apply` (2l1b0.50), by source path.
    pub fn forgotten_source_stamps(&self) -> Result<HashMap<String, SourceFileStamp>> {
        let rows: Vec<(String, Option<i64>, Option<i64>)> = self.conn.query_map_collect(
            "SELECT source_path, size_bytes, mtime_ms FROM forgotten_sources",
            fparams![],
            |row| {
                Ok((
                    row.get_typed::<String>(0)?,
                    row.get_typed::<Option<i64>>(1)?,
                    row.get_typed::<Option<i64>>(2)?,
                ))
            },
        )?;
        Ok(rows
            .into_iter()
            .map(|(path, size_bytes, mtime_ms)| {
                (
                    path,
                    SourceFileStamp {
                        size_bytes,
                        mtime_ms,
                    },
                )
            })
            .collect())
    }

    /// Resolve only source paths in the current ingest batch. Ordinary-file
    /// exclusions retain their nullable size/mtime and changed-file semantics;
    /// the lookup does not enumerate unrelated forgotten transcripts.
    pub fn forgotten_source_stamps_for(
        &self,
        paths: &[String],
    ) -> Result<HashMap<String, SourceFileStamp>> {
        if paths.is_empty() || !historical_table_exists(&self.conn, "forgotten_sources")? {
            return Ok(HashMap::new());
        }
        // Most archives have never forgotten a source. Bound that common
        // case to one row probe instead of a failed seek for every candidate.
        if self
            .conn
            .query("SELECT 1 FROM forgotten_sources LIMIT 1")?
            .is_empty()
        {
            return Ok(HashMap::new());
        }
        let statement = self.conn.prepare(
            "SELECT size_bytes, mtime_ms FROM forgotten_sources
             INDEXED BY sqlite_autoindex_forgotten_sources_1
             WHERE source_path = ?1 LIMIT 1",
        )?;
        let mut matched = HashMap::new();
        for path in paths {
            if matched.contains_key(path) {
                continue;
            }
            let rows = statement.query_with_params(&[SqliteValue::Text(path.clone().into())])?;
            if let Some(row) = rows.first() {
                matched.insert(
                    path.clone(),
                    SourceFileStamp {
                        size_bytes: row.get_typed(0)?,
                        mtime_ms: row.get_typed(1)?,
                    },
                );
            }
        }
        Ok(matched)
    }

    /// Discover stored exclusions from one read snapshot. Old schemas lacking
    /// either tombstone table are valid read-only inputs; listing never runs a
    /// migration or consults provider transcripts for discarded metadata.
    pub fn list_forget_exclusions(
        &self,
        selector: &ForgetExclusionSelector<'_>,
    ) -> Result<ForgetExclusionList> {
        let compiled = CompiledForgetExclusionSelector::new(selector, false)?;
        let mut tx = self.conn.transaction()?;
        let result = self.collect_forget_exclusions(&tx, &compiled)?;
        tx.rollback()?;
        Ok(result)
    }

    fn collect_forget_exclusions(
        &self,
        tx: &FrankenTransaction<'_>,
        compiled: &CompiledForgetExclusionSelector<'_>,
    ) -> Result<ForgetExclusionList> {
        let mut exclusions = if historical_table_exists(&self.conn, "forgotten_conversations")? {
            tx.query_map_collect(
                "SELECT source_id, agent_slug, source_path, external_id, forgotten_at_ms
                 FROM forgotten_conversations
                 ORDER BY source_id, agent_slug, source_path, external_id",
                fparams![],
                |row| {
                    Ok(ForgottenConversationExclusion {
                        key: ForgottenConversationKey {
                            source_id: row.get_typed(0)?,
                            agent_slug: row.get_typed(1)?,
                            source_path: row.get_typed(2)?,
                            external_id: row.get_typed(3)?,
                        },
                        forgotten_at_ms: row.get_typed(4)?,
                        blocked_by_source_stamp: false,
                    })
                },
            )?
        } else {
            Vec::new()
        };
        exclusions.retain(|record| compiled.matches_session(&record.key));

        let mut legacy_sources: Vec<ForgottenSourceExclusion> = if compiled.source_only()
            && historical_table_exists(&self.conn, "forgotten_sources")?
        {
            tx.query_map_collect(
                "SELECT source_path, size_bytes, mtime_ms, forgotten_at_ms
                 FROM forgotten_sources ORDER BY source_path",
                fparams![],
                |row| {
                    Ok(ForgottenSourceExclusion {
                        source_path: row.get_typed(0)?,
                        size_bytes: row.get_typed(1)?,
                        mtime_ms: row.get_typed(2)?,
                        forgotten_at_ms: row.get_typed(3)?,
                    })
                },
            )?
        } else {
            Vec::new()
        };
        legacy_sources.retain(|record| compiled.matches_path(&record.source_path));
        let legacy_paths: HashSet<String> = if compiled.source_only() {
            legacy_sources
                .iter()
                .map(|record| record.source_path.clone())
                .collect()
        } else {
            let paths: Vec<_> = exclusions
                .iter()
                .map(|record| record.key.source_path.clone())
                .collect();
            self.forgotten_source_stamps_for(&paths)?
                .into_keys()
                .collect()
        };
        for record in &mut exclusions {
            record.blocked_by_source_stamp = legacy_paths.contains(&record.key.source_path);
        }
        Ok(ForgetExclusionList {
            exclusions,
            legacy_sources,
        })
    }

    /// Explicitly release selected exclusions, leaving canonical rows and raw
    /// evidence untouched. The caller holds the maintenance lease for apply.
    /// A subsequent ordinary `index` revisits preserved historical sources;
    /// current path/workspace exclusions continue to apply independently.
    pub fn reinclude_forget_exclusions(
        &self,
        selector: &ForgetExclusionSelector<'_>,
        dry_run: bool,
    ) -> Result<ReincludeForgetExclusionsResult> {
        let compiled = CompiledForgetExclusionSelector::new(selector, true)?;
        let mut tx = self.conn.transaction()?;
        let listed = self.collect_forget_exclusions(&tx, &compiled)?;
        let blocked_by_source_stamps = if compiled.source_only() {
            Vec::new()
        } else {
            listed
                .exclusions
                .iter()
                .filter(|record| record.blocked_by_source_stamp)
                .map(|record| record.key.source_path.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        };
        let mut result = ReincludeForgetExclusionsResult {
            dry_run,
            exclusions_matched: listed.exclusions.len(),
            legacy_sources_matched: listed.legacy_sources.len(),
            blocked_by_source_stamps,
            sample_sessions: listed.exclusions.iter().take(20).cloned().collect(),
            sample_source_paths: listed
                .legacy_sources
                .iter()
                .take(20)
                .map(|record| record.source_path.clone())
                .collect(),
            ..Default::default()
        };
        let has_matches = !listed.exclusions.is_empty() || !listed.legacy_sources.is_empty();
        if !has_matches {
            tx.rollback()?;
            return Ok(result);
        }
        // Completion keys also include scan-root/provenance information. Match
        // the saved observations, retaining unrelated sources' reuse evidence.
        // Shared stores expose logical database/session handles; the ledger
        // instead observes the physical database, possibly through an alias.
        let affected_paths: BTreeSet<_> = listed
            .exclusions
            .iter()
            .map(|record| record.key.source_path.clone())
            .chain(
                listed
                    .legacy_sources
                    .iter()
                    .map(|record| record.source_path.clone()),
            )
            .collect();
        let mut replay_paths: HashSet<PathBuf> = affected_paths.iter().map(PathBuf::from).collect();
        for path in &affected_paths {
            if let Some(source) = Path::new(path).ancestors().find(|path| path.is_file()) {
                replay_paths.insert(source.to_path_buf());
            }
        }
        let canonical_paths: HashSet<_> = replay_paths
            .iter()
            .filter_map(|path| dunce::canonicalize(path).ok())
            .collect();
        let affected = |path: &str| {
            replay_paths.contains(Path::new(path))
                || dunce::canonicalize(path)
                    .ok()
                    .is_some_and(|path| canonical_paths.contains(&path))
        };
        // A missing store can still be identified by its recorded primary.
        // Never apply ancestor matching to dependencies: directory observations
        // are shared with unrelated siblings and are not physical sources.
        let primary_ancestors: HashSet<_> = affected_paths
            .iter()
            .flat_map(|path| Path::new(path).ancestors().skip(1))
            .filter(|path| !path.is_dir())
            .collect();
        let mut completion_keys = Vec::new();
        let mut cursor = "source_ingest_v1:".to_owned();
        loop {
            let page: Vec<(String, String)> = tx.query_map_collect(
                "SELECT key, value FROM meta WHERE key > ?1 AND key < 'source_ingest_v1;'
                 ORDER BY key LIMIT 256",
                fparams![cursor.as_str()],
                |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
            )?;
            let Some(last) = page.last() else { break };
            cursor = last.0.clone();
            for (key, observation) in page {
                let Ok(observation) = serde_json::from_str::<serde_json::Value>(&observation)
                else {
                    // Malformed evidence cannot suppress a source scan.
                    continue;
                };
                if observation["primary"]["path"].as_str().is_some_and(|path| {
                    affected(path) || primary_ancestors.contains(Path::new(path))
                }) || observation["dependencies"]
                    .as_array()
                    .is_some_and(|dependencies| {
                        dependencies
                            .iter()
                            .any(|dependency| dependency["path"].as_str().is_some_and(affected))
                    })
                {
                    completion_keys.push(key);
                }
            }
        }
        result.source_observations_matched = completion_keys.len();
        if dry_run {
            tx.rollback()?;
            return Ok(result);
        }
        if !result.blocked_by_source_stamps.is_empty() {
            bail!(
                "cannot reinclude selected sessions while a source-wide exclusion also covers {}; preview a source-only --source-path or --source-glob release before applying it",
                result.blocked_by_source_stamps.join(", ")
            );
        }
        for record in &listed.exclusions {
            tx.execute_compat(
                "DELETE FROM forgotten_conversations
                 WHERE source_id = ?1 AND agent_slug = ?2
                   AND source_path = ?3 AND external_id = ?4",
                fparams![
                    record.key.source_id.as_str(),
                    record.key.agent_slug.as_str(),
                    record.key.source_path.as_str(),
                    record.key.external_id.as_str()
                ],
            )?;
        }
        for record in &listed.legacy_sources {
            tx.execute_compat(
                "DELETE FROM forgotten_sources WHERE source_path = ?1",
                fparams![record.source_path.as_str()],
            )?;
        }
        for key in &completion_keys {
            result.source_observations_removed +=
                tx.execute_compat("DELETE FROM meta WHERE key = ?1", fparams![key.as_str()])?;
        }
        // Lifting a tombstone alone does not revisit old sessions: connector
        // cutoffs and remote mirror fingerprints can still skip their store.
        // Reset affected connectors atomically with the release. Source stamps
        // predate provider metadata, so those require one full rediscovery.
        let all_connectors = !listed.legacy_sources.is_empty();
        let session_agents: HashSet<_> = listed
            .exclusions
            .iter()
            .map(|record| record.key.agent_slug.trim().to_ascii_lowercase())
            .collect();
        let source_ids: HashSet<_> = listed
            .exclusions
            .iter()
            .map(|record| record.key.source_id.as_str())
            .collect();
        let metadata_keys: Vec<String> = tx.query_map_collect(
            "SELECT key FROM meta WHERE
                (key >= 'last_scan_ts:connector:' AND key < 'last_scan_ts:connector;')
                OR (key >= 'mirror_scan_fp:' AND key < 'mirror_scan_fp;')",
            fparams![],
            |row| row.get_typed(0),
        )?;
        let mut connector_keys = BTreeSet::new();
        let mut mirror_keys = Vec::new();
        for key in metadata_keys {
            if let Some(connector) = key.strip_prefix("last_scan_ts:connector:") {
                if all_connectors
                    || Self::connector_agent_slug_candidates(connector)
                        .iter()
                        .any(|agent| session_agents.contains(agent))
                {
                    connector_keys.insert(key);
                }
            } else if let Some(source_id) = key.strip_prefix("mirror_scan_fp:")
                && (all_connectors || source_ids.contains(source_id))
            {
                mirror_keys.push(key);
            }
        }
        for agent in &session_agents {
            // Absence falls back to the global scan watermark. A present zero
            // also covers a connector with no previous dedicated watermark.
            connector_keys.insert(Self::connector_last_scan_ts_meta_key(agent));
            if matches!(agent.as_str(), "claude" | "claude-code" | "claude_code") {
                for alias in ["claude", "claude-code", "claude_code"] {
                    connector_keys.insert(Self::connector_last_scan_ts_meta_key(alias));
                }
            }
        }
        for key in connector_keys {
            tx.execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, '0')",
                fparams![key.as_str()],
            )?;
        }
        if all_connectors {
            tx.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('last_scan_ts', '0')")?;
        }
        for key in mirror_keys {
            tx.execute_compat("DELETE FROM meta WHERE key = ?1", fparams![key.as_str()])?;
        }
        tx.commit()?;
        result.exclusions_removed = listed.exclusions.len();
        result.legacy_sources_removed = listed.legacy_sources.len();
        Ok(result)
    }

    /// Session tombstones survive changes to unrelated sessions in the same
    /// source store. They are independent of both canonical row IDs and file
    /// size/mtime, so neither a rescan nor a full index revives them.
    pub fn forgotten_conversation_keys(&self) -> Result<HashSet<ForgottenConversationKey>> {
        Ok(self
            .conn
            .query_map_collect(
                "SELECT source_id, agent_slug, source_path, external_id FROM forgotten_conversations",
                fparams![],
                |row| {
                    Ok(ForgottenConversationKey {
                        source_id: row.get_typed(0)?,
                        agent_slug: row.get_typed(1)?,
                        source_path: row.get_typed(2)?,
                        external_id: row.get_typed(3)?,
                    })
                },
            )?
            .into_iter()
            .collect())
    }

    /// Resolve only the current ingest batch against the durable composite
    /// identity index. Preparing one exact seek avoids loading every archived
    /// exclusion for each batch, and keeps SQL/parameter/result sizes bounded
    /// independently of the size of the exclusion archive.
    pub fn forgotten_conversation_keys_for(
        &self,
        keys: &[ForgottenConversationKey],
    ) -> Result<HashSet<ForgottenConversationKey>> {
        if keys.is_empty() || !historical_table_exists(&self.conn, "forgotten_conversations")? {
            return Ok(HashSet::new());
        }
        if self
            .conn
            .query("SELECT 1 FROM forgotten_conversations LIMIT 1")?
            .is_empty()
        {
            return Ok(HashSet::new());
        }
        let statement = self.conn.prepare(
            "SELECT 1 FROM forgotten_conversations
             INDEXED BY sqlite_autoindex_forgotten_conversations_1
             WHERE source_id = ?1 AND agent_slug = ?2
               AND source_path = ?3 AND external_id = ?4
             LIMIT 1",
        )?;
        let mut matched = HashSet::new();
        for key in keys {
            if matched.contains(key) {
                continue;
            }
            let rows = statement.query_with_params(&[
                SqliteValue::Text(key.source_id.clone().into()),
                SqliteValue::Text(key.agent_slug.clone().into()),
                SqliteValue::Text(key.source_path.clone().into()),
                SqliteValue::Text(key.external_id.clone().into()),
            ])?;
            if !rows.is_empty() {
                matched.insert(key.clone());
            }
        }
        Ok(matched)
    }

    /// Canonical deletion commits before derived assets can be rebuilt. Keep
    /// this marker until those assets have been published successfully, so an
    /// identical retry repairs them even when no canonical rows still match.
    pub fn forget_derived_rebuild_pending(&self) -> Result<bool> {
        let pending: Option<String> = self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = 'forget_derived_rebuild_pending'",
                fparams![],
                |row| row.get_typed(0),
            )
            .optional()?;
        Ok(pending.is_some())
    }

    /// The caller must hold the maintenance lease and clear this only after
    /// canonical-derived and lexical rebuilding have both succeeded.
    pub fn clear_forget_derived_rebuild_pending(&self) -> Result<()> {
        self.conn
            .execute("DELETE FROM meta WHERE key = 'forget_derived_rebuild_pending'")?;
        Ok(())
    }

    /// Drops the tombstones of forgotten sources that changed since the
    /// forget, so their next ingest is permanent.
    pub fn clear_forgotten_sources(&self, paths: &[String]) -> Result<()> {
        let mut tx = self.conn.transaction()?;
        for path in paths {
            tx.execute_compat(
                "DELETE FROM forgotten_sources WHERE source_path = ?1",
                fparams![path.as_str()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn forget_conversations_by_source_glob(
        &self,
        pattern: &str,
        dry_run: bool,
    ) -> Result<ForgetConversationsResult> {
        self.forget_conversations(
            &ForgetConversationsFilter {
                source_glob: Some(pattern),
                ..Default::default()
            },
            dry_run,
        )
    }

    /// Select canonical conversations by intersecting source, workspace and
    /// age predicates. One snapshot covers selection, tombstones and deletion;
    /// a dry run uses only reads and explicitly releases its snapshot. The
    /// caller rebuilds derived search/analytics assets after an applied change.
    ///
    /// Candidate envelopes and message aggregates are read in bounded pages.
    /// No message body or metadata payload is hydrated for a preview, and no
    /// SQL statement grows with the total number of selected conversations.
    pub fn forget_conversations(
        &self,
        filter: &ForgetConversationsFilter<'_>,
        dry_run: bool,
    ) -> Result<ForgetConversationsResult> {
        const PAGE_SIZE: usize = 256;
        let parse_glob = |value: Option<&str>, label: &str| -> Result<Option<glob::Pattern>> {
            value
                .map(|pattern| {
                    let trimmed = pattern.trim();
                    if trimmed.is_empty() {
                        bail!("forget {label} pattern cannot be empty");
                    }
                    glob::Pattern::new(trimmed)
                        .with_context(|| format!("invalid forget {label} pattern: {trimmed}"))
                })
                .transpose()
        };
        let source_glob = parse_glob(filter.source_glob, "source-glob")?;
        let workspace_glob = parse_glob(filter.workspace_glob, "workspace-glob")?;
        if source_glob.is_none() && workspace_glob.is_none() && filter.before_ms.is_none() {
            bail!("forget requires a source-glob, workspace-glob or age cutoff");
        }

        let mut report = ForgetConversationsResult {
            pattern: filter.source_glob.unwrap_or_default().trim().to_owned(),
            source_glob: filter.source_glob.map(|value| value.trim().to_owned()),
            workspace_glob: filter.workspace_glob.map(|value| value.trim().to_owned()),
            before_ms: filter.before_ms,
            dry_run,
            ..Default::default()
        };
        let mut tx = self.conn.transaction()?;
        let mut next_id = i64::MIN;
        let mut matched = Vec::new();
        let mut selected_by_path: HashMap<String, usize> = HashMap::new();
        loop {
            let page: Vec<ForgetCandidate> = tx.query_map_collect(
                "SELECT c.id, c.source_id, a.slug, c.source_path, c.external_id, w.path
                 FROM conversations c
                 JOIN agents a ON a.id = c.agent_id
                 LEFT JOIN workspaces w ON w.id = c.workspace_id
                 WHERE c.id >= ?1 ORDER BY c.id LIMIT 256",
                fparams![next_id],
                |row| {
                    Ok(ForgetCandidate {
                        id: row.get_typed(0)?,
                        source_id: row.get_typed(1)?,
                        agent_slug: row.get_typed(2)?,
                        source_path: row.get_typed(3)?,
                        external_id: row.get_typed(4)?,
                        workspace: row.get_typed(5)?,
                    })
                },
            )?;
            let Some(last) = page.last() else { break };
            let following_id = last.id.checked_add(1);
            let candidates: Vec<_> = page
                .into_iter()
                .filter(|candidate| {
                    source_glob.as_ref().is_none_or(|pattern| {
                        candidate
                            .source_path
                            .as_deref()
                            .is_some_and(|path| pattern.matches(path))
                    }) && workspace_glob.as_ref().is_none_or(|pattern| {
                        candidate
                            .workspace
                            .as_deref()
                            .is_some_and(|path| pattern.matches(path))
                    })
                })
                .collect();
            if !candidates.is_empty() {
                let placeholders = vec!["?"; candidates.len()].join(",");
                let parameters: Vec<_> = candidates
                    .iter()
                    .map(|candidate| ParamValue::from(candidate.id))
                    .collect();
                let statistics: HashMap<i64, (i64, i64, Option<i64>)> = tx
                    .query_map_collect(
                        &format!(
                            "SELECT conversation_id, COUNT(*), COUNT(created_at), MAX(created_at)
                             FROM messages WHERE conversation_id IN ({placeholders})
                             GROUP BY conversation_id"
                        ),
                        &parameters,
                        |row| {
                            Ok((
                                row.get_typed(0)?,
                                (row.get_typed(1)?, row.get_typed(2)?, row.get_typed(3)?),
                            ))
                        },
                    )?
                    .into_iter()
                    .collect();
                for candidate in candidates {
                    let (message_count, dated_count, latest) =
                        statistics.get(&candidate.id).copied().unwrap_or_default();
                    if filter.before_ms.is_some_and(|cutoff| {
                        message_count == 0
                            || dated_count != message_count
                            || latest.is_none_or(|timestamp| timestamp >= cutoff)
                    }) {
                        continue;
                    }
                    report.messages_matched = report
                        .messages_matched
                        .checked_add(
                            usize::try_from(message_count)
                                .context("invalid matched message count")?,
                        )
                        .context("matched message count overflow")?;
                    if let Some(path) = &candidate.source_path {
                        *selected_by_path.entry(path.clone()).or_default() += 1;
                        if report.sample_source_paths.len() < 20 {
                            report.sample_source_paths.push(path.clone());
                        }
                    }
                    if report.sample_sessions.len() < 20 {
                        report.sample_sessions.push(ForgetSessionSample {
                            source_path: candidate.source_path.clone(),
                            external_id: candidate.external_id.clone(),
                            workspace: candidate.workspace.clone(),
                            last_message_at: latest,
                        });
                    }
                    matched.push(candidate);
                }
            }
            let Some(following_id) = following_id else {
                break;
            };
            next_id = following_id;
        }
        report.conversations_matched = matched.len();
        if matched.is_empty() {
            tx.rollback()?;
            return Ok(report);
        }

        // Source-wide stamps must never suppress unselected siblings. Count
        // all rows for only the matched paths, using bounded parameter lists.
        let paths: Vec<_> = selected_by_path.keys().map(String::as_str).collect();
        let mut total_by_path: HashMap<String, usize> = HashMap::new();
        for page in paths.chunks(PAGE_SIZE) {
            let placeholders = vec!["?"; page.len()].join(",");
            let parameters: Vec<_> = page.iter().map(|path| ParamValue::from(*path)).collect();
            let counts: Vec<(String, i64)> = tx.query_map_collect(
                &format!("SELECT source_path, COUNT(*) FROM conversations WHERE source_path IN ({placeholders}) GROUP BY source_path"),
                &parameters,
                |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
            )?;
            for (path, count) in counts {
                total_by_path.insert(
                    path,
                    usize::try_from(count).context("invalid source conversation count")?,
                );
            }
        }
        // A source remains shared even after earlier forgets removed all but
        // one of its sessions.
        let previously_shared: HashSet<String> =
            if historical_table_exists(&self.conn, "forgotten_conversations")? {
            tx.query_map_collect(
                "SELECT source_path FROM forgotten_conversations",
                fparams![],
                |row| row.get_typed(0),
            )?
            .into_iter()
            .collect()
        } else {
            HashSet::new()
        };
        let mut session_tombstones = Vec::new();
        let mut file_tombstones = BTreeSet::new();
        let mut session_only_paths = BTreeSet::new();
        for candidate in &matched {
            let Some(path) = &candidate.source_path else {
                continue;
            };
            let total = *total_by_path
                .get(path)
                .context("matched source lost its conversation count")?;
            let shared =
                total > 1 || source_path_is_shared_store(path) || previously_shared.contains(path);
            let external_id = candidate
                .external_id
                .as_deref()
                .filter(|id| !id.trim().is_empty());
            if shared && let Some(external_id) = external_id {
                session_tombstones.push(ForgottenConversationKey {
                    source_id: candidate.source_id.clone(),
                    agent_slug: candidate.agent_slug.clone(),
                    source_path: path.clone(),
                    external_id: external_id.to_owned(),
                });
                session_only_paths.insert(path.clone());
            } else {
                if selected_by_path.get(path).copied() != Some(total) {
                    bail!(
                        "cannot forget only part of shared source {path}: a selected conversation has no stable external ID; select the entire source with --source-glob"
                    );
                }
                file_tombstones.insert(path.clone());
            }
        }
        if dry_run {
            tx.rollback()?;
            return Ok(report);
        }

        tx.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('forget_derived_rebuild_pending', '1')",
        )?;
        let forgotten_at_ms = Self::now_millis();
        for key in &session_tombstones {
            tx.execute_compat(
                "INSERT OR REPLACE INTO forgotten_conversations
                     (source_id, agent_slug, source_path, external_id, forgotten_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                fparams![
                    key.source_id.as_str(),
                    key.agent_slug.as_str(),
                    key.source_path.as_str(),
                    key.external_id.as_str(),
                    forgotten_at_ms
                ],
            )?;
        }
        for path in session_only_paths.difference(&file_tombstones) {
            tx.execute_compat(
                "DELETE FROM forgotten_sources WHERE source_path = ?1",
                fparams![path.as_str()],
            )?;
        }
        for path in &file_tombstones {
            let stamp = SourceFileStamp::of(Path::new(path));
            tx.execute_compat(
                "INSERT OR REPLACE INTO forgotten_sources
                     (source_path, size_bytes, mtime_ms, forgotten_at_ms)
                 VALUES (?1, ?2, ?3, ?4)",
                fparams![
                    path.as_str(),
                    stamp.size_bytes,
                    stamp.mtime_ms,
                    forgotten_at_ms
                ],
            )?;
        }
        for candidate in &matched {
            delete_duplicate_conversation_rows(&tx, candidate.id)?;
        }
        clear_semantic_embed_watermark_after_deletion(&tx)?;
        tx.commit()?;
        self.invalidate_fts_messages_present_cache();
        report.conversations_deleted = matched.len();
        Ok(report)
    }

    /// `coding_agent_session_search-uhhxy` (gh #302 ask #2): collapse
    /// PRE-EXISTING duplicate conversation rows created before the
    /// external_id `projects/` canonicalization fix. The watcher and
    /// full-rebuild ingest paths historically derived `external_id` with
    /// and without a leading `projects/` segment for the SAME session file,
    /// so a single `source_path` accumulated two conversation rows that
    /// differ only by that prefix. This collapses each such pair: it keeps
    /// the bare (canonical, `projects/`-free) row and drops the
    /// `projects/`-prefixed twin AND all of its child rows (messages cascade
    /// via the FK, plus the non-cascading external-lookup tables deleted
    /// explicitly). The lexical FTS index is NOT rebuilt here — the caller
    /// rebuilds it after `--apply` so callers can batch.
    ///
    /// `dry_run=true` (the default at the CLI) inspects only: it reports the
    /// duplicate pairs and the row/message counts that WOULD be removed
    /// without mutating anything. `dry_run=false` performs the deletion in a
    /// single transaction.
    pub fn collapse_external_id_prefix_duplicates(
        &self,
        dry_run: bool,
    ) -> Result<ExternalIdDuplicateCleanupResult> {
        // Find every (source_id, agent_id, source_path) group that holds BOTH
        // a `projects/`-prefixed external_id row and a bare twin whose
        // external_id equals the prefixed id with the leading `projects/`
        // stripped. We match the twin by value so we never drop a prefixed row
        // that lacks a real bare counterpart (that would be data loss, not
        // dedup). Only the prefixed row is a deletion candidate.
        let candidate_rows: Vec<ExternalIdDuplicateCandidate> = self.conn.query_map_collect(
            "SELECT dup.id, keep.id, dup.source_path, dup.external_id, keep.external_id
             FROM conversations dup
             JOIN conversations keep
               ON keep.source_id = dup.source_id
              AND keep.agent_id = dup.agent_id
              AND keep.source_path = dup.source_path
              AND keep.id <> dup.id
             WHERE dup.external_id LIKE 'projects/%'
               AND keep.external_id = SUBSTR(dup.external_id, 10)
             ORDER BY dup.id",
            fparams![],
            |row| {
                Ok(ExternalIdDuplicateCandidate {
                    drop_conversation_id: row.get_typed(0)?,
                    keep_conversation_id: row.get_typed(1)?,
                    source_path: row.get_typed(2)?,
                    drop_external_id: row.get_typed(3)?,
                    keep_external_id: row.get_typed(4)?,
                })
            },
        )?;

        let mut result = ExternalIdDuplicateCleanupResult {
            dry_run,
            ..Default::default()
        };

        if candidate_rows.is_empty() {
            return Ok(result);
        }

        // Tally the message rows that belong to the drop candidates so the
        // dry-run report is faithful to what `--apply` would remove.
        let drop_ids: Vec<i64> = candidate_rows
            .iter()
            .map(|candidate| candidate.drop_conversation_id)
            .collect();
        let mut messages_affected: i64 = 0;
        for drop_id in &drop_ids {
            let count: i64 = self.conn.query_row_map(
                "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
                fparams![*drop_id],
                |row| row.get_typed(0),
            )?;
            messages_affected = messages_affected.saturating_add(count);
        }
        result.conversations_affected = candidate_rows.len();
        result.messages_affected = messages_affected.max(0) as usize;
        result.pairs = candidate_rows;

        if dry_run {
            return Ok(result);
        }

        // Apply: delete each prefixed twin and its children in one
        // transaction. Messages, snippets, conversation_tags, token_usage,
        // etc. carry `ON DELETE CASCADE`; the two external-lookup tables do
        // not, so delete them explicitly first (mirrors `purge_agent_archive`).
        let mut tx = self.conn.transaction()?;
        for candidate in &result.pairs {
            delete_duplicate_conversation_rows(&tx, candidate.drop_conversation_id)?;
        }
        clear_semantic_embed_watermark_after_deletion(&tx)?;
        tx.commit()?;

        // The derived FTS shadow rows for the dropped messages are now stale.
        // Invalidate the presence cache; the caller is responsible for the
        // actual lexical rebuild (so multi-step cleanups rebuild once).
        self.invalidate_fts_messages_present_cache();

        Ok(result)
    }

    /// Remove the rows a remote mirror attributed to an agent that does not
    /// own the mirrored store (mxr52). Before mx520 every connector scanned
    /// every mirror container, so a synced Pi, Prime, Factory or Gemini
    /// session was also archived as a Claude conversation. A row is deleted
    /// only when the same source file is also archived under one of the
    /// store's owning agents, so every deletion is a duplicate; a
    /// misattributed row without that counterpart is reported and kept.
    pub fn collapse_misattributed_mirror_rows(
        &self,
        containers: &[MirrorContainerOwners],
        dry_run: bool,
    ) -> Result<MisattributedMirrorCleanupResult> {
        let mut result = MisattributedMirrorCleanupResult {
            dry_run,
            ..Default::default()
        };
        for container in containers {
            let source_rows: Vec<(i64, String, String)> = self.conn.query_map_collect(
                "SELECT c.id, a.slug, c.source_path FROM conversations c
                 JOIN agents a ON a.id = c.agent_id
                 WHERE c.source_id = ?1",
                fparams![container.source_id.as_str()],
                |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
            )?;
            let mut by_path: HashMap<&str, Vec<(i64, &str)>> = HashMap::new();
            for (id, agent, source_path) in &source_rows {
                if Path::new(source_path).starts_with(&container.container) {
                    by_path
                        .entry(source_path.as_str())
                        .or_default()
                        .push((*id, agent.as_str()));
                }
            }
            let owns = |agent: &str| container.owner_agents.iter().any(|owner| owner == agent);
            for (source_path, rows) in by_path {
                let owner_conversation_id = rows
                    .iter()
                    .filter(|(_, agent)| owns(agent))
                    .map(|(id, _)| *id)
                    .min();
                for (conversation_id, agent) in rows.iter().filter(|(_, agent)| !owns(agent)) {
                    result.rows.push(MisattributedMirrorRow {
                        conversation_id: *conversation_id,
                        source_id: container.source_id.clone(),
                        source_path: source_path.to_string(),
                        agent: (*agent).to_string(),
                        owner_agents: container.owner_agents.clone(),
                        owner_conversation_id,
                    });
                }
            }
        }
        result.rows.sort_by_key(|row| row.conversation_id);
        let drop_ids: Vec<i64> = result
            .rows
            .iter()
            .filter(|row| row.owner_conversation_id.is_some())
            .map(|row| row.conversation_id)
            .collect();
        result.unmatched = result.rows.len() - drop_ids.len();
        result.conversations_affected = drop_ids.len();
        for drop_id in &drop_ids {
            result.messages_affected = result
                .messages_affected
                .saturating_add(self.conversation_message_count(*drop_id)?);
        }
        if dry_run || drop_ids.is_empty() {
            return Ok(result);
        }

        let mut tx = self.conn.transaction()?;
        for drop_id in &drop_ids {
            delete_duplicate_conversation_rows(&tx, *drop_id)?;
        }
        clear_semantic_embed_watermark_after_deletion(&tx)?;
        tx.commit()?;
        self.invalidate_fts_messages_present_cache();
        Ok(result)
    }

    /// Retire the stale per-segment Codex rows a merged thread supersedes.
    ///
    /// FAD's paginated-thread merge emits one conversation per Codex thread
    /// whose `metadata.segments[].external_id` lists the ids each rollout
    /// segment used to own. Archives indexed before the merge hold one row
    /// per segment; once the merged (identity) row is persisted, the other
    /// rows of the same source whose `external_id` appears in that list are
    /// orphans. This deletes them through the same transaction path as the
    /// other dedup cleanups (`delete_duplicate_conversation_rows` + semantic
    /// watermark clear + FTS presence-cache invalidate) and returns their
    /// lexical document slots so the caller can drop the derived canonical
    /// and region documents. The identity row itself is never a candidate.
    ///
    /// Keyed strictly on the caller-supplied `segment_external_ids` (the
    /// authoritative `metadata.segments` list), never on filename patterns.
    /// Returns an empty list when the identity row is absent or nothing
    /// matches, so it is idempotent and safe on single-file threads.
    pub fn retire_superseded_codex_segment_rows(
        &self,
        identity_source_path: &str,
        identity_external_id: &str,
        segment_external_ids: &[String],
    ) -> Result<Vec<RetiredCodexSegmentRow>> {
        if segment_external_ids.is_empty() {
            return Ok(Vec::new());
        }
        // Resolve the merged thread's identity row to bind the source and
        // agent the stale siblings must share.
        let identity_rows: Vec<(i64, String, i64)> = self.conn.query_map_collect(
            "SELECT c.id, c.source_id, c.agent_id FROM conversations c JOIN agents a ON a.id = c.agent_id WHERE c.source_path = ?1 AND c.external_id = ?2 AND a.slug = 'codex' LIMIT 1",
            fparams![identity_source_path, identity_external_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        )?;
        let Some((identity_id, source_id, agent_id)) = identity_rows.into_iter().next() else {
            return Ok(Vec::new());
        };

        let has_regions = historical_table_exists(&self.conn, "viewer_search_regions")?;
        let mut retired: Vec<RetiredCodexSegmentRow> = Vec::new();
        let mut seen: HashSet<i64> = HashSet::new();
        for segment_external_id in segment_external_ids {
            let candidates: Vec<(i64, String, String)> = self.conn.query_map_collect(
                "SELECT id, source_path, external_id FROM conversations WHERE source_id = ?1 AND agent_id = ?2 AND external_id = ?3 AND source_path <> ?4 AND id <> ?5",
                fparams![
                    source_id.as_str(),
                    agent_id,
                    segment_external_id.as_str(),
                    identity_source_path,
                    identity_id
                ],
                |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
            )?;
            for (conversation_id, source_path, external_id) in candidates {
                if !seen.insert(conversation_id) {
                    continue;
                }
                let message_indices: Vec<i64> = self.conn.query_map_collect(
                    "SELECT idx FROM messages WHERE conversation_id = ?1",
                    fparams![conversation_id],
                    |row| row.get_typed(0),
                )?;
                let region_slots: Vec<i64> = if has_regions {
                    self.conn.query_map_collect(
                        "SELECT quill_idx FROM viewer_search_regions WHERE conversation_id = ?1",
                        fparams![conversation_id],
                        |row| row.get_typed(0),
                    )?
                } else {
                    Vec::new()
                };
                retired.push(RetiredCodexSegmentRow {
                    conversation_id,
                    source_id: source_id.clone(),
                    source_path,
                    external_id,
                    message_indices,
                    region_slots,
                });
            }
        }
        if retired.is_empty() {
            return Ok(retired);
        }

        let mut tx = self.conn.transaction()?;
        for row in &retired {
            delete_duplicate_conversation_rows(&tx, row.conversation_id)?;
        }
        clear_semantic_embed_watermark_after_deletion(&tx)?;
        tx.commit()?;
        self.invalidate_fts_messages_present_cache();
        Ok(retired)
    }
}

/// Result of purging archived data for a single agent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentArchivePurgeResult {
    pub conversations_deleted: usize,
    pub messages_deleted: usize,
}

/// Selectors for an explicit `cass forget` operation. Present selectors are
/// intersected, and an empty selection request is refused. `before_ms` is an
/// absolute, exclusive cutoff for the latest message timestamp; conversations
/// with no messages or any unknown message timestamp are kept.
#[derive(Debug, Clone, Copy, Default)]
pub struct ForgetConversationsFilter<'a> {
    pub source_glob: Option<&'a str>,
    pub workspace_glob: Option<&'a str>,
    pub before_ms: Option<i64>,
}

/// A durable session identity in a source that can hold multiple conversations.
/// Agent/source provenance prevents a same-named session in a different store
/// from inheriting another conversation's tombstone.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct ForgottenConversationKey {
    pub source_id: String,
    pub agent_slug: String,
    pub source_path: String,
    pub external_id: String,
}

/// Select already-forgotten identities without inferring the workspace or
/// message times that v23 never stored. Exact identity predicates intersect;
/// a literal source path and a source glob are mutually exclusive.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ForgetExclusionSelector<'a> {
    pub source_glob: Option<&'a str>,
    pub source_path: Option<&'a str>,
    pub source_id: Option<&'a str>,
    pub agent_slug: Option<&'a str>,
    pub external_id: Option<&'a str>,
}

impl ForgetExclusionSelector<'_> {
    pub fn validate(&self, require_selector: bool) -> Result<()> {
        CompiledForgetExclusionSelector::new(self, require_selector).map(|_| ())
    }
}

struct CompiledForgetExclusionSelector<'a> {
    selector: ForgetExclusionSelector<'a>,
    source_glob: Option<glob::Pattern>,
}

impl<'a> CompiledForgetExclusionSelector<'a> {
    fn new(selector: &ForgetExclusionSelector<'a>, require_selector: bool) -> Result<Self> {
        if selector.source_glob.is_some() && selector.source_path.is_some() {
            bail!("source-path and source-glob are mutually exclusive exclusion selectors");
        }
        let fields = [
            ("source-glob", selector.source_glob),
            ("source-path", selector.source_path),
            ("source-id", selector.source_id),
            ("agent", selector.agent_slug),
            ("external-id", selector.external_id),
        ];
        if require_selector && fields.iter().all(|(_, value)| value.is_none()) {
            bail!("reinclude requires at least one exclusion selector");
        }
        for (name, value) in fields {
            if value.is_some_and(|value| value.trim().is_empty()) {
                bail!("exclusion {name} selector cannot be empty");
            }
        }
        let source_glob = selector
            .source_glob
            .map(|pattern| {
                glob::Pattern::new(pattern.trim())
                    .with_context(|| format!("invalid exclusion source-glob pattern: {pattern}"))
            })
            .transpose()?;
        Ok(Self {
            selector: *selector,
            source_glob,
        })
    }

    pub(super) fn source_only(&self) -> bool {
        self.selector.source_id.is_none()
            && self.selector.agent_slug.is_none()
            && self.selector.external_id.is_none()
    }

    fn matches_path(&self, path: &str) -> bool {
        self.selector.source_path.is_none_or(|exact| exact == path)
            && self
                .source_glob
                .as_ref()
                .is_none_or(|pattern| pattern.matches(path))
    }

    fn matches_session(&self, key: &ForgottenConversationKey) -> bool {
        self.matches_path(&key.source_path)
            && self
                .selector
                .source_id
                .is_none_or(|source_id| source_id == key.source_id)
            && self
                .selector
                .agent_slug
                .is_none_or(|agent| agent == key.agent_slug)
            && self
                .selector
                .external_id
                .is_none_or(|external_id| external_id == key.external_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ForgottenConversationExclusion {
    #[serde(flatten)]
    pub key: ForgottenConversationKey,
    pub forgotten_at_ms: i64,
    /// A separate source-wide stamp also suppresses this path. Releasing one
    /// session must not implicitly release unnamed siblings covered by it.
    pub blocked_by_source_stamp: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ForgottenSourceExclusion {
    pub source_path: String,
    pub size_bytes: Option<i64>,
    pub mtime_ms: Option<i64>,
    pub forgotten_at_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ForgetExclusionList {
    pub exclusions: Vec<ForgottenConversationExclusion>,
    /// Source stamps have no provider/provenance/session metadata. They are
    /// listed only when no identity predicate asks us to infer those fields.
    pub legacy_sources: Vec<ForgottenSourceExclusion>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ReincludeForgetExclusionsResult {
    pub dry_run: bool,
    pub exclusions_matched: usize,
    pub exclusions_removed: usize,
    pub legacy_sources_matched: usize,
    pub legacy_sources_removed: usize,
    pub source_observations_matched: usize,
    pub source_observations_removed: usize,
    pub blocked_by_source_stamps: Vec<String>,
    pub sample_sessions: Vec<ForgottenConversationExclusion>,
    /// Bounded sample of the separately selected legacy source stamps.
    pub sample_source_paths: Vec<String>,
}

/// A bounded preview identifies the selected session even when several
/// conversations share the same source path. `last_message_at` is the latest
/// known message timestamp; age selection still keeps partly undated sessions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ForgetSessionSample {
    pub source_path: Option<String>,
    pub external_id: Option<String>,
    pub workspace: Option<String>,
    pub last_message_at: Option<i64>,
}

/// Result of a `cass forget` prune (#292 ask #2, GH #537). On a dry run the
/// `*_deleted` fields are zero and the `*_matched` fields report what WOULD be
/// removed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ForgetConversationsResult {
    /// The source glob, retained for existing report consumers; empty when
    /// selection uses workspace or age alone.
    pub pattern: String,
    pub source_glob: Option<String>,
    pub workspace_glob: Option<String>,
    pub before_ms: Option<i64>,
    pub dry_run: bool,
    pub conversations_matched: usize,
    pub messages_matched: usize,
    pub conversations_deleted: usize,
    /// Bounded (<= 20) sample of matched source paths, for operator review.
    pub sample_source_paths: Vec<String>,
    /// Bounded (<= 20) sample of the actual sessions selected for deletion.
    pub sample_sessions: Vec<ForgetSessionSample>,
}

/// Only the narrow envelope needed to select and tombstone a conversation.
/// Message bodies and wide metadata never enter a forget preview.
struct ForgetCandidate {
    id: i64,
    source_id: String,
    agent_slug: String,
    source_path: Option<String>,
    external_id: Option<String>,
    workspace: Option<String>,
}

pub(super) fn source_path_is_shared_store(path: &str) -> bool {
    use std::io::Read as _;

    // Devin uses a virtual `<database>/<session-id>` source path. Inspect
    // file ancestors as well as the final component, but a real directory
    // named `project.db` does not make its ordinary transcripts shared stores.
    Path::new(path).ancestors().any(|candidate| {
        if candidate.is_dir() {
            return false;
        }
        if candidate
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| {
                ["db", "sqlite", "sqlite3", "vscdb"]
                    .iter()
                    .any(|known| extension.eq_ignore_ascii_case(known))
            })
        {
            return true;
        }
        if !candidate.is_file() {
            return false;
        }
        let Ok(mut file) = fs::File::open(candidate) else {
            return false;
        };
        let mut header = [0_u8; 16];
        file.read_exact(&mut header).is_ok() && &header == b"SQLite format 3\0"
    })
}

/// A source file's size and modification time, as `cass forget` recorded it
/// in `forgotten_sources` (2l1b0.50). `None` fields mean the file was absent
/// or its metadata unreadable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceFileStamp {
    pub size_bytes: Option<i64>,
    pub mtime_ms: Option<i64>,
}

impl SourceFileStamp {
    pub fn of(path: &Path) -> Self {
        let Ok(metadata) = fs::metadata(path) else {
            return Self::default();
        };
        Self {
            size_bytes: i64::try_from(metadata.len()).ok(),
            mtime_ms: metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok()),
        }
    }
}

/// A single PRE-EXISTING duplicate conversation pair detected by
/// `collapse_external_id_prefix_duplicates`: a `projects/`-prefixed row
/// (the drop candidate) and its bare canonical twin (kept), both pointing
/// at the same `source_path`. See gh #302 ask #2 / bead uhhxy.
#[derive(Debug, Clone, Serialize)]
pub struct ExternalIdDuplicateCandidate {
    /// Conversation row id of the `projects/`-prefixed twin to drop.
    pub drop_conversation_id: i64,
    /// Conversation row id of the bare canonical row to keep.
    pub keep_conversation_id: i64,
    /// Shared source path of the duplicated pair.
    pub source_path: String,
    /// external_id of the dropped (prefixed) row.
    pub drop_external_id: String,
    /// external_id of the kept (bare) row.
    pub keep_external_id: String,
}

/// Result of `collapse_external_id_prefix_duplicates`. In `dry_run` mode
/// the counts/pairs describe what WOULD be removed; otherwise they describe
/// what was removed.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ExternalIdDuplicateCleanupResult {
    pub dry_run: bool,
    /// Number of `projects/`-prefixed conversation rows collapsed.
    pub conversations_affected: usize,
    /// Number of message rows belonging to the dropped twins.
    pub messages_affected: usize,
    /// The detected duplicate pairs (drop + keep ids, paths, external_ids).
    pub pairs: Vec<ExternalIdDuplicateCandidate>,
}

/// One stale per-segment Codex row retired by
/// `FrankenStorage::retire_superseded_codex_segment_rows`, carrying the
/// lexical document slots it owned so the caller can drop its derived
/// canonical + region documents from the live Quill generation. Before the
/// FAD paginated-thread merge each rollout segment was archived as its own
/// conversation; the merged thread supersedes those per-segment rows.
#[derive(Debug, Clone, Serialize)]
pub struct RetiredCodexSegmentRow {
    pub conversation_id: i64,
    pub source_id: String,
    pub source_path: String,
    pub external_id: String,
    /// Canonical message-document slots (`messages.idx`).
    pub message_indices: Vec<i64>,
    /// Region-document slots (`viewer_search_regions.quill_idx`); empty when
    /// the viewer extension profile is absent.
    pub region_slots: Vec<i64>,
}

/// A remote mirror container and the agents that own the store its
/// configured path names (mxr52).
#[derive(Debug, Clone)]
pub struct MirrorContainerOwners {
    pub source_id: String,
    pub container: PathBuf,
    pub owner_agents: Vec<String>,
}

/// A conversation a remote mirror attributed to an agent that does not own
/// the mirrored store.
#[derive(Debug, Clone, Serialize)]
pub struct MisattributedMirrorRow {
    pub conversation_id: i64,
    pub source_id: String,
    pub source_path: String,
    pub agent: String,
    pub owner_agents: Vec<String>,
    /// The row archiving the same source file under an owning agent; `None`
    /// means the misattributed row is the only copy and is kept.
    pub owner_conversation_id: Option<i64>,
}

/// Outcome of [`FrankenStorage::collapse_misattributed_mirror_rows`].
#[derive(Debug, Clone, Default, Serialize)]
pub struct MisattributedMirrorCleanupResult {
    pub dry_run: bool,
    /// Misattributed rows removed (or, on a dry run, that would be), each
    /// with an owner counterpart.
    pub conversations_affected: usize,
    /// Message rows belonging to those conversations.
    pub messages_affected: usize,
    /// Misattributed rows without an owner counterpart: reported, never removed.
    pub unmatched: usize,
    pub rows: Vec<MisattributedMirrorRow>,
}
