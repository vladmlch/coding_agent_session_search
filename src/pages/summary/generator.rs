use super::exclusions::ExclusionSet;
use super::types::{
    AgentSummaryItem, DateHistogramEntry, DateRange, EncryptionSummary, PrePublishSummary,
    ScanReportSummary, SummaryFilters, WorkspaceSummaryItem, estimate_compressed_size,
};
use crate::franken_sync::Connection;
use crate::franken_sync::Row;
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};

const SUMMARY_ID_CHUNK_SIZE: usize = 500;

#[derive(Default)]
struct WorkspaceAggregate {
    conversation_ids: Vec<i64>,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    sample_titles: Vec<String>,
}

#[derive(Default)]
struct ExclusionRecount {
    conversation_ids: Vec<i64>,
    total_messages: usize,
    total_characters: usize,
    earliest_ts: Option<i64>,
    latest_ts: Option<i64>,
}

/// Generator for pre-publish summaries.
pub struct SummaryGenerator<'a> {
    db: &'a Connection,
}

impl<'a> SummaryGenerator<'a> {
    /// Create a new summary generator.
    pub fn new(db: &'a Connection) -> Self {
        Self { db }
    }

    /// Generate a pre-publish summary with optional filters.
    pub fn generate(&self, filters: Option<&SummaryFilters>) -> Result<PrePublishSummary> {
        let filters = filters.cloned().unwrap_or_default();

        // Build WHERE clause for filters
        let (where_clause, params) = self.build_filter_clause(&filters);

        // Get basic counts
        let (total_conversations, total_messages, total_characters) =
            self.get_counts(&where_clause, &params)?;

        // Get time range
        let (earliest_ts, latest_ts) = self.get_time_range(&where_clause, &params)?;

        // Get date histogram
        let date_histogram = self.get_date_histogram(&where_clause, &params)?;

        // Get workspace summary
        let workspaces = self.get_workspace_summary(&where_clause, &params)?;

        // Get agent summary
        let agents = self.get_agent_summary(&where_clause, &params, total_conversations)?;

        // Estimate size (rough: ~60% of raw character count after compression)
        let estimated_size_bytes = estimate_compressed_size(total_characters);

        Ok(PrePublishSummary {
            total_conversations,
            total_messages,
            total_characters,
            estimated_size_bytes,
            earliest_timestamp: earliest_ts.and_then(DateTime::from_timestamp_millis),
            latest_timestamp: latest_ts.and_then(DateTime::from_timestamp_millis),
            date_histogram,
            workspaces,
            agents,
            secret_scan: ScanReportSummary::default(),
            encryption_config: Some(EncryptionSummary::default()),
            key_slots: Vec::new(),
            generated_at: Utc::now(),
        })
    }

    /// Generate a summary with exclusions applied.
    pub fn generate_with_exclusions(
        &self,
        filters: Option<&SummaryFilters>,
        exclusions: &ExclusionSet,
    ) -> Result<PrePublishSummary> {
        let mut summary = self.generate(filters)?;

        // Mark excluded workspaces
        for ws in &mut summary.workspaces {
            ws.included = !exclusions.is_workspace_excluded(&ws.path);
        }

        if exclusions.has_exclusions() {
            // Recalculate counts excluding excluded items
            let recount = self.recalculate_with_exclusions(filters, exclusions)?;

            summary.total_conversations = recount.conversation_ids.len();
            summary.total_messages = recount.total_messages;
            summary.total_characters = recount.total_characters;
            summary.estimated_size_bytes = estimate_compressed_size(recount.total_characters);
            summary.earliest_timestamp = recount
                .earliest_ts
                .and_then(DateTime::from_timestamp_millis);
            summary.latest_timestamp = recount.latest_ts.and_then(DateTime::from_timestamp_millis);
            summary.date_histogram =
                self.get_date_histogram_for_conversation_ids(&recount.conversation_ids)?;
            summary.agents = self.get_agent_summary_for_conversation_ids(
                &recount.conversation_ids,
                summary.total_conversations,
            )?;

            let mut included_workspace_summaries = self
                .get_workspace_summary_for_conversation_ids(&recount.conversation_ids)?
                .into_iter()
                .map(|workspace| (workspace.path.clone(), workspace))
                .collect::<HashMap<_, _>>();
            for workspace in &mut summary.workspaces {
                if !workspace.included {
                    continue;
                }
                if let Some(updated) = included_workspace_summaries.remove(workspace.path.as_str())
                {
                    *workspace = updated;
                } else {
                    workspace.conversation_count = 0;
                    workspace.message_count = 0;
                    workspace.date_range = DateRange {
                        earliest: None,
                        latest: None,
                    };
                    workspace.sample_titles.clear();
                }
                workspace.included = true;
            }
        }

        Ok(summary)
    }

    /// Build filter WHERE clause.
    fn build_filter_clause(&self, filters: &SummaryFilters) -> (String, Vec<ParamValue>) {
        let mut clauses = Vec::new();
        let mut params: Vec<ParamValue> = Vec::new();

        if let Some(agents) = &filters.agents {
            if agents.is_empty() {
                clauses.push("1=0".to_string());
            } else {
                let placeholders: Vec<&str> = (0..agents.len()).map(|_| "?").collect();
                clauses.push(format!(
                    "c.agent_id IN (SELECT id FROM agents WHERE slug IN ({}))",
                    placeholders.join(", ")
                ));
                for agent in agents {
                    params.push(ParamValue::from(agent.as_str()));
                }
            }
        }

        if let Some(workspaces) = &filters.workspaces {
            if workspaces.is_empty() {
                clauses.push("1=0".to_string());
            } else {
                let placeholders: Vec<&str> = (0..workspaces.len()).map(|_| "?").collect();
                clauses.push(format!(
                    "c.workspace_id IN (SELECT id FROM workspaces WHERE path IN ({}))",
                    placeholders.join(", ")
                ));
                for ws in workspaces {
                    params.push(ParamValue::from(ws.as_str()));
                }
            }
        }

        if let Some(since) = filters.since_ts {
            clauses.push("c.started_at >= ?".to_string());
            params.push(ParamValue::from(since));
        }

        if let Some(until) = filters.until_ts {
            clauses.push("c.started_at <= ?".to_string());
            params.push(ParamValue::from(until));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" AND {}", clauses.join(" AND "))
        };

        (where_clause, params)
    }

    /// Count messages across a known set of conversation IDs.
    fn count_messages_for_conversation_ids(&self, conversation_ids: &[i64]) -> Result<usize> {
        let (message_count, _) =
            self.count_messages_and_characters_for_conversation_ids(conversation_ids)?;
        Ok(message_count)
    }

    /// Count messages and characters across a known set of conversation IDs.
    fn count_messages_and_characters_for_conversation_ids(
        &self,
        conversation_ids: &[i64],
    ) -> Result<(usize, usize)> {
        if conversation_ids.is_empty() {
            return Ok((0, 0));
        }

        let mut total_messages = 0usize;
        let mut total_characters = 0usize;
        for chunk in conversation_ids.chunks(SUMMARY_ID_CHUNK_SIZE) {
            let params: Vec<ParamValue> = chunk.iter().copied().map(ParamValue::from).collect();
            let placeholders = vec!["?"; params.len()].join(", ");
            let query = format!(
                "SELECT COUNT(*), SUM(LENGTH(content))
                 FROM messages
                 WHERE conversation_id IN ({placeholders})"
            );
            let (message_count, character_count): (i64, i64) = self
                .db
                .query_map_collect(&query, &params, |row: &Row| {
                    Ok((
                        row.get_typed::<Option<i64>>(0)?.unwrap_or(0),
                        row.get_typed::<Option<i64>>(1)?.unwrap_or(0),
                    ))
                })?
                .into_iter()
                .next()
                .unwrap_or((0, 0));
            total_messages = total_messages.saturating_add(message_count as usize);
            total_characters = total_characters.saturating_add(character_count as usize);
        }

        Ok((total_messages, total_characters))
    }

    /// Get basic counts.
    fn get_counts(
        &self,
        where_clause: &str,
        params: &[ParamValue],
    ) -> Result<(usize, usize, usize)> {
        // Count conversations
        let conv_query = format!(
            "SELECT COUNT(*) FROM conversations c WHERE 1=1{}",
            where_clause
        );
        let total_conversations: i64 = self
            .db
            .query_row_map(&conv_query, params, |row: &Row| row.get_typed(0))
            .context("Failed to count conversations")?;

        // Count messages and characters using a subquery on the filtered conversations.
        let msg_query = format!(
            "SELECT COUNT(*), SUM(LENGTH(content))
             FROM messages
             WHERE conversation_id IN (SELECT c.id FROM conversations c WHERE 1=1{})",
            where_clause
        );
        let (total_messages, total_characters): (i64, i64) = self
            .db
            .query_map_collect(&msg_query, params, |row: &Row| {
                Ok((
                    row.get_typed::<Option<i64>>(0)?.unwrap_or(0),
                    row.get_typed::<Option<i64>>(1)?.unwrap_or(0),
                ))
            })
            .context("Failed to count messages")?
            .into_iter()
            .next()
            .unwrap_or((0, 0));

        Ok((
            total_conversations as usize,
            total_messages as usize,
            total_characters as usize,
        ))
    }

    /// Get time range.
    fn get_time_range(
        &self,
        where_clause: &str,
        params: &[ParamValue],
    ) -> Result<(Option<i64>, Option<i64>)> {
        let query = format!(
            "SELECT MIN(c.started_at), MAX(c.started_at) FROM conversations c WHERE 1=1{}",
            where_clause
        );
        let result: (Option<i64>, Option<i64>) = self
            .db
            .query_row_map(&query, params, |row: &Row| {
                Ok((row.get_typed(0)?, row.get_typed(1)?))
            })
            .context("Failed to get time range")?;
        Ok(result)
    }

    /// Get date histogram.
    fn get_date_histogram(
        &self,
        where_clause: &str,
        params: &[ParamValue],
    ) -> Result<Vec<DateHistogramEntry>> {
        // Compute integer day buckets from millisecond timestamps
        // (seconds-since-epoch / 86400) via a subquery.
        let query = format!(
            "SELECT created_at / 1000 / 86400,
                    COUNT(*)
             FROM messages
             WHERE created_at IS NOT NULL
               AND conversation_id IN (SELECT c.id FROM conversations c WHERE 1=1{})
             GROUP BY created_at / 1000 / 86400
             ORDER BY created_at / 1000 / 86400",
            where_clause
        );

        // Count distinct conversations per day using an explicitly aliased
        // `day_pairs` subquery.
        let conv_query = format!(
            "SELECT day_epoch, COUNT(*)
             FROM (
                SELECT DISTINCT conversation_id, created_at / 1000 / 86400 AS day_epoch
                FROM messages
                WHERE created_at IS NOT NULL
                  AND conversation_id IN (SELECT c.id FROM conversations c WHERE 1=1{})
             ) AS day_pairs
             GROUP BY day_epoch",
            where_clause
        );

        let day_msg_rows = self.db.query_map_collect(&query, params, |row: &Row| {
            let day_epoch: i64 = row.get_typed::<Option<i64>>(0)?.unwrap_or(0);
            let msg_count: i64 = row.get_typed::<Option<i64>>(1)?.unwrap_or(0);
            Ok((day_epoch, msg_count as usize))
        })?;

        let day_conv_rows = self
            .db
            .query_map_collect(&conv_query, params, |row: &Row| {
                let day_epoch: i64 = row.get_typed::<Option<i64>>(0)?.unwrap_or(0);
                let conv_count: i64 = row.get_typed::<Option<i64>>(1)?.unwrap_or(0);
                Ok((day_epoch, conv_count as usize))
            })?;

        let conv_map: std::collections::HashMap<i64, usize> = day_conv_rows.into_iter().collect();

        use chrono::{NaiveDate, TimeDelta};
        let epoch_base = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let entries: Vec<DateHistogramEntry> = day_msg_rows
            .into_iter()
            .map(|(day_epoch, message_count)| {
                let date = epoch_base
                    .checked_add_signed(TimeDelta::days(day_epoch))
                    .map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| format!("{day_epoch}"));
                DateHistogramEntry {
                    date,
                    message_count,
                    conversation_count: conv_map.get(&day_epoch).copied().unwrap_or(0),
                }
            })
            .collect();
        Ok(entries)
    }

    /// Get workspace summary.
    fn get_workspace_summary(
        &self,
        where_clause: &str,
        params: &[ParamValue],
    ) -> Result<Vec<WorkspaceSummaryItem>> {
        let query = format!(
            "SELECT c.id, c.workspace_id, c.title, c.started_at
             FROM conversations c
             WHERE 1=1{}
             ORDER BY c.started_at DESC",
            where_clause
        );

        let conv_rows = self.db.query_map_collect(&query, params, |row: &Row| {
            Ok((
                row.get_typed::<i64>(0)?,
                row.get_typed::<Option<i64>>(1)?,
                row.get_typed::<Option<String>>(2)?,
                row.get_typed::<Option<i64>>(3)?,
            ))
        })?;

        let mut workspace_ids: Vec<i64> = conv_rows
            .iter()
            .filter_map(|(_, workspace_id, _, _)| *workspace_id)
            .collect();
        workspace_ids.sort_unstable();
        workspace_ids.dedup();

        let workspace_map = if workspace_ids.is_empty() {
            HashMap::new()
        } else {
            let workspace_params: Vec<ParamValue> = workspace_ids
                .iter()
                .copied()
                .map(ParamValue::from)
                .collect();
            let placeholders = vec!["?"; workspace_params.len()].join(", ");
            let workspace_query =
                format!("SELECT id, path FROM workspaces WHERE id IN ({placeholders})");
            self.db
                .query_map_collect(&workspace_query, &workspace_params, |row: &Row| {
                    Ok((row.get_typed::<i64>(0)?, row.get_typed::<String>(1)?))
                })?
                .into_iter()
                .collect::<HashMap<_, _>>()
        };

        let mut aggregates: HashMap<String, WorkspaceAggregate> = HashMap::new();
        for (conversation_id, workspace_id, title, started_at) in conv_rows {
            let Some(workspace_id) = workspace_id else {
                continue;
            };
            let Some(workspace) = workspace_map.get(&workspace_id) else {
                continue;
            };

            let aggregate = aggregates.entry(workspace.clone()).or_default();
            aggregate.conversation_ids.push(conversation_id);
            aggregate.min_ts = match (aggregate.min_ts, started_at) {
                (Some(existing), Some(value)) => Some(existing.min(value)),
                (None, value) => value,
                (existing, None) => existing,
            };
            aggregate.max_ts = match (aggregate.max_ts, started_at) {
                (Some(existing), Some(value)) => Some(existing.max(value)),
                (None, value) => value,
                (existing, None) => existing,
            };
            if let Some(title) = title
                && !title.is_empty()
                && aggregate.sample_titles.len() < 5
            {
                aggregate.sample_titles.push(title);
            }
        }

        self.workspace_items_from_aggregates(aggregates)
    }

    /// Get agent summary.
    fn get_agent_summary(
        &self,
        where_clause: &str,
        params: &[ParamValue],
        total_conversations: usize,
    ) -> Result<Vec<AgentSummaryItem>> {
        // LEFT JOIN + COALESCE on agents so the summary correctly bucketizes
        // legacy V1 conversations with NULL agent_id under 'unknown' (and
        // avoids a runtime row-decode crash when c.agent_id is NULL).
        // Fetching the slug directly in the join also removes the need for
        // the separate agent_id -> slug resolution query.
        let query = format!(
            "SELECT c.id, COALESCE(a.slug, 'unknown')
             FROM conversations c
             LEFT JOIN agents a ON c.agent_id = a.id
             WHERE 1=1{}",
            where_clause
        );

        let conv_rows = self.db.query_map_collect(&query, params, |row: &Row| {
            Ok((row.get_typed::<i64>(0)?, row.get_typed::<String>(1)?))
        })?;

        let mut aggregates: HashMap<String, Vec<i64>> = HashMap::new();
        for (conversation_id, agent_slug) in conv_rows {
            aggregates
                .entry(agent_slug)
                .or_default()
                .push(conversation_id);
        }

        let mut agents = Vec::new();
        for (agent, conversation_ids) in aggregates {
            let conv_count = conversation_ids.len();
            let msg_count = self.count_messages_for_conversation_ids(&conversation_ids)?;
            let percentage = if total_conversations > 0 {
                (conv_count as f64 / total_conversations as f64) * 100.0
            } else {
                0.0
            };

            agents.push(AgentSummaryItem {
                name: agent,
                conversation_count: conv_count,
                message_count: msg_count,
                percentage,
                included: true,
            });
        }

        agents.sort_by(|a, b| {
            b.conversation_count
                .cmp(&a.conversation_count)
                .then_with(|| a.name.cmp(&b.name))
        });

        Ok(agents)
    }

    fn get_date_histogram_for_conversation_ids(
        &self,
        conversation_ids: &[i64],
    ) -> Result<Vec<DateHistogramEntry>> {
        if conversation_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut message_counts: HashMap<i64, usize> = HashMap::new();
        let mut conversation_counts: HashMap<i64, HashSet<i64>> = HashMap::new();
        for chunk in conversation_ids.chunks(SUMMARY_ID_CHUNK_SIZE) {
            let params: Vec<ParamValue> = chunk.iter().copied().map(ParamValue::from).collect();
            let placeholders = vec!["?"; params.len()].join(", ");
            let query = format!(
                "SELECT conversation_id, created_at / 1000 / 86400, COUNT(*)
                 FROM messages
                 WHERE created_at IS NOT NULL
                   AND conversation_id IN ({placeholders})
                 GROUP BY conversation_id, created_at / 1000 / 86400"
            );

            for (conversation_id, day_epoch, message_count) in
                self.db.query_map_collect(&query, &params, |row: &Row| {
                    Ok((
                        row.get_typed::<i64>(0)?,
                        row.get_typed::<Option<i64>>(1)?.unwrap_or(0),
                        row.get_typed::<Option<i64>>(2)?.unwrap_or(0) as usize,
                    ))
                })?
            {
                *message_counts.entry(day_epoch).or_insert(0) += message_count;
                conversation_counts
                    .entry(day_epoch)
                    .or_default()
                    .insert(conversation_id);
            }
        }

        use chrono::{NaiveDate, TimeDelta};
        let epoch_base = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let mut days: Vec<_> = message_counts.keys().copied().collect();
        days.sort_unstable();
        Ok(days
            .into_iter()
            .map(|day_epoch| {
                let date = epoch_base
                    .checked_add_signed(TimeDelta::days(day_epoch))
                    .map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| format!("{day_epoch}"));
                DateHistogramEntry {
                    date,
                    message_count: message_counts.get(&day_epoch).copied().unwrap_or(0),
                    conversation_count: conversation_counts
                        .get(&day_epoch)
                        .map(HashSet::len)
                        .unwrap_or(0),
                }
            })
            .collect())
    }

    fn get_workspace_summary_for_conversation_ids(
        &self,
        conversation_ids: &[i64],
    ) -> Result<Vec<WorkspaceSummaryItem>> {
        if conversation_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut aggregates: HashMap<String, WorkspaceAggregate> = HashMap::new();
        for chunk in conversation_ids.chunks(SUMMARY_ID_CHUNK_SIZE) {
            let params: Vec<ParamValue> = chunk.iter().copied().map(ParamValue::from).collect();
            let placeholders = vec!["?"; params.len()].join(", ");
            let query = format!(
                "SELECT c.id, w.path, c.title, c.started_at
                 FROM conversations c
                 JOIN workspaces w ON c.workspace_id = w.id
                 WHERE c.id IN ({placeholders})
                 ORDER BY c.started_at DESC"
            );

            for (conversation_id, workspace, title, started_at) in
                self.db.query_map_collect(&query, &params, |row: &Row| {
                    Ok((
                        row.get_typed::<i64>(0)?,
                        row.get_typed::<String>(1)?,
                        row.get_typed::<Option<String>>(2)?,
                        row.get_typed::<Option<i64>>(3)?,
                    ))
                })?
            {
                let aggregate = aggregates.entry(workspace).or_default();
                aggregate.conversation_ids.push(conversation_id);
                aggregate.min_ts = match (aggregate.min_ts, started_at) {
                    (Some(existing), Some(value)) => Some(existing.min(value)),
                    (None, value) => value,
                    (existing, None) => existing,
                };
                aggregate.max_ts = match (aggregate.max_ts, started_at) {
                    (Some(existing), Some(value)) => Some(existing.max(value)),
                    (None, value) => value,
                    (existing, None) => existing,
                };
                if let Some(title) = title
                    && !title.is_empty()
                    && aggregate.sample_titles.len() < 5
                {
                    aggregate.sample_titles.push(title);
                }
            }
        }

        self.workspace_items_from_aggregates(aggregates)
    }

    fn workspace_items_from_aggregates(
        &self,
        aggregates: HashMap<String, WorkspaceAggregate>,
    ) -> Result<Vec<WorkspaceSummaryItem>> {
        let mut workspaces = Vec::new();
        for (workspace, aggregate) in aggregates {
            let msg_count =
                self.count_messages_for_conversation_ids(&aggregate.conversation_ids)?;
            let display_name = std::path::Path::new(&workspace)
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| workspace.clone());

            workspaces.push(WorkspaceSummaryItem {
                path: workspace,
                display_name,
                conversation_count: aggregate.conversation_ids.len(),
                message_count: msg_count,
                date_range: DateRange::from_timestamps(aggregate.min_ts, aggregate.max_ts),
                sample_titles: aggregate.sample_titles,
                included: true,
            });
        }

        workspaces.sort_by(|a, b| {
            b.conversation_count
                .cmp(&a.conversation_count)
                .then_with(|| a.path.cmp(&b.path))
        });

        Ok(workspaces)
    }

    fn get_agent_summary_for_conversation_ids(
        &self,
        conversation_ids: &[i64],
        total_conversations: usize,
    ) -> Result<Vec<AgentSummaryItem>> {
        if conversation_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut aggregates: HashMap<String, Vec<i64>> = HashMap::new();
        for chunk in conversation_ids.chunks(SUMMARY_ID_CHUNK_SIZE) {
            let params: Vec<ParamValue> = chunk.iter().copied().map(ParamValue::from).collect();
            let placeholders = vec!["?"; params.len()].join(", ");
            let query = format!(
                "SELECT c.id, COALESCE(a.slug, 'unknown')
                 FROM conversations c
                 LEFT JOIN agents a ON c.agent_id = a.id
                 WHERE c.id IN ({placeholders})"
            );

            for (conversation_id, agent_slug) in
                self.db.query_map_collect(&query, &params, |row: &Row| {
                    Ok((row.get_typed::<i64>(0)?, row.get_typed::<String>(1)?))
                })?
            {
                aggregates
                    .entry(agent_slug)
                    .or_default()
                    .push(conversation_id);
            }
        }

        let mut agents = Vec::new();
        for (agent, conversation_ids) in aggregates {
            let conv_count = conversation_ids.len();
            let msg_count = self.count_messages_for_conversation_ids(&conversation_ids)?;
            let percentage = if total_conversations > 0 {
                (conv_count as f64 / total_conversations as f64) * 100.0
            } else {
                0.0
            };

            agents.push(AgentSummaryItem {
                name: agent,
                conversation_count: conv_count,
                message_count: msg_count,
                percentage,
                included: true,
            });
        }

        agents.sort_by(|a, b| {
            b.conversation_count
                .cmp(&a.conversation_count)
                .then_with(|| a.name.cmp(&b.name))
        });

        Ok(agents)
    }

    /// Recalculate counts with exclusions.
    fn recalculate_with_exclusions(
        &self,
        filters: Option<&SummaryFilters>,
        exclusions: &ExclusionSet,
    ) -> Result<ExclusionRecount> {
        let (where_clause, params) = filters
            .map(|active_filters| self.build_filter_clause(active_filters))
            .unwrap_or_default();

        let query = format!(
            "SELECT c.id, c.workspace_id, c.title, c.started_at
             FROM conversations c
             WHERE 1=1{}",
            where_clause
        );

        let conv_rows = self.db.query_map_collect(&query, &params, |row: &Row| {
            Ok((
                row.get_typed::<i64>(0)?,
                row.get_typed::<Option<i64>>(1)?,
                row.get_typed::<Option<String>>(2)?,
                row.get_typed::<Option<i64>>(3)?,
            ))
        })?;
        let mut workspace_ids: Vec<i64> = conv_rows
            .iter()
            .filter_map(|(_, workspace_id, _, _)| *workspace_id)
            .collect();
        workspace_ids.sort_unstable();
        workspace_ids.dedup();
        let workspace_map = if workspace_ids.is_empty() {
            HashMap::new()
        } else {
            let workspace_params: Vec<ParamValue> = workspace_ids
                .iter()
                .copied()
                .map(ParamValue::from)
                .collect();
            let placeholders = vec!["?"; workspace_params.len()].join(", ");
            let workspace_query =
                format!("SELECT id, path FROM workspaces WHERE id IN ({placeholders})");
            self.db
                .query_map_collect(&workspace_query, &workspace_params, |row: &Row| {
                    Ok((row.get_typed::<i64>(0)?, row.get_typed::<String>(1)?))
                })?
                .into_iter()
                .collect()
        };

        let mut included_conversation_ids = Vec::new();
        let mut earliest_ts: Option<i64> = None;
        let mut latest_ts: Option<i64> = None;
        for (id, workspace_id, title, started_at) in conv_rows {
            let workspace = workspace_id.and_then(|id| workspace_map.get(&id).cloned());
            let title_str = title.as_deref().unwrap_or("");

            if exclusions.should_exclude(workspace.as_deref(), id, title_str) {
                continue;
            }

            included_conversation_ids.push(id);
            earliest_ts = match (earliest_ts, started_at) {
                (Some(existing), Some(value)) => Some(existing.min(value)),
                (None, value) => value,
                (existing, None) => existing,
            };
            latest_ts = match (latest_ts, started_at) {
                (Some(existing), Some(value)) => Some(existing.max(value)),
                (None, value) => value,
                (existing, None) => existing,
            };
        }

        if included_conversation_ids.is_empty() {
            return Ok(ExclusionRecount::default());
        }

        let (total_messages, total_characters) =
            self.count_messages_and_characters_for_conversation_ids(&included_conversation_ids)?;

        Ok(ExclusionRecount {
            conversation_ids: included_conversation_ids,
            total_messages,
            total_characters,
            earliest_ts,
            latest_ts,
        })
    }
}
