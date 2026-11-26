use super::storage_core::FrankenStorage;
use crate::franken_sync::{Connection as FrankenConnection, compat::RowExt as FrankenRowExt};
use crate::model::types::Agent;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

// SqliteStorage impl block removed: SqliteStorage is now a type alias for FrankenStorage.
// All methods are available through FrankenStorage.

// -------------------------------------------------------------------------
// IndexingCache (Opt 7.2) - N+1 Prevention for Agent/Workspace IDs
// -------------------------------------------------------------------------

/// Cache for agent and workspace IDs during batch indexing.
///
/// Prevents N+1 database queries by caching the results of ensure_agent
/// and ensure_workspace calls within a batch. This is per-batch and
/// single-threaded, so no synchronization is needed.
///
/// # Usage
/// ```ignore
/// let mut cache = IndexingCache::new();
/// for conv in conversations {
///     let agent_id = cache.get_or_insert_agent(storage, &agent)?;
///     let workspace_id = cache.get_or_insert_workspace(storage, workspace)?;
///     // ... use agent_id and workspace_id
/// }
/// ```
///
/// # Rollback
/// Set environment variable `CASS_SQLITE_CACHE=0` to bypass caching
/// and use direct DB calls (useful for debugging).
#[derive(Debug, Default)]
pub struct IndexingCache {
    agent_ids: HashMap<String, i64>,
    workspace_ids: HashMap<PathBuf, i64>,
    hits: u64,
    misses: u64,
}

pub trait IndexingCacheStorage {
    fn ensure_indexing_agent(&self, agent: &Agent) -> Result<i64>;
    fn ensure_indexing_workspace(&self, path: &Path, display_name: Option<&str>) -> Result<i64>;
}

impl IndexingCacheStorage for FrankenStorage {
    fn ensure_indexing_agent(&self, agent: &Agent) -> Result<i64> {
        self.ensure_agent(agent)
    }

    fn ensure_indexing_workspace(&self, path: &Path, display_name: Option<&str>) -> Result<i64> {
        self.ensure_workspace(path, display_name)
    }
}

// IndexingCacheStorage for SqliteStorage removed: SqliteStorage is a type alias for FrankenStorage.

impl IndexingCache {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Self {
            agent_ids: HashMap::new(),
            workspace_ids: HashMap::new(),
            hits: 0,
            misses: 0,
        }
    }

    /// Check if caching is enabled via environment variable.
    /// Returns true unless CASS_SQLITE_CACHE is set to "0" or "false".
    pub fn is_enabled() -> bool {
        Self::is_enabled_from(dotenvy::var("CASS_SQLITE_CACHE").ok().as_deref())
    }

    /// Pure half of [`Self::is_enabled`] over an injected raw value, so the
    /// truthiness contract is testable without mutating process-global
    /// environment (qu81y).
    pub fn is_enabled_from(raw: Option<&str>) -> bool {
        raw.map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true)
    }

    /// Get or insert an agent ID, using cache if available.
    ///
    /// Returns the cached ID if present, otherwise calls ensure_agent
    /// and caches the result.
    pub fn get_or_insert_agent<S>(&mut self, storage: &S, agent: &Agent) -> Result<i64>
    where
        S: IndexingCacheStorage + ?Sized,
    {
        if let Some(&cached) = self.agent_ids.get(&agent.slug) {
            self.hits += 1;
            return Ok(cached);
        }

        self.misses += 1;
        let id = storage.ensure_indexing_agent(agent)?;
        self.agent_ids.insert(agent.slug.clone(), id);
        Ok(id)
    }

    /// Get or insert a workspace ID, using cache if available.
    ///
    /// Returns the cached ID if present, otherwise calls ensure_workspace
    /// and caches the result.
    pub fn get_or_insert_workspace(
        &mut self,
        storage: &(impl IndexingCacheStorage + ?Sized),
        path: &Path,
        display_name: Option<&str>,
    ) -> Result<i64> {
        if let Some(&cached) = self.workspace_ids.get(path) {
            self.hits += 1;
            return Ok(cached);
        }

        self.misses += 1;
        let id = storage.ensure_indexing_workspace(path, display_name)?;
        self.workspace_ids.insert(path.to_path_buf(), id);
        Ok(id)
    }

    /// Get cache statistics: (hits, misses, hit_rate).
    pub fn stats(&self) -> (u64, u64, f64) {
        let total = self.hits + self.misses;
        let hit_rate = if total > 0 {
            self.hits as f64 / total as f64
        } else {
            0.0
        };
        (self.hits, self.misses, hit_rate)
    }

    /// Clear the cache, resetting all state.
    pub fn clear(&mut self) {
        self.agent_ids.clear();
        self.workspace_ids.clear();
        self.hits = 0;
        self.misses = 0;
    }

    /// Number of cached agents.
    pub fn agent_count(&self) -> usize {
        self.agent_ids.len()
    }

    /// Number of cached workspaces.
    pub fn workspace_count(&self) -> usize {
        self.workspace_ids.len()
    }
}

// -------------------------------------------------------------------------
// StatsAggregator (kzxu) - Batched Daily Stats Updates
// -------------------------------------------------------------------------
// Aggregates daily stats in memory during batch ingestion, then flushes
// to the database in a single batched INSERT...ON CONFLICT operation.
// This prevents N×4 database writes (4 permutations per conversation).

/// Accumulated statistics delta for a single (day_id, agent, source) combination.
#[derive(Clone, Copy, Debug, Default)]
pub struct StatsDelta {
    pub session_count_delta: i64,
    pub message_count_delta: i64,
    pub total_chars_delta: i64,
}

/// In-memory aggregator for batched daily stats updates.
///
/// During batch ingestion, we accumulate deltas per (day_id, agent, source) key.
/// After processing all conversations, call `expand()` to generate the 4
/// permutations per raw entry, then flush via `SqliteStorage::update_daily_stats_batched`.
///
/// # Example
/// ```ignore
/// let mut agg = StatsAggregator::new();
/// for conv in conversations {
///     agg.record(&conv.agent_slug, source_id, day_id, msg_count, char_count);
/// }
/// let entries = agg.expand();
/// storage.update_daily_stats_batched(&entries)?;
/// ```
#[derive(Debug, Default)]
pub struct StatsAggregator {
    /// Raw deltas keyed by (day_id, agent_slug, source_id).
    /// Only stores specific (non-"all") combinations.
    deltas: HashMap<(i64, String, String), StatsDelta>,
}

impl StatsAggregator {
    /// Create a new empty aggregator.
    pub fn new() -> Self {
        Self {
            deltas: HashMap::new(),
        }
    }

    /// Record a conversation's contribution to stats (session + messages + chars).
    ///
    /// This increments session_count by 1.
    ///
    /// # Arguments
    /// * `agent_slug` - The specific agent slug (not "all")
    /// * `source_id` - The specific source ID (not "all")
    /// * `day_id` - Days since 2020-01-01 (from `SqliteStorage::day_id_from_millis`)
    /// * `message_count` - Number of messages in the conversation
    /// * `total_chars` - Total character count across all messages
    pub fn record(
        &mut self,
        agent_slug: &str,
        source_id: &str,
        day_id: i64,
        message_count: i64,
        total_chars: i64,
    ) {
        self.record_delta(agent_slug, source_id, day_id, 1, message_count, total_chars);
    }

    /// Record an arbitrary delta. Use this for append-only updates where
    /// `session_count_delta` may be 0 but message/char deltas are non-zero.
    pub fn record_delta(
        &mut self,
        agent_slug: &str,
        source_id: &str,
        day_id: i64,
        session_count_delta: i64,
        message_count_delta: i64,
        total_chars_delta: i64,
    ) {
        if session_count_delta == 0 && message_count_delta == 0 && total_chars_delta == 0 {
            return;
        }
        let key = (day_id, agent_slug.to_owned(), source_id.to_owned());
        let delta = self.deltas.entry(key).or_default();
        delta.session_count_delta += session_count_delta;
        delta.message_count_delta += message_count_delta;
        delta.total_chars_delta += total_chars_delta;
    }

    /// Expand raw deltas into the 4 permutation keys:
    /// - (agent, source) - specific both
    /// - ("all", source) - all agents, specific source
    /// - (agent, "all") - specific agent, all sources
    /// - ("all", "all") - totals
    ///
    /// Returns entries sorted by (day_id, agent_slug, source_id) for deterministic batching.
    pub fn expand(&self) -> Vec<(i64, String, String, StatsDelta)> {
        let mut expanded: HashMap<(i64, String, String), StatsDelta> = HashMap::new();

        for ((day_id, agent, source), delta) in &self.deltas {
            let permutations = [
                (agent.as_str(), source.as_str()),
                ("all", source.as_str()),
                (agent.as_str(), "all"),
                ("all", "all"),
            ];

            // Ensure we don't double-apply deltas if agent/source is already "all".
            for idx in 0..permutations.len() {
                let (a, s) = permutations[idx];
                if permutations[..idx].contains(&(a, s)) {
                    continue;
                }
                let key = (*day_id, a.to_owned(), s.to_owned());
                let entry = expanded.entry(key).or_default();
                entry.session_count_delta += delta.session_count_delta;
                entry.message_count_delta += delta.message_count_delta;
                entry.total_chars_delta += delta.total_chars_delta;
            }
        }

        let mut out: Vec<(i64, String, String, StatsDelta)> = expanded
            .into_iter()
            .map(|((d, a, s), delta)| (d, a, s, delta))
            .collect();
        out.sort_by(|(d1, a1, s1, _), (d2, a2, s2, _)| {
            d1.cmp(d2).then_with(|| a1.cmp(a2)).then_with(|| s1.cmp(s2))
        });
        out
    }

    /// Check if the aggregator is empty (no data recorded).
    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    /// Get number of distinct raw (day, agent, source) combinations recorded.
    pub fn raw_entry_count(&self) -> usize {
        self.deltas.len()
    }
}

// -------------------------------------------------------------------------
// TokenStatsAggregator — Batched Token Analytics Daily Stats
// -------------------------------------------------------------------------
// Mirrors StatsAggregator pattern for token-level metrics.
// Aggregates token usage in memory during batch ingestion, then flushes
// to token_daily_stats in a single batched INSERT...ON CONFLICT operation.

/// Accumulated token statistics delta for a single (day_id, agent, source, model_family) combination.
#[derive(Clone, Debug, Default)]
pub struct TokenStatsDelta {
    pub api_call_count: i64,
    pub user_message_count: i64,
    pub assistant_message_count: i64,
    pub tool_message_count: i64,
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_cache_read_tokens: i64,
    pub total_cache_creation_tokens: i64,
    pub total_thinking_tokens: i64,
    pub grand_total_tokens: i64,
    pub total_content_chars: i64,
    pub total_tool_calls: i64,
    pub estimated_cost_usd: f64,
    pub session_count: i64,
}

/// In-memory aggregator for batched token daily stats updates.
///
/// During batch ingestion, accumulate token deltas per (day_id, agent, source, model_family) key.
/// After processing, call `expand()` to generate the 5 permutation keys, then flush via
/// `update_token_daily_stats_batched_in_tx`.
#[derive(Debug, Default)]
pub struct TokenStatsAggregator {
    /// Raw deltas keyed by (day_id, agent_slug, source_id, model_family).
    pub(super) deltas: HashMap<(i64, String, String, String), TokenStatsDelta>,
}

impl TokenStatsAggregator {
    pub fn new() -> Self {
        Self {
            deltas: HashMap::new(),
        }
    }

    /// Record a single message's token contribution.
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &mut self,
        agent_slug: &str,
        source_id: &str,
        day_id: i64,
        model_family: &str,
        role: &str,
        usage: &crate::connectors::ExtractedTokenUsage,
        content_chars: i64,
        estimated_cost_usd: f64,
    ) {
        let key = (
            day_id,
            agent_slug.to_owned(),
            source_id.to_owned(),
            model_family.to_owned(),
        );
        let delta = self.deltas.entry(key).or_default();

        delta.api_call_count += 1;
        match role {
            "user" => delta.user_message_count += 1,
            "assistant" | "agent" => delta.assistant_message_count += 1,
            "tool" => delta.tool_message_count += 1,
            _ => {}
        }

        delta.total_input_tokens += usage.input_tokens.unwrap_or(0);
        delta.total_output_tokens += usage.output_tokens.unwrap_or(0);
        delta.total_cache_read_tokens += usage.cache_read_tokens.unwrap_or(0);
        delta.total_cache_creation_tokens += usage.cache_creation_tokens.unwrap_or(0);
        delta.total_thinking_tokens += usage.thinking_tokens.unwrap_or(0);
        delta.grand_total_tokens += usage.total_tokens().unwrap_or(0);
        delta.total_content_chars += content_chars;
        delta.total_tool_calls += usage.tool_call_count as i64;
        delta.estimated_cost_usd += estimated_cost_usd;
    }

    /// Record a session count bump for a given day/agent/source/model.
    pub fn record_session(
        &mut self,
        agent_slug: &str,
        source_id: &str,
        day_id: i64,
        model_family: &str,
    ) {
        let key = (
            day_id,
            agent_slug.to_owned(),
            source_id.to_owned(),
            model_family.to_owned(),
        );
        self.deltas.entry(key).or_default().session_count += 1;
    }

    /// Expand raw deltas into 5 permutation keys for the 4-dimensional composite PK:
    /// - (agent, source, model)  — specific all three
    /// - ("all", source, model)  — all agents
    /// - (agent, "all", model)   — all sources
    /// - (agent, source, "all")  — all models
    /// - ("all", "all", "all")   — global total
    pub fn expand(&self) -> Vec<(i64, String, String, String, TokenStatsDelta)> {
        let mut expanded: HashMap<(i64, String, String, String), TokenStatsDelta> = HashMap::new();

        for ((day_id, agent, source, model), delta) in &self.deltas {
            let permutations = [
                (agent.as_str(), source.as_str(), model.as_str()),
                ("all", source.as_str(), model.as_str()),
                (agent.as_str(), "all", model.as_str()),
                (agent.as_str(), source.as_str(), "all"),
                ("all", "all", "all"),
            ];

            for idx in 0..permutations.len() {
                let (a, s, m) = permutations[idx];
                // Deduplicate if agent/source/model is already "all"
                if permutations[..idx].contains(&(a, s, m)) {
                    continue;
                }
                let key = (*day_id, a.to_owned(), s.to_owned(), m.to_owned());
                let entry = expanded.entry(key).or_default();
                entry.api_call_count += delta.api_call_count;
                entry.user_message_count += delta.user_message_count;
                entry.assistant_message_count += delta.assistant_message_count;
                entry.tool_message_count += delta.tool_message_count;
                entry.total_input_tokens += delta.total_input_tokens;
                entry.total_output_tokens += delta.total_output_tokens;
                entry.total_cache_read_tokens += delta.total_cache_read_tokens;
                entry.total_cache_creation_tokens += delta.total_cache_creation_tokens;
                entry.total_thinking_tokens += delta.total_thinking_tokens;
                entry.grand_total_tokens += delta.grand_total_tokens;
                entry.total_content_chars += delta.total_content_chars;
                entry.total_tool_calls += delta.total_tool_calls;
                entry.estimated_cost_usd += delta.estimated_cost_usd;
                entry.session_count += delta.session_count;
            }
        }

        let mut out: Vec<(i64, String, String, String, TokenStatsDelta)> = expanded
            .into_iter()
            .map(|((d, a, s, m), delta)| (d, a, s, m, delta))
            .collect();
        out.sort_by(|(d1, a1, s1, m1, _), (d2, a2, s2, m2, _)| {
            d1.cmp(d2)
                .then_with(|| a1.cmp(a2))
                .then_with(|| s1.cmp(s2))
                .then_with(|| m1.cmp(m2))
        });
        out
    }

    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    pub fn raw_entry_count(&self) -> usize {
        self.deltas.len()
    }
}

// -------------------------------------------------------------------------
// AnalyticsRollupAggregator — Batched usage_hourly + usage_daily Updates
// -------------------------------------------------------------------------
// Accumulates per-message deltas in memory, then flushes to both
// usage_hourly and usage_daily in a single batched operation.

/// Delta for a single (bucket, agent_slug, workspace_id, source_id) rollup key.
#[derive(Clone, Debug, Default)]
pub struct UsageRollupDelta {
    pub message_count: i64,
    pub user_message_count: i64,
    pub assistant_message_count: i64,
    pub tool_call_count: i64,
    pub plan_message_count: i64,
    pub plan_content_tokens_est_total: i64,
    pub plan_api_tokens_total: i64,
    pub api_coverage_message_count: i64,
    pub content_tokens_est_total: i64,
    pub content_tokens_est_user: i64,
    pub content_tokens_est_assistant: i64,
    pub api_tokens_total: i64,
    pub api_input_tokens_total: i64,
    pub api_output_tokens_total: i64,
    pub api_cache_read_tokens_total: i64,
    pub api_cache_creation_tokens_total: i64,
    pub api_thinking_tokens_total: i64,
}

/// Pending message_metrics row for batch insertion.
#[derive(Debug, Clone)]
pub struct MessageMetricsEntry {
    pub message_id: i64,
    pub created_at_ms: i64,
    pub hour_id: i64,
    pub day_id: i64,
    pub agent_slug: String,
    pub workspace_id: i64,
    pub source_id: String,
    pub role: String,
    pub content_chars: i64,
    pub content_tokens_est: i64,
    pub model_name: Option<String>,
    pub model_family: String,
    pub model_tier: String,
    pub provider: String,
    pub api_input_tokens: Option<i64>,
    pub api_output_tokens: Option<i64>,
    pub api_cache_read_tokens: Option<i64>,
    pub api_cache_creation_tokens: Option<i64>,
    pub api_thinking_tokens: Option<i64>,
    pub api_service_tier: Option<String>,
    pub api_data_source: String,
    pub tool_call_count: i64,
    pub has_tool_calls: bool,
    pub has_plan: bool,
}

/// In-memory aggregator for batched usage_hourly and usage_daily rollup updates.
///
/// Keyed by (bucket_id, agent_slug, workspace_id, source_id).
/// Maintains separate hourly and daily delta maps.
#[derive(Debug, Default)]
pub struct AnalyticsRollupAggregator {
    pub(super) hourly: HashMap<(i64, String, i64, String), UsageRollupDelta>,
    pub(super) daily: HashMap<(i64, String, i64, String), UsageRollupDelta>,
    pub(super) models_daily: HashMap<(i64, String, i64, String, String, String), UsageRollupDelta>,
}

impl AnalyticsRollupAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a single message's contribution to both hourly and daily rollups.
    pub fn record(&mut self, entry: &MessageMetricsEntry) {
        let content_est = entry.content_tokens_est;
        let api_total = entry.api_input_tokens.unwrap_or(0)
            + entry.api_output_tokens.unwrap_or(0)
            + entry.api_cache_read_tokens.unwrap_or(0)
            + entry.api_cache_creation_tokens.unwrap_or(0)
            + entry.api_thinking_tokens.unwrap_or(0);
        let is_api = entry.api_data_source == "api";
        let is_user = entry.role == "user";
        let is_assistant = entry.role == "assistant" || entry.role == "agent";

        // Apply to both hourly and daily
        for (map, bucket_id) in [
            (&mut self.hourly, entry.hour_id),
            (&mut self.daily, entry.day_id),
        ] {
            let key = (
                bucket_id,
                entry.agent_slug.clone(),
                entry.workspace_id,
                entry.source_id.clone(),
            );
            let d = map.entry(key).or_default();
            d.message_count += 1;
            if is_user {
                d.user_message_count += 1;
                d.content_tokens_est_user += content_est;
            }
            if is_assistant {
                d.assistant_message_count += 1;
                d.content_tokens_est_assistant += content_est;
            }
            d.tool_call_count += entry.tool_call_count;
            if entry.has_plan {
                d.plan_message_count += 1;
                d.plan_content_tokens_est_total += content_est;
                if is_api {
                    d.plan_api_tokens_total += api_total;
                }
            }
            if is_api {
                d.api_coverage_message_count += 1;
                d.api_tokens_total += api_total;
                d.api_input_tokens_total += entry.api_input_tokens.unwrap_or(0);
                d.api_output_tokens_total += entry.api_output_tokens.unwrap_or(0);
                d.api_cache_read_tokens_total += entry.api_cache_read_tokens.unwrap_or(0);
                d.api_cache_creation_tokens_total += entry.api_cache_creation_tokens.unwrap_or(0);
                d.api_thinking_tokens_total += entry.api_thinking_tokens.unwrap_or(0);
            }
            d.content_tokens_est_total += content_est;
        }

        let model_key = (
            entry.day_id,
            entry.agent_slug.clone(),
            entry.workspace_id,
            entry.source_id.clone(),
            entry.model_family.clone(),
            entry.model_tier.clone(),
        );
        let d = self.models_daily.entry(model_key).or_default();
        d.message_count += 1;
        if is_user {
            d.user_message_count += 1;
            d.content_tokens_est_user += content_est;
        }
        if is_assistant {
            d.assistant_message_count += 1;
            d.content_tokens_est_assistant += content_est;
        }
        d.tool_call_count += entry.tool_call_count;
        if entry.has_plan {
            d.plan_message_count += 1;
            d.plan_content_tokens_est_total += content_est;
            if is_api {
                d.plan_api_tokens_total += api_total;
            }
        }
        if is_api {
            d.api_coverage_message_count += 1;
            d.api_tokens_total += api_total;
            d.api_input_tokens_total += entry.api_input_tokens.unwrap_or(0);
            d.api_output_tokens_total += entry.api_output_tokens.unwrap_or(0);
            d.api_cache_read_tokens_total += entry.api_cache_read_tokens.unwrap_or(0);
            d.api_cache_creation_tokens_total += entry.api_cache_creation_tokens.unwrap_or(0);
            d.api_thinking_tokens_total += entry.api_thinking_tokens.unwrap_or(0);
        }
        d.content_tokens_est_total += content_est;
    }

    pub fn is_empty(&self) -> bool {
        self.hourly.is_empty() && self.daily.is_empty() && self.models_daily.is_empty()
    }

    pub fn hourly_entry_count(&self) -> usize {
        self.hourly.len()
    }

    pub fn daily_entry_count(&self) -> usize {
        self.daily.len()
    }

    pub fn models_daily_entry_count(&self) -> usize {
        self.models_daily.len()
    }
}

/// Whether the current role should be considered for plan attribution.
///
/// Plan attribution v2 defaults to assistant/agent messages only.
pub(super) fn has_plan_for_role(role: &str, content: &str) -> bool {
    let role = role.trim();
    (role.eq_ignore_ascii_case("assistant") || role.eq_ignore_ascii_case("agent"))
        && has_plan_heuristic(content)
}

/// Heuristic to detect "plan" messages.
///
/// v2 behavior:
/// - Require an explicit plan marker near the top of the message.
/// - Require structured steps (numbered or bullets) to reduce false positives.
/// - Avoid classifying tool-output blobs as plans.
pub(super) fn has_plan_heuristic(content: &str) -> bool {
    if content.len() < 24 {
        return false;
    }

    let lower = content.to_lowercase();

    // Ignore tool-output-like blobs unless they also have a strong plan header.
    let looks_like_tool_blob = lower.contains("```")
        || lower.contains("\"tool\"")
        || lower.contains("stdout:")
        || lower.contains("stderr:")
        || lower.contains("exit code:");

    let mut lines: Vec<&str> = Vec::with_capacity(60);
    let mut in_fenced_code = false;
    for raw in lower.lines() {
        let line = raw.trim();
        if line.starts_with("```") {
            in_fenced_code = !in_fenced_code;
            continue;
        }
        if in_fenced_code || line.is_empty() {
            continue;
        }
        lines.push(line);
        if lines.len() >= 60 {
            break;
        }
    }

    let header_pos = lines.iter().position(|line| {
        line.starts_with("## plan")
            || line.starts_with("# plan")
            || line.starts_with("plan:")
            || line.starts_with("implementation plan")
            || line.starts_with("next steps:")
            || line.starts_with("action plan:")
    });
    let preview_top = lines.iter().take(8).copied().collect::<Vec<_>>().join("\n");
    let header_near_top = header_pos.is_some_and(|idx| idx <= 6) || preview_top.contains("plan:");

    if !header_near_top {
        return false;
    }
    if looks_like_tool_blob && header_pos.is_none() {
        return false;
    }

    let numbered_steps = lines
        .iter()
        .filter(|line| is_numbered_step_line(line))
        .count();
    let bullet_steps = lines
        .iter()
        .filter(|line| {
            line.starts_with("- ")
                || line.starts_with("* ")
                || line.starts_with("+ ")
                || line.starts_with("- [ ] ")
                || line.starts_with("- [x] ")
        })
        .count();

    numbered_steps >= 2 || (numbered_steps >= 1 && bullet_steps >= 1) || bullet_steps >= 3
}

fn is_numbered_step_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    let digit_count = trimmed.chars().take_while(|c| c.is_ascii_digit()).count();
    if digit_count == 0 || digit_count > 3 {
        return false;
    }
    let rest = &trimmed[digit_count..];
    rest.starts_with(". ") || rest.starts_with(") ")
}

/// Pending token_usage row to be batch-inserted.
#[derive(Debug, Clone)]
pub struct TokenUsageEntry {
    pub message_id: i64,
    pub conversation_id: i64,
    pub agent_id: i64,
    pub workspace_id: Option<i64>,
    pub source_id: String,
    pub timestamp_ms: i64,
    pub day_id: i64,
    pub model_name: Option<String>,
    pub model_family: Option<String>,
    pub model_tier: Option<String>,
    pub service_tier: Option<String>,
    pub provider: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_creation_tokens: Option<i64>,
    pub thinking_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub estimated_cost_usd: Option<f64>,
    pub role: String,
    pub content_chars: i64,
    pub has_tool_calls: bool,
    pub tool_call_count: u32,
    pub data_source: String,
}

// -------------------------------------------------------------------------
// PricingTable — In-memory cache for model_pricing lookups (bead z9fse.10)
// -------------------------------------------------------------------------

/// One pricing row loaded from the `model_pricing` table.
#[derive(Debug, Clone)]
pub struct PricingEntry {
    pub model_pattern: String,
    pub provider: String,
    pub input_cost_per_mtok: f64,
    pub output_cost_per_mtok: f64,
    pub cache_read_cost_per_mtok: Option<f64>,
    pub cache_creation_cost_per_mtok: Option<f64>,
    /// Effective date as day_id (days since 2020-01-01).
    pub effective_day_id: i64,
}

/// Diagnostics for pricing coverage during a batch operation.
#[derive(Debug, Clone, Default)]
pub struct PricingDiagnostics {
    pub priced_count: u64,
    pub unpriced_count: u64,
    /// Top unknown model names → count.
    pub unknown_models: HashMap<String, u64>,
}

impl PricingDiagnostics {
    pub(super) fn record_priced(&mut self) {
        self.priced_count += 1;
    }

    pub(super) fn record_unpriced(&mut self, model_name: Option<&str>) {
        self.unpriced_count += 1;
        let key = model_name.unwrap_or("(none)").to_string();
        *self.unknown_models.entry(key).or_insert(0) += 1;
    }

    /// Log a summary of pricing coverage.
    pub fn log_summary(&self) {
        let total = self.priced_count + self.unpriced_count;
        if total == 0 {
            return;
        }
        let pct = (self.priced_count as f64 / total as f64) * 100.0;
        tracing::info!(
            target: "cass::analytics::pricing",
            priced = self.priced_count,
            unpriced = self.unpriced_count,
            total = total,
            coverage_pct = format!("{pct:.1}%"),
            "pricing coverage"
        );
        if !self.unknown_models.is_empty() {
            let mut sorted: Vec<_> = self.unknown_models.iter().collect();
            sorted.sort_by(|a, b| b.1.cmp(a.1));
            for (model, count) in sorted.iter().take(5) {
                tracing::debug!(
                    target: "cass::analytics::pricing",
                    model = model.as_str(),
                    count = count,
                    "unknown model (no pricing)"
                );
            }
        }
    }
}

/// In-memory pricing table loaded from `model_pricing` for fast lookups.
#[derive(Debug, Clone)]
pub struct PricingTable {
    pub(super) entries: Vec<PricingEntry>,
}

impl PricingTable {
    /// Load all pricing entries from the database.
    pub fn load(conn: &FrankenConnection) -> Result<Self> {
        Self::franken_load(conn)
    }

    /// Load all pricing entries from a SQLite connection.
    pub fn franken_load(conn: &FrankenConnection) -> Result<Self> {
        let rows = conn.query(
            "SELECT model_pattern, provider, input_cost_per_mtok, output_cost_per_mtok,
                    cache_read_cost_per_mtok, cache_creation_cost_per_mtok, effective_date
             FROM model_pricing
             ORDER BY effective_date DESC",
        )?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            let effective_date: String = row.get_typed(6)?;
            let effective_day_id = date_str_to_day_id(&effective_date)?;
            entries.push(PricingEntry {
                model_pattern: row.get_typed(0)?,
                provider: row.get_typed(1)?,
                input_cost_per_mtok: row.get_typed(2)?,
                output_cost_per_mtok: row.get_typed(3)?,
                cache_read_cost_per_mtok: row.get_typed(4)?,
                cache_creation_cost_per_mtok: row.get_typed(5)?,
                effective_day_id,
            });
        }
        Ok(Self { entries })
    }

    /// Look up the best pricing entry for a given model name and date.
    ///
    /// Selection rules:
    /// 1. Pattern must match model_name (SQL LIKE semantics).
    /// 2. effective_day_id must be <= message_day_id.
    /// 3. Among matches, prefer the most recent effective_date.
    /// 4. Tie-break by pattern specificity (longest pattern wins).
    pub fn lookup(&self, model_name: &str, message_day_id: i64) -> Option<&PricingEntry> {
        let mut best: Option<&PricingEntry> = None;

        for entry in &self.entries {
            if entry.effective_day_id > message_day_id {
                continue;
            }
            if !sql_like_match(model_name, &entry.model_pattern) {
                continue;
            }

            match best {
                None => best = Some(entry),
                Some(current) => {
                    if entry.effective_day_id > current.effective_day_id
                        || (entry.effective_day_id == current.effective_day_id
                            && entry.model_pattern.len() > current.model_pattern.len())
                    {
                        best = Some(entry);
                    }
                }
            }
        }

        best
    }

    /// Compute estimated cost in USD for a set of token counts.
    ///
    /// Returns `None` if no pricing entry matches or if no token counts are available.
    pub fn compute_cost(
        &self,
        model_name: Option<&str>,
        message_day_id: i64,
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
        cache_read_tokens: Option<i64>,
        cache_creation_tokens: Option<i64>,
    ) -> Option<f64> {
        let model = model_name?;
        let pricing = self.lookup(model, message_day_id)?;

        if input_tokens.is_none() && output_tokens.is_none() {
            return None;
        }

        let mut cost = 0.0;
        let cache_read = cache_read_tokens.unwrap_or(0);
        let cache_creation = cache_creation_tokens.unwrap_or(0);
        // input_tokens includes cache tokens as a subset; subtract them
        // so we don't charge at both the full input rate AND the cache rate.
        let non_cache_input = input_tokens
            .unwrap_or(0)
            .saturating_sub(cache_read)
            .saturating_sub(cache_creation)
            .max(0);
        cost += non_cache_input as f64 * pricing.input_cost_per_mtok / 1_000_000.0;
        cost += output_tokens.unwrap_or(0) as f64 * pricing.output_cost_per_mtok / 1_000_000.0;

        if let Some(cache_price) = pricing.cache_read_cost_per_mtok {
            cost += cache_read as f64 * cache_price / 1_000_000.0;
        }
        if let Some(cache_price) = pricing.cache_creation_cost_per_mtok {
            cost += cache_creation as f64 * cache_price / 1_000_000.0;
        }

        Some(cost)
    }

    /// Whether the pricing table has any entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Convert "YYYY-MM-DD" date string to day_id (days since 2020-01-01),
/// matching the format produced by `day_id_from_millis`.
pub(super) fn date_str_to_day_id(s: &str) -> Result<i64> {
    use chrono::NaiveDate;
    const EPOCH_2020: NaiveDate = match NaiveDate::from_ymd_opt(2020, 1, 1) {
        Some(d) => d,
        None => unreachable!(),
    };
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map(|d| (d - EPOCH_2020).num_days())
        .with_context(|| format!("invalid effective_date '{s}'"))
}

/// SQL LIKE pattern matcher (case-insensitive). `%` = any sequence, `_` = any single char.
pub(super) fn sql_like_match(value: &str, pattern: &str) -> bool {
    sql_like_match_bytes(
        value.to_ascii_lowercase().as_bytes(),
        pattern.to_ascii_lowercase().as_bytes(),
    )
}

/// Determine the byte length of the UTF-8 character starting at `b`.
fn utf8_char_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b < 0xE0 {
        2
    } else if b < 0xF0 {
        3
    } else {
        4
    }
}

fn sql_like_match_bytes(val: &[u8], pat: &[u8]) -> bool {
    if pat.is_empty() {
        return val.is_empty();
    }
    match pat[0] {
        b'%' => {
            let mut p = 1;
            while p < pat.len() && pat[p] == b'%' {
                p += 1;
            }
            let rest = &pat[p..];
            // Iterate only at UTF-8 char boundaries
            let mut i = 0;
            while i <= val.len() {
                if sql_like_match_bytes(&val[i..], rest) {
                    return true;
                }
                if i < val.len() {
                    i += utf8_char_len(val[i]);
                } else {
                    break;
                }
            }
            false
        }
        b'_' => {
            // Match one full UTF-8 character, not just one byte
            if val.is_empty() {
                return false;
            }
            let char_len = utf8_char_len(val[0]);
            val.len() >= char_len && sql_like_match_bytes(&val[char_len..], &pat[1..])
        }
        c => !val.is_empty() && val[0] == c && sql_like_match_bytes(&val[1..], &pat[1..]),
    }
}
