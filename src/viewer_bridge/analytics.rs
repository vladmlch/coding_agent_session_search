//! SQL analytics for the `cass_owned_queries_v1` read lane (Task 10).
//!
//! `projects` / `dashboard` / `health` aggregate over canonical columns plus
//! `viewer_session_facts`/`viewer_session_metrics` only — a fixed scope is
//! resolved ONCE into provider/project/activity/subagent predicates and the
//! identical clause is bound into every statement of the request (oracle:
//! `_build_filter_clause` + `resolve_analytics_scope`). Nothing reparses
//! source journals and nothing materializes the catalog.
//!
//! Semantics mirror `viewer/storage/analytics/*` and the CASS-lane
//! `session_metrics.py`/`cass_analytics.py` folds:
//!
//! * `SUM`/`AVG`/`COUNT` keep SQL NULL semantics — an aggregate nobody
//!   measured stays NULL, never a fabricated zero (`unknown != 0`). Every
//!   reported metric also carries `{total, known_count, unknown_count,
//!   quality}` coverage.
//! * NULL-activity rows never match an activity window (`_matches` rule:
//!   `activity_ms is None` fails any bound); they join the unwindowed
//!   universe like any other row.
//! * Median TTFE and the category/tool/file-churn explosions are computed
//!   over bounded rows — the JSON paths are FrankenSQLite-qualified
//!   (`json_extract`/`json_each`/`json_valid`/`json_type` probed at call
//!   time) with a deterministic Rust fold as the honest fallback.
//! * Dashboard recommendations come from the folded aggregates
//!   (`audit_aggregates`), never from reparsing top-N sessions.
//! * `health(scope)` reports project rules/instruction files under a
//!   clearly separated `dynamic_resources` section — bounded filesystem
//!   reads with provenance, never folded into the persisted score.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Result;
use coding_agent_search::franken_sync::compat::RowExt;
use coding_agent_search::franken_sync::{Connection, SqliteValue};
use serde_json::{Map, Value, json};

use super::facts_bag;
use super::identity;
use super::protocol::{AnalyticsScope, refuse};
use super::revisions;

/// Canonical activity expression: a tail-state row replaces the
/// denormalized columns when present (same precedence as `query.rs`).
const ACTIVITY_SQL: &str = "\
CASE WHEN t.conversation_id IS NOT NULL \
THEN COALESCE(t.last_message_created_at, t.ended_at, c.started_at) \
ELSE COALESCE(c.last_message_created_at, c.ended_at, c.started_at) END";

/// Column-derived duration (oracle `record_from_columns` fallback):
/// `(ended - started)/1000` only when both bounds are known and ordered.
const DURATION_SQL: &str = "\
CASE WHEN c.ended_at IS NOT NULL AND c.started_at IS NOT NULL \
     AND c.ended_at >= c.started_at \
THEN (c.ended_at - c.started_at) / 1000.0 END";

/// Per-session cache-hit rate over canonical totals (oracle
/// `calculate_cache_hit_rate`, `input_includes_cache=false`).
const HIT_RATE_SQL: &str = "\
CASE WHEN COALESCE(c.total_input_tokens,0) + COALESCE(c.total_cache_read_tokens,0) > 0 \
THEN MIN(100.0, c.total_cache_read_tokens * 100.0 / \
     (c.total_input_tokens + c.total_cache_read_tokens)) END";

/// Bounded numeric fold inputs (median TTFE, category/tool/churn
/// explosions): the scope can never silently exceed this many sessions.
const ANALYTICS_ROW_LIMIT: i64 = 100_001;
/// Dynamic resource read bound per file.
const RULES_FILE_BYTES: u64 = 256 * 1024;
/// Distinct workspace paths inspected for rules files per health call.
const RULES_PROJECT_LIMIT: i64 = 8;
/// File-churn result cap (oracle `calculate_file_churn` limit).
const FILE_CHURN_LIMIT: i64 = 10;
/// Recent-session slice size (oracle `get_recent_sessions` default).
const RECENT_LIMIT: i64 = 10;

const COACHING_NOTES: &[&str] = &[
    "Maintain project guidelines in CLAUDE.md / AGENTS.md to maximize prefix prompt caching.",
    "Keep tasks tightly scoped to achieve high one-shot resolution rates.",
];

/// Oracle `CATEGORY_LABELS` — the 13 canonical task categories.
fn category_label(category: &str) -> String {
    match category {
        "coding" => "Coding & Implementation",
        "debugging" => "Debugging & Bug Fixing",
        "feature" => "Feature Development",
        "refactoring" => "Code Refactoring",
        "testing" => "Testing & Quality Assurance",
        "exploration" => "Codebase Exploration",
        "planning" => "Planning & Architecture",
        "delegation" => "Subagent Delegation",
        "git" => "Git & Version Control",
        "build_deploy" => "Build & Deployment",
        "conversation" => "General Conversation",
        "brainstorming" => "Brainstorming & Design",
        "general" => "General Tasks",
        other => {
            return other
                .chars()
                .next()
                .map_or_else(String::new, |c| c.to_uppercase().collect::<String>())
                + &other[1..];
        }
    }
    .to_string()
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn round1(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

// ---------------------------------------------------------------------------
// Scope resolution — applied exactly once
// ---------------------------------------------------------------------------

struct ResolvedScope {
    agent_ids: Option<Vec<i64>>,
    workspace_ids: Option<Vec<i64>>,
    activity_from: Option<i64>,
    activity_to: Option<i64>,
    include_subagents: bool,
    /// Raw `project` argument for response echo (path/name as sent).
    project_arg: Option<String>,
}

/// Rolling UTC cutoff in ms (oracle `period_cutoff_ms`): `today` floors to
/// the UTC day; `7d`/`week`/`30d`/`month` subtract days at whole-second
/// precision.
fn period_cutoff_ms(period: &str, now_ms: i64) -> Result<Option<i64>> {
    let now_s = now_ms - now_ms.rem_euclid(1_000);
    let cutoff = match period.trim().to_lowercase().as_str() {
        "today" => Some(now_ms - now_ms.rem_euclid(86_400_000)),
        "7d" | "7days" | "week" => Some(now_s - 7 * 86_400_000),
        "30d" | "30days" | "month" => Some(now_s - 30 * 86_400_000),
        "" | "all" => None,
        _ => {
            return Err(refuse("unsupported_filter", "Unsupported dashboard period"));
        }
    };
    Ok(cutoff)
}

fn resolve_provider(connection: &Connection, raw: &str) -> Result<Option<Vec<i64>>> {
    if matches!(
        raw.trim().to_lowercase().as_str(),
        "" | "all" | "all_providers" | "all providers"
    ) {
        return Ok(None);
    }
    let provider = identity::normalize_agent_slug(raw).map_err(|_| {
        refuse(
            "unsupported_filter",
            "Provider scope is not a valid provider namespace",
        )
    })?;
    let mut ids = Vec::new();
    for row in connection.query("SELECT id, slug FROM agents")? {
        let slug: Option<String> = row.get_typed(1)?;
        let Some(slug) = slug else { continue };
        if let Ok(candidate) = identity::normalize_agent_slug(&slug)
            && candidate == provider
        {
            ids.push(row.get_typed::<i64>(0)?);
        }
    }
    Ok(Some(ids))
}

fn resolve_project(connection: &Connection, raw: &str) -> Result<Option<Vec<i64>>> {
    if matches!(
        raw.trim().to_lowercase().as_str(),
        "" | "all" | "all_projects" | "all projects"
    ) {
        return Ok(None);
    }
    let mut exact = Vec::new();
    let mut display = Vec::new();
    for row in connection.query("SELECT id, path FROM workspaces")? {
        let id: i64 = row.get_typed(0)?;
        let path: Option<String> = row.get_typed(1)?;
        let Some(path) = path else { continue };
        if path == raw {
            exact.push(id);
        } else if identity::path_name(&path) == raw {
            display.push(id);
        }
    }
    if !exact.is_empty() {
        return Ok(Some(exact));
    }
    if display.len() > 1 {
        return Err(refuse(
            "unsupported_filter",
            "Project scope is ambiguous; use the workspace path",
        ));
    }
    Ok(Some(display))
}

fn resolve_scope(connection: &Connection, scope: &AnalyticsScope) -> Result<ResolvedScope> {
    let agent_ids = match &scope.provider {
        Some(provider) => resolve_provider(connection, provider)?,
        None => None,
    };
    let workspace_ids = match &scope.project {
        Some(project) => resolve_project(connection, project)?,
        None => None,
    };

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let mut since = scope.since_ms;
    if let Some(period) = &scope.period
        && let Some(cutoff) = period_cutoff_ms(period, now_ms)?
    {
        since = Some(since.map_or(cutoff, |s| s.max(cutoff)));
    }
    if matches!((since, scope.until_ms), (Some(a), Some(b)) if a >= b) {
        return Err(refuse(
            "unsupported_filter",
            "Dashboard activity bounds are empty",
        ));
    }
    for bound in [since, scope.until_ms] {
        if bound.is_some_and(|value| value == i64::MIN || value == i64::MAX) {
            return Err(refuse(
                "unsupported_filter",
                "Dashboard activity boundary must be UTC milliseconds",
            ));
        }
    }

    Ok(ResolvedScope {
        agent_ids,
        workspace_ids,
        activity_from: since,
        activity_to: scope.until_ms,
        include_subagents: scope.include_subagents.unwrap_or(false),
        project_arg: scope.project.clone(),
    })
}

// ---------------------------------------------------------------------------
// Statement assembly
// ---------------------------------------------------------------------------

fn bind(params: &mut Vec<SqliteValue>, value: SqliteValue) -> String {
    params.push(value);
    format!("?{}", params.len())
}

fn ids_clause(
    column: &str,
    ids: &Option<Vec<i64>>,
    params: &mut Vec<SqliteValue>,
) -> Option<String> {
    let ids = ids.as_ref()?;
    if ids.is_empty() {
        return Some("1 = 0".to_owned());
    }
    let placeholders = ids
        .iter()
        .map(|id| bind(params, SqliteValue::Integer(*id)))
        .collect::<Vec<_>>()
        .join(",");
    Some(format!("{column} IN ({placeholders})"))
}

/// The resolved scope compiled to a WHERE clause (params rebuilt per
/// statement — the clause text and binding ORDER are identical for every
/// aggregate of one request).
fn scope_where(scope: &ResolvedScope, params: &mut Vec<SqliteValue>, extra: &mut Vec<String>) {
    if let Some(clause) = ids_clause("c.agent_id", &scope.agent_ids, params) {
        extra.push(clause);
    }
    if let Some(clause) = ids_clause("c.workspace_id", &scope.workspace_ids, params) {
        extra.push(clause);
    }
    if let Some(from) = scope.activity_from {
        extra.push(format!(
            "{ACTIVITY_SQL} >= {}",
            bind(params, SqliteValue::Integer(from))
        ));
    }
    if let Some(to) = scope.activity_to {
        extra.push(format!(
            "{ACTIVITY_SQL} < {}",
            bind(params, SqliteValue::Integer(to))
        ));
    }
    if !scope.include_subagents {
        extra.push("COALESCE(f.is_subagent,0) = 0".to_owned());
    }
}

/// `true` when the facts-owned Task-10 metric table exists. Archives whose
/// profile predates it degrade every metric to NULL — unknown, never 0.
fn metrics_installed(connection: &Connection) -> Result<bool> {
    let rows = connection.query(
        "SELECT 1 FROM sqlite_master WHERE type='table' \
         AND name='viewer_session_metrics' LIMIT 1",
    )?;
    Ok(!rows.is_empty())
}

/// FROM clause with or without the metrics join (a never-matching derived
/// row when the table is absent keeps every `m.*` reference NULL).
fn from_sql(metrics: bool) -> String {
    let metrics_join = if metrics {
        "LEFT JOIN viewer_session_metrics m ON m.conversation_id = c.id"
    } else {
        "LEFT JOIN (SELECT NULL AS conversation_id, NULL AS ttfe_seconds, \
         NULL AS one_shot_rate, NULL AS retries_count, \
         NULL AS self_corrections_count, NULL AS user_corrections_count, \
         NULL AS task_breakdown_json, NULL AS tools_used_json, \
         NULL AS health_score, NULL AS health_grade, \
         NULL AS health_issues_count, NULL AS potential_waste_tokens, \
         NULL AS potential_savings_usd) m ON 1 = 0"
    };
    format!(
        "FROM conversations c \
         LEFT JOIN conversation_tail_state t ON t.conversation_id = c.id \
         LEFT JOIN viewer_session_facts f ON f.conversation_id = c.id \
         LEFT JOIN viewer_annotations va ON va.conversation_id = c.id \
         {metrics_join} \
         LEFT JOIN workspaces w ON w.id = c.workspace_id \
         LEFT JOIN agents a ON a.id = c.agent_id \
         LEFT JOIN sources s ON s.id = c.source_id"
    )
}

fn run_scoped(
    connection: &Connection,
    metrics: bool,
    select: &str,
    scope: &ResolvedScope,
    extra_where: &[&str],
    tail: &str,
) -> Result<Vec<coding_agent_search::franken_sync::Row>> {
    let mut params = Vec::new();
    let mut clauses = Vec::new();
    scope_where(scope, &mut params, &mut clauses);
    clauses.extend(extra_where.iter().map(|c| (*c).to_owned()));
    let where_sql = if clauses.is_empty() {
        "1 = 1".to_owned()
    } else {
        clauses.join(" AND ")
    };
    let sql = format!(
        "SELECT {select} {} WHERE {where_sql} {tail}",
        from_sql(metrics)
    );
    Ok(connection.query_with_params(&sql, &params)?)
}

fn require_extensions(connection: &Connection) -> Result<()> {
    if !revisions::extensions_installed(connection)? {
        return Err(refuse(
            "viewer_extensions_missing",
            "The viewer extension profile is not installed; run --init-extensions first",
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Core aggregate — one pass over the scoped universe
// ---------------------------------------------------------------------------

/// Column map of `core_aggregates` (one SELECT, index-addressed).
const CORE_COLUMNS: &[(&str, &str)] = &[
    ("step", "f.step_count"),
    ("turn", "f.turn_count"),
    ("tool_call", "c.tool_call_count"),
    ("user_msg", "c.user_message_count"),
    ("assistant_msg", "c.assistant_message_count"),
    ("api_call", "c.api_call_count"),
    ("cost", "c.estimated_cost_usd"),
    ("tokens", "c.grand_total_tokens"),
    ("input", "c.total_input_tokens"),
    ("output", "c.total_output_tokens"),
    ("cache_read", "c.total_cache_read_tokens"),
    ("cache_write", "c.total_cache_creation_tokens"),
    ("one_shot", "m.one_shot_rate"),
    ("retries", "m.retries_count"),
    ("self_corr", "m.self_corrections_count"),
    ("user_corr", "m.user_corrections_count"),
    ("health_score", "m.health_score"),
    ("waste", "m.potential_waste_tokens"),
    ("savings", "m.potential_savings_usd"),
];

#[derive(Debug, Default)]
struct Core {
    total_sessions: i64,
    /// Per `(name)`: (sum, known) — `sum` is SQL SUM (NULL over all-NULL).
    sums: Map<String, Value>,
    known: Map<String, Value>,
    /// Single-row folds carried piggyback on the core scan: the uncertain
    /// classification count and the scope's `MIN(w.path)` — previously two
    /// further full join-scans of the same scoped universe.
    uncertain: Option<i64>,
    project_path: Option<String>,
}

fn core_aggregates(connection: &Connection, metrics: bool, scope: &ResolvedScope) -> Result<Core> {
    let mut select = String::from("COUNT(*)");
    for (_, column) in CORE_COLUMNS {
        select.push_str(&format!(", SUM({column}), COUNT({column})"));
    }
    select.push_str(&format!(", SUM({DURATION_SQL}), COUNT({DURATION_SQL})"));
    select.push_str(&format!(", AVG({HIT_RATE_SQL}), AVG(m.one_shot_rate)"));
    // Piggyback folds on the SAME single-row scan: the classification
    // count is a plain SUM over scoped rows, and `MIN` ignores NULLs, so
    // the prior `w.path IS NOT NULL AND w.path != ''` extra-WHERE is a
    // CASE guard inside the aggregate — byte-identical results without a
    // second scan of the joined universe.
    select.push_str(&format!(", {UNCERTAIN_CLASSIFICATION_SQL}"));
    select.push_str(", MIN(CASE WHEN w.path != '' THEN w.path END)");
    let rows = run_scoped(connection, metrics, &select, scope, &[], "")?;
    let row = rows
        .first()
        .ok_or_else(|| refuse("canonical_read_failed", "Aggregate query returned no row"))?;
    let mut core = Core {
        total_sessions: row.get_typed(0)?,
        ..Core::default()
    };
    let mut index = 1_usize;
    for (name, _) in CORE_COLUMNS {
        let sum: Option<f64> = row.get_typed(index)?;
        let known: i64 = row.get_typed(index + 1)?;
        if let Some(sum) = sum {
            core.sums.insert((*name).to_owned(), json!(sum));
        } else {
            core.sums.insert((*name).to_owned(), Value::Null);
        }
        core.known.insert((*name).to_owned(), json!(known));
        index += 2;
    }
    let duration_sum: Option<f64> = row.get_typed(index)?;
    let duration_known: i64 = row.get_typed(index + 1)?;
    core.sums.insert("duration".to_owned(), json!(duration_sum));
    core.known
        .insert("duration".to_owned(), json!(duration_known));
    let hit_avg: Option<f64> = row.get_typed(index + 2)?;
    core.sums.insert("cache_hit_avg".to_owned(), json!(hit_avg));
    let one_shot_avg: Option<f64> = row.get_typed(index + 3)?;
    core.sums
        .insert("one_shot_avg".to_owned(), json!(one_shot_avg));
    core.uncertain = row.get_typed(index + 4)?;
    core.project_path = row.get_typed(index + 5)?;
    Ok(core)
}

fn sum_f64(core: &Core, name: &str) -> Option<f64> {
    core.sums.get(name).and_then(Value::as_f64)
}

fn known_count(core: &Core, name: &str) -> i64 {
    core.known.get(name).and_then(Value::as_i64).unwrap_or(0)
}

/// Oracle `OptionalAggregate`: `total` NULL iff all unknown; `partial`
/// mixes known and unknown; `quality` reports the coverage.
fn optional_aggregate(core: &Core, name: &str) -> Value {
    let total = sum_f64(core, name);
    let known = known_count(core, name);
    let unknown = core.total_sessions - known;
    let quality = if total.is_none() {
        "unknown"
    } else if unknown > 0 {
        "partial"
    } else {
        "known"
    };
    json!({
        "total": total,
        "known_count": known,
        "unknown_count": unknown,
        "quality": quality,
    })
}

fn metric_coverage(core: &Core, extra: &[(&str, &str)]) -> Value {
    let mut map = Map::new();
    for (name, _) in CORE_COLUMNS {
        map.insert((*name).to_owned(), optional_aggregate(core, name));
    }
    map.insert(
        "duration_seconds".to_owned(),
        optional_aggregate(core, "duration"),
    );
    for (key, name) in extra {
        map.insert((*key).to_owned(), optional_aggregate(core, name));
    }
    Value::Object(map)
}

// ---------------------------------------------------------------------------
// Median TTFE (bounded sorted numeric fold)
// ---------------------------------------------------------------------------

/// One per-session fold row feeding EVERY per-session Rust fold of the
/// dashboard — median TTFE, categories, provider/model histograms, and
/// the JSON1-absent tool/churn fallbacks. Previously each helper paid its
/// own full scan of the six-table joined universe; one shared scan now
/// materializes the column union (bounded by the same
/// [`ANALYTICS_ROW_LIMIT`] every prior fold enforced) and the folds run in
/// memory. Column order is fixed by the `SHARED_*` indexes below.
const SHARED_FOLD_SELECT: &str = "\
c.id, a.slug, c.primary_model, c.estimated_cost_usd, m.one_shot_rate, \
f.task_category, f.step_count, m.tools_used_json, m.task_breakdown_json, \
m.ttfe_seconds";

/// Index map of [`SHARED_FOLD_SELECT`].
mod shared_col {
    pub const A_SLUG: usize = 1;
    pub const C_MODEL: usize = 2;
    pub const C_COST: usize = 3;
    pub const M_ONE_SHOT: usize = 4;
    pub const F_CATEGORY: usize = 5;
    pub const F_STEP: usize = 6;
    pub const M_TOOLS_JSON: usize = 7;
    pub const M_BREAKDOWN_JSON: usize = 8;
    pub const M_TTFE: usize = 9;
}

fn shared_fold_rows(
    connection: &Connection,
    metrics: bool,
    scope: &ResolvedScope,
) -> Result<Vec<coding_agent_search::franken_sync::Row>> {
    let rows = run_scoped(
        connection,
        metrics,
        SHARED_FOLD_SELECT,
        scope,
        &[],
        &format!("ORDER BY c.id LIMIT {ANALYTICS_ROW_LIMIT}"),
    )?;
    if rows.len() as i64 >= ANALYTICS_ROW_LIMIT {
        return Err(refuse(
            "canonical_payload_too_large",
            "Analytics fold input exceeds the row bound",
        ));
    }
    Ok(rows)
}

fn median_ttfe(
    connection: &Connection,
    metrics: bool,
    scope: &ResolvedScope,
    shared: Option<&[coding_agent_search::franken_sync::Row]>,
) -> Result<Option<f64>> {
    let owned;
    let rows = match shared {
        Some(rows) => rows,
        None => {
            owned = shared_fold_rows(connection, metrics, scope)?;
            &owned
        }
    };
    // The scoped query's `m.ttfe_seconds IS NOT NULL AND >= 0` gate and its
    // ORDER BY move to an in-memory filter/sort over the shared row set —
    // the exact same value set reaches the median.
    let mut values = Vec::new();
    for row in rows {
        let ttfe: Option<f64> = row.get_typed(shared_col::M_TTFE)?;
        if let Some(value) = ttfe
            && value >= 0.0
        {
            values.push(value);
        }
    }
    if values.is_empty() {
        return Ok(None);
    }
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    let median = if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    };
    Ok(Some(round1(median)))
}

// ---------------------------------------------------------------------------
// Punchcard — strftime + GROUP BY, qualified at call time
// ---------------------------------------------------------------------------

fn punchcard(connection: &Connection, metrics: bool, scope: &ResolvedScope) -> Result<Vec<Value>> {
    let mut cells: Vec<Value> = Vec::with_capacity(168);
    for weekday in 0..7 {
        for hour in 0..24 {
            cells.push(json!({
                "weekday": weekday, "hour": hour,
                "session_count": 0, "cost_usd": 0.0, "token_count": 0,
            }));
        }
    }
    let ts = format!("datetime({ACTIVITY_SQL} / 1000.0, 'unixepoch', 'localtime')");
    let select = format!(
        "(CAST(strftime('%w', {ts}) AS INTEGER) + 6) % 7, \
         CAST(strftime('%H', {ts}) AS INTEGER), \
         COUNT(*), COALESCE(SUM(c.estimated_cost_usd),0.0), \
         COALESCE(SUM(c.grand_total_tokens),0)"
    );
    let rows = run_scoped(
        connection,
        metrics,
        &select,
        scope,
        &[&format!("{ACTIVITY_SQL} IS NOT NULL")],
        "GROUP BY 1, 2",
    )?;
    for row in rows {
        let weekday: i64 = row.get_typed(0)?;
        let hour: i64 = row.get_typed(1)?;
        if !(0..7).contains(&weekday) || !(0..24).contains(&hour) {
            continue;
        }
        let cell = &mut cells[(weekday * 24 + hour) as usize];
        cell["session_count"] = json!(row.get_typed::<i64>(2)?);
        cell["cost_usd"] = json!(round2(row.get_typed::<f64>(3)?));
        cell["token_count"] = json!(row.get_typed::<i64>(4)?);
    }
    Ok(cells)
}

// ---------------------------------------------------------------------------
// JSON1-qualified explosions: file churn + tool frequency
// ---------------------------------------------------------------------------

/// `true` when the pinned engine qualifies the JSON1 surface the dashboard
/// explosions need (`json_each` + `json_valid`/`json_type`/`json_extract`).
/// Probed per call — never assumed.
fn json1_qualified(connection: &Connection) -> bool {
    connection
        .query("SELECT j.value FROM json_each('[1]') j WHERE json_valid('{\"a\":1}') AND json_type('{\"a\":1}') = 'object' AND json_extract('{\"a\":1}','$.a') = 1")
        .is_ok()
}

/// `reworked_files` object of one session's `task_breakdown_json`, or a
/// shared empty map — mirrors the SQL `CASE` guard so the fallback fold is
/// byte-identical in outcome.
fn reworked_of(breakdown: Option<&str>) -> Map<String, Value> {
    let empty = Map::new();
    let Some(text) = breakdown else { return empty };
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return empty;
    };
    match value.get("reworked_files").and_then(Value::as_object) {
        Some(map) => map.clone(),
        None => empty,
    }
}

/// The Rust fold half of `file_churn` over a [`SHARED_FOLD_SELECT`] row
/// set — the same `reworked_of` guard and the same edit/sessions sort the
/// prior per-helper query fed.
fn fold_file_churn(rows: &[coding_agent_search::franken_sync::Row]) -> Result<Vec<Value>> {
    let mut churn: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    for row in rows {
        let breakdown: Option<String> = row.get_typed(shared_col::M_BREAKDOWN_JSON)?;
        for (path, edits) in reworked_of(breakdown.as_deref()) {
            if path.is_empty() {
                continue;
            }
            let Some(edits) = edits.as_i64() else {
                continue;
            };
            let entry = churn.entry(path).or_insert((0, 0));
            entry.0 += 1;
            entry.1 += edits;
        }
    }
    let mut items: Vec<Value> = churn
        .into_iter()
        .map(|(file_path, (sessions, edits))| {
            json!({
                "file_path": file_path,
                "sessions_count": sessions,
                "edits_count": edits,
            })
        })
        .collect();
    items.sort_by(|a, b| {
        b["edits_count"]
            .as_i64()
            .cmp(&a["edits_count"].as_i64())
            .then(
                b["sessions_count"]
                    .as_i64()
                    .cmp(&a["sessions_count"].as_i64()),
            )
    });
    items.truncate(FILE_CHURN_LIMIT as usize);
    Ok(items)
}

fn file_churn(
    connection: &Connection,
    metrics: bool,
    scope: &ResolvedScope,
    shared: Option<&[coding_agent_search::franken_sync::Row]>,
) -> Result<Vec<Value>> {
    if metrics && json1_qualified(connection) {
        let json = "CASE WHEN m.task_breakdown_json IS NOT NULL \
             AND json_valid(m.task_breakdown_json) \
             AND json_type(m.task_breakdown_json, '$.reworked_files') = 'object' \
             THEN json_extract(m.task_breakdown_json, '$.reworked_files') \
             ELSE '{}' END";
        let mut params = Vec::new();
        let mut clauses = Vec::new();
        scope_where(scope, &mut params, &mut clauses);
        clauses.push("j.key IS NOT NULL".to_owned());
        clauses.push("j.key != ''".to_owned());
        let where_sql = clauses.join(" AND ");
        let limit_bind = bind(&mut params, SqliteValue::Integer(FILE_CHURN_LIMIT));
        let sql = format!(
            "SELECT j.key, COUNT(DISTINCT c.id), SUM(CAST(j.value AS INTEGER)) \
             {}, json_each({json}) j WHERE {where_sql} \
             GROUP BY j.key ORDER BY 3 DESC, 2 DESC LIMIT {limit_bind}",
            from_sql(metrics)
        );
        let rows = connection.query_with_params(&sql, &params)?;
        return rows
            .iter()
            .map(|row| {
                Ok(json!({
                    "file_path": row.get_typed::<String>(0)?,
                    "sessions_count": row.get_typed::<i64>(1)?,
                    "edits_count": row.get_typed::<i64>(2)?,
                }))
            })
            .collect();
    }
    // Rust fold — same guard and ordering over a bounded row set.
    let owned;
    let rows = match shared {
        Some(rows) => rows,
        None => {
            owned = shared_fold_rows(connection, metrics, scope)?;
            &owned
        }
    };
    fold_file_churn(rows)
}

/// The Rust fold half of `tools_frequency` over a [`SHARED_FOLD_SELECT`]
/// row set — identical JSON-array walk and count merge.
fn fold_tools_frequency(
    rows: &[coding_agent_search::franken_sync::Row],
    sessions_per_tool: &mut BTreeMap<String, i64>,
) -> Result<()> {
    for row in rows {
        let text: Option<String> = row.get_typed(shared_col::M_TOOLS_JSON)?;
        let Some(text) = text else { continue };
        let Ok(Value::Array(names)) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        for name in names {
            let Some(name) = name.as_str() else { continue };
            if name.is_empty() {
                continue;
            }
            *sessions_per_tool.entry(name.to_string()).or_insert(0) += 1;
        }
    }
    Ok(())
}

fn tools_frequency(
    connection: &Connection,
    metrics: bool,
    scope: &ResolvedScope,
    shared: Option<&[coding_agent_search::franken_sync::Row]>,
) -> Result<(Map<String, Value>, Vec<String>)> {
    let mut sessions_per_tool: BTreeMap<String, i64> = BTreeMap::new();
    if metrics && json1_qualified(connection) {
        let json = "CASE WHEN m.tools_used_json IS NOT NULL \
             AND json_valid(m.tools_used_json) \
             AND json_type(m.tools_used_json) = 'array' \
             THEN m.tools_used_json ELSE '[]' END";
        let mut params = Vec::new();
        let mut clauses = Vec::new();
        scope_where(scope, &mut params, &mut clauses);
        clauses.push("j.value IS NOT NULL".to_owned());
        clauses.push("j.value != ''".to_owned());
        let where_sql = clauses.join(" AND ");
        let sql = format!(
            "SELECT j.value, COUNT(*) {}, json_each({json}) j \
             WHERE {where_sql} GROUP BY j.value ORDER BY 2 DESC, 1",
            from_sql(metrics)
        );
        for row in connection.query_with_params(&sql, &params)? {
            let name: String = row.get_typed(0)?;
            sessions_per_tool.insert(name, row.get_typed(1)?);
        }
    } else {
        let owned;
        let rows = match shared {
            Some(rows) => rows,
            None => {
                owned = shared_fold_rows(connection, metrics, scope)?;
                &owned
            }
        };
        fold_tools_frequency(rows, &mut sessions_per_tool)?;
    }
    // Per-TOOL SESSION counts (the dashboard `tools_frequency`), ordered
    // count-desc then name — `tools_used` is the sorted union.
    let mut ordered: Vec<(String, i64)> = sessions_per_tool.into_iter().collect();
    ordered.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let mut frequency = Map::new();
    let mut used = Vec::new();
    for (name, count) in &ordered {
        frequency.insert(name.clone(), json!(count));
        used.push(name.clone());
    }
    used.sort();
    Ok((frequency, used))
}

// ---------------------------------------------------------------------------
// Category distribution — bounded per-session fold (pro-rates cost by
// turn share, falls back to primary category; identical math to
// `session_metrics.py::aggregate_dashboard` and the SQL CTE it mirrors).
// ---------------------------------------------------------------------------

/// The per-session category explosion over a [`SHARED_FOLD_SELECT`] row
/// set — same fallback-category rule, pro-rated cost and one-shot mean the
/// prior per-helper query fed.
fn fold_categories(rows: &[coding_agent_search::franken_sync::Row]) -> Result<Vec<Value>> {
    struct Bucket {
        turns: f64,
        cost: f64,
        one_shots: Vec<f64>,
    }
    let mut exploded: BTreeMap<String, Bucket> = BTreeMap::new();
    for row in rows {
        let cost: Option<f64> = row.get_typed(shared_col::C_COST)?;
        let session_cost = cost.unwrap_or(0.0);
        let one_shot: Option<f64> = row.get_typed(shared_col::M_ONE_SHOT)?;
        let task_category: Option<String> = row.get_typed(shared_col::F_CATEGORY)?;
        let step_count: Option<i64> = row.get_typed(shared_col::F_STEP)?;
        let breakdown: Option<String> = row.get_typed(shared_col::M_BREAKDOWN_JSON)?;
        let mut cats: Vec<(String, f64)> = Vec::new();
        if let Some(breakdown) = breakdown
            && let Ok(Value::Object(map)) = serde_json::from_str::<Value>(&breakdown)
        {
            for (key, value) in &map {
                if key == "reworked_files" {
                    continue;
                }
                if let Some(turns) = value.as_f64()
                    && turns > 0.0
                {
                    cats.push((key.to_lowercase(), turns));
                }
            }
        }
        if cats.is_empty() {
            let category = task_category
                .as_deref()
                .filter(|c| !c.is_empty())
                .unwrap_or("general")
                .to_lowercase();
            let entry = exploded.entry(category).or_insert_with(|| Bucket {
                turns: 0.0,
                cost: 0.0,
                one_shots: Vec::new(),
            });
            entry.turns += (step_count.unwrap_or(0).max(1)) as f64;
            entry.cost += session_cost;
            if let Some(rate) = one_shot {
                entry.one_shots.push(rate);
            }
        } else {
            let total_turns: f64 = cats.iter().map(|(_, turns)| *turns).sum();
            for (category, turns) in cats {
                let entry = exploded.entry(category).or_insert_with(|| Bucket {
                    turns: 0.0,
                    cost: 0.0,
                    one_shots: Vec::new(),
                });
                entry.turns += turns;
                entry.cost += session_cost * (turns / total_turns);
                if let Some(rate) = one_shot {
                    entry.one_shots.push(rate);
                }
            }
        }
    }
    let total_cat_turns: f64 = exploded.values().map(|b| b.turns).sum();
    let mut items: Vec<Value> = exploded
        .into_iter()
        .map(|(category, bucket)| {
            let percentage = if total_cat_turns > 0.0 {
                round1((bucket.turns / total_cat_turns) * 100.0)
            } else {
                0.0
            };
            json!({
                "category": category,
                "label": category_label(&category),
                "turn_count": bucket.turns.round() as i64,
                "percentage": percentage,
                "one_shot_rate": if bucket.one_shots.is_empty() {
                    Value::Null
                } else {
                    json!(round1(bucket.one_shots.iter().sum::<f64>() / bucket.one_shots.len() as f64))
                },
                "cost_usd": round2(bucket.cost),
            })
        })
        .collect();
    items.sort_by(|a, b| {
        b["turn_count"]
            .as_i64()
            .cmp(&a["turn_count"].as_i64())
            .then(a["category"].as_str().cmp(&b["category"].as_str()))
    });
    Ok(items)
}

// ---------------------------------------------------------------------------
// Provider/model distribution + recent sessions
// ---------------------------------------------------------------------------

/// The provider/model histograms over a [`SHARED_FOLD_SELECT`] row set —
/// each row weighs exactly one scoped session, so per-row counting is the
/// `GROUP BY a.slug, c.primary_model` + `COUNT(*)` the prior helper ran,
/// without a second joined-universe scan.
#[allow(clippy::type_complexity)]
fn fold_provider_model(
    rows: &[coding_agent_search::franken_sync::Row],
) -> Result<(Map<String, Value>, Map<String, Value>, i64)> {
    let mut providers: BTreeMap<String, i64> = BTreeMap::new();
    let mut models: BTreeMap<String, i64> = BTreeMap::new();
    let mut unknown_model = 0_i64;
    for row in rows {
        let slug: Option<String> = row.get_typed(shared_col::A_SLUG)?;
        let model: Option<String> = row.get_typed(shared_col::C_MODEL)?;
        if let Some(slug) = slug.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            let provider =
                identity::normalize_agent_slug(slug).unwrap_or_else(|_| slug.to_string());
            *providers.entry(provider).or_insert(0) += 1;
        }
        match model.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            Some(model) => *models.entry(model.to_string()).or_insert(0) += 1,
            None => unknown_model += 1,
        }
    }
    let providers: Map<String, Value> = providers.into_iter().map(|(k, v)| (k, json!(v))).collect();
    let models: Map<String, Value> = models.into_iter().map(|(k, v)| (k, json!(v))).collect();
    Ok((providers, models, unknown_model))
}

/// Uncertain-classification fold over the scoped universe: a row whose
/// facts row was never persisted (`f.is_subagent IS NULL`) or whose
/// persisted coverage isn't `complete` has no measured verdict yet —
/// `classification_complete` is therefore `uncertain == 0`, never a
/// constant (Task 7).
const UNCERTAIN_CLASSIFICATION_SQL: &str = "\
SUM(CASE WHEN f.is_subagent IS NULL \
     OR COALESCE(f.coverage_state,'') <> 'complete' \
THEN 1 ELSE 0 END)";

fn recent_sessions(
    connection: &Connection,
    metrics: bool,
    scope: &ResolvedScope,
) -> Result<Vec<Value>> {
    // The full summary-item shape the list lane emits plus the persisted
    // fact/metrics bag (Task 7): the dashboard's per-session surface must
    // not drop measurement the archive already holds.
    let select = format!(
        "c.id, c.source_id, c.external_id, c.source_path, c.origin_host, s.kind, \
         s.host_label, c.title, c.started_at, {ACTIVITY_SQL}, c.estimated_cost_usd, \
         c.grand_total_tokens, c.primary_model, w.path, COALESCE(va.is_pinned,0), \
         va.custom_title, f.task_category, f.is_subagent, f.conversation_id, \
         COALESCE(t.ended_at, c.ended_at), \
         COALESCE(t.last_message_created_at, c.last_message_created_at), \
         c.total_input_tokens, c.total_output_tokens, \
         c.total_cache_read_tokens, c.total_cache_creation_tokens, \
         f.coverage_state, f.parent_conversation_id, \
         f.turn_count, f.step_count, f.thinking_block_count, f.artifact_count, \
         f.subagent_count, f.tool_error_count, f.tool_calls_with_exit_status, \
         f.file_fact_count, f.compaction_count, f.model_set_json, f.first_prompt, \
         m.ttfe_seconds, m.one_shot_rate, m.retries_count, \
         m.self_corrections_count, m.user_corrections_count, \
         m.tools_used_json, m.task_breakdown_json, m.health_score, m.health_grade, \
         m.health_issues_count, m.potential_waste_tokens, m.potential_savings_usd"
    );
    let mut params = Vec::new();
    let mut clauses = Vec::new();
    scope_where(scope, &mut params, &mut clauses);
    let where_sql = if clauses.is_empty() {
        "1 = 1".to_owned()
    } else {
        clauses.join(" AND ")
    };
    // Oracle: `ORDER BY updated_at DESC, id DESC` — NULLs last, id tie-break.
    let limit_bind = bind(&mut params, SqliteValue::Integer(RECENT_LIMIT));
    let sql = format!(
        "SELECT {select} {} WHERE {where_sql} \
         ORDER BY ({ACTIVITY_SQL} IS NULL), {ACTIVITY_SQL} DESC, c.id DESC \
         LIMIT {limit_bind}",
        from_sql(metrics)
    );
    let rows = connection.query_with_params(&sql, &params)?;
    let mut items = Vec::new();
    for row in &rows {
        let workspace_path: Option<String> = row.get_typed(13)?;
        let origin_host: Option<String> = row
            .get_typed::<Option<String>>(4)?
            .or(row.get_typed::<Option<String>>(6)?);
        // The persisted fact bag: `null` when no facts row rides this row
        // (`f.conversation_id` is the presence marker), otherwise every
        // measured column — a known zero stays a zero.
        let model_set_json: Option<String> = row.get_typed(36)?;
        let tools_used_json: Option<String> = row.get_typed(43)?;
        let task_breakdown_json: Option<String> = row.get_typed(44)?;
        let bag = facts_bag::SessionFactBag {
            turn_count: row.get_typed(27)?,
            step_count: row.get_typed(28)?,
            thinking_block_count: row.get_typed(29)?,
            artifact_count: row.get_typed(30)?,
            subagent_count: row.get_typed(31)?,
            tool_error_count: row.get_typed(32)?,
            tool_calls_with_exit_status: row.get_typed(33)?,
            file_fact_count: row.get_typed(34)?,
            compaction_count: row.get_typed(35)?,
            model_set: facts_bag::decode_string_list(model_set_json.as_deref()),
            first_prompt: row.get_typed(37)?,
            ttfe_seconds: row.get_typed(38)?,
            one_shot_rate: row.get_typed(39)?,
            retries_count: row.get_typed(40)?,
            self_corrections_count: row.get_typed(41)?,
            user_corrections_count: row.get_typed(42)?,
            tools_used: facts_bag::decode_string_list(tools_used_json.as_deref()),
            task_breakdown: task_breakdown_json.and_then(|text| serde_json::from_str(&text).ok()),
            health_score: row.get_typed(45)?,
            health_grade: row.get_typed(46)?,
            health_issues_count: row.get_typed(47)?,
            potential_waste_tokens: row.get_typed(48)?,
            potential_savings_usd: row.get_typed(49)?,
        };
        let facts_present: Option<i64> = row.get_typed(18)?;
        items.push(json!({
            "conversation_id": row.get_typed::<i64>(0)?,
            "source_id": row.get_typed::<Option<String>>(1)?,
            "external_id": row.get_typed::<Option<String>>(2)?,
            "source_path": row.get_typed::<Option<String>>(3)?,
            "origin_host": origin_host,
            "source_kind": row.get_typed::<Option<String>>(5)?,
            "title": row.get_typed::<Option<String>>(7)?,
            "custom_title": row.get_typed::<Option<String>>(15)?,
            "is_pinned": row.get_typed::<i64>(14)? != 0,
            "started_at_ms": row.get_typed::<Option<i64>>(8)?,
            "ended_at_ms": row.get_typed::<Option<i64>>(19)?,
            "last_message_created_at_ms": row.get_typed::<Option<i64>>(20)?,
            "activity_ms": row.get_typed::<Option<i64>>(9)?,
            "estimated_cost_usd": row.get_typed::<Option<f64>>(10)?,
            "total_tokens": row.get_typed::<Option<i64>>(11)?,
            "total_input_tokens": row.get_typed::<Option<i64>>(21)?,
            "total_output_tokens": row.get_typed::<Option<i64>>(22)?,
            "total_cache_read_tokens": row.get_typed::<Option<i64>>(23)?,
            "total_cache_creation_tokens": row.get_typed::<Option<i64>>(24)?,
            "primary_model": row.get_typed::<Option<String>>(12)?,
            "workspace_path": workspace_path,
            "project": workspace_path.as_deref().map(identity::path_name).filter(|n| !n.is_empty()),
            "task_category": row.get_typed::<Option<String>>(16)?,
            "is_subagent": row.get_typed::<Option<i64>>(17)?.map(|v| v != 0),
            "classification_coverage": row.get_typed::<Option<String>>(25)?,
            "parent_conversation_id": row.get_typed::<Option<i64>>(26)?,
            "metrics": bag.emit(facts_present.is_some()),
        }));
    }
    let ids: Vec<i64> = items
        .iter()
        .map(|item| item["conversation_id"].as_i64().expect("decoded id"))
        .collect();
    let public_ids = identity::resolve_public_ids(connection, &ids)?;
    // Parent public ids resolve through the same verified canonical
    // identity probe the list lane uses — one bounded primary-key lookup
    // per distinct parent, never a registry walk.
    let parent_ids: std::collections::BTreeSet<i64> = items
        .iter()
        .filter_map(|item| item["parent_conversation_id"].as_i64())
        .collect();
    let mut parent_public_ids: std::collections::BTreeMap<i64, String> =
        std::collections::BTreeMap::new();
    for parent_id in parent_ids {
        if let Ok(identity) = identity::resolve_public_id(connection, parent_id) {
            parent_public_ids.insert(parent_id, identity.public_id);
        }
    }
    for item in &mut items {
        let id = item["conversation_id"].as_i64().expect("decoded id");
        if let Some(identity) = public_ids.get(&id) {
            item["public_id"] = json!(identity.public_id);
            item["provider"] = json!(identity.provider);
        } else {
            item["public_id"] = Value::Null;
            item["provider"] = Value::Null;
        }
        item["parent_public_id"] = item["parent_conversation_id"]
            .as_i64()
            .and_then(|parent_id| parent_public_ids.get(&parent_id).cloned())
            .map_or(Value::Null, Value::String);
    }
    Ok(items)
}

// ---------------------------------------------------------------------------
// audit_aggregates — dashboard recommendations folded from aggregates
// (oracle `viewer/health/issues.py::audit_aggregates`).
// ---------------------------------------------------------------------------

fn audit_aggregates(
    total_sessions: i64,
    total_retries: i64,
    top_churn_file: Option<(&str, i64, i64)>,
    cache_hit_rate: f64,
    total_tokens: i64,
    total_input_tokens: i64,
    total_cost_usd: f64,
) -> Vec<Value> {
    let mut findings = Vec::new();
    if total_retries > 3 {
        findings.push(json!({
            "id": "tool-edit-retries",
            "title": "Frequent File Edit Failures",
            "severity": "medium",
            "finding_class": "nudge",
            "description": format!(
                "{total_retries} tool execution retries recorded across {total_sessions} sessions."
            ),
            "estimated_tokens_wasted": total_retries * 800,
            "estimated_savings_usd": round2(total_retries as f64 * 0.005 + 1e-9),
            "remediation": "Provide concise file path context and structural snippets in prompts to prevent locator drift.",
        }));
    }
    if let Some((path, edits, sessions)) = top_churn_file
        && edits > 5
    {
        findings.push(json!({
            "id": "high-file-churn",
            "title": "Reworked Architecture Hotspot",
            "severity": "medium",
            "finding_class": "nudge",
            "description": format!(
                "File '{path}' underwent {edits} edits across {sessions} sessions."
            ),
            "estimated_tokens_wasted": edits * 1200,
            "estimated_savings_usd": round2(edits as f64 * 0.008),
            "remediation": "Consider refactoring and decomposing this module into smaller cohesive units.",
        }));
    }
    if cache_hit_rate < 30.0 && total_tokens > 50_000 {
        findings.push(json!({
            "id": "low-prompt-cache",
            "title": "Suboptimal Prompt Cache Hit Rate",
            "severity": "low",
            "finding_class": "keep",
            "description": format!("Cache hit rate is {cache_hit_rate:.1}%."),
            "estimated_tokens_wasted": total_input_tokens / 4,
            "estimated_savings_usd": round2(total_cost_usd * 0.15),
            "remediation": "Keep unchanging system instructions and repository guidelines at the beginning of context.",
        }));
    }
    findings
}

/// Aggregate grade for the dashboard health block (oracle
/// `dashboard.py::_grade_for_score` — no `A+` band).
fn dashboard_grade(score: i64) -> &'static str {
    match score {
        90.. => "A",
        80..=89 => "B",
        70..=79 => "C",
        60..=69 => "D",
        _ => "F",
    }
}

/// Session-level grade (oracle `issues.py::score_to_grade` — `A+` at 95+).
fn health_grade(score: i64) -> &'static str {
    match score {
        95.. => "A+",
        90..=94 => "A",
        80..=89 => "B",
        70..=79 => "C",
        60..=69 => "D",
        _ => "F",
    }
}

/// Cache-hit rate fold (oracle dashboard rule): aggregate totals win when
/// input + cache-read tokens were measured; otherwise AVG of per-session
/// rates; otherwise NULL.
fn cache_hit_rate(core: &Core) -> Option<f64> {
    let input = sum_f64(core, "input").unwrap_or(0.0);
    let cache_read = sum_f64(core, "cache_read").unwrap_or(0.0);
    if input + cache_read > 0.0 {
        return Some(round1((cache_read / (input + cache_read)) * 100.0));
    }
    if core.total_sessions > 0 {
        return sum_f64(core, "cache_hit_avg").map(round1);
    }
    None
}

// ---------------------------------------------------------------------------
// Dynamic resources — project instruction/memory files (health boundary)
// ---------------------------------------------------------------------------

/// Bounded rules-file probe for each workspace path in scope: existence,
/// byte/line counts and provenance — a resource READ, never a persisted
/// metric input and never folded into the health score.
fn dynamic_rules_resources(
    connection: &Connection,
    metrics: bool,
    scope: &ResolvedScope,
) -> Result<Value> {
    let rows = run_scoped(
        connection,
        metrics,
        "DISTINCT w.path",
        scope,
        &["w.path IS NOT NULL", "w.path != ''"],
        &format!("ORDER BY w.path LIMIT {RULES_PROJECT_LIMIT}"),
    )?;
    let mut files = Vec::new();
    for row in &rows {
        let path: String = row.get_typed(0)?;
        for name in ["CLAUDE.md", "AGENTS.md"] {
            let file_path = Path::new(&path).join(name);
            let mut entry = Map::new();
            entry.insert("path".to_owned(), json!(file_path.to_string_lossy()));
            entry.insert("project_path".to_owned(), json!(path));
            entry.insert("name".to_owned(), json!(name));
            entry.insert("provenance".to_owned(), json!("filesystem"));
            match std::fs::metadata(&file_path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    entry.insert("state".to_owned(), json!("missing"));
                }
                Err(_) => {
                    entry.insert("state".to_owned(), json!("unreadable"));
                }
                Ok(metadata) => match std::fs::read(&file_path) {
                    Ok(bytes) => {
                        let bounded = bytes.len() as u64 <= RULES_FILE_BYTES;
                        let text = String::from_utf8_lossy(&bytes);
                        let lines = text.lines().count() as i64;
                        entry.insert("state".to_owned(), json!("available"));
                        entry.insert("bytes".to_owned(), json!(bytes.len() as i64));
                        entry.insert("lines".to_owned(), json!(lines));
                        if !bounded {
                            entry.insert("state".to_owned(), json!("oversized"));
                        }
                        if !metadata.is_file() {
                            entry.insert("state".to_owned(), json!("unreadable"));
                        }
                    }
                    Err(_) => {
                        entry.insert("state".to_owned(), json!("unreadable"));
                    }
                },
            }
            files.push(Value::Object(entry));
        }
    }
    Ok(json!({
        "rules_files": files,
        "folded_into_score": false,
        "projects_examined": rows.len(),
    }))
}

// ---------------------------------------------------------------------------
// RPC entry points
// ---------------------------------------------------------------------------

/// `projects(scope)` — GROUP BY workspace over the scoped universe with
/// per-metric `OptionalAggregate` coverage (CASS lane `CanonicalAnalytics`
/// projects fold). Rows without a workspace path stay out of the grouping
/// exactly like the oracle's `by_project` fold.
pub fn projects(connection: &Connection, scope: &AnalyticsScope) -> Result<Value> {
    require_extensions(connection)?;
    let scope = resolve_scope(connection, scope)?;
    let metrics = metrics_installed(connection)?;

    const PROJECT_METRICS: &[(&str, &str)] = &[
        ("total_input_tokens", "c.total_input_tokens"),
        ("total_output_tokens", "c.total_output_tokens"),
        ("total_cache_read_tokens", "c.total_cache_read_tokens"),
        (
            "total_cache_creation_tokens",
            "c.total_cache_creation_tokens",
        ),
        ("grand_total_tokens", "c.grand_total_tokens"),
        ("estimated_cost_usd", "c.estimated_cost_usd"),
        ("api_call_count", "c.api_call_count"),
        ("tool_call_count", "c.tool_call_count"),
        ("user_message_count", "c.user_message_count"),
        ("assistant_message_count", "c.assistant_message_count"),
    ];
    let mut select = format!(
        "w.path, COUNT(*), MAX({ACTIVITY_SQL}), \
         SUM({DURATION_SQL}), COUNT({DURATION_SQL})"
    );
    for (_, column) in PROJECT_METRICS {
        select.push_str(&format!(", SUM({column}), COUNT({column})"));
    }
    select.push_str(&format!(", {UNCERTAIN_CLASSIFICATION_SQL}"));
    let rows = run_scoped(
        connection,
        metrics,
        &select,
        &scope,
        &["w.path IS NOT NULL", "w.path != ''"],
        "GROUP BY c.workspace_id ORDER BY w.path",
    )?;
    let mut items = Vec::new();
    for row in &rows {
        let path: String = row.get_typed(0)?;
        let session_count: i64 = row.get_typed(1)?;
        let last_activity: Option<i64> = row.get_typed(2)?;
        let mut index = 3_usize;
        let duration_sum: Option<f64> = row.get_typed(index)?;
        let duration_known: i64 = row.get_typed(index + 1)?;
        index += 2;
        let mut metric_map = Map::new();
        let aggregate = |total: Option<f64>, known: i64| -> Value {
            let unknown = session_count - known;
            json!({
                "total": total,
                "known_count": known,
                "unknown_count": unknown,
                "quality": if total.is_none() { "unknown" }
                    else if unknown > 0 { "partial" } else { "known" },
            })
        };
        metric_map.insert(
            "duration_seconds".to_owned(),
            aggregate(duration_sum, duration_known),
        );
        for (name, _) in PROJECT_METRICS {
            let total: Option<f64> = row.get_typed(index)?;
            let known: i64 = row.get_typed(index + 1)?;
            index += 2;
            metric_map.insert((*name).to_owned(), aggregate(total, known));
        }
        let uncertain: i64 = row.get_typed::<Option<i64>>(index)?.unwrap_or(0);
        items.push(json!({
            "project_path": path,
            "project_name": identity::path_name(&path),
            "session_count": session_count,
            "last_activity_ms": last_activity,
            "metrics": Value::Object(metric_map),
            "classification": {
                "complete": uncertain == 0,
                "uncertain_count": uncertain,
            },
        }));
    }
    Ok(json!({
        "projects": items,
        "generation": revisions::generation(connection)?,
        "complete": true,
    }))
}

/// `dashboard(scope)` — the full aggregate payload: summary, financials,
/// quality KPIs (median TTFE), punchcard, file churn, category/tool
/// distributions, provider/model histograms, health audit and the recent
/// slice — all folded from the same scoped universe.
pub fn dashboard(connection: &Connection, scope: &AnalyticsScope) -> Result<Value> {
    require_extensions(connection)?;
    let scope = resolve_scope(connection, scope)?;
    let metrics = metrics_installed(connection)?;
    let core = core_aggregates(connection, metrics, &scope)?;
    let total_sessions = core.total_sessions;

    let tot_cost_raw = sum_f64(&core, "cost");
    let tot_cost = tot_cost_raw.unwrap_or(0.0);
    let hit_rate = cache_hit_rate(&core);
    let financials = json!({
        "total_cost_usd": tot_cost_raw.map(round2),
        "avg_cost_per_session": if total_sessions > 0 && tot_cost_raw.is_some() {
            json!(round2(tot_cost / total_sessions as f64))
        } else {
            Value::Null
        },
        "total_tokens": sum_f64(&core, "tokens").map(|v| v as i64),
        "total_input_tokens": sum_f64(&core, "input").map(|v| v as i64),
        "total_output_tokens": sum_f64(&core, "output").map(|v| v as i64),
        "total_cache_read_tokens": sum_f64(&core, "cache_read").map(|v| v as i64),
        "total_cache_write_tokens": sum_f64(&core, "cache_write").map(|v| v as i64),
        "cache_hit_rate": hit_rate,
    });
    let summary = json!({
        "total_sessions": total_sessions,
        "total_steps": sum_f64(&core, "step").map(|v| v as i64),
        "total_turns": sum_f64(&core, "turn").map(|v| v as i64),
        "total_duration_seconds": sum_f64(&core, "duration").map(round1),
        "total_tool_calls": sum_f64(&core, "tool_call").map(|v| v as i64),
        "total_user_messages": sum_f64(&core, "user_msg").map(|v| v as i64),
        "total_assistant_messages": sum_f64(&core, "assistant_msg").map(|v| v as i64),
    });
    // One materialized per-session row set feeds every Rust-fold
    // explosion below — the same scoped universe scanned ONCE instead of
    // once per aggregate helper.
    let shared = shared_fold_rows(connection, metrics, &scope)?;
    let med_ttfe = median_ttfe(connection, metrics, &scope, Some(&shared))?;
    let quality = json!({
        "avg_one_shot_rate": if total_sessions > 0 {
            sum_f64(&core, "one_shot_avg").map(round1).map_or(Value::Null, Value::from)
        } else {
            Value::Null
        },
        "total_retries": sum_f64(&core, "retries").map(|v| v as i64),
        "total_self_corrections": sum_f64(&core, "self_corr").map(|v| v as i64),
        "total_user_corrections": sum_f64(&core, "user_corr").map(|v| v as i64),
        "median_ttfe_seconds": med_ttfe,
    });

    let punchcard = punchcard(connection, metrics, &scope)?;
    let churn = file_churn(connection, metrics, &scope, Some(&shared))?;
    let categories = fold_categories(&shared)?;
    let (tools_frequency, tools_used) =
        tools_frequency(connection, metrics, &scope, Some(&shared))?;
    let (providers_dist, model_dist, unknown_model) = fold_provider_model(&shared)?;
    let recent = recent_sessions(connection, metrics, &scope)?;

    // Measured classification coverage over the FULL selected universe
    // (Task 7): `complete` only when every eligible session's persisted
    // verdict is known — folded inside the core scan, unbounded as before.
    let uncertain: i64 = core.uncertain.unwrap_or(0);

    // project_path: first non-empty workspace path in scope (oracle
    // `_project_path_query`), folded inside the core scan — or the scope
    // argument's resolved name.
    let project_path: String = core
        .project_path
        .clone()
        .or_else(|| scope.project_arg.clone())
        .unwrap_or_default();
    let project_name = scope
        .project_arg
        .clone()
        .unwrap_or_else(|| "all".to_owned());

    // Health audit over folded aggregates (oracle `_health_audit`): the
    // average health score divides by MEASURED rows only.
    let scores_sum = sum_f64(&core, "health_score").unwrap_or(0.0);
    let scores_count = known_count(&core, "health_score");
    let avg_score = if scores_count > 0 {
        Some((scores_sum / scores_count as f64) as i64)
    } else {
        None
    };
    let total_retries = sum_f64(&core, "retries").unwrap_or(0.0) as i64;
    let top_churn = churn.first().and_then(|item| {
        Some((
            item["file_path"].as_str()?.to_owned(),
            item["edits_count"].as_i64()?,
            item["sessions_count"].as_i64()?,
        ))
    });
    let findings = audit_aggregates(
        total_sessions,
        total_retries,
        top_churn.as_ref().map(|(p, e, s)| (p.as_str(), *e, *s)),
        hit_rate.unwrap_or(0.0),
        sum_f64(&core, "tokens").unwrap_or(0.0) as i64,
        sum_f64(&core, "input").unwrap_or(0.0) as i64,
        tot_cost,
    );
    let health_audit = json!({
        "score": avg_score,
        "grade": avg_score.map(dashboard_grade),
        "total_wasted_tokens": sum_f64(&core, "waste").map(|v| v as i64),
        "total_savings_usd": sum_f64(&core, "savings").map(round2),
        "findings": findings,
        "coaching_notes": COACHING_NOTES,
    });

    Ok(json!({
        "project_name": project_name,
        "project_path": project_path,
        "total_conversations": total_sessions,
        "total_sessions": total_sessions,
        "total_turns": summary["total_turns"],
        "providers_distribution": providers_dist,
        "model_distribution": model_dist,
        "unknown_model_count": unknown_model,
        "tools_frequency": tools_frequency,
        "tools_used": tools_used,
        "financials": financials,
        "quality": quality,
        "punchcard": punchcard,
        "file_churn": churn,
        "categories": categories,
        "health_audit": health_audit,
        "summary": summary,
        "recent_sessions": recent,
        "recent_conversations": recent,
        "classification": {
            "complete": uncertain == 0,
            "uncertain_count": uncertain,
        },
        "metric_coverage": metric_coverage(&core, &[
            ("total_duration_seconds", "duration"),
            ("avg_one_shot_rate", "one_shot"),
            ("cache_hit_rate", "cache_hit_avg"),
        ]),
        "generation": revisions::generation(connection)?,
        "complete": true,
    }))
}

/// `health(scope)` — persisted per-session health metrics folded with the
/// aggregate findings, plus a clearly separated `dynamic_resources`
/// section (project rules files read on demand; provenance, never score
/// inputs). `score`/`grade` divide by measured rows only.
pub fn health(connection: &Connection, scope: &AnalyticsScope) -> Result<Value> {
    require_extensions(connection)?;
    let scope = resolve_scope(connection, scope)?;
    let metrics = metrics_installed(connection)?;
    let core = core_aggregates(connection, metrics, &scope)?;
    let total_sessions = core.total_sessions;

    let scores_sum = sum_f64(&core, "health_score").unwrap_or(0.0);
    let scores_count = known_count(&core, "health_score");
    let avg_score = if scores_count > 0 {
        Some((scores_sum / scores_count as f64) as i64)
    } else {
        None
    };
    let hit_rate = cache_hit_rate(&core);
    let churn = file_churn(connection, metrics, &scope, None)?;
    let total_retries = sum_f64(&core, "retries").unwrap_or(0.0) as i64;
    let top_churn = churn.first().and_then(|item| {
        Some((
            item["file_path"].as_str()?.to_owned(),
            item["edits_count"].as_i64()?,
            item["sessions_count"].as_i64()?,
        ))
    });
    let findings = audit_aggregates(
        total_sessions,
        total_retries,
        top_churn.as_ref().map(|(p, e, s)| (p.as_str(), *e, *s)),
        hit_rate.unwrap_or(0.0),
        sum_f64(&core, "tokens").unwrap_or(0.0) as i64,
        sum_f64(&core, "input").unwrap_or(0.0) as i64,
        sum_f64(&core, "cost").unwrap_or(0.0),
    );
    let dynamic = dynamic_rules_resources(connection, metrics, &scope)?;

    Ok(json!({
        "total_sessions": total_sessions,
        "score": avg_score,
        "grade": avg_score.map(health_grade),
        "scores_known": scores_count,
        "scores_unknown": total_sessions - scores_count,
        "total_wasted_tokens": sum_f64(&core, "waste").map(|v| v as i64),
        "total_savings_usd": sum_f64(&core, "savings").map(round2),
        "findings": findings,
        "coaching_notes": COACHING_NOTES,
        "dynamic_resources": dynamic,
        "metric_coverage": metric_coverage(&core, &[]),
        "generation": revisions::generation(connection)?,
        "complete": true,
    }))
}
