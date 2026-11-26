use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::Result;
use coding_agent_search::franken_sync::compat::RowExt;
use coding_agent_search::franken_sync::{Connection, SqliteValue};
use coding_agent_search::viewer_extensions::facts::is_source_absent_gap;
use serde_json::Value;

use super::super::deadline::Deadline;
use super::super::identity;
use super::super::protocol::{ListFilters, refuse};
use super::super::query;
use super::dsl::{Plan, PlanNode};
use super::eval::{ascii_fold, coverage_field, like_match, tool_match};
use super::regions::{HydrateStats, Region, RegionLoadMode, SessionCtx, seekable_block_field};

/// Sentinel `possible` key for a narrowed predicate's non-resident Unknown
/// candidates (see [`BlockSeekEvidence`]). A region key it can never be
/// (region keys never contain NUL), so it never survives an intersection
/// with a literal's member keys — it exists to keep the predicate node's
/// own verdict `Unknown` exactly where the eager lane's per-region Unknown
/// keys would have, when those regions are precisely the rows the seek lane
/// did not attach. Consumers that only read set membership through
/// `combine`/`negate`/`truth` see byte-identical verdicts and reasons.
pub(super) const UNKNOWN_PREDICATE_SENTINEL: &str = "\u{0}predicate_unknown";

/// `IN()` chunk width for the value seeks (SQL placeholder budget).
const SEEK_VALUE_CHUNK: usize = 500;

/// Per-request SQL value-seek evidence for one [`RegionLoadMode::SeekBlockDomain`]
/// plan. Built once after the region attach, consulted by every predicate
/// evaluation. Three exactness pillars (the report's equivalence argument):
///
/// 1. **True keys** — per distinct predicate, the seek reads the WHOLE
///    `viewer_search_regions` table by value (the DISTINCT vocabulary is
///    filtered through the SAME Rust matcher the eager per-region arm
///    uses, then sought through `IN()`; exit/error_type/has_error compare
///    the stored column directly), so `known` is the exact resident-True
///    key set per conversation — identical to the eager `collection`'s,
///    without loading any of those rows.
/// 2. **Resident Unknown keys** — the narrowed collection still evaluates
///    `region_value` over the attached (literal-gated) residents, so every
///    Unknown key the eager lane would intersect with literal member keys
///    is present (literal members are always residents — the Quill gate
///    attached them).
/// 3. **Non-resident Unknown existence** — one NULL-candidate probe per
///    predicate field, run only when some universe session's coverage
///    actually blocks that field, answers "could a non-attached region
///    discharge Unknown here"; the sentinel key turns that into the same
///    `Unknown` verdict with the same `value_unknown` reason the eager
///    lane produced per region.
///
/// The probe conditions mirror `unknown_field` exactly: `tool`/`exit` add
/// the scope discharge (`scope_known = 0 OR scope = 'tools'`), `file`/
/// `error_type`/`has_error` probe the bare NULL column. Vocabulary reads
/// (DISTINCT column values) are index-only and NOT counted in
/// `regions_loaded`; seek and probe result rows ARE counted.
pub(super) struct BlockSeekEvidence {
    /// (field, op, json(expected)) → conversation → the exact True region
    /// keys. Inserted for EVERY seekable predicate of the plan (possibly
    /// empty), so a lookup can never silently miss.
    predicates: HashMap<(String, String, String), HashMap<i64, BTreeSet<String>>>,
    /// field → conversations holding a region whose NULL value can
    /// discharge Unknown (the probe rows). `has_mcp` never appears: its
    /// Unknown is coverage-conditional only.
    probes: HashMap<String, HashSet<i64>>,
    /// Conversations with any tool name that names an MCP server
    /// (`has_mcp`'s `named` arm: ascii-fold contains "mcp", or contains
    /// '/') — proven table-wide by the same tool_name vocabulary seek.
    pub(super) mcp_named: HashSet<i64>,
}

fn block_predicate_key(field: &str, op: &str, expected: &Value) -> (String, String, String) {
    (
        field.to_owned(),
        op.to_owned(),
        serde_json::to_string(expected).unwrap_or_default(),
    )
}

impl BlockSeekEvidence {
    pub(super) fn keys_for(
        &self,
        field: &str,
        op: &str,
        expected: &Value,
    ) -> Option<&HashMap<i64, BTreeSet<String>>> {
        self.predicates
            .get(&block_predicate_key(field, op, expected))
    }

    /// Does this conversation hold a region whose NULL value can discharge
    /// Unknown for `field`? `None` when no probe ran (no blocking session
    /// existed — nothing can discharge Unknown).
    pub(super) fn probe_hit(&self, field: &str, conversation_id: i64) -> bool {
        self.probes
            .get(field)
            .is_some_and(|candidates| candidates.contains(&conversation_id))
    }
}

/// Collect every seekable block predicate of the plan (any nesting, any
/// polarity — a `Not` child still evaluates the predicate node itself).
fn collect_seek_predicates(node: &PlanNode, out: &mut Vec<(String, String, Value)>) {
    match node {
        PlanNode::Predicate {
            field, op, value, ..
        } => {
            if seekable_block_field(field) {
                out.push((field.clone(), op.clone(), value.clone()));
            }
        }
        PlanNode::Not(child) => collect_seek_predicates(child, out),
        PlanNode::And(children) | PlanNode::Or(children) => {
            for child in children {
                collect_seek_predicates(child, out);
            }
        }
        PlanNode::Literal { .. } | PlanNode::All => {}
    }
}

/// DISTINCT non-NULL vocabulary of one region column — the value set the
/// per-field Rust matcher folds over, read once per request per column
/// (memoized by the caller). Index-only: never counted in `regions_loaded`.
fn read_distinct_column(
    connection: &Connection,
    column: &str,
    deadline: &Deadline,
) -> Result<Vec<String>> {
    deadline.check("seek", format!("DISTINCT {column} vocabulary read"))?;
    let rows = connection.query_with_params(
        &format!(
            "SELECT DISTINCT {column} FROM viewer_search_regions \
             WHERE {column} IS NOT NULL"
        ),
        &[],
    )?;
    let mut values = Vec::with_capacity(rows.len());
    for row in &rows {
        values.push(row.get_typed(0)?);
    }
    Ok(values)
}

/// Seek `(conversation_id, region_key)` pairs by an `IN()` chunk of
/// matched column values; group the keys per conversation, filtered to the
/// request's universe. Counts every returned row in `regions_loaded` —
/// the receipt means "region rows read by this request".
fn seek_region_keys_by_values(
    connection: &Connection,
    column: &str,
    values: &[&str],
    universe: &HashSet<i64>,
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<HashMap<i64, BTreeSet<String>>> {
    let mut per_conversation: HashMap<i64, BTreeSet<String>> = HashMap::new();
    for chunk in values.chunks(SEEK_VALUE_CHUNK) {
        if chunk.is_empty() {
            continue;
        }
        deadline.check(
            "seek",
            format!("{column} value seek over {} value(s)", chunk.len()),
        )?;
        let placeholders = (1..=chunk.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let params: Vec<SqliteValue> = chunk
            .iter()
            .map(|value| SqliteValue::Text((*value).into()))
            .collect();
        let rows = connection.query_with_params(
            &format!(
                "SELECT conversation_id, region_key FROM viewer_search_regions \
                 WHERE {column} IN ({placeholders})"
            ),
            &params,
        )?;
        stats.regions_loaded += rows.len();
        for row in &rows {
            let cid: i64 = row.get_typed(0)?;
            let key: String = row.get_typed(1)?;
            if universe.contains(&cid) {
                per_conversation.entry(cid).or_default().insert(key);
            }
        }
    }
    Ok(per_conversation)
}

/// The NULL-candidate probe SQL for one field — exactly `unknown_field`'s
/// discharge condition in SQL: the region's value column is NULL and, for
/// the tools-scope fields, the scope cannot prove the region is not a
/// tools block (`scope_known = 0 OR scope = 'tools'`). One table-wide
/// bounded pass per field, only run when some universe session's coverage
/// actually blocks that field.
fn block_probe_sql(field: &str) -> &'static str {
    match field {
        "tool" => {
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
                  WHERE tool_name IS NULL AND (scope_known = 0 OR scope = 'tools')"
        }
        "exit" => {
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
                   WHERE exit_code IS NULL AND (scope_known = 0 OR scope = 'tools')"
        }
        "file" => {
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
                   WHERE file_path IS NULL"
        }
        "error_type" => {
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
                        WHERE error_type IS NULL"
        }
        "has_error" => {
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
                        WHERE has_error IS NULL"
        }
        // has_mcp needs no probe: its Unknown is coverage-conditional only.
        _ => "",
    }
}

/// Build the [`BlockSeekEvidence`] for a [`RegionLoadMode::SeekBlockDomain`]
/// plan. `None` for every other mode (no cost).
pub(super) fn build_block_seek_evidence(
    connection: &Connection,
    sessions: &[SessionCtx],
    plan: &Plan,
    region_load: &RegionLoadMode,
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<Option<BlockSeekEvidence>> {
    if !matches!(region_load, RegionLoadMode::SeekBlockDomain) {
        return Ok(None);
    }
    let universe: HashSet<i64> = sessions
        .iter()
        .map(|session| session.conversation_id)
        .collect();
    let mut predicates: Vec<(String, String, Value)> = Vec::new();
    collect_seek_predicates(&plan.root, &mut predicates);
    let mut evidence = BlockSeekEvidence {
        predicates: HashMap::new(),
        probes: HashMap::new(),
        mcp_named: HashSet::new(),
    };
    let mut tool_vocabulary: Option<Vec<String>> = None;
    let mut file_vocabulary: Option<Vec<String>> = None;
    let mut mcp_named_done = false;
    let mut seen: HashSet<(String, String, String)> = HashSet::new();
    for (field, op, expected) in &predicates {
        let key = block_predicate_key(field, op, expected);
        if !seen.insert(key.clone()) {
            continue;
        }
        match field.as_str() {
            "tool" => {
                if tool_vocabulary.is_none() {
                    tool_vocabulary =
                        Some(read_distinct_column(connection, "tool_name", deadline)?);
                }
                let vocabulary = tool_vocabulary.as_deref().unwrap_or(&[]);
                let pattern = expected.as_str().unwrap_or_default();
                let matched: Vec<&str> = vocabulary
                    .iter()
                    .filter(|value| tool_match(value, pattern, op))
                    .map(String::as_str)
                    .collect();
                let per_conversation = seek_region_keys_by_values(
                    connection,
                    "tool_name",
                    &matched,
                    &universe,
                    stats,
                    deadline,
                )?;
                evidence.predicates.insert(key, per_conversation);
            }
            "file" => {
                if file_vocabulary.is_none() {
                    file_vocabulary =
                        Some(read_distinct_column(connection, "file_path", deadline)?);
                }
                let vocabulary = file_vocabulary.as_deref().unwrap_or(&[]);
                let pattern = expected.as_str().unwrap_or_default();
                let matched: Vec<&str> = vocabulary
                    .iter()
                    .filter(|value| like_match(value, pattern, false, !pattern.contains('*')))
                    .map(String::as_str)
                    .collect();
                let per_conversation = seek_region_keys_by_values(
                    connection,
                    "file_path",
                    &matched,
                    &universe,
                    stats,
                    deadline,
                )?;
                evidence.predicates.insert(key, per_conversation);
            }
            "exit" => {
                // Planner guarantee: comparison op + i64 expected, so the
                // SQL comparison is exactly the Rust compare_numbers over
                // the stored integers (NULL never matches a SQL comparison,
                // matching the Rust unknown_field discharge).
                let sql_op = match op.as_str() {
                    "EQ" => "=",
                    "NEQ" => "!=",
                    "GT" => ">",
                    "GTE" => ">=",
                    "LT" => "<",
                    _ => "<=",
                };
                let bound = SqliteValue::Integer(expected.as_i64().unwrap_or_default());
                let rows = connection.query_with_params(
                    &format!(
                        "SELECT conversation_id, region_key FROM viewer_search_regions \
                         WHERE exit_code {sql_op} ?1"
                    ),
                    &[bound],
                )?;
                stats.regions_loaded += rows.len();
                let mut per_conversation: HashMap<i64, BTreeSet<String>> = HashMap::new();
                for row in &rows {
                    let cid: i64 = row.get_typed(0)?;
                    let key: String = row.get_typed(1)?;
                    if universe.contains(&cid) {
                        per_conversation.entry(cid).or_default().insert(key);
                    }
                }
                evidence.predicates.insert(key, per_conversation);
            }
            "error_type" => {
                let bound = SqliteValue::Text(expected.as_str().unwrap_or_default().into());
                let rows = connection.query_with_params(
                    "SELECT conversation_id, region_key FROM viewer_search_regions \
                     WHERE error_type = ?1",
                    &[bound],
                )?;
                stats.regions_loaded += rows.len();
                let mut per_conversation: HashMap<i64, BTreeSet<String>> = HashMap::new();
                for row in &rows {
                    let cid: i64 = row.get_typed(0)?;
                    let key: String = row.get_typed(1)?;
                    if universe.contains(&cid) {
                        per_conversation.entry(cid).or_default().insert(key);
                    }
                }
                evidence.predicates.insert(key, per_conversation);
            }
            "has_error" => {
                // The eager arm ignores the op (plain bool equality), so
                // the seek mirrors it with `=`.
                let wanted = expected.as_bool().unwrap_or(false);
                let bound = SqliteValue::Integer(i64::from(wanted));
                let rows = connection.query_with_params(
                    "SELECT conversation_id, region_key FROM viewer_search_regions \
                     WHERE has_error = ?1",
                    &[bound],
                )?;
                stats.regions_loaded += rows.len();
                let mut per_conversation: HashMap<i64, BTreeSet<String>> = HashMap::new();
                for row in &rows {
                    let cid: i64 = row.get_typed(0)?;
                    let key: String = row.get_typed(1)?;
                    if universe.contains(&cid) {
                        per_conversation.entry(cid).or_default().insert(key);
                    }
                }
                evidence.predicates.insert(key, per_conversation);
            }
            "has_mcp" => {
                // The `named` arm: any region tool_name that ascii-folds
                // to contain "mcp" or contains '/'. Table-wide vocabulary
                // seek, computed once for the whole request.
                if !mcp_named_done {
                    mcp_named_done = true;
                    if tool_vocabulary.is_none() {
                        tool_vocabulary =
                            Some(read_distinct_column(connection, "tool_name", deadline)?);
                    }
                    let vocabulary = tool_vocabulary.as_deref().unwrap_or(&[]);
                    let matched: Vec<&str> = vocabulary
                        .iter()
                        .filter(|value| {
                            let folded = ascii_fold(value);
                            folded.contains("mcp") || value.contains('/')
                        })
                        .map(String::as_str)
                        .collect();
                    if !matched.is_empty() {
                        evidence.mcp_named = seek_distinct_conversations(
                            connection,
                            "tool_name",
                            &matched,
                            &universe,
                            stats,
                            deadline,
                        )?;
                    }
                }
                // has_mcp evaluates to a constant; no per-key entry is
                // consulted, but insert one so `keys_for` can never miss.
                evidence.predicates.insert(key, HashMap::new());
            }
            _ => {
                // Unreachable: collect_seek_predicates only collects
                // seekable fields.
            }
        }
    }
    // NULL-candidate probes, one bounded pass per DISTINCT field (the
    // probe question is field-level, not per-op) that some universe
    // session's coverage actually blocks.
    let mut fields: BTreeSet<&str> = BTreeSet::new();
    for (field, _, _) in &predicates {
        if !matches!(field.as_str(), "has_mcp") {
            fields.insert(field.as_str());
        }
    }
    for field in fields {
        let coverage = coverage_field(field);
        if !sessions
            .iter()
            .any(|session| session.field_blocks_strict(coverage))
        {
            continue;
        }
        let sql = block_probe_sql(field);
        if sql.is_empty() {
            continue;
        }
        deadline.check("seek", format!("{field} NULL-candidate probe"))?;
        let rows = connection.query_with_params(sql, &[])?;
        stats.regions_loaded += rows.len();
        let mut candidates: HashSet<i64> = HashSet::new();
        for row in &rows {
            candidates.insert(row.get_typed(0)?);
        }
        evidence.probes.insert(field.to_owned(), candidates);
    }
    Ok(Some(evidence))
}

/// Conversations owning a region whose column is IN the matched values —
/// the has_mcp `named` existence set. Same seeking shape as
/// [`seek_region_keys_by_values`] (the `(conversation_id, region_key)`
/// projection SEEKs `idx_viewer_search_regions_*`, whereas `SELECT DISTINCT
/// conversation_id` can degrade to a full-table SCAN),
/// deduped here in Rust. Counts every returned row in `regions_loaded` —
/// the receipt means "region rows read by this request", not "distinct
/// conversations found".
fn seek_distinct_conversations(
    connection: &Connection,
    column: &str,
    values: &[&str],
    universe: &HashSet<i64>,
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<HashSet<i64>> {
    let mut found: HashSet<i64> = HashSet::new();
    for chunk in values.chunks(SEEK_VALUE_CHUNK) {
        if chunk.is_empty() {
            continue;
        }
        deadline.check(
            "seek",
            format!("{column} conversation seek over {} value(s)", chunk.len()),
        )?;
        let placeholders = (1..=chunk.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let params: Vec<SqliteValue> = chunk
            .iter()
            .map(|value| SqliteValue::Text((*value).into()))
            .collect();
        let rows = connection.query_with_params(
            &format!(
                "SELECT conversation_id, region_key FROM viewer_search_regions \
                 WHERE {column} IN ({placeholders})"
            ),
            &params,
        )?;
        stats.regions_loaded += rows.len();
        for row in &rows {
            let cid: i64 = row.get_typed(0)?;
            if universe.contains(&cid) {
                found.insert(cid);
            }
        }
    }
    Ok(found)
}

// ---------------------------------------------------------------------------
// Session loading — the SQL-narrowed universe
// ---------------------------------------------------------------------------

/// Load the filtered universe: every `ListFilters` dimension pushed to SQL
/// (provider/source/project/pinned/tag/classification/windows), the same
/// unknown-facts probes as `list_sessions`. Returns session skeletons in
/// `conversation_id` order.
pub(super) fn load_universe(
    connection: &Connection,
    filters: &ListFilters,
) -> Result<(Vec<i64>, String, Vec<SqliteValue>)> {
    let resolved = query::resolve_filters_public(connection, filters)?;
    query::run_probes_public(connection, &resolved)?;
    let (sql, params) = resolved.universe_sql();
    let rows = connection.query_with_params(&sql, &params)?;
    let mut ids = Vec::with_capacity(rows.len());
    for row in &rows {
        ids.push(row.get_typed::<i64>(0)?);
    }
    // The unordered `IN (…)` form feeds the batched auxiliary reads — the
    // identical filter clauses, never a re-derived id list.
    let (subquery, subquery_params) = resolved.universe_subquery();
    Ok((ids, subquery, subquery_params))
}

/// `conversations` columns the session context needs (one set-based row
/// per universe member, no joins).
#[derive(Default)]
struct ConvParts {
    title: Option<String>,
    started_at: Option<i64>,
    ended_at: Option<i64>,
    last_message_created_at: Option<i64>,
    approx_tokens: Option<i64>,
    grand_total_tokens: Option<i64>,
    estimated_cost_usd: Option<f64>,
    primary_model: Option<String>,
    workspace_id: Option<i64>,
    agent_id: Option<i64>,
}

/// `viewer_annotations` fields the session context needs.
#[derive(Default)]
struct AnnParts {
    is_pinned: bool,
    custom_title: Option<String>,
    notes: Option<String>,
}

/// `viewer_session_facts` fields the session context needs.
#[derive(Default)]
struct FactParts {
    is_subagent: Option<bool>,
    category: Option<String>,
    coverage_state: Option<String>,
    coverage_detail_json: Option<String>,
}

/// One session-context projection assembled from the split set reads —
/// same fields as the old per-session SELECT and the 6-way LEFT JOIN it
/// replaced, minus tags/regions which the loader fills from its own
/// batched reads.
#[allow(clippy::too_many_arguments)]
fn session_ctx_from_parts(
    conversation_id: i64,
    conv: ConvParts,
    ann: AnnParts,
    fact: FactParts,
    activity_ms: Option<i64>,
    docs_state: Option<&str>,
    docs_version: Option<i64>,
    workspace_path: Option<String>,
    agent_slug: Option<String>,
) -> Result<SessionCtx> {
    let provider =
        identity::normalize_agent_slug(&agent_slug.unwrap_or_default()).unwrap_or_default();
    let mut coverage: HashMap<String, String> = HashMap::new();
    let mut coverage_reasons: HashMap<String, Vec<String>> = HashMap::new();
    if let Some(detail) = fact
        .coverage_detail_json
        .as_deref()
        .and_then(|t| serde_json::from_str::<Value>(t).ok())
        && let Some(fields) = detail.get("fields").and_then(Value::as_object)
    {
        for (field, entry) in fields {
            if let Some(state) = entry.get("state").and_then(Value::as_str) {
                coverage.insert(field.clone(), state.to_owned());
            }
            if let Some(reasons) = entry.get("reasons").and_then(Value::as_array) {
                let parsed: Vec<String> = reasons
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect();
                if !parsed.is_empty() {
                    coverage_reasons.insert(field.clone(), parsed);
                }
            }
        }
    }
    let facts_present = fact.coverage_state.is_some();
    if !facts_present {
        coverage.insert("is_subagent".to_owned(), "pending".to_owned());
        coverage.insert("category".to_owned(), "pending".to_owned());
    }
    let tokens = conv.grand_total_tokens.or(conv.approx_tokens);

    // The persisted publication mark decides inventory completeness: no
    // marks row (publication never committed) or a non-`published` docs
    // state (republished rows whose Quill region documents have not
    // converged, or a meta edit awaiting doc replacement) means this
    // session's region set cannot prove `False` — it degrades to honest
    // pending coverage instead of silently missing hits.
    let docs_pending =
        docs_state != Some(coding_agent_search::viewer_extensions::search_regions::DOCS_PUBLISHED);
    // Fast-lane convergence: `published` marks under the CURRENT
    // REGION_VERSION. A version-skew mark means the persisted region rows
    // predate the current chunking/packing contract, so their session×scope
    // documents are not trustworthy membership evidence — the session keeps
    // the whole query on the legacy lane (see [`super::engine::search_sessions`]).
    let session_docs_current = !docs_pending
        && docs_version
            == Some(coding_agent_search::viewer_extensions::search_regions::REGION_VERSION);
    let inventory_partial = docs_pending;
    if inventory_partial {
        coverage.insert("block_inventory".to_owned(), "pending".to_owned());
        coverage.insert("text_inventory".to_owned(), "pending".to_owned());
    } else {
        // Published marks prove the region inventory whole; provider facts
        // that independently report a partial inventory keep precedence.
        coverage
            .entry("block_inventory".to_owned())
            .or_insert_with(|| "complete".to_owned());
        coverage
            .entry("text_inventory".to_owned())
            .or_insert_with(|| "complete".to_owned());
    }

    // Source-absent classification (owner decision 2026-11-07): every
    // field whose incomplete coverage is PROVEN text absent at the
    // source — all of its reason codes are on the closed allowlist —
    // contributes its reasons to the per-item annotation. The docs-pending
    // overwrite above is `pending` state, which never classifies, so
    // unconverged region documents keep blocking strict search.
    let source_text_absent: Vec<String> = coverage
        .iter()
        .filter(|(field, state)| {
            is_source_absent_gap(
                state.as_str(),
                coverage_reasons
                    .get(field.as_str())
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            )
        })
        .flat_map(|(field, _)| {
            coverage_reasons
                .get(field.as_str())
                .map(Vec::as_slice)
                .unwrap_or(&[])
                .iter()
                .cloned()
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    Ok(SessionCtx {
        conversation_id,
        public_id: None,
        provider,
        project: workspace_path
            .as_deref()
            .map(identity::path_name)
            .filter(|name| !name.is_empty()),
        project_path: workspace_path,
        title: conv.title,
        custom_title: ann.custom_title,
        notes: ann.notes,
        is_pinned: ann.is_pinned,
        tags: Vec::new(),
        is_subagent: fact.is_subagent,
        category: fact.category,
        tokens,
        cost: conv.estimated_cost_usd,
        duration: match (conv.started_at, conv.ended_at) {
            (Some(start), Some(end)) => Some((end - start) as f64 / 1000.0),
            _ => None,
        },
        activity_ms,
        created_ms: conv.started_at,
        model: conv.primary_model,
        regions: Vec::new(),
        coverage,
        coverage_reasons,
        source_text_absent,
        inventory_partial,
        session_docs_current,
    })
}

pub(super) fn region_from(
    conversation_id: i64,
    row: coding_agent_search::viewer_extensions::search_regions::RegionRow,
) -> Region {
    Region {
        key: row.region_key,
        scope: row.scope,
        scope_known: row.scope_known,
        quill_idx: row.quill_idx,
        conversation_id,
        region_ordinal: row.region_ordinal,
        has_fragment: row.has_fragment,
        message_idx: row.message_idx,
        message_id: row.message_id,
        step_index: row.step_index,
        turn_index: row.turn_index,
        tool_name: row.tool_name,
        file_path: row.file_path,
        exit_code: row.exit_code,
        has_error: row.has_error,
        error_type: row.error_type,
        text_source: row.text_source,
        indexed_text: row.indexed_text,
    }
}

/// The whole candidate universe in a handful of per-table set reads —
/// `conversations`, `viewer_annotations`, `viewer_session_facts`,
/// `conversation_tail_state`, `viewer_search_region_marks`, `viewer_tags`
/// and the persisted region inventory — each bound by the SAME
/// `IN (universe subquery)`, with `workspaces`/`agents` read once as
/// small lookup maps. A single 6-way LEFT JOIN costs one engine probe
/// per joined table per universe row, which a >50k universe cannot
/// afford inside the request deadline; splitting turns that into one
/// linear scan per table plus Rust-side key assembly. NO `messages` read
/// happens here: the persisted `viewer_search_regions` rows are the whole
/// searchable surface, and the marks row decides whether that surface is
/// complete evidence or honest `pending`/`unavailable` coverage.
pub(super) fn load_sessions_indexed(
    connection: &Connection,
    universe_ids: &[i64],
    id_subquery: &str,
    id_params: &[SqliteValue],
    stats: &mut HydrateStats,
    load_universe_regions: bool,
    deadline: &Deadline,
) -> Result<Vec<SessionCtx>> {
    deadline.check("hydrate", "conversations read")?;
    let conv_rows = connection.query_with_params(
        &format!(
            "SELECT c.id, c.title, c.started_at, c.ended_at, \
             c.last_message_created_at, c.approx_tokens, c.grand_total_tokens, \
             c.estimated_cost_usd, c.primary_model, c.workspace_id, c.agent_id \
             FROM conversations c WHERE c.id IN ({id_subquery}) ORDER BY c.id"
        ),
        id_params,
    )?;
    let mut conv: HashMap<i64, ConvParts> = HashMap::with_capacity(universe_ids.len());
    for row in &conv_rows {
        let cid: i64 = row.get_typed(0)?;
        if conv
            .insert(
                cid,
                ConvParts {
                    title: row.get_typed(1)?,
                    started_at: row.get_typed(2)?,
                    ended_at: row.get_typed(3)?,
                    last_message_created_at: row.get_typed(4)?,
                    approx_tokens: row.get_typed(5)?,
                    grand_total_tokens: row.get_typed(6)?,
                    estimated_cost_usd: row.get_typed(7)?,
                    primary_model: row.get_typed(8)?,
                    workspace_id: row.get_typed(9)?,
                    agent_id: row.get_typed(10)?,
                },
            )
            .is_some()
        {
            return Err(refuse(
                "canonical_coordinates_invalid",
                "Ambiguous canonical row",
            ));
        }
    }
    deadline.check("hydrate", "viewer_annotations read")?;
    let ann_rows = connection.query_with_params(
        &format!(
            "SELECT conversation_id, is_pinned, custom_title, notes \
             FROM viewer_annotations WHERE conversation_id IN ({id_subquery})"
        ),
        id_params,
    )?;
    let mut ann: HashMap<i64, AnnParts> = HashMap::new();
    for row in &ann_rows {
        ann.insert(
            row.get_typed(0)?,
            AnnParts {
                is_pinned: row.get_typed::<Option<i64>>(1)?.unwrap_or(0) != 0,
                custom_title: row.get_typed(2)?,
                notes: row.get_typed(3)?,
            },
        );
    }
    deadline.check("hydrate", "viewer_session_facts read")?;
    let fact_rows = connection.query_with_params(
        &format!(
            "SELECT conversation_id, is_subagent, task_category, coverage_state, \
             coverage_detail_json FROM viewer_session_facts \
             WHERE conversation_id IN ({id_subquery})"
        ),
        id_params,
    )?;
    let mut facts: HashMap<i64, FactParts> = HashMap::new();
    for row in &fact_rows {
        facts.insert(
            row.get_typed(0)?,
            FactParts {
                is_subagent: row.get_typed::<Option<i64>>(1)?.map(|v| v != 0),
                category: row.get_typed(2)?,
                coverage_state: row.get_typed(3)?,
                coverage_detail_json: row.get_typed(4)?,
            },
        );
    }
    deadline.check("hydrate", "conversation_tail_state read")?;
    let tail_rows = connection.query_with_params(
        &format!(
            "SELECT conversation_id, last_message_created_at \
             FROM conversation_tail_state WHERE conversation_id IN ({id_subquery})"
        ),
        id_params,
    )?;
    let mut tail: HashMap<i64, Option<i64>> = HashMap::new();
    for row in &tail_rows {
        tail.insert(row.get_typed(0)?, row.get_typed(1)?);
    }
    deadline.check("hydrate", "region marks read")?;
    let mark_rows = connection.query_with_params(
        &format!(
            "SELECT conversation_id, docs_state, region_version \
             FROM viewer_search_region_marks \
             WHERE conversation_id IN ({id_subquery})"
        ),
        id_params,
    )?;
    let mut marks: HashMap<i64, (String, i64)> = HashMap::new();
    for row in &mark_rows {
        marks.insert(row.get_typed(0)?, (row.get_typed(1)?, row.get_typed(2)?));
    }
    // Agent/workspace slugs: small lookup tables, read once — the LEFT
    // JOIN equivalent without a probe per universe row.
    deadline.check("hydrate", "workspaces read")?;
    let workspace_rows = connection.query("SELECT id, path FROM workspaces")?;
    let mut workspaces: HashMap<i64, Option<String>> = HashMap::new();
    for row in &workspace_rows {
        workspaces.insert(row.get_typed(0)?, row.get_typed(1)?);
    }
    deadline.check("hydrate", "agents read")?;
    let agent_rows = connection.query("SELECT id, slug FROM agents")?;
    let mut agents: HashMap<i64, Option<String>> = HashMap::new();
    for row in &agent_rows {
        agents.insert(row.get_typed(0)?, row.get_typed(1)?);
    }
    deadline.check("hydrate", "viewer_tags read")?;
    let tag_rows = connection.query_with_params(
        &format!(
            "SELECT conversation_id, tag FROM viewer_tags \
             WHERE conversation_id IN ({id_subquery}) \
             ORDER BY conversation_id, tag"
        ),
        id_params,
    )?;
    let mut tags: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    for row in &tag_rows {
        tags.entry(row.get_typed::<i64>(0)?)
            .or_default()
            .push(row.get_typed::<String>(1)?);
    }
    // Stage 1b: the whole-universe region read only happens for plans that
    // can consult ANY region (see [`super::regions::plan_region_load_mode`]).
    // On-demand plans attach regions later — the gated (conversation,
    // quill_idx) seeks on the legacy lane, the emitted page's sessions on
    // the fast lane — so a zero-hit or rare-term request never pays the
    // archive-sized read (762k rows on the 6k-session archive). The single
    // biggest read on the eager lane — the whole-universe region scan
    // (~831k rows on the 4.9 GB owner archive). Uninterruptible (lib-side,
    // one statement), so it is bounded by the backstop grace when it
    // starts before expiry; the checks around it refuse cooperatively
    // before and after.
    deadline.check("hydrate", "universe region rows read")?;
    let region_rows = if load_universe_regions {
        Some(
            coding_agent_search::viewer_extensions::search_regions::regions_for_universe_subquery(
                connection,
                id_subquery,
                id_params,
            )?,
        )
    } else {
        None
    };
    let mut ordered = Vec::with_capacity(universe_ids.len());
    for (index, cid) in universe_ids.iter().enumerate() {
        deadline.check(
            "hydrate",
            format!("{index} of {} sessions assembled", universe_ids.len()),
        )?;
        let Some(conv_p) = conv.remove(cid) else {
            return Err(refuse(
                "canonical_coordinates_invalid",
                "Universe row vanished between read passes",
            ));
        };
        let activity_ms = tail
            .get(cid)
            .copied()
            .flatten()
            .or(conv_p.last_message_created_at)
            .or(conv_p.ended_at)
            .or(conv_p.started_at);
        let workspace_path = conv_p
            .workspace_id
            .and_then(|id| workspaces.get(&id).cloned().flatten());
        let agent_slug = conv_p
            .agent_id
            .and_then(|id| agents.get(&id).cloned().flatten());
        let (docs_state, docs_version) = marks
            .remove(cid)
            .map(|(state, version)| (Some(state), Some(version)))
            .unwrap_or((None, None));
        let mut session = session_ctx_from_parts(
            *cid,
            conv_p,
            ann.remove(cid).unwrap_or_default(),
            facts.remove(cid).unwrap_or_default(),
            activity_ms,
            docs_state.as_deref(),
            docs_version,
            workspace_path,
            agent_slug,
        )?;
        session.tags = tags.remove(cid).unwrap_or_default();
        session.regions = region_rows
            .as_ref()
            .and_then(|rows| rows.get(cid))
            .map(|rows| {
                rows.iter()
                    .cloned()
                    .map(|row| region_from(*cid, row))
                    .collect()
            })
            .unwrap_or_default();
        if load_universe_regions {
            stats.regions_loaded += session.regions.len();
        }
        ordered.push(session);
    }
    Ok(ordered)
}
