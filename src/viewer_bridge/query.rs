//! Indexed `list_sessions` for the `cass_owned_queries_v1` read lane.
//!
//! One bounded statement per page — `WHERE` + `ORDER BY` + `LIMIT limit+1`
//! with a keyset cursor. Never an offset walk and never a whole-catalog
//! materialization: the planner gets one statement whose LIMIT is the page
//! bound plus one, and continuation rows are reached through a
//! `(pinned, sort-value, conversation_id)` keyset predicate instead of a
//! scanned offset.
//!
//! Semantics mirror the viewer oracle
//! (`viewer/backends/cass/catalog_filters.py` + `summary.py`):
//!
//! * **activity** uses canonical tail-state precedence: a
//!   `conversation_tail_state` row replaces the denormalized
//!   `conversations` columns wholesale (`activity = last_message_created_at
//!   ?? ended_at ?? started_at`), exactly as `catalog.rs` decodes it.
//! * **sort order** is `pinned DESC, sort-value <dir> NULLS LAST,
//!   conversation_id ASC` — pinned first (the list default), known values
//!   ordered, unknowns last, deterministic `conversation_id` tie-break.
//!   Ties break on `conversation_id`, not the oracle's public-CID string:
//!   both are deterministic total orders; the canonical coordinate is the
//!   index-friendly one.
//! * **classification filters** (`is_subagent`, `category`) read
//!   `viewer_session_facts` through `LEFT JOIN`: known values filter
//!   membership; a surviving candidate with an *unknown* classification is
//!   a typed `data_unavailable`/`facts_pending` refusal — never silently
//!   dropped, never silently kept. Default browse keeps unknowns (with a
//!   `warnings` entry) and excludes only proven subagents.
//! * **activity/created windows** on rows whose timestamp is unknown are a
//!   `data_unavailable` refusal (oracle rule: unknown ≠ excluded).
//! * **unsupported filters/sorts** refuse `unsupported_filter`.
//!
//! FrankenSQLite 0.4.12 has no expression indexes, so the computed
//! activity sort cannot avoid a sorter pass over the matched candidate set;
//! the matched set is bounded by the pushed-down filters, and `EXPLAIN
//! QUERY PLAN` output is reported by the extensions test instead of
//! speculating about index coverage.

use anyhow::Result;
use coding_agent_search::franken_sync::compat::RowExt;
use coding_agent_search::franken_sync::{Connection, SqliteValue};
use serde_json::{Map, Value, json};

use super::annotations;
use super::identity;
use super::protocol::{ListFilters, RESPONSE_BYTES, refuse};
use super::revisions;

/// Per-column text guard mirroring `catalog.rs`: absurd payloads become
/// NULL, never a multi-MiB cell in a bounded page.
const TEXT_GUARD_BYTES: i64 = 512 * 1024;
/// Canonical-row byte budget (same profile as `catalog.rs::ROW_BYTES`).
const ROW_BYTES: i64 = 512 * 1024;
/// Cursor payload bound; a cursor is client-echoed opaque state.
const CURSOR_MAX_BYTES: usize = 4_096;

/// Canonical activity expression: a tail-state row replaces the
/// denormalized columns when present; activity =
/// `last_message_created_at ?? ended_at ?? started_at`.
const ACTIVITY_SQL: &str = "\
CASE WHEN t.conversation_id IS NOT NULL \
THEN COALESCE(t.last_message_created_at, t.ended_at, c.started_at) \
ELSE COALESCE(c.last_message_created_at, c.ended_at, c.started_at) END";

const FROM_SQL: &str = "\
FROM conversations c \
LEFT JOIN conversation_tail_state t ON t.conversation_id = c.id \
LEFT JOIN viewer_annotations va ON va.conversation_id = c.id \
LEFT JOIN viewer_session_facts f ON f.conversation_id = c.id \
LEFT JOIN workspaces w ON w.id = c.workspace_id \
LEFT JOIN sources s ON s.id = c.source_id";

fn guard(column: &str) -> String {
    format!(
        "CASE WHEN COALESCE(length(CAST({column} AS BLOB)),0)<={TEXT_GUARD_BYTES} \
         THEN {column} ELSE NULL END"
    )
}

// ---------------------------------------------------------------------------
// Sort plan
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortKey {
    Activity,
    Created,
    Project,
    StepCount,
    ArtifactCount,
}

impl SortKey {
    /// SQL expression producing the sort value (NULL = unknown).
    fn expr(self) -> &'static str {
        match self {
            Self::Activity => ACTIVITY_SQL,
            Self::Created => "c.started_at",
            Self::Project => "w.path",
            Self::StepCount => "f.step_count",
            Self::ArtifactCount => "f.artifact_count",
        }
    }

    /// Cursor value type: `project_asc` sorts by workspace path TEXT.
    fn cursor_value_is_text(self) -> bool {
        matches!(self, Self::Project)
    }
}

fn sort_plan(sort: Option<&str>) -> Result<(&'static str, SortKey, bool)> {
    // `relevance` belongs to the search lane, never to browsing.
    let unsupported = || {
        refuse(
            "unsupported_filter",
            format!("Unsupported session list sort: {}", sort.unwrap_or("")),
        )
    };
    Ok(match sort.unwrap_or("updated_desc") {
        "updated_desc" => ("updated_desc", SortKey::Activity, true),
        "updated_asc" => ("updated_asc", SortKey::Activity, false),
        "created_desc" => ("created_desc", SortKey::Created, true),
        "created_asc" => ("created_asc", SortKey::Created, false),
        "project_asc" => ("project_asc", SortKey::Project, false),
        "step_desc" => ("step_desc", SortKey::StepCount, true),
        "artifact_desc" => ("artifact_desc", SortKey::ArtifactCount, true),
        _ => return Err(unsupported()),
    })
}

// ---------------------------------------------------------------------------
// Filters: validation + dimension resolution
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub(crate) struct ResolvedFilters {
    /// `Some(set)` = provider filter resolved to canonical `agents.id`s
    /// (an empty set is a deterministic empty result, not an error).
    agent_ids: Option<Vec<i64>>,
    source_id: Option<String>,
    workspace_ids: Option<Vec<i64>>,
    pinned: Option<i64>,
    tag: Option<String>,
    is_subagent: Option<bool>,
    include_subagents: bool,
    category: Option<String>,
    activity_from: Option<i64>,
    activity_to: Option<i64>,
    created_from: Option<i64>,
    created_to: Option<i64>,
}

fn normalize_provider_filter(connection: &Connection, raw: &str) -> Result<Option<Vec<i64>>> {
    if matches!(
        raw.trim().to_lowercase().as_str(),
        "" | "all" | "all_providers"
    ) {
        return Ok(None);
    }
    let provider = identity::normalize_agent_slug(raw).map_err(|_| {
        refuse(
            "invalid_request",
            "Provider filter is not a valid provider namespace",
        )
    })?;
    let mut ids = Vec::new();
    // `agents` is a bounded dimension table; normalize each slug in Rust
    // because the canonical store keeps unnormalized slugs.
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

fn normalize_project_filter(connection: &Connection, raw: &str) -> Result<Option<Vec<i64>>> {
    if matches!(
        raw.trim().to_lowercase().as_str(),
        "" | "all" | "all_projects"
    ) {
        return Ok(None);
    }
    // `_project_path`: exact workspace path wins; otherwise a unique
    // display-name match; ambiguity and misses are honest, never guessed.
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
            "Project name is ambiguous; use the workspace path",
        ));
    }
    Ok(Some(display))
}

fn bounded_filter_text(value: &str, what: &str) -> Result<()> {
    if value.is_empty() || value.len() > 4096 || value.trim() != value {
        return Err(refuse(
            "invalid_request",
            format!("{what} filter must be trimmed and within bounds"),
        ));
    }
    Ok(())
}

fn resolve_filters(connection: &Connection, filters: &ListFilters) -> Result<ResolvedFilters> {
    let mut resolved = ResolvedFilters::default();

    if let Some(provider) = &filters.provider {
        if !matches!(
            provider.trim().to_lowercase().as_str(),
            "" | "all" | "all_providers"
        ) {
            bounded_filter_text(provider, "Provider")?;
        }
        resolved.agent_ids = normalize_provider_filter(connection, provider)?;
    }
    if let Some(source_id) = &filters.source_id {
        bounded_filter_text(source_id, "Source")?;
        resolved.source_id = Some(source_id.clone());
    }
    if let Some(project) = &filters.project {
        if !matches!(
            project.trim().to_lowercase().as_str(),
            "" | "all" | "all_projects"
        ) {
            bounded_filter_text(project, "Project")?;
        }
        resolved.workspace_ids = normalize_project_filter(connection, project)?;
    }
    if let Some(pinned) = filters.pinned {
        resolved.pinned = Some(i64::from(pinned));
    }
    if let Some(raw_tag) = &filters.tag {
        if raw_tag.len() > 4096 {
            return Err(refuse("invalid_request", "Tag filter exceeds bounds"));
        }
        let tag = annotations::sanitize_tag(raw_tag);
        if tag.is_empty() && !raw_tag.trim().is_empty() {
            return Err(refuse(
                "unsupported_filter",
                "Tag is empty after normalization",
            ));
        }
        if !tag.is_empty() {
            resolved.tag = Some(tag);
        }
    }
    resolved.is_subagent = filters.is_subagent;
    resolved.include_subagents = filters.include_subagents.unwrap_or(false);
    if let Some(category) = &filters.category {
        if !matches!(
            category.trim().to_lowercase().as_str(),
            "" | "all" | "all_categories"
        ) {
            bounded_filter_text(category, "Category")?;
            resolved.category = Some(category.clone());
        }
    }

    // `updated_after_ms` and `activity_from_ms` are the same lower bound
    // (oracle: `updated_after` is an activity comparison); both set is a
    // malformed request.
    if filters.updated_after_ms.is_some() && filters.activity_from_ms.is_some() {
        return Err(refuse(
            "invalid_request",
            "updated_after_ms and activity_from_ms are the same bound; send one",
        ));
    }
    resolved.activity_from = filters.activity_from_ms.or(filters.updated_after_ms);
    resolved.activity_to = filters.activity_to_ms;
    resolved.created_from = filters.created_from_ms;
    resolved.created_to = filters.created_to_ms;
    for (from, to) in [
        (resolved.activity_from, resolved.activity_to),
        (resolved.created_from, resolved.created_to),
    ] {
        if matches!((from, to), (Some(a), Some(b)) if a >= b) {
            return Err(refuse(
                "invalid_request",
                "Date window lower bound must be below the upper bound",
            ));
        }
    }
    Ok(resolved)
}

// ---------------------------------------------------------------------------
// SQL assembly
// ---------------------------------------------------------------------------

fn bind(params: &mut Vec<SqliteValue>, value: SqliteValue) -> String {
    params.push(value);
    format!("?{}", params.len())
}

fn text(value: &str) -> SqliteValue {
    SqliteValue::Text(value.to_owned().into())
}

/// Candidate-condition groups for ONE query (probe or page). Rebuilt per
/// query so `?N` placeholders always match that query's param list.
struct Conditions {
    /// Provider/source/workspace/pinned/tag — always computable.
    base: Vec<String>,
    /// Rows whose *known* `is_subagent` fails the request are excluded
    /// before unknown checks (`NOT (known AND <> requested)`), or the
    /// implicit browse exclusion (drop proven subagents, keep unknowns).
    subagent_exclusion: Option<String>,
    /// Same shape for `category`.
    category_exclusion: Option<String>,
    /// Window predicates on the activity expression.
    activity: Vec<String>,
    /// Window predicates on `c.started_at`.
    created: Vec<String>,
    /// Final classification predicates applied by the page query itself.
    classification: Vec<String>,
}

fn build_conditions(resolved: &ResolvedFilters, params: &mut Vec<SqliteValue>) -> Conditions {
    let mut base = Vec::new();
    if let Some(ids) = &resolved.agent_ids {
        if ids.is_empty() {
            base.push("1 = 0".to_owned());
        } else {
            let placeholders = ids
                .iter()
                .map(|id| bind(params, SqliteValue::Integer(*id)))
                .collect::<Vec<_>>()
                .join(",");
            base.push(format!("c.agent_id IN ({placeholders})"));
        }
    }
    if let Some(source_id) = &resolved.source_id {
        base.push(format!("c.source_id = {}", bind(params, text(source_id))));
    }
    if let Some(ids) = &resolved.workspace_ids {
        if ids.is_empty() {
            base.push("1 = 0".to_owned());
        } else {
            let placeholders = ids
                .iter()
                .map(|id| bind(params, SqliteValue::Integer(*id)))
                .collect::<Vec<_>>()
                .join(",");
            base.push(format!("c.workspace_id IN ({placeholders})"));
        }
    }
    if let Some(pinned) = resolved.pinned {
        base.push(format!(
            "COALESCE(va.is_pinned,0) = {}",
            bind(params, SqliteValue::Integer(pinned))
        ));
    }
    if let Some(tag) = &resolved.tag {
        base.push(format!(
            "EXISTS (SELECT 1 FROM viewer_tags vt \
             WHERE vt.conversation_id = c.id AND vt.tag = {})",
            bind(params, text(tag))
        ));
    }

    let subagent_exclusion = match resolved.is_subagent {
        Some(true) => Some("NOT (f.is_subagent IS NOT NULL AND f.is_subagent = 0)".to_owned()),
        Some(false) => Some("NOT (f.is_subagent IS NOT NULL AND f.is_subagent <> 0)".to_owned()),
        None if !resolved.include_subagents => Some("COALESCE(f.is_subagent,0) = 0".to_owned()),
        None => None,
    };
    let category_exclusion = resolved.category.as_ref().map(|category| {
        format!(
            "NOT (f.task_category IS NOT NULL AND f.task_category <> {})",
            bind(params, text(category))
        )
    });

    let mut activity = Vec::new();
    if let Some(from) = resolved.activity_from {
        activity.push(format!(
            "{ACTIVITY_SQL} >= {}",
            bind(params, SqliteValue::Integer(from))
        ));
    }
    if let Some(to) = resolved.activity_to {
        // Oracle: `activity >= activity_to_ms` is excluded (strictly below).
        activity.push(format!(
            "{ACTIVITY_SQL} < {}",
            bind(params, SqliteValue::Integer(to))
        ));
    }
    let mut created = Vec::new();
    if let Some(from) = resolved.created_from {
        created.push(format!(
            "c.started_at >= {}",
            bind(params, SqliteValue::Integer(from))
        ));
    }
    if let Some(to) = resolved.created_to {
        created.push(format!(
            "c.started_at < {}",
            bind(params, SqliteValue::Integer(to))
        ));
    }

    let mut classification = Vec::new();
    match (resolved.is_subagent, resolved.include_subagents) {
        (Some(true), _) => {
            classification.push("f.is_subagent IS NOT NULL AND f.is_subagent <> 0".to_owned());
        }
        (Some(false), _) => {
            classification.push("f.is_subagent IS NOT NULL AND f.is_subagent = 0".to_owned());
        }
        (None, false) => {
            classification.push("COALESCE(f.is_subagent,0) = 0".to_owned());
        }
        (None, true) => {}
    }
    if let Some(category) = &resolved.category {
        classification.push(format!(
            "f.task_category = {}",
            bind(params, text(category))
        ));
    }

    Conditions {
        base,
        subagent_exclusion,
        category_exclusion,
        activity,
        created,
        classification,
    }
}

/// Run a bounded probe (`SELECT <select> ... LIMIT 1`) over the candidate
/// set: the base conditions, the window predicates the probe does NOT
/// itself check (a NULL-activity row fails `activity >= ?` silently, so the
/// unknown-activity probe must not carry the activity predicates), the
/// classification *exclusions* (a row excluded by a known-false predicate
/// never reaches unknown checks), and `extra` (the NULL probe itself).
fn probe(
    connection: &Connection,
    resolved: &ResolvedFilters,
    select: &str,
    include_activity: bool,
    include_created: bool,
    extra: impl Fn(&Conditions, &mut Vec<String>),
) -> Result<Vec<coding_agent_search::franken_sync::Row>> {
    let mut params = Vec::new();
    let conds = build_conditions(resolved, &mut params);
    let mut clauses = conds.base.clone();
    if include_activity {
        clauses.extend(conds.activity.iter().cloned());
    }
    if include_created {
        clauses.extend(conds.created.iter().cloned());
    }
    extra(&conds, &mut clauses);
    let where_sql = if clauses.is_empty() {
        "1 = 1".to_owned()
    } else {
        clauses.join(" AND ")
    };
    let sql = format!("SELECT {select} {FROM_SQL} WHERE {where_sql} LIMIT 1");
    connection
        .query_with_params(&sql, &params)
        .map_err(Into::into)
}

// ---------------------------------------------------------------------------
// Shared read surface for `search_sessions` (Task 9): the same filter
// resolution and honest-refusal probes `list_sessions` runs, exposed so the
// search lane narrows its candidate universe through ONE SQL statement
// instead of evaluating the whole catalog.
// ---------------------------------------------------------------------------

/// `resolve_filters` for the search lane (same validation/normalization).
pub(crate) fn resolve_filters_public(
    connection: &Connection,
    filters: &ListFilters,
) -> Result<ResolvedFilters> {
    resolve_filters(connection, filters)
}

/// `run_probes` for the search lane (same honest-refusal order/warnings).
pub(crate) fn run_probes_public(
    connection: &Connection,
    resolved: &ResolvedFilters,
) -> Result<Vec<String>> {
    run_probes(connection, resolved)
}

impl ResolvedFilters {
    /// `SELECT c.id ... WHERE <conditions> ORDER BY c.id` — the bounded
    /// candidate universe for session search. Base + classification +
    /// window predicates, never a sort/limit (search sorts by relevance).
    pub(crate) fn universe_sql(&self) -> (String, Vec<SqliteValue>) {
        let mut params = Vec::new();
        let conds = build_conditions(self, &mut params);
        let mut clauses = conds.base.clone();
        clauses.extend(conds.activity.iter().cloned());
        clauses.extend(conds.created.iter().cloned());
        clauses.extend(conds.classification.iter().cloned());
        let where_sql = if clauses.is_empty() {
            "1 = 1".to_owned()
        } else {
            clauses.join(" AND ")
        };
        (
            format!("SELECT c.id {FROM_SQL} WHERE {where_sql} ORDER BY c.id"),
            params,
        )
    }
}

/// `data_unavailable` vs `facts_pending`: oracle `coverage == "pending"`
/// (including an absent facts row — extraction has not run yet) is the
/// retryable `facts_pending`; any other coverage is `data_unavailable`.
fn classification_refusal(coverage: Option<String>) -> anyhow::Error {
    match coverage.as_deref() {
        Some("pending") | None => refuse(
            "facts_pending",
            "Explicit classification filter requires unavailable session facts",
        ),
        _ => refuse(
            "data_unavailable",
            "Explicit classification filter requires unavailable session facts",
        ),
    }
}

/// Pre-page probes mirroring `filter_catalog`'s honest-refusal order:
/// unknown activity/created timestamps under a window filter, then unknown
/// classification under an explicit classification filter. A found row is
/// a typed refusal; the page query itself never runs on unanswerable sets.
fn run_probes(connection: &Connection, resolved: &ResolvedFilters) -> Result<Vec<String>> {
    let mut warnings = Vec::new();

    let has_activity_window = resolved.activity_from.is_some() || resolved.activity_to.is_some();
    if has_activity_window {
        let rows = probe(connection, resolved, "1", false, true, |conds, clauses| {
            if let Some(excl) = &conds.subagent_exclusion {
                clauses.push(excl.clone());
            }
            if let Some(excl) = &conds.category_exclusion {
                clauses.push(excl.clone());
            }
            clauses.push(format!("{ACTIVITY_SQL} IS NULL"));
        })?;
        if !rows.is_empty() {
            return Err(refuse(
                "data_unavailable",
                "Session activity time is unknown",
            ));
        }
    }
    let has_created_window = resolved.created_from.is_some() || resolved.created_to.is_some();
    if has_created_window {
        let rows = probe(connection, resolved, "1", true, false, |conds, clauses| {
            if let Some(excl) = &conds.subagent_exclusion {
                clauses.push(excl.clone());
            }
            if let Some(excl) = &conds.category_exclusion {
                clauses.push(excl.clone());
            }
            clauses.push("c.started_at IS NULL".to_owned());
        })?;
        if !rows.is_empty() {
            return Err(refuse(
                "data_unavailable",
                "Session creation time is unknown for a filtered row",
            ));
        }
    }
    if resolved.is_subagent.is_some() {
        let rows = probe(
            connection,
            resolved,
            "f.coverage_state",
            true,
            true,
            |conds, clauses| {
                if let Some(excl) = &conds.category_exclusion {
                    clauses.push(excl.clone());
                }
                clauses.push("f.is_subagent IS NULL".to_owned());
            },
        )?;
        if let Some(row) = rows.first() {
            return Err(classification_refusal(row.get_typed::<Option<String>>(0)?));
        }
    }
    if resolved.category.is_some() {
        let rows = probe(
            connection,
            resolved,
            "f.coverage_state",
            true,
            true,
            |conds, clauses| {
                if let Some(excl) = &conds.subagent_exclusion {
                    clauses.push(excl.clone());
                }
                clauses.push("f.task_category IS NULL".to_owned());
            },
        )?;
        if let Some(row) = rows.first() {
            return Err(classification_refusal(row.get_typed::<Option<String>>(0)?));
        }
    }

    // Implicit browse keeps unknown classifications (membership stays
    // exact); the warning reports that classification was not established.
    if resolved.is_subagent.is_none() && !resolved.include_subagents {
        let rows = probe(
            connection,
            resolved,
            "f.coverage_state",
            true,
            true,
            |_conds, clauses| {
                clauses.push("f.is_subagent IS NULL".to_owned());
            },
        )?;
        if let Some(row) = rows.first() {
            let coverage: Option<String> = row.get_typed(0)?;
            warnings.push(
                match coverage.as_deref() {
                    Some("pending") | None => "classification_pending",
                    _ => "classification_unavailable",
                }
                .to_owned(),
            );
        }
    }
    Ok(warnings)
}

// ---------------------------------------------------------------------------
// Keyset cursor
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Cursor {
    /// Pinned flag of the last emitted row (only when pin_first applies).
    pin: Option<i64>,
    /// `true` when the last emitted row's sort value was NULL.
    null_flag: bool,
    /// The sort value of the last emitted row (INTEGER or TEXT).
    value: Option<Value>,
    /// The last emitted `conversation_id` (strictly increasing tie-break).
    id: i64,
}

fn encode_cursor(sort_name: &str, pin_first: bool, cursor: &Cursor) -> String {
    let mut payload = Map::new();
    payload.insert("s".into(), json!(sort_name));
    payload.insert("pf".into(), json!(pin_first));
    payload.insert("nf".into(), json!(cursor.null_flag));
    payload.insert("id".into(), json!(cursor.id));
    if let Some(pin) = cursor.pin {
        payload.insert("pin".into(), json!(pin));
    }
    if let Some(value) = &cursor.value {
        payload.insert("v".into(), value.clone());
    }
    Value::Object(payload).to_string()
}

fn decode_cursor(cursor: &str, sort_name: &str, key: SortKey, pin_first: bool) -> Result<Cursor> {
    let expired = || refuse("cursor_expired", "Invalid session list cursor");
    if cursor.len() > CURSOR_MAX_BYTES || !cursor.is_ascii() {
        return Err(expired());
    }
    let value: Value = serde_json::from_str(cursor).map_err(|_| expired())?;
    let object = value.as_object().ok_or_else(expired)?;
    // The bound sort must match this request's effective sort; the sort
    // name is echoed for debuggability, the cursor structure is the gate.
    if object.get("s").and_then(Value::as_str) != Some(sort_name) {
        return Err(expired());
    }
    if object.get("pf").and_then(Value::as_bool) != Some(pin_first) {
        return Err(expired());
    }
    let null_flag = object
        .get("nf")
        .and_then(Value::as_bool)
        .ok_or_else(expired)?;
    let id = object
        .get("id")
        .and_then(Value::as_i64)
        .ok_or_else(expired)?;
    if id <= 0 {
        return Err(expired());
    }
    let pin = if pin_first {
        match object.get("pin").and_then(Value::as_i64) {
            Some(pin @ (0 | 1)) => Some(pin),
            _ => return Err(expired()),
        }
    } else {
        None
    };
    let value = if null_flag {
        None
    } else {
        let value = object.get("v").ok_or_else(expired)?;
        let valid = if key.cursor_value_is_text() {
            value
                .as_str()
                .is_some_and(|text| !text.is_empty() && text.len() <= 4096)
        } else {
            value.is_i64()
        };
        if !valid {
            return Err(expired());
        }
        Some(value.clone())
    };
    Ok(Cursor {
        pin,
        null_flag,
        value,
        id,
    })
}

/// `(pinned, sort-value, conversation_id)` keyset predicate appended to the
/// page query's WHERE. NULL sort values always order last; inside the NULL
/// group only the `conversation_id` tie-break applies.
fn keyset_clause(
    cursor: &Cursor,
    key: SortKey,
    desc: bool,
    pin_first: bool,
    params: &mut Vec<SqliteValue>,
) -> String {
    let value_expr = key.expr();
    let null_expr = format!("(CASE WHEN {value_expr} IS NULL THEN 1 ELSE 0 END)");
    let pin_expr = "COALESCE(va.is_pinned,0)";
    let cmp = if desc { "<" } else { ">" };
    match (pin_first, cursor.null_flag) {
        (true, false) => {
            let pin = cursor.pin.expect("pin_first cursor carries pin");
            let pin_bind = bind(params, SqliteValue::Integer(pin));
            let v_bind = bind(params, sql_value(cursor));
            let v_bind2 = bind(params, sql_value(cursor));
            let id_bind = bind(params, SqliteValue::Integer(cursor.id));
            format!(
                "({pin_expr} < {pin_bind} \
                 OR ({pin_expr} = {pin_bind} AND \
                     ({null_expr} = 1 \
                      OR {value_expr} {cmp} {v_bind} \
                      OR ({value_expr} = {v_bind2} AND c.id > {id_bind}))))"
            )
        }
        (true, true) => {
            let pin = cursor.pin.expect("pin_first cursor carries pin");
            let pin_bind = bind(params, SqliteValue::Integer(pin));
            let pin_bind2 = bind(params, SqliteValue::Integer(pin));
            let id_bind = bind(params, SqliteValue::Integer(cursor.id));
            format!(
                "({pin_expr} < {pin_bind} \
                 OR ({pin_expr} = {pin_bind2} AND {null_expr} = 1 AND c.id > {id_bind}))"
            )
        }
        (false, false) => {
            let v_bind = bind(params, sql_value(cursor));
            let v_bind2 = bind(params, sql_value(cursor));
            let id_bind = bind(params, SqliteValue::Integer(cursor.id));
            format!(
                "({null_expr} = 1 \
                 OR {value_expr} {cmp} {v_bind} \
                 OR ({value_expr} = {v_bind2} AND c.id > {id_bind}))"
            )
        }
        (false, true) => {
            let id_bind = bind(params, SqliteValue::Integer(cursor.id));
            format!("({null_expr} = 1 AND c.id > {id_bind})")
        }
    }
}

fn sql_value(cursor: &Cursor) -> SqliteValue {
    match cursor.value.as_ref().expect("non-null cursor value") {
        Value::String(text) => SqliteValue::Text(text.clone().into()),
        other => SqliteValue::Integer(other.as_i64().expect("integer cursor value")),
    }
}

// ---------------------------------------------------------------------------
// Page query + decode
// ---------------------------------------------------------------------------

fn select_columns() -> String {
    let mut columns = vec![
        "c.id".to_owned(),         // 0
        "c.source_id".to_owned(),  // 1
        guard("c.external_id"),    // 2
        guard("c.source_path"),    // 3
        guard("c.origin_host"),    // 4
        "s.kind".to_owned(),       // 5
        "s.host_label".to_owned(), // 6
        guard("c.title"),          // 7
        "c.started_at".to_owned(), // 8
        "CASE WHEN t.conversation_id IS NOT NULL THEN t.ended_at \
         ELSE c.ended_at END"
            .to_owned(), // 9
        "CASE WHEN t.conversation_id IS NOT NULL THEN t.last_message_idx \
         ELSE c.last_message_idx END"
            .to_owned(), // 10
        "CASE WHEN t.conversation_id IS NOT NULL THEN t.last_message_created_at \
         ELSE c.last_message_created_at END"
            .to_owned(), // 11
        format!("{ACTIVITY_SQL}"), // 12
        guard("c.primary_model"),  // 13
        "c.approx_tokens".to_owned(), // 14
        "c.grand_total_tokens".to_owned(), // 15
        "c.estimated_cost_usd".to_owned(), // 16
        "c.api_call_count".to_owned(), // 17
        "c.tool_call_count".to_owned(), // 18
        "c.user_message_count".to_owned(), // 19
        "c.assistant_message_count".to_owned(), // 20
        "COALESCE(va.is_pinned,0)".to_owned(), // 21
        guard("va.custom_title"),  // 22
        guard("va.notes"),         // 23
        "f.is_subagent".to_owned(), // 24
        "f.task_category".to_owned(), // 25
        "f.coverage_state".to_owned(), // 26
        "f.step_count".to_owned(), // 27
        "f.artifact_count".to_owned(), // 28
        "f.parent_conversation_id".to_owned(), // 29
        guard("w.path"),           // 30
    ];
    let byte_sum = [
        "c.source_id",
        "c.external_id",
        "c.source_path",
        "c.origin_host",
        "c.title",
        "c.primary_model",
        "va.custom_title",
        "va.notes",
        "w.path",
        "s.kind",
        "s.host_label",
        "f.task_category",
    ]
    .iter()
    .map(|column| format!("COALESCE(length(CAST({column} AS BLOB)),0)"))
    .collect::<Vec<_>>()
    .join("+");
    columns.push(byte_sum); // 31
    columns.join(",")
}

fn build_page_sql(
    resolved: &ResolvedFilters,
    key: SortKey,
    desc: bool,
    pin_first: bool,
    cursor: Option<&Cursor>,
    limit: usize,
) -> (String, Vec<SqliteValue>) {
    let mut params = Vec::new();
    let conds = build_conditions(resolved, &mut params);
    let mut clauses = conds.base.clone();
    clauses.extend(conds.activity.iter().cloned());
    clauses.extend(conds.created.iter().cloned());
    clauses.extend(conds.classification.iter().cloned());
    if let Some(cursor) = cursor {
        clauses.push(keyset_clause(cursor, key, desc, pin_first, &mut params));
    }
    let value_expr = key.expr();
    let mut order = String::new();
    if pin_first {
        order.push_str("COALESCE(va.is_pinned,0) DESC, ");
    }
    order.push_str(&format!(
        "(CASE WHEN {value_expr} IS NULL THEN 1 ELSE 0 END), \
         {value_expr} {}, c.id",
        if desc { "DESC" } else { "ASC" }
    ));
    let limit_bind = bind(&mut params, SqliteValue::Integer((limit + 1) as i64));
    let where_sql = if clauses.is_empty() {
        "1 = 1".to_owned()
    } else {
        clauses.join(" AND ")
    };
    let sql = format!(
        "SELECT {} {FROM_SQL} WHERE {where_sql} ORDER BY {order} LIMIT {limit_bind}",
        select_columns()
    );
    (sql, params)
}

fn nonnegative_val(value: Option<i64>) -> Result<Value> {
    if value.is_some_and(|n| n < 0) {
        return Err(refuse(
            "canonical_coordinates_invalid",
            "Negative canonical counter",
        ));
    }
    Ok(json!(value))
}

fn push_bounded(rows: &mut Vec<Value>, row: Value, bytes: &mut usize) -> Result<bool> {
    let row_bytes = serde_json::to_vec(&row)?.len() + 2;
    if *bytes + row_bytes > RESPONSE_BYTES - 8192 {
        if rows.is_empty() {
            return Err(refuse(
                "canonical_payload_too_large",
                "Page exceeds bridge response budget; request fewer rows",
            ));
        }
        return Ok(false);
    }
    *bytes += row_bytes;
    rows.push(row);
    Ok(true)
}

/// The `(pinned, nf, value, id)` tuple of an emitted row — becomes the
/// `next_cursor` position.
fn cursor_tuple(
    row: &coding_agent_search::franken_sync::Row,
    key: SortKey,
    pin_first: bool,
) -> Result<Cursor> {
    let value = match key {
        SortKey::Activity => row.get_typed::<Option<i64>>(12)?.map(Value::from),
        SortKey::Created => row.get_typed::<Option<i64>>(8)?.map(Value::from),
        SortKey::Project => row.get_typed::<Option<String>>(30)?.map(Value::from),
        SortKey::StepCount => row.get_typed::<Option<i64>>(27)?.map(Value::from),
        SortKey::ArtifactCount => row.get_typed::<Option<i64>>(28)?.map(Value::from),
    };
    Ok(Cursor {
        pin: pin_first.then(|| row.get_typed::<i64>(21)).transpose()?,
        null_flag: value.is_none(),
        value,
        id: row.get_typed(0)?,
    })
}

/// `list_sessions(ListSessionsRequest{filters, sort, cursor, limit})`
/// page body: `items` + `next_cursor` + `generation` + `complete` +
/// `warnings`. `Session::list_sessions` wraps it in the snapshot
/// discipline (`archive_stamp`/`catalog_revision` come from the caller).
pub fn list_sessions(
    connection: &Connection,
    filters: &ListFilters,
    sort: Option<&str>,
    pin_first: Option<bool>,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Value> {
    if !revisions::extensions_installed(connection)? {
        return Err(refuse(
            "viewer_extensions_missing",
            "The viewer extension profile is not installed; run --init-extensions first",
        ));
    }
    let resolved = resolve_filters(connection, filters)?;
    let (sort_name, key, desc) = sort_plan(sort)?;
    let pin_first = pin_first.unwrap_or(true);
    let cursor = match cursor {
        Some(raw) => Some(decode_cursor(raw, sort_name, key, pin_first)?),
        None => None,
    };
    let warnings = run_probes(connection, &resolved)?;

    let (sql, params) = build_page_sql(&resolved, key, desc, pin_first, cursor.as_ref(), limit);
    let rows = connection.query_with_params(&sql, &params)?;

    let generation = revisions::generation(connection)?;
    if rows.is_empty() {
        return Ok(json!({
            "items": Vec::<Value>::new(),
            "next_cursor": Value::Null,
            "generation": generation,
            "complete": true,
            "warnings": warnings,
        }));
    }

    // Decode bounded; a page may serialize fewer rows than the SQL
    // returned when the response byte budget intervenes.
    let take_count = rows.len().min(limit);
    let mut items = Vec::new();
    let mut bytes = 0_usize;
    let mut last: Option<(i64, Cursor)> = None;
    let mut truncated = false;
    for row in rows.iter().take(take_count) {
        let total_bytes: i64 = row.get_typed(31)?;
        if !(0..=ROW_BYTES).contains(&total_bytes) {
            return Err(refuse(
                "canonical_payload_too_large",
                "Canonical row exceeds the strict bridge byte profile",
            ));
        }
        let item = decode_item(row)?;
        let position = cursor_tuple(row, key, pin_first)?;
        let id: i64 = row.get_typed(0)?;
        if !push_bounded(&mut items, item, &mut bytes)? {
            truncated = true;
            break;
        }
        last = Some((id, position));
    }

    // Items keep insertion order; each carries its canonical coordinate.
    let emitted_ids: Vec<i64> = items
        .iter()
        .map(|item| item["conversation_id"].as_i64().expect("decoded id"))
        .collect();
    let public_ids = identity::resolve_public_ids(connection, &emitted_ids)?;
    let tags = annotations::tags_for(connection, &emitted_ids)?;
    for item in &mut items {
        let id = item["conversation_id"].as_i64().expect("decoded id");
        if let Some(identity) = public_ids.get(&id) {
            item["public_id"] = json!(identity.public_id);
            item["provider"] = json!(identity.provider);
        }
        item["tags"] = json!(tags.get(&id).cloned().unwrap_or_default());
    }

    let has_more = truncated || rows.len() > limit;
    let next_cursor = if has_more {
        let (_, position) = last.expect("non-empty page has a cursor position");
        Value::String(encode_cursor(sort_name, pin_first, &position))
    } else {
        Value::Null
    };

    Ok(json!({
        "items": items,
        "next_cursor": next_cursor,
        "generation": generation,
        "complete": true,
        "warnings": warnings,
    }))
}

/// Bounded item decode — the same coordinates `catalog.rs` serves plus the
/// annotation/facts overlay. `public_id`/`provider`/`tags` are filled in
/// after page emission (`identity` + `tags_for`).
fn decode_item(row: &coding_agent_search::franken_sync::Row) -> Result<Value> {
    let id: i64 = row.get_typed(0)?;
    if id <= 0 {
        return Err(refuse(
            "canonical_coordinates_invalid",
            "Nonpositive conversation identity",
        ));
    }
    let cost: Option<f64> = row.get_typed(16)?;
    if cost.is_some_and(|cost| !cost.is_finite() || cost < 0.0) {
        return Err(refuse(
            "canonical_coordinates_invalid",
            "Invalid canonical cost",
        ));
    }
    let origin_host: Option<String> = row
        .get_typed::<Option<String>>(4)?
        .or(row.get_typed::<Option<String>>(6)?);
    let workspace_path: Option<String> = row.get_typed(30)?;
    let project = workspace_path
        .as_deref()
        .map(identity::path_name)
        .filter(|name| !name.is_empty());
    let is_subagent: Option<i64> = row.get_typed(24)?;

    let mut item = Map::new();
    item.insert("conversation_id".into(), json!(id));
    item.insert("public_id".into(), Value::Null);
    item.insert("provider".into(), Value::Null);
    item.insert("source_id".into(), json!(row.get_typed::<String>(1)?));
    item.insert(
        "source_kind".into(),
        json!(row.get_typed::<Option<String>>(5)?),
    );
    item.insert(
        "source_path".into(),
        json!(row.get_typed::<Option<String>>(3)?),
    );
    item.insert(
        "external_id".into(),
        json!(row.get_typed::<Option<String>>(2)?),
    );
    item.insert("origin_host".into(), json!(origin_host));
    item.insert("workspace_path".into(), json!(workspace_path));
    item.insert("project".into(), json!(project));
    item.insert("title".into(), json!(row.get_typed::<Option<String>>(7)?));
    item.insert(
        "custom_title".into(),
        json!(row.get_typed::<Option<String>>(22)?),
    );
    item.insert("notes".into(), json!(row.get_typed::<Option<String>>(23)?));
    item.insert("is_pinned".into(), json!(row.get_typed::<i64>(21)? != 0));
    item.insert("tags".into(), json!(Vec::<String>::new()));
    item.insert(
        "started_at_ms".into(),
        json!(row.get_typed::<Option<i64>>(8)?),
    );
    item.insert(
        "ended_at_ms".into(),
        json!(row.get_typed::<Option<i64>>(9)?),
    );
    item.insert(
        "last_message_created_at_ms".into(),
        json!(row.get_typed::<Option<i64>>(11)?),
    );
    item.insert(
        "last_message_idx".into(),
        nonnegative_val(row.get_typed(10)?)?,
    );
    item.insert(
        "activity_ms".into(),
        json!(row.get_typed::<Option<i64>>(12)?),
    );
    item.insert(
        "primary_model".into(),
        json!(row.get_typed::<Option<String>>(13)?),
    );
    for (key, index) in [
        ("approx_tokens", 14),
        ("grand_total_tokens", 15),
        ("api_call_count", 17),
        ("tool_call_count", 18),
        ("user_message_count", 19),
        ("assistant_message_count", 20),
    ] {
        item.insert(key.into(), nonnegative_val(row.get_typed(index)?)?);
    }
    item.insert("estimated_cost_usd".into(), json!(cost));
    item.insert(
        "is_subagent".into(),
        json!(is_subagent.map(|value| value != 0)),
    );
    item.insert(
        "task_category".into(),
        json!(row.get_typed::<Option<String>>(25)?),
    );
    item.insert(
        "classification_coverage".into(),
        json!(row.get_typed::<Option<String>>(26)?),
    );
    item.insert(
        "parent_conversation_id".into(),
        json!(row.get_typed::<Option<i64>>(29)?),
    );
    Ok(Value::Object(item))
}

/// `EXPLAIN` support for the extensions test: the exact page SQL + params
/// the serving path would run (probes included). Not part of the wire
/// protocol — a qualification/audit surface.
#[allow(dead_code)] // Test-only audit entry point (included via #[path]).
pub fn planned_page_sql(
    connection: &Connection,
    filters: &ListFilters,
    sort: Option<&str>,
    pin_first: Option<bool>,
    cursor: Option<&str>,
    limit: usize,
) -> Result<(String, Vec<SqliteValue>)> {
    let resolved = resolve_filters(connection, filters)?;
    let (sort_name, key, desc) = sort_plan(sort)?;
    let pin_first = pin_first.unwrap_or(true);
    let cursor = match cursor {
        Some(raw) => Some(decode_cursor(raw, sort_name, key, pin_first)?),
        None => None,
    };
    Ok(build_page_sql(
        &resolved,
        key,
        desc,
        pin_first,
        cursor.as_ref(),
        limit,
    ))
}
