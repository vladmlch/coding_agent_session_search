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
//! * **sort order** is the shared oracle's total order exactly:
//!   `pinned DESC, knownness DESC, sort-value <dir>, public tie key ASC,
//!   conversation_id ASC` — pinned first (the list default), known values
//!   ordered, unknowns last, and ties resolved by the verified public
//!   identity (`{provider}:{native}`), not an internal coordinate. The
//!   tuple is persisted on `viewer_session_sort` (schema v5): pin state
//!   and the five scalar sort values are trigger-maintained mirrors; the
//!   tie key is recomputed through the same `identity` module the emit
//!   path verifies with (`viewer_extensions::sort` writes it inside the
//!   ingest generation transaction; the maintenance lanes converge it).
//! * **classification filters** (`is_subagent`, `category`) read the sort
//!   row's facts mirrors: known values filter membership; a surviving
//!   candidate with an *unknown* classification is a typed
//!   `data_unavailable`/`facts_pending` refusal — never silently dropped,
//!   never silently kept. Default browse keeps unknowns (with a
//!   `warnings` entry) and excludes only proven subagents.
//! * **activity/created windows** on rows whose timestamp is unknown are a
//!   `data_unavailable` refusal (oracle rule: unknown ≠ excluded).
//! * **unresolved identities** inside the candidate set refuse rather than
//!   emit an incomparable order: a pending key is `facts_pending`, a
//!   persisted resolver conflict surfaces its own kind.
//! * **unsupported filters/sorts** refuse `unsupported_filter`.
//!
//! A page is a handful of bounded statements, never one sorter pass: the
//! order query walks one per-sort composite index on `viewer_session_sort`
//! (`is_pinned DESC, knownness DESC, value, tie_key` — rowid is the
//! implicit final key) and returns at most `limit+1` ids plus their order
//! keys; payload hydration then runs one `WHERE id IN (page ids)` probe
//! per side table — repeated primary-key seeks under FrankenSQLite —
//! and assembles the payload rows in Rust. `pin_first=false` runs the
//! same indexed scan once per pin partition and merge-selects, still
//! `O(limit)`.

use anyhow::Result;
use coding_agent_search::franken_sync::compat::RowExt;
use coding_agent_search::franken_sync::{Connection, SqliteValue};
use serde_json::{Map, Value, json};

use super::annotations;
use super::facts_bag;
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

/// Canonical activity expression semantics: a tail-state row replaces
/// the denormalized columns when present; activity =
/// `last_message_created_at ?? ended_at ?? started_at`. Persisted into
/// `viewer_session_sort` by the v5 triggers and reproduced in the Rust
/// hydration assembly (`hydrate_rows`).

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
    /// Persisted sort-value column on `viewer_session_sort` (NULL = unknown).
    fn value_column(self) -> &'static str {
        match self {
            Self::Activity => "vs.activity_ms",
            Self::Created => "vs.created_ms",
            Self::Project => "vs.project_path",
            Self::StepCount => "vs.step_count",
            Self::ArtifactCount => "vs.artifact_count",
        }
    }

    /// Persisted knownness flag for the sort value (`1` = known).
    fn known_column(self) -> &'static str {
        match self {
            Self::Activity => "vs.activity_known",
            Self::Created => "vs.created_known",
            Self::Project => "vs.project_known",
            Self::StepCount => "vs.steps_known",
            Self::ArtifactCount => "vs.artifacts_known",
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
    if let Some(category) = &filters.category
        && !matches!(
            category.trim().to_lowercase().as_str(),
            "" | "all" | "all_categories"
        )
    {
        bounded_filter_text(category, "Category")?;
        resolved.category = Some(category.clone());
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
            base.push(format!("vs.agent_id IN ({placeholders})"));
        }
    }
    if let Some(source_id) = &resolved.source_id {
        base.push(format!("vs.source_id = {}", bind(params, text(source_id))));
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
            base.push(format!("vs.workspace_id IN ({placeholders})"));
        }
    }
    if let Some(pinned) = resolved.pinned {
        base.push(format!(
            "vs.is_pinned = {}",
            bind(params, SqliteValue::Integer(pinned))
        ));
    }
    if let Some(tag) = &resolved.tag {
        // Tags stay a per-row EXISTS probe on the keyed `viewer_tags` index:
        // they are user state, never a mirrored sort column.
        base.push(format!(
            "EXISTS (SELECT 1 FROM viewer_tags vt \
             WHERE vt.conversation_id = vs.conversation_id AND vt.tag = {})",
            bind(params, text(tag))
        ));
    }

    let subagent_exclusion = match resolved.is_subagent {
        Some(true) => Some("NOT (vs.is_subagent IS NOT NULL AND vs.is_subagent = 0)".to_owned()),
        Some(false) => Some("NOT (vs.is_subagent IS NOT NULL AND vs.is_subagent <> 0)".to_owned()),
        None if !resolved.include_subagents => Some("COALESCE(vs.is_subagent,0) = 0".to_owned()),
        None => None,
    };
    let category_exclusion = resolved.category.as_ref().map(|category| {
        format!(
            "NOT (vs.task_category IS NOT NULL AND vs.task_category <> {})",
            bind(params, text(category))
        )
    });

    let mut activity = Vec::new();
    if let Some(from) = resolved.activity_from {
        activity.push(format!(
            "vs.activity_ms >= {}",
            bind(params, SqliteValue::Integer(from))
        ));
    }
    if let Some(to) = resolved.activity_to {
        // Oracle: `activity >= activity_to_ms` is excluded (strictly below).
        activity.push(format!(
            "vs.activity_ms < {}",
            bind(params, SqliteValue::Integer(to))
        ));
    }
    let mut created = Vec::new();
    if let Some(from) = resolved.created_from {
        created.push(format!(
            "vs.created_ms >= {}",
            bind(params, SqliteValue::Integer(from))
        ));
    }
    if let Some(to) = resolved.created_to {
        created.push(format!(
            "vs.created_ms < {}",
            bind(params, SqliteValue::Integer(to))
        ));
    }

    let mut classification = Vec::new();
    match (resolved.is_subagent, resolved.include_subagents) {
        (Some(true), _) => {
            classification.push("vs.is_subagent IS NOT NULL AND vs.is_subagent <> 0".to_owned());
        }
        (Some(false), _) => {
            classification.push("vs.is_subagent IS NOT NULL AND vs.is_subagent = 0".to_owned());
        }
        (None, false) => {
            classification.push("COALESCE(vs.is_subagent,0) = 0".to_owned());
        }
        (None, true) => {}
    }
    if let Some(category) = &resolved.category {
        classification.push(format!(
            "vs.task_category = {}",
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
    // Probes run against the persisted sort row: a surviving candidate is
    // the same row the order query would emit, on the same columns.
    let sql = format!("SELECT {select} FROM viewer_session_sort vs WHERE {where_sql} LIMIT 1");
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
    /// `SELECT vs.conversation_id ... WHERE <conditions> ORDER BY
    /// vs.conversation_id` — the bounded candidate universe for session
    /// search over the persisted sort row. Base + classification + window
    /// predicates, never a sort/limit (search sorts by relevance).
    pub(crate) fn universe_sql(&self) -> (String, Vec<SqliteValue>) {
        let (subquery, params) = self.universe_subquery();
        (format!("{subquery} ORDER BY vs.conversation_id"), params)
    }

    /// `SELECT vs.conversation_id FROM viewer_session_sort vs WHERE <same
    /// clauses>` WITHOUT the ordering — the set-based `IN (…)` form the
    /// search lane's batched universe reads embed, so every auxiliary read
    /// (summary join, tags, region inventory) materializes the identical
    /// filtered candidate set instead of re-transporting an id list.
    pub(crate) fn universe_subquery(&self) -> (String, Vec<SqliteValue>) {
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
            format!("SELECT vs.conversation_id FROM viewer_session_sort vs WHERE {where_sql}"),
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
/// The owned-query lane serves ordering and filtering exclusively through
/// `viewer_session_sort` — a profile behind EXTENSION_SCHEMA_VERSION (v6)
/// is a stale extension install, refused with the same kind as a missing
/// one.
fn require_sort_profile(connection: &Connection) -> Result<()> {
    let version = coding_agent_search::viewer_extensions::schema::installed_version(connection)?;
    match version {
        Some(v)
            if v >= coding_agent_search::viewer_extensions::schema::EXTENSION_SCHEMA_VERSION =>
        {
            Ok(())
        }
        _ => Err(refuse(
            "viewer_extensions_missing",
            "The viewer extension profile is missing or stale (schema v6 sort keys required); \
             run --init-extensions first",
        )),
    }
}

fn run_probes(connection: &Connection, resolved: &ResolvedFilters) -> Result<Vec<String>> {
    require_sort_profile(connection)?;
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
            clauses.push("vs.activity_ms IS NULL".to_owned());
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
            clauses.push("vs.created_ms IS NULL".to_owned());
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
            "vs.coverage_state",
            true,
            true,
            |conds, clauses| {
                if let Some(excl) = &conds.category_exclusion {
                    clauses.push(excl.clone());
                }
                clauses.push("vs.is_subagent IS NULL".to_owned());
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
            "vs.coverage_state",
            true,
            true,
            |conds, clauses| {
                if let Some(excl) = &conds.subagent_exclusion {
                    clauses.push(excl.clone());
                }
                clauses.push("vs.task_category IS NULL".to_owned());
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
            "vs.coverage_state",
            true,
            true,
            |_conds, clauses| {
                clauses.push("vs.is_subagent IS NULL".to_owned());
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

/// Candidate-set probe for unresolved tie keys — every sort orders unknowns
/// and equal values by the verified public identity, so a NULL `tie_key`
/// inside the filtered set makes the order non-computable. A pending key
/// refuses `facts_pending`; a persisted resolver refusal surfaces its own
/// kind (`identity_conflict`, `provenance_conflict`, `session_not_found`).
fn run_tie_probe(connection: &Connection, resolved: &ResolvedFilters) -> Result<()> {
    let rows = probe(
        connection,
        resolved,
        "vs.tie_error",
        true,
        true,
        |conds, clauses| {
            clauses.extend(conds.classification.iter().cloned());
            clauses.push("vs.tie_key IS NULL".to_owned());
        },
    )?;
    if let Some(row) = rows.first() {
        let stored: Option<String> = row.get_typed(0)?;
        let kind = match stored.as_deref() {
            Some("identity_conflict") => "identity_conflict",
            Some("provenance_conflict") => "provenance_conflict",
            Some("session_not_found") => "session_not_found",
            Some("canonical_coordinates_invalid") => "canonical_coordinates_invalid",
            _ => "facts_pending",
        };
        return Err(refuse(
            kind,
            "Unresolved public session identity in the browse candidate set; \
             repair with `cass index` (the ingest lane converges pending keys) \
             or `cass-viewer-bridge --init-extensions`",
        ));
    }
    Ok(())
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
    /// The verified public tie key of the last emitted row — the oracle's
    /// explicit-order tie coordinate for unknowns and equal values.
    tie: String,
    /// The last emitted `conversation_id` (strictly increasing final key).
    id: i64,
}

fn encode_cursor(sort_name: &str, pin_first: bool, cursor: &Cursor) -> String {
    let mut payload = Map::new();
    payload.insert("s".into(), json!(sort_name));
    payload.insert("pf".into(), json!(pin_first));
    payload.insert("nf".into(), json!(cursor.null_flag));
    payload.insert("t".into(), json!(cursor.tie));
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
    let tie = object
        .get("t")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty() && text.len() <= 4096)
        .ok_or_else(expired)?
        .to_owned();
    Ok(Cursor {
        pin,
        null_flag,
        value,
        tie,
        id,
    })
}

/// `(pinned, knownness, sort-value, tie-key, conversation_id)` keyset
/// predicate appended to the order query's WHERE — the exact tuple the
/// per-sort composite index orders by. Unknowns (`*_known = 0`) sit behind
/// every known row inside a pin partition and order among themselves by
/// `(tie_key, conversation_id)` only; the index's implicit rowid tail makes
/// `conversation_id` a strictly increasing final key.
fn keyset_clause(
    cursor: &Cursor,
    key: SortKey,
    desc: bool,
    pin_first: bool,
    params: &mut Vec<SqliteValue>,
) -> String {
    let value_col = key.value_column();
    let known_col = key.known_column();
    let cmp = if desc { "<" } else { ">" };
    let id_bind = bind(params, SqliteValue::Integer(cursor.id));
    let tie_bind = bind(params, SqliteValue::Text(cursor.tie.clone().into()));
    let tie_bind2 = bind(params, SqliteValue::Text(cursor.tie.clone().into()));
    // Rows behind a cursor position are `(key-part greater)` or
    // `(equal prefix AND greater tail)` — the same lexicographic split at
    // every level: pin, then knownness, then value, then tie, then rowid.
    let tie_tail = format!(
        "(vs.tie_key > {tie_bind} OR (vs.tie_key = {tie_bind2} AND vs.conversation_id > {id_bind}))"
    );
    let value_tail = if cursor.null_flag {
        // Unknown group: only (tie, id) advances.
        format!("({known_col} = 0 AND {tie_tail})")
    } else {
        let v_bind = bind(params, sql_value(cursor));
        let v_bind2 = bind(params, sql_value(cursor));
        format!(
            "({known_col} = 0 \
             OR {value_col} {cmp} {v_bind} \
             OR ({value_col} = {v_bind2} AND {tie_tail}))"
        )
    };
    if pin_first {
        let pin = cursor.pin.expect("pin_first cursor carries pin");
        let pin_bind = bind(params, SqliteValue::Integer(pin));
        let pin_bind2 = bind(params, SqliteValue::Integer(pin));
        format!(
            "(vs.is_pinned < {pin_bind} \
             OR (vs.is_pinned = {pin_bind2} AND {value_tail}))"
        )
    } else {
        format!("({value_tail})")
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

/// The 32-cell payload contract `decode_item` consumes (cell index →
/// meaning). Built in Rust by `hydrate_rows` from bounded per-table `IN`
/// probes — the shape the old six-table join produced:
/// `id, source_id, external_id*, source_path*, origin_host*, source_kind,
/// source_label, title*, started_at, ended_at†, last_message_idx†,
/// last_message_created_at†, activity‡, primary_model*, approx_tokens,
/// grand_total_tokens, estimated_cost_usd, api_call_count,
/// tool_call_count, user_message_count, assistant_message_count,
/// is_pinned§, custom_title*, notes*, is_subagent, task_category,
/// coverage_state, step_count, artifact_count, parent_conversation_id,
/// workspace_path*, byte_sum∥`
/// (`*` = TEXT_GUARD_BYTES-guarded, `†` = tail-state precedence,
/// `‡` = `lmca ?? ended ?? started`, `§` = absent annotation → 0,
/// `∥` = byte-length sum over the raw text cells).
///
/// Phase A — the index-ordered scan. One statement per pin partition
/// (exactly one when `pin_first` applies or a pinned filter already binds
/// the partition): `ORDER BY` mirrors the composite index's column order
/// verbatim so FrankenSQLite satisfies the sort from the index itself —
/// no sorter, no full-candidate materialization, `LIMIT limit+1` rows.
///
/// The selected columns are exactly the order-key tuple; `cursor_tuple`
/// resumes from them, so a concurrent write between the phases can never
/// move the emitted page's recorded position.
fn build_order_sql(
    resolved: &ResolvedFilters,
    key: SortKey,
    desc: bool,
    pin_first: bool,
    pin_partition: Option<i64>,
    cursor: Option<&Cursor>,
    limit: usize,
) -> (String, Vec<SqliteValue>) {
    let mut params = Vec::new();
    let conds = build_conditions(resolved, &mut params);
    let mut clauses = conds.base.clone();
    clauses.extend(conds.activity.iter().cloned());
    clauses.extend(conds.created.iter().cloned());
    clauses.extend(conds.classification.iter().cloned());
    if let Some(pin) = pin_partition {
        clauses.push(format!(
            "vs.is_pinned = {}",
            bind(&mut params, SqliteValue::Integer(pin))
        ));
    }
    if let Some(cursor) = cursor {
        clauses.push(keyset_clause(cursor, key, desc, pin_first, &mut params));
    }
    let limit_bind = bind(&mut params, SqliteValue::Integer((limit + 1) as i64));
    let where_sql = if clauses.is_empty() {
        "1 = 1".to_owned()
    } else {
        clauses.join(" AND ")
    };
    // Column order MUST mirror `idx_viewer_sort_*` exactly: fsqlite
    // satisfies the ORDER BY only when every term is an index column —
    // a trailing `conversation_id` term (the implicit rowid tail, which
    // still orders the scan) would force a sorter pass (probe-verified).
    // `*_known DESC` puts known values (flag 1) before unknowns (0) —
    // the oracle's NULLS LAST.
    let sql = format!(
        "SELECT vs.conversation_id, vs.is_pinned, {known}, {value}, vs.tie_key \
         FROM viewer_session_sort vs \
         WHERE {where_sql} \
         ORDER BY vs.is_pinned DESC, {known} DESC, {value} {dir}, vs.tie_key ASC \
         LIMIT {limit_bind}",
        known = key.known_column(),
        value = key.value_column(),
        dir = if desc { "DESC" } else { "ASC" },
    );
    (sql, params)
}

/// Phase B — payload hydration for one page id set. FrankenSQLite cannot
/// compile `IN (list)` inside a JOIN and degrades OR-ed equalities to a
/// scan (both probe-verified), while an `IN` list on a standalone table
/// resolves to repeated `SeekRowid` lookups — so the page runs one
/// bounded probe per side table and assembles the `decode_item` row
/// contract in Rust. Per page that is a handful of `O(page)` statements
/// instead of `O(page)` interpreted six-table joins.
const HYDRATE_CONVERSATIONS_SQL: &str = "\
SELECT c.id, c.source_id, c.external_id, c.source_path, c.origin_host, \
c.title, c.started_at, c.ended_at, c.last_message_idx, \
c.last_message_created_at, c.primary_model, c.approx_tokens, \
c.grand_total_tokens, c.estimated_cost_usd, c.api_call_count, \
c.tool_call_count, c.user_message_count, c.assistant_message_count, \
c.workspace_id, c.total_input_tokens, c.total_output_tokens, \
c.total_cache_read_tokens, c.total_cache_creation_tokens \
FROM conversations c WHERE c.id IN (";

const HYDRATE_TAIL_SQL: &str = "\
SELECT t.conversation_id, t.ended_at, t.last_message_idx, \
t.last_message_created_at \
FROM conversation_tail_state t WHERE t.conversation_id IN (";

const HYDRATE_ANNOTATIONS_SQL: &str = "\
SELECT va.conversation_id, va.is_pinned, va.custom_title, va.notes \
FROM viewer_annotations va WHERE va.conversation_id IN (";

const HYDRATE_FACTS_SQL: &str = "\
SELECT f.conversation_id, f.is_subagent, f.task_category, \
f.coverage_state, f.step_count, f.artifact_count, f.parent_conversation_id, \
f.turn_count, f.thinking_block_count, f.subagent_count, f.tool_error_count, \
f.tool_calls_with_exit_status, f.file_fact_count, f.compaction_count, \
f.model_set_json, f.first_prompt \
FROM viewer_session_facts f WHERE f.conversation_id IN (";

/// Metrics probe — the `viewer_session_metrics` twin of the facts bag.
/// Presence is probed on `sqlite_master` first (the table only exists in
/// the full extension profile), so a facts-only archive still serves
/// every fact field with the metric fields typed NULL.
const HYDRATE_METRICS_SQL: &str = "\
SELECT m.conversation_id, m.ttfe_seconds, m.one_shot_rate, m.retries_count, \
m.self_corrections_count, m.user_corrections_count, m.tools_used_json, \
m.task_breakdown_json, m.health_score, m.health_grade, \
m.health_issues_count, m.potential_waste_tokens, m.potential_savings_usd \
FROM viewer_session_metrics m WHERE m.conversation_id IN (";

const HYDRATE_WORKSPACES_SQL: &str = "SELECT w.id, w.path FROM workspaces w WHERE w.id IN (";

const HYDRATE_SOURCES_SQL: &str =
    "SELECT s.id, s.kind, s.host_label FROM sources s WHERE s.id IN (";

/// `decode_item`'s row contract, assembled in Rust from the bounded
/// per-table probes — index-for-index the same shape the old single
/// `FROM_SQL` join produced (see `select_columns`), with the four
/// cumulative token sub-totals appended (cells 32–35) and the merged
/// persisted fact/metrics bag carried alongside (Task 7).
struct HydratedRow {
    cells: Vec<SqliteValue>,
    /// `null` when no `viewer_session_facts` row was persisted for this
    /// session; otherwise the merged facts+metrics bag.
    metrics: Value,
}

impl HydratedRow {
    /// Same semantics as `RowExt::get_typed` on a fetched `Row`.
    fn get_typed<T: coding_agent_search::franken_sync::compat::FromSqliteValue>(
        &self,
        index: usize,
    ) -> Result<T, coding_agent_search::franken_sync::FrankenError> {
        let value = self.cells.get(index).ok_or_else(|| {
            coding_agent_search::franken_sync::FrankenError::NoSuchColumn {
                name: format!("column index {index}"),
            }
        })?;
        T::from_sqlite_value(value)
    }
}

/// Text payload guard mirroring the SQL `guard()` expression: payloads
/// beyond the strict byte profile decode as NULL.
fn guarded_text(value: Option<String>) -> SqliteValue {
    match value {
        Some(text) if text.len() as i64 <= TEXT_GUARD_BYTES => SqliteValue::Text(text.into()),
        _ => SqliteValue::Null,
    }
}

fn integer_or_null(value: Option<i64>) -> SqliteValue {
    value.map(SqliteValue::Integer).unwrap_or(SqliteValue::Null)
}

/// `length(CAST(x AS BLOB))` on a Rust-held optional text.
fn byte_len(value: Option<&str>) -> i64 {
    value.map(|text| text.len() as i64).unwrap_or(0)
}

fn run_in_probe(
    connection: &Connection,
    head: &str,
    tail: &str,
    ids: &[i64],
) -> Result<Vec<coding_agent_search::franken_sync::Row>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut params = Vec::with_capacity(ids.len());
    let placeholders = ids
        .iter()
        .map(|id| bind(&mut params, SqliteValue::Integer(*id)))
        .collect::<Vec<_>>()
        .join(",");
    Ok(connection.query_with_params(&format!("{head}{placeholders}{tail}"), &params)?)
}

/// Hydrate every emitted id with bounded standalone-table `IN` probes —
/// each probe compiles to repeated `SeekRowid` on a primary-key or keyed
/// index, so page hydration is `O(page)` seeks, never a scan and never a
/// per-row interpreted join. Tail-state precedence, the annotation/facts
/// overlays, the text guards and the byte profile are applied in Rust to
/// reproduce `decode_item`'s 32-cell contract exactly.
fn hydrate_rows(
    connection: &Connection,
    ids: &[i64],
) -> Result<std::collections::BTreeMap<i64, HydratedRow>> {
    use coding_agent_search::franken_sync::compat::RowExt;

    // ---- conversations: the outer row of the old join -------------------
    struct Conv {
        source_id: Option<String>,
        external_id: Option<String>,
        source_path: Option<String>,
        origin_host: Option<String>,
        title: Option<String>,
        started_at: Option<i64>,
        ended_at: Option<i64>,
        last_message_idx: Option<i64>,
        last_message_created_at: Option<i64>,
        primary_model: Option<String>,
        approx_tokens: Option<i64>,
        grand_total_tokens: Option<i64>,
        estimated_cost_usd: Option<f64>,
        api_call_count: Option<i64>,
        tool_call_count: Option<i64>,
        user_message_count: Option<i64>,
        assistant_message_count: Option<i64>,
        workspace_id: Option<i64>,
        total_input_tokens: Option<i64>,
        total_output_tokens: Option<i64>,
        total_cache_read_tokens: Option<i64>,
        total_cache_creation_tokens: Option<i64>,
    }
    let mut convs: std::collections::BTreeMap<i64, Conv> = std::collections::BTreeMap::new();
    let mut workspace_ids = std::collections::BTreeSet::new();
    for row in run_in_probe(connection, HYDRATE_CONVERSATIONS_SQL, ")", ids)? {
        let workspace_id: Option<i64> = row.get_typed(18)?;
        if let Some(id) = workspace_id {
            workspace_ids.insert(id);
        }
        convs.insert(
            row.get_typed(0)?,
            Conv {
                source_id: row.get_typed(1)?,
                external_id: row.get_typed(2)?,
                source_path: row.get_typed(3)?,
                origin_host: row.get_typed(4)?,
                title: row.get_typed(5)?,
                started_at: row.get_typed(6)?,
                ended_at: row.get_typed(7)?,
                last_message_idx: row.get_typed(8)?,
                last_message_created_at: row.get_typed(9)?,
                primary_model: row.get_typed(10)?,
                approx_tokens: row.get_typed(11)?,
                grand_total_tokens: row.get_typed(12)?,
                estimated_cost_usd: row.get_typed(13)?,
                api_call_count: row.get_typed(14)?,
                tool_call_count: row.get_typed(15)?,
                user_message_count: row.get_typed(16)?,
                assistant_message_count: row.get_typed(17)?,
                workspace_id,
                total_input_tokens: row.get_typed(19)?,
                total_output_tokens: row.get_typed(20)?,
                total_cache_read_tokens: row.get_typed(21)?,
                total_cache_creation_tokens: row.get_typed(22)?,
            },
        );
    }
    if convs.is_empty() {
        return Ok(std::collections::BTreeMap::new());
    }

    // ---- tail state: replaces the denormalized columns wholesale -------
    struct Tail {
        ended_at: Option<i64>,
        last_message_idx: Option<i64>,
        last_message_created_at: Option<i64>,
    }
    let mut tails: std::collections::BTreeMap<i64, Tail> = std::collections::BTreeMap::new();
    for row in run_in_probe(connection, HYDRATE_TAIL_SQL, ")", ids)? {
        tails.insert(
            row.get_typed(0)?,
            Tail {
                ended_at: row.get_typed(1)?,
                last_message_idx: row.get_typed(2)?,
                last_message_created_at: row.get_typed(3)?,
            },
        );
    }

    // ---- annotations + facts: LEFT JOIN semantics = absent Option ------
    struct Annotation {
        is_pinned: i64,
        custom_title: Option<String>,
        notes: Option<String>,
    }
    let mut annotations: std::collections::BTreeMap<i64, Annotation> =
        std::collections::BTreeMap::new();
    for row in run_in_probe(connection, HYDRATE_ANNOTATIONS_SQL, ")", ids)? {
        annotations.insert(
            row.get_typed(0)?,
            Annotation {
                is_pinned: row.get_typed::<Option<i64>>(1)?.unwrap_or(0),
                custom_title: row.get_typed(2)?,
                notes: row.get_typed(3)?,
            },
        );
    }
    struct Facts {
        is_subagent: Option<i64>,
        task_category: Option<String>,
        coverage_state: Option<String>,
        step_count: Option<i64>,
        artifact_count: Option<i64>,
        parent_conversation_id: Option<i64>,
        /// Raw `model_set_json`/`first_prompt` cells — kept for the byte
        /// profile (the bag holds the decoded/guarded values).
        model_set_json: Option<String>,
        first_prompt: Option<String>,
        bag: facts_bag::SessionFactBag,
    }
    let mut facts: std::collections::BTreeMap<i64, Facts> = std::collections::BTreeMap::new();
    for row in run_in_probe(connection, HYDRATE_FACTS_SQL, ")", ids)? {
        let model_set_json: Option<String> = row.get_typed(14)?;
        let first_prompt: Option<String> = row.get_typed(15)?;
        facts.insert(
            row.get_typed(0)?,
            Facts {
                is_subagent: row.get_typed(1)?,
                task_category: row.get_typed(2)?,
                coverage_state: row.get_typed(3)?,
                step_count: row.get_typed(4)?,
                artifact_count: row.get_typed(5)?,
                parent_conversation_id: row.get_typed(6)?,
                bag: facts_bag::SessionFactBag {
                    turn_count: row.get_typed(7)?,
                    step_count: row.get_typed(4)?,
                    thinking_block_count: row.get_typed(8)?,
                    artifact_count: row.get_typed(5)?,
                    subagent_count: row.get_typed(9)?,
                    tool_error_count: row.get_typed(10)?,
                    tool_calls_with_exit_status: row.get_typed(11)?,
                    file_fact_count: row.get_typed(12)?,
                    compaction_count: row.get_typed(13)?,
                    model_set: facts_bag::decode_string_list(model_set_json.as_deref()),
                    // Same per-column guard the canonical text cells get:
                    // absurd payloads stay unknown, never page bytes.
                    first_prompt: first_prompt
                        .as_ref()
                        .filter(|text| text.len() as i64 <= TEXT_GUARD_BYTES)
                        .cloned(),
                    ..Default::default()
                },
                model_set_json,
                first_prompt,
            },
        );
    }

    // ---- viewer_session_metrics: the quality/health half of the bag ----
    // Additive table — probe `sqlite_master` first so a facts-only
    // archive still serves every fact field with the metric half NULL.
    let metrics_installed = !connection
        .query(
            "SELECT 1 FROM sqlite_master WHERE type='table' \
             AND name='viewer_session_metrics' LIMIT 1",
        )?
        .is_empty();
    struct Metrics {
        /// Raw JSON cells kept for the byte profile.
        tools_used_json: Option<String>,
        task_breakdown_json: Option<String>,
        bag: facts_bag::SessionFactBag,
    }
    let mut metrics: std::collections::BTreeMap<i64, Metrics> = std::collections::BTreeMap::new();
    if metrics_installed {
        for row in run_in_probe(connection, HYDRATE_METRICS_SQL, ")", ids)? {
            let tools_used_json: Option<String> = row.get_typed(6)?;
            let task_breakdown_json: Option<String> = row.get_typed(7)?;
            metrics.insert(
                row.get_typed(0)?,
                Metrics {
                    tools_used_json: tools_used_json.clone(),
                    task_breakdown_json: task_breakdown_json.clone(),
                    bag: facts_bag::SessionFactBag {
                        ttfe_seconds: row.get_typed(1)?,
                        one_shot_rate: row.get_typed(2)?,
                        retries_count: row.get_typed(3)?,
                        self_corrections_count: row.get_typed(4)?,
                        user_corrections_count: row.get_typed(5)?,
                        tools_used: facts_bag::decode_string_list(tools_used_json.as_deref()),
                        task_breakdown: task_breakdown_json
                            .and_then(|text| serde_json::from_str(&text).ok()),
                        health_score: row.get_typed(8)?,
                        health_grade: row.get_typed(9)?,
                        health_issues_count: row.get_typed(10)?,
                        potential_waste_tokens: row.get_typed(11)?,
                        potential_savings_usd: row.get_typed(12)?,
                        ..Default::default()
                    },
                },
            );
        }
    }

    // ---- dimension lookups: the page's workspaces + sources ------------
    let workspace_id_list: Vec<i64> = workspace_ids.iter().copied().collect();
    let mut workspace_paths: std::collections::BTreeMap<i64, Option<String>> =
        std::collections::BTreeMap::new();
    for row in run_in_probe(connection, HYDRATE_WORKSPACES_SQL, ")", &workspace_id_list)? {
        workspace_paths.insert(row.get_typed(0)?, row.get_typed(1)?);
    }
    // `sources.id` is a TEXT key — the same bounded `IN` shape as the id
    // probes, still page-scoped (never a full dimension-table read).
    let source_ids: std::collections::BTreeSet<String> = convs
        .values()
        .filter_map(|conv| conv.source_id.clone())
        .collect();
    let mut sources: std::collections::BTreeMap<String, (Option<String>, Option<String>)> =
        std::collections::BTreeMap::new();
    if !source_ids.is_empty() {
        let mut source_params = Vec::with_capacity(source_ids.len());
        let source_placeholders = source_ids
            .iter()
            .map(|id| bind(&mut source_params, SqliteValue::Text(id.clone().into())))
            .collect::<Vec<_>>()
            .join(",");
        for row in connection.query_with_params(
            &format!("{HYDRATE_SOURCES_SQL}{source_placeholders})"),
            &source_params,
        )? {
            sources.insert(
                row.get_typed::<String>(0)?,
                (row.get_typed(1)?, row.get_typed(2)?),
            );
        }
    }

    // ---- assemble the 32-cell decode contract ---------------------------
    let mut out = std::collections::BTreeMap::new();
    for (id, conv) in convs {
        let tail = tails.get(&id);
        let ended_at = tail.map(|t| t.ended_at).unwrap_or(conv.ended_at);
        let last_message_idx = tail
            .map(|t| t.last_message_idx)
            .unwrap_or(conv.last_message_idx);
        let last_message_created_at = tail
            .map(|t| t.last_message_created_at)
            .unwrap_or(conv.last_message_created_at);
        let activity = last_message_created_at.or(ended_at).or(conv.started_at);
        let annotation = annotations.get(&id);
        let fact = facts.get(&id);
        let workspace_path = conv
            .workspace_id
            .and_then(|ws| workspace_paths.get(&ws).cloned().flatten());
        let (source_kind, source_label) = conv
            .source_id
            .as_deref()
            .and_then(|source| sources.get(source))
            .cloned()
            .unwrap_or((None, None));

        // The persisted fact/metrics bag rides every emitted item (Task 7):
        // facts fields + metrics fields merged in one wire object, NULL
        // when the facts row was never persisted — the unknown contract.
        let mut bag = fact.map(|f| f.bag.clone()).unwrap_or_default();
        if let Some(entry) = metrics.get(&id) {
            let metric = &entry.bag;
            bag.ttfe_seconds = metric.ttfe_seconds;
            bag.one_shot_rate = metric.one_shot_rate;
            bag.retries_count = metric.retries_count;
            bag.self_corrections_count = metric.self_corrections_count;
            bag.user_corrections_count = metric.user_corrections_count;
            bag.tools_used = metric.tools_used.clone();
            bag.task_breakdown = metric.task_breakdown.clone();
            bag.health_score = metric.health_score;
            bag.health_grade = metric.health_grade.clone();
            bag.health_issues_count = metric.health_issues_count;
            bag.potential_waste_tokens = metric.potential_waste_tokens;
            bag.potential_savings_usd = metric.potential_savings_usd;
        }
        let metrics_bag = bag.emit(fact.is_some());

        // The byte profile sums RAW cell bytes (never the guarded value),
        // mirroring the SQL `COALESCE(length(CAST(col AS BLOB)),0)` list.
        let byte_sum = byte_len(conv.source_id.as_deref())
            + byte_len(conv.external_id.as_deref())
            + byte_len(conv.source_path.as_deref())
            + byte_len(conv.origin_host.as_deref())
            + byte_len(conv.title.as_deref())
            + byte_len(conv.primary_model.as_deref())
            + byte_len(annotation.and_then(|a| a.custom_title.as_deref()))
            + byte_len(annotation.and_then(|a| a.notes.as_deref()))
            + byte_len(workspace_path.as_deref())
            + byte_len(source_kind.as_deref())
            + byte_len(source_label.as_deref())
            + byte_len(fact.and_then(|f| f.task_category.as_deref()))
            + byte_len(fact.and_then(|f| f.first_prompt.as_deref()))
            + byte_len(fact.and_then(|f| f.model_set_json.as_deref()))
            + byte_len(metrics.get(&id).and_then(|m| m.tools_used_json.as_deref()))
            + byte_len(
                metrics
                    .get(&id)
                    .and_then(|m| m.task_breakdown_json.as_deref()),
            );

        let cells = vec![
            SqliteValue::Integer(id), // 0
            conv.source_id
                .clone()
                .map_or(SqliteValue::Null, |s| SqliteValue::Text(s.into())), // 1
            guarded_text(conv.external_id.clone()), // 2
            guarded_text(conv.source_path.clone()), // 3
            guarded_text(conv.origin_host.clone()), // 4
            source_kind
                .clone()
                .map_or(SqliteValue::Null, |s| SqliteValue::Text(s.into())), // 5
            source_label
                .clone()
                .map_or(SqliteValue::Null, |s| SqliteValue::Text(s.into())), // 6
            guarded_text(conv.title.clone()), // 7
            integer_or_null(conv.started_at), // 8
            integer_or_null(ended_at), // 9
            integer_or_null(last_message_idx), // 10
            integer_or_null(last_message_created_at), // 11
            integer_or_null(activity), // 12
            guarded_text(conv.primary_model.clone()), // 13
            integer_or_null(conv.approx_tokens), // 14
            integer_or_null(conv.grand_total_tokens), // 15
            conv.estimated_cost_usd
                .map_or(SqliteValue::Null, SqliteValue::Float), // 16
            integer_or_null(conv.api_call_count), // 17
            integer_or_null(conv.tool_call_count), // 18
            integer_or_null(conv.user_message_count), // 19
            integer_or_null(conv.assistant_message_count), // 20
            SqliteValue::Integer(annotation.map(|a| a.is_pinned).unwrap_or(0)), // 21
            guarded_text(annotation.and_then(|a| a.custom_title.clone())), // 22
            guarded_text(annotation.and_then(|a| a.notes.clone())), // 23
            integer_or_null(fact.and_then(|f| f.is_subagent)), // 24
            fact.and_then(|f| f.task_category.clone())
                .map_or(SqliteValue::Null, |s| SqliteValue::Text(s.into())), // 25
            fact.and_then(|f| f.coverage_state.clone())
                .map_or(SqliteValue::Null, |s| SqliteValue::Text(s.into())), // 26
            integer_or_null(fact.and_then(|f| f.step_count)), // 27
            integer_or_null(fact.and_then(|f| f.artifact_count)), // 28
            integer_or_null(fact.and_then(|f| f.parent_conversation_id)), // 29
            guarded_text(workspace_path), // 30
            SqliteValue::Integer(byte_sum), // 31
            integer_or_null(conv.total_input_tokens), // 32
            integer_or_null(conv.total_output_tokens), // 33
            integer_or_null(conv.total_cache_read_tokens), // 34
            integer_or_null(conv.total_cache_creation_tokens), // 35
        ];
        out.insert(
            id,
            HydratedRow {
                cells,
                metrics: metrics_bag,
            },
        );
    }
    Ok(out)
}

/// The order-key tuple of one phase-A row, for merging flat streams and
/// minting the next cursor.
struct OrderRow {
    id: i64,
    pin: i64,
    known: i64,
    value: Option<Value>,
    tie: String,
}

fn decode_order_row(
    row: &coding_agent_search::franken_sync::Row,
    key: SortKey,
) -> Result<OrderRow> {
    let value: Option<Value> = if key.cursor_value_is_text() {
        row.get_typed::<Option<String>>(3)?.map(Value::from)
    } else {
        row.get_typed::<Option<i64>>(3)?.map(Value::from)
    };
    Ok(OrderRow {
        id: row.get_typed(0)?,
        pin: row.get_typed(1)?,
        known: row.get_typed(2)?,
        value,
        tie: row.get_typed::<Option<String>>(4)?.unwrap_or_default(),
    })
}

/// Lexicographic comparison on `(knownness, value, tie, id)` — the flat
/// (`pin_first=false`) merge key. Pin is stream-constant inside a
/// partition, so it never enters the comparison.
fn order_row_cmp(a: &OrderRow, b: &OrderRow, desc: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    // Knownness DESC: known values (1) before unknowns (0) — NULLS LAST.
    match b.known.cmp(&a.known) {
        Ordering::Equal => {}
        other => return other,
    }
    if a.known != 0 {
        // Both values known: compare by sort direction. Unknowns share
        // known=0 and skip straight to the public tie key.
        let value_ord = match (&a.value, &b.value) {
            (Some(Value::String(x)), Some(Value::String(y))) => x.cmp(y),
            (Some(x), Some(y)) => {
                let xi = x.as_i64().unwrap_or(i64::MIN);
                let yi = y.as_i64().unwrap_or(i64::MIN);
                xi.cmp(&yi)
            }
            _ => Ordering::Equal,
        };
        let directed = if desc { value_ord.reverse() } else { value_ord };
        match directed {
            Ordering::Equal => {}
            other => return other,
        }
    }
    match a.tie.cmp(&b.tie) {
        Ordering::Equal => a.id.cmp(&b.id),
        other => other,
    }
}

/// Run the index-ordered scan for each pin partition and merge-select the
/// page: one stream for `pin_first`/pinned-filter requests, two merged
/// streams for `pin_first=false` (pin is not an order key there, so the
/// two partitions interleave by `(knownness, value, tie, id)` alone).
fn run_order_streams(
    connection: &Connection,
    resolved: &ResolvedFilters,
    key: SortKey,
    desc: bool,
    pin_first: bool,
    cursor: Option<&Cursor>,
    limit: usize,
) -> Result<Vec<OrderRow>> {
    // The pinned filter (when present) already collapses the request to
    // one partition — the base predicate carries it, so a second explicit
    // partition split would duplicate work for zero coverage.
    let partitions: Vec<Option<i64>> = if pin_first || resolved.pinned.is_some() {
        vec![None]
    } else {
        vec![Some(1), Some(0)]
    };
    let mut streams: Vec<std::vec::IntoIter<OrderRow>> = Vec::with_capacity(partitions.len());
    for partition in partitions {
        let (sql, params) =
            build_order_sql(resolved, key, desc, pin_first, partition, cursor, limit);
        let rows = connection.query_with_params(&sql, &params)?;
        let mut decoded = Vec::with_capacity(rows.len());
        for row in &rows {
            decoded.push(decode_order_row(row, key)?);
        }
        streams.push(decoded.into_iter());
    }
    if streams.len() == 1 {
        return Ok(streams.pop().expect("one stream").collect());
    }
    // Two-way merge: each stream is already in flat order, so repeatedly
    // emitting the smaller head is a stable total-order merge.
    let mut merged = Vec::with_capacity(limit + 1);
    loop {
        let take_left = match (streams[0].as_slice().first(), streams[1].as_slice().first()) {
            (Some(left), Some(right)) => order_row_cmp(left, right, desc).is_le(),
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        let head = if take_left {
            streams[0].next().expect("head checked")
        } else {
            streams[1].next().expect("head checked")
        };
        merged.push(head);
    }
    Ok(merged)
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

/// The `(pinned, nf, value, tie, id)` tuple of an emitted row — becomes
/// the `next_cursor` position. Sourced from the phase-A order row (the
/// index tuple the page was cut at), never the payload row, so a racing
/// write between the two bounded statements cannot move the resumed
/// position.
fn cursor_tuple(row: &OrderRow, pin_first: bool) -> Cursor {
    Cursor {
        pin: pin_first.then_some(row.pin),
        null_flag: row.known == 0,
        value: if row.known == 0 {
            None
        } else {
            row.value.clone()
        },
        tie: row.tie.clone(),
        id: row.id,
    }
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
    // The candidate set must carry a resolvable public identity for every
    // row — the oracle's explicit order compares `{provider}:{native}` tie
    // keys for unknowns and equal values, so a NULL key in the filtered set
    // means the order cannot be computed honestly.
    run_tie_probe(connection, &resolved)?;

    // Phase A: the persisted order-key scan — one indexed stream (two
    // merged streams for pin_first=false) bounded at limit+1 rows.
    let ordered = run_order_streams(
        connection,
        &resolved,
        key,
        desc,
        pin_first,
        cursor.as_ref(),
        limit,
    )?;

    let generation = revisions::generation(connection)?;
    if ordered.is_empty() {
        return Ok(json!({
            "items": Vec::<Value>::new(),
            "next_cursor": Value::Null,
            "generation": generation,
            "complete": true,
            "warnings": warnings,
        }));
    }

    // Phase B: hydrate exactly this page's ids — one bounded `IN` probe
    // per side table (repeated primary-key seeks), assembled in Rust;
    // `messages` is never touched.
    let take_count = ordered.len().min(limit);
    let page_ids: Vec<i64> = ordered.iter().take(take_count).map(|row| row.id).collect();
    let by_id = hydrate_rows(connection, &page_ids)?;

    // Decode bounded; a page may serialize fewer rows than the order scan
    // returned when the response byte budget intervenes.
    let mut items = Vec::new();
    let mut bytes = 0_usize;
    let mut last: Option<Cursor> = None;
    let mut truncated = false;
    for order_row in ordered.iter().take(take_count) {
        let position = cursor_tuple(order_row, pin_first);
        let Some(row) = by_id.get(&order_row.id) else {
            // The conversation row vanished between the scans (canonical
            // delete raced the page): the order position IS consumed —
            // resuming past it keeps the surviving tail reachable.
            last = Some(position);
            continue;
        };
        let total_bytes: i64 = row.get_typed(31)?;
        if !(0..=ROW_BYTES).contains(&total_bytes) {
            return Err(refuse(
                "canonical_payload_too_large",
                "Canonical row exceeds the strict bridge byte profile",
            ));
        }
        let item = decode_item(row)?;
        if !push_bounded(&mut items, item, &mut bytes)? {
            truncated = true;
            break;
        }
        last = Some(position);
    }

    // Items keep the order-scan sequence; each carries its canonical
    // coordinate.
    let emitted_ids: Vec<i64> = items
        .iter()
        .map(|item| item["conversation_id"].as_i64().expect("decoded id"))
        .collect();
    let public_ids = identity::resolve_public_ids(connection, &emitted_ids)?;
    let tags = annotations::tags_for(connection, &emitted_ids)?;
    // Parent identities resolve through the same canonical identity
    // machinery — one bounded primary-key probe per distinct parent id
    // (identity::resolve_public_id recomputes and verifies the public
    // id; an unresolvable/ambiguous parent stays typed NULL).
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
        }
        if let Some(parent_id) = item["parent_conversation_id"].as_i64() {
            item["parent_public_id"] = json!(parent_public_ids.get(&parent_id));
        }
        item["tags"] = json!(tags.get(&id).cloned().unwrap_or_default());
    }

    let has_more = truncated || ordered.len() > limit;
    let next_cursor = if has_more {
        match &last {
            Some(position) => Value::String(encode_cursor(sort_name, pin_first, position)),
            // `limit == 0` is the only way a non-empty order scan leaves no
            // examined position — emit a resume-free page rather than the
            // old path's panic.
            None => Value::Null,
        }
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
fn decode_item(row: &HydratedRow) -> Result<Value> {
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
        ("total_input_tokens", 32),
        ("total_output_tokens", 33),
        ("total_cache_read_tokens", 34),
        ("total_cache_creation_tokens", 35),
    ] {
        item.insert(key.into(), nonnegative_val(row.get_typed(index)?)?);
    }
    item.insert("estimated_cost_usd".into(), json!(cost));
    // The persisted fact/metrics bag (Task 7): `null` when the facts row
    // was never persisted — unknown stays NULL, known zero stays 0.
    item.insert("metrics".into(), row.metrics.clone());
    // `parent_public_id` is filled after page emission: resolving one
    // parent's verified public identity is a bounded canonical identity
    // probe (`identity::resolve_public_id`), never a registry scan.
    item.insert("parent_public_id".into(), Value::Null);
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

/// The exact statements a page would run — the index-ordered scan(s) plus
/// the bounded payload hydration — for `EXPLAIN`/opcode audits in the
/// extensions test. Not part of the wire protocol.
#[allow(dead_code)] // Test-only audit entry point (included via #[path]).
pub struct PlannedPage {
    /// Phase-A order scans: one entry for `pin_first`/pinned-filter
    /// requests, two (pin partition 1 then 0) for `pin_first=false`.
    pub order_queries: Vec<(String, Vec<SqliteValue>)>,
    /// Phase-B payload hydration — the bounded `WHERE c.id IN (…)` probe
    /// template the page runs once per emitted page (plus the four side
    /// probes on tail-state/annotations/facts/workspaces).
    pub hydrate_query: String,
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
) -> Result<PlannedPage> {
    let resolved = resolve_filters(connection, filters)?;
    let (sort_name, key, desc) = sort_plan(sort)?;
    let pin_first = pin_first.unwrap_or(true);
    let cursor = match cursor {
        Some(raw) => Some(decode_cursor(raw, sort_name, key, pin_first)?),
        None => None,
    };
    let partitions: Vec<Option<i64>> = if pin_first || resolved.pinned.is_some() {
        vec![None]
    } else {
        vec![Some(1), Some(0)]
    };
    let order_queries = partitions
        .into_iter()
        .map(|partition| {
            build_order_sql(
                &resolved,
                key,
                desc,
                pin_first,
                partition,
                cursor.as_ref(),
                limit,
            )
        })
        .collect();
    Ok(PlannedPage {
        order_queries,
        // The audit contract: a bounded `IN` probe on the conversations
        // primary key — never a joined scan, never `messages`.
        hydrate_query: format!("{HYDRATE_CONVERSATIONS_SQL}?1..?N)"),
    })
}
