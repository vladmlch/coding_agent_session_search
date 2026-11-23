use anyhow::Result;
use coding_agent_search::franken_sync::compat::RowExt;
use coding_agent_search::franken_sync::{Connection, SqliteValue};

use super::super::annotations;
use super::super::identity;
use super::super::protocol::{ListFilters, refuse};

// ---------------------------------------------------------------------------
// Filters: validation + dimension resolution
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct ResolvedFilters {
    /// `Some(set)` = provider filter resolved to canonical `agents.id`s
    /// (an empty set is a deterministic empty result, not an error).
    agent_ids: Option<Vec<i64>>,
    source_id: Option<String>,
    workspace_ids: Option<Vec<i64>>,
    pub(super) pinned: Option<i64>,
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

pub(super) fn resolve_filters(
    connection: &Connection,
    filters: &ListFilters,
) -> Result<ResolvedFilters> {
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

pub(super) fn bind(params: &mut Vec<SqliteValue>, value: SqliteValue) -> String {
    params.push(value);
    format!("?{}", params.len())
}

fn text(value: &str) -> SqliteValue {
    SqliteValue::Text(value.to_owned().into())
}

/// Candidate-condition groups for ONE query (probe or page). Rebuilt per
/// query so `?N` placeholders always match that query's param list.
pub(super) struct Conditions {
    /// Provider/source/workspace/pinned/tag — always computable.
    pub(super) base: Vec<String>,
    /// Rows whose *known* `is_subagent` fails the request are excluded
    /// before unknown checks (`NOT (known AND <> requested)`), or the
    /// implicit browse exclusion (drop proven subagents, keep unknowns).
    subagent_exclusion: Option<String>,
    /// Same shape for `category`.
    category_exclusion: Option<String>,
    /// Window predicates on the activity expression.
    pub(super) activity: Vec<String>,
    /// Window predicates on `c.started_at`.
    pub(super) created: Vec<String>,
    /// Final classification predicates applied by the page query itself.
    pub(super) classification: Vec<String>,
}

pub(super) fn build_conditions(
    resolved: &ResolvedFilters,
    params: &mut Vec<SqliteValue>,
) -> Conditions {
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
pub fn resolve_filters_public(
    connection: &Connection,
    filters: &ListFilters,
) -> Result<ResolvedFilters> {
    resolve_filters(connection, filters)
}

/// `run_probes` for the search lane (same honest-refusal order/warnings).
pub fn run_probes_public(
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
    pub fn universe_sql(&self) -> (String, Vec<SqliteValue>) {
        let (subquery, params) = self.universe_subquery();
        (format!("{subquery} ORDER BY vs.conversation_id"), params)
    }

    /// `SELECT vs.conversation_id FROM viewer_session_sort vs WHERE <same
    /// clauses>` WITHOUT the ordering — the set-based `IN (…)` form the
    /// search lane's batched universe reads embed, so every auxiliary read
    /// (summary join, tags, region inventory) materializes the identical
    /// filtered candidate set instead of re-transporting an id list.
    pub fn universe_subquery(&self) -> (String, Vec<SqliteValue>) {
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

pub(super) fn run_probes(
    connection: &Connection,
    resolved: &ResolvedFilters,
) -> Result<Vec<String>> {
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
pub(super) fn run_tie_probe(connection: &Connection, resolved: &ResolvedFilters) -> Result<()> {
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
