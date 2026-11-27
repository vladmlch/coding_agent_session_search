use anyhow::Result;
use coding_agent_search::franken_sync::{Connection, FrankenError, SqliteValue};
use serde_json::{Map, Value, json};

use super::super::facts_bag;
use super::super::identity;
use super::super::protocol::refuse;
use super::TEXT_GUARD_BYTES;
use super::filters::bind;

/// Phase B — payload hydration for one page id set. An `IN` list on a
/// standalone table resolves to repeated `SeekRowid` lookups — so the
/// page runs one bounded probe per side table and assembles the
/// `decode_item` row contract in Rust. Per page that is a handful of
/// `O(page)` statements instead of `O(page)` multi-table joins.
pub(super) const HYDRATE_CONVERSATIONS_SQL: &str = "\
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

/// `decode_item`'s typed row contract, assembled in Rust from the bounded
/// per-table probes — the same fields the old single `FROM_SQL` join
/// produced, with cumulative token sub-totals and the merged persisted
/// fact/metrics bag carried alongside (Task 7).
pub(super) struct HydratedRow {
    pub(super) id: i64,
    pub(super) source_id: Option<String>,
    pub(super) external_id: Option<String>,
    pub(super) source_path: Option<String>,
    pub(super) origin_host: Option<String>,
    pub(super) source_kind: Option<String>,
    pub(super) source_label: Option<String>,
    pub(super) title: Option<String>,
    /// Truncation fact for the bounded `title` preview.
    pub(super) title_truncated: bool,
    pub(super) title_bytes: Option<i64>,
    pub(super) started_at: Option<i64>,
    pub(super) ended_at: Option<i64>,
    pub(super) last_message_idx: Option<i64>,
    pub(super) last_message_created_at: Option<i64>,
    pub(super) activity: Option<i64>,
    pub(super) primary_model: Option<String>,
    pub(super) approx_tokens: Option<i64>,
    pub(super) grand_total_tokens: Option<i64>,
    pub(super) estimated_cost_usd: Option<f64>,
    pub(super) api_call_count: Option<i64>,
    pub(super) tool_call_count: Option<i64>,
    pub(super) user_message_count: Option<i64>,
    pub(super) assistant_message_count: Option<i64>,
    pub(super) is_pinned: i64,
    pub(super) custom_title: Option<String>,
    pub(super) notes: Option<String>,
    pub(super) is_subagent: Option<i64>,
    pub(super) task_category: Option<String>,
    pub(super) classification_coverage: Option<&'static str>,
    pub(super) parent_conversation_id: Option<i64>,
    pub(super) workspace_path: Option<String>,
    pub(super) total_bytes: i64,
    pub(super) total_input_tokens: Option<i64>,
    pub(super) total_output_tokens: Option<i64>,
    pub(super) total_cache_read_tokens: Option<i64>,
    pub(super) total_cache_creation_tokens: Option<i64>,
    /// `null` when no `viewer_session_facts` row was persisted for this
    /// session; otherwise the merged facts+metrics bag.
    pub(super) metrics: Value,
}

/// Text payload guard mirroring the SQL `guard()` expression: payloads
/// beyond the strict byte profile decode as NULL.
fn guarded_text_opt(value: Option<String>) -> Option<String> {
    value.filter(|text| text.len() as i64 <= TEXT_GUARD_BYTES)
}

/// `length(CAST(x AS BLOB))` on a Rust-held optional text.
pub(super) fn byte_len(value: Option<&str>) -> i64 {
    value.map(|text| text.len() as i64).unwrap_or(0)
}

fn run_in_probe<T>(
    connection: &Connection,
    head: &str,
    tail: &str,
    ids: &[i64],
    map: impl FnMut(&rusqlite::Row<'_>) -> std::result::Result<T, FrankenError>,
) -> Result<Vec<T>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut params = Vec::with_capacity(ids.len());
    let placeholders = ids
        .iter()
        .map(|id| bind(&mut params, SqliteValue::Integer(*id)))
        .collect::<Vec<_>>()
        .join(",");
    Ok(connection.query_map(&format!("{head}{placeholders}{tail}"), &params, map)?)
}

/// Hydrate every emitted id with bounded standalone-table `IN` probes —
/// each probe compiles to repeated `SeekRowid` on a primary-key or keyed
/// index, so page hydration is `O(page)` seeks, never a scan and never a
/// per-row interpreted join. Tail-state precedence, the annotation/facts
/// overlays, the text guards and the byte profile are applied in Rust to
/// reproduce `decode_item`'s typed contract exactly.
pub(super) fn hydrate_rows(
    connection: &Connection,
    ids: &[i64],
) -> Result<std::collections::BTreeMap<i64, HydratedRow>> {
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
    for (id, conv) in run_in_probe(connection, HYDRATE_CONVERSATIONS_SQL, ")", ids, |row| {
        Ok((
            row.get::<_, i64>(0)?,
            Conv {
                source_id: row.get(1)?,
                external_id: row.get(2)?,
                source_path: row.get(3)?,
                origin_host: row.get(4)?,
                title: row.get(5)?,
                started_at: row.get(6)?,
                ended_at: row.get(7)?,
                last_message_idx: row.get(8)?,
                last_message_created_at: row.get(9)?,
                primary_model: row.get(10)?,
                approx_tokens: row.get(11)?,
                grand_total_tokens: row.get(12)?,
                estimated_cost_usd: row.get(13)?,
                api_call_count: row.get(14)?,
                tool_call_count: row.get(15)?,
                user_message_count: row.get(16)?,
                assistant_message_count: row.get(17)?,
                workspace_id: row.get(18)?,
                total_input_tokens: row.get(19)?,
                total_output_tokens: row.get(20)?,
                total_cache_read_tokens: row.get(21)?,
                total_cache_creation_tokens: row.get(22)?,
            },
        ))
    })? {
        if let Some(ws_id) = conv.workspace_id {
            workspace_ids.insert(ws_id);
        }
        convs.insert(id, conv);
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
    for (id, tail) in run_in_probe(connection, HYDRATE_TAIL_SQL, ")", ids, |row| {
        Ok((
            row.get::<_, i64>(0)?,
            Tail {
                ended_at: row.get(1)?,
                last_message_idx: row.get(2)?,
                last_message_created_at: row.get(3)?,
            },
        ))
    })? {
        tails.insert(id, tail);
    }

    // ---- annotations + facts: LEFT JOIN semantics = absent Option ------
    struct Annotation {
        is_pinned: i64,
        custom_title: Option<String>,
        notes: Option<String>,
    }
    let mut annotations: std::collections::BTreeMap<i64, Annotation> =
        std::collections::BTreeMap::new();
    for (id, ann) in run_in_probe(connection, HYDRATE_ANNOTATIONS_SQL, ")", ids, |row| {
        Ok((
            row.get::<_, i64>(0)?,
            Annotation {
                is_pinned: row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                custom_title: row.get(2)?,
                notes: row.get(3)?,
            },
        ))
    })? {
        annotations.insert(id, ann);
    }
    struct Facts {
        is_subagent: Option<i64>,
        task_category: Option<String>,
        parent_conversation_id: Option<i64>,
        bag: facts_bag::SessionFactBag,
    }
    let mut facts: std::collections::BTreeMap<i64, Facts> = std::collections::BTreeMap::new();
    for (id, fact) in run_in_probe(connection, HYDRATE_FACTS_SQL, ")", ids, |row| {
        let model_set_json: Option<String> = row.get(14)?;
        let first_prompt_raw: Option<String> = row.get(15)?;
        // Bounded previews: one oversized prompt/model set never refuses a
        // page; the bag carries the preview plus its truncation fact, and
        // the byte profile counts the emitted preview, not the raw cell.
        let (model_set, model_set_truncated, model_set_bytes) = facts_bag::bound_json_preview(
            model_set_json.clone(),
            facts_bag::decode_string_list(model_set_json.as_deref()),
        );
        let (first_prompt, first_prompt_truncated, first_prompt_bytes) =
            facts_bag::bound_text_preview(first_prompt_raw);
        let step_count: Option<i64> = row.get(4)?;
        let artifact_count: Option<i64> = row.get(5)?;
        Ok((
            row.get::<_, i64>(0)?,
            Facts {
                is_subagent: row.get(1)?,
                task_category: row.get(2)?,
                // Column 3 (the overall facts coverage_state) is deliberately
                // not read: classification coverage derives from the verdict
                // alone (facts_bag::classification_coverage).
                parent_conversation_id: row.get(6)?,
                bag: facts_bag::SessionFactBag {
                    turn_count: row.get(7)?,
                    step_count,
                    thinking_block_count: row.get(8)?,
                    artifact_count,
                    subagent_count: row.get(9)?,
                    tool_error_count: row.get(10)?,
                    tool_calls_with_exit_status: row.get(11)?,
                    file_fact_count: row.get(12)?,
                    compaction_count: row.get(13)?,
                    model_set,
                    model_set_truncated,
                    model_set_bytes,
                    first_prompt,
                    first_prompt_truncated,
                    first_prompt_bytes,
                    ..Default::default()
                },
            },
        ))
    })? {
        facts.insert(id, fact);
    }

    // ---- viewer_session_metrics: the quality/health half of the bag ----
    // Additive table — probe `sqlite_master` first so a facts-only
    // archive still serves every fact field with the metric half NULL.
    let metrics_installed = !connection
        .query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' \
             AND name='viewer_session_metrics' LIMIT 1",
            &[],
            |_| Ok(()),
        )?
        .is_empty();
    struct Metrics {
        bag: facts_bag::SessionFactBag,
    }
    let mut metrics: std::collections::BTreeMap<i64, Metrics> = std::collections::BTreeMap::new();
    if metrics_installed {
        for (id, entry) in run_in_probe(connection, HYDRATE_METRICS_SQL, ")", ids, |row| {
            let tools_used_json: Option<String> = row.get(6)?;
            let task_breakdown_json: Option<String> = row.get(7)?;
            let (tools_used, tools_used_truncated, tools_used_bytes) =
                facts_bag::bound_json_preview(
                    tools_used_json.clone(),
                    facts_bag::decode_string_list(tools_used_json.as_deref()),
                );
            let (task_breakdown, task_breakdown_truncated, task_breakdown_bytes) =
                facts_bag::bound_json_preview(
                    task_breakdown_json.clone(),
                    task_breakdown_json.and_then(|text| serde_json::from_str(&text).ok()),
                );
            Ok((
                row.get::<_, i64>(0)?,
                Metrics {
                    bag: facts_bag::SessionFactBag {
                        ttfe_seconds: row.get(1)?,
                        one_shot_rate: row.get(2)?,
                        retries_count: row.get(3)?,
                        self_corrections_count: row.get(4)?,
                        user_corrections_count: row.get(5)?,
                        tools_used,
                        tools_used_truncated,
                        tools_used_bytes,
                        task_breakdown,
                        task_breakdown_truncated,
                        task_breakdown_bytes,
                        health_score: row.get(8)?,
                        health_grade: row.get(9)?,
                        health_issues_count: row.get(10)?,
                        potential_waste_tokens: row.get(11)?,
                        potential_savings_usd: row.get(12)?,
                        ..Default::default()
                    },
                },
            ))
        })? {
            metrics.insert(id, entry);
        }
    }

    // ---- dimension lookups: the page's workspaces + sources ------------
    let workspace_id_list: Vec<i64> = workspace_ids.iter().copied().collect();
    let mut workspace_paths: std::collections::BTreeMap<i64, Option<String>> =
        std::collections::BTreeMap::new();
    for (id, path) in run_in_probe(
        connection,
        HYDRATE_WORKSPACES_SQL,
        ")",
        &workspace_id_list,
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
    )? {
        workspace_paths.insert(id, path);
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
            .map(|id| bind(&mut source_params, SqliteValue::Text(id.clone())))
            .collect::<Vec<_>>()
            .join(",");
        for (id, pair) in connection.query_map(
            &format!("{HYDRATE_SOURCES_SQL}{source_placeholders})"),
            &source_params,
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    (
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ),
                ))
            },
        )? {
            sources.insert(id, pair);
        }
    }

    // ---- assemble the typed decode contract ----------------------------
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
            bag.merge_metrics(&entry.bag);
        }
        let metrics_bag = bag.emit(fact.is_some());

        // The bounded `title` preview: cut on a UTF-8 char boundary with an
        // explicit truncation fact, so one oversized title never refuses a
        // page. Annotation/identity/category/model cells stay RAW-counted.
        let (title_preview, title_truncated, title_bytes) =
            facts_bag::bound_text_preview(conv.title);

        let custom_title = guarded_text_opt(annotation.and_then(|a| a.custom_title.clone()));
        let notes = guarded_text_opt(annotation.and_then(|a| a.notes.clone()));
        let task_category = fact.and_then(|f| f.task_category.clone());

        // The byte profile sums the EMITTED preview bytes for the bounded
        // free-text/JSON fields (`title` + the bag's `preview_bytes`) and
        // RAW cell bytes for annotations, identity, category and model —
        // mirroring the SQL `COALESCE(length(CAST(col AS BLOB)),0)` list.
        // Annotations are never truncated, so an annotation-only overflow
        // still trips the strict `ROW_BYTES` refusal below.
        let byte_sum = byte_len(conv.source_id.as_deref())
            + byte_len(conv.external_id.as_deref())
            + byte_len(conv.source_path.as_deref())
            + byte_len(conv.origin_host.as_deref())
            + byte_len(title_preview.as_deref())
            + byte_len(conv.primary_model.as_deref())
            + byte_len(annotation.and_then(|a| a.custom_title.as_deref()))
            + byte_len(annotation.and_then(|a| a.notes.as_deref()))
            + byte_len(workspace_path.as_deref())
            + byte_len(source_kind.as_deref())
            + byte_len(source_label.as_deref())
            + byte_len(task_category.as_deref())
            + bag.preview_bytes();

        out.insert(
            id,
            HydratedRow {
                id,
                source_id: conv.source_id,
                external_id: guarded_text_opt(conv.external_id),
                source_path: guarded_text_opt(conv.source_path),
                origin_host: guarded_text_opt(conv.origin_host),
                source_kind,
                source_label,
                title: title_preview,
                title_truncated,
                title_bytes,
                started_at: conv.started_at,
                ended_at,
                last_message_idx,
                last_message_created_at,
                activity,
                primary_model: guarded_text_opt(conv.primary_model),
                approx_tokens: conv.approx_tokens,
                grand_total_tokens: conv.grand_total_tokens,
                estimated_cost_usd: conv.estimated_cost_usd,
                api_call_count: conv.api_call_count,
                tool_call_count: conv.tool_call_count,
                user_message_count: conv.user_message_count,
                assistant_message_count: conv.assistant_message_count,
                is_pinned: annotation.map(|a| a.is_pinned).unwrap_or(0),
                custom_title,
                notes,
                is_subagent: fact.and_then(|f| f.is_subagent),
                task_category,
                classification_coverage: fact
                    .map(|f| facts_bag::classification_coverage(f.is_subagent)),
                parent_conversation_id: fact.and_then(|f| f.parent_conversation_id),
                workspace_path: guarded_text_opt(workspace_path),
                total_bytes: byte_sum,
                total_input_tokens: conv.total_input_tokens,
                total_output_tokens: conv.total_output_tokens,
                total_cache_read_tokens: conv.total_cache_read_tokens,
                total_cache_creation_tokens: conv.total_cache_creation_tokens,
                metrics: metrics_bag,
            },
        );
    }
    Ok(out)
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

/// Bounded item decode — the same coordinates `catalog.rs` serves plus the
/// annotation/facts overlay. `public_id`/`provider`/`tags` are filled in
/// after page emission (`identity` + `tags_for`).
pub(super) fn decode_item(row: &HydratedRow) -> Result<Value> {
    let id = row.id;
    if id <= 0 {
        return Err(refuse(
            "canonical_coordinates_invalid",
            "Nonpositive conversation identity",
        ));
    }
    let cost = row.estimated_cost_usd;
    if cost.is_some_and(|cost| !cost.is_finite() || cost < 0.0) {
        return Err(refuse(
            "canonical_coordinates_invalid",
            "Invalid canonical cost",
        ));
    }
    let source_id = row
        .source_id
        .as_deref()
        .ok_or(FrankenError::DatatypeMismatch)?;
    let origin_host = row.origin_host.as_deref().or(row.source_label.as_deref());
    let project = row
        .workspace_path
        .as_deref()
        .map(identity::path_name)
        .filter(|name| !name.is_empty());

    let mut item = Map::new();
    item.insert("conversation_id".into(), json!(id));
    item.insert("public_id".into(), Value::Null);
    item.insert("provider".into(), Value::Null);
    item.insert("source_id".into(), json!(source_id));
    item.insert("source_kind".into(), json!(row.source_kind));
    item.insert("source_path".into(), json!(row.source_path));
    item.insert("external_id".into(), json!(row.external_id));
    item.insert("origin_host".into(), json!(origin_host));
    item.insert("workspace_path".into(), json!(row.workspace_path));
    item.insert("project".into(), json!(project));
    item.insert("title".into(), json!(row.title));
    item.insert("title_truncated".into(), json!(row.title_truncated));
    item.insert("title_bytes".into(), json!(row.title_bytes));
    item.insert("custom_title".into(), json!(row.custom_title));
    item.insert("notes".into(), json!(row.notes));
    item.insert("is_pinned".into(), json!(row.is_pinned != 0));
    item.insert("tags".into(), json!(Vec::<String>::new()));
    item.insert("started_at_ms".into(), json!(row.started_at));
    item.insert("ended_at_ms".into(), json!(row.ended_at));
    item.insert(
        "last_message_created_at_ms".into(),
        json!(row.last_message_created_at),
    );
    item.insert(
        "last_message_idx".into(),
        nonnegative_val(row.last_message_idx)?,
    );
    item.insert("activity_ms".into(), json!(row.activity));
    item.insert("primary_model".into(), json!(row.primary_model));
    for (key, value) in [
        ("approx_tokens", row.approx_tokens),
        ("grand_total_tokens", row.grand_total_tokens),
        ("api_call_count", row.api_call_count),
        ("tool_call_count", row.tool_call_count),
        ("user_message_count", row.user_message_count),
        ("assistant_message_count", row.assistant_message_count),
        ("total_input_tokens", row.total_input_tokens),
        ("total_output_tokens", row.total_output_tokens),
        ("total_cache_read_tokens", row.total_cache_read_tokens),
        (
            "total_cache_creation_tokens",
            row.total_cache_creation_tokens,
        ),
    ] {
        item.insert(key.into(), nonnegative_val(value)?);
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
        json!(row.is_subagent.map(|value| value != 0)),
    );
    item.insert("task_category".into(), json!(row.task_category));
    item.insert(
        "classification_coverage".into(),
        json!(row.classification_coverage),
    );
    item.insert(
        "parent_conversation_id".into(),
        json!(row.parent_conversation_id),
    );
    Ok(Value::Object(item))
}
