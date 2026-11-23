use anyhow::Result;
use coding_agent_search::franken_sync::{Connection, SqliteValue};
use serde_json::{Map, Value, json};

use super::super::facts_bag;
use super::super::identity;
use super::super::protocol::refuse;
use super::TEXT_GUARD_BYTES;
use super::filters::bind;

/// Phase B — payload hydration for one page id set. FrankenSQLite cannot
/// compile `IN (list)` inside a JOIN and degrades OR-ed equalities to a
/// scan (both probe-verified), while an `IN` list on a standalone table
/// resolves to repeated `SeekRowid` lookups — so the page runs one
/// bounded probe per side table and assembles the `decode_item` row
/// contract in Rust. Per page that is a handful of `O(page)` statements
/// instead of `O(page)` interpreted six-table joins.
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

/// `decode_item`'s row contract, assembled in Rust from the bounded
/// per-table probes — index-for-index the same shape the old single
/// `FROM_SQL` join produced (see `select_columns`), with the four
/// cumulative token sub-totals appended (cells 32–35) and the merged
/// persisted fact/metrics bag carried alongside (Task 7).
pub(super) struct HydratedRow {
    cells: Vec<SqliteValue>,
    /// `null` when no `viewer_session_facts` row was persisted for this
    /// session; otherwise the merged facts+metrics bag.
    metrics: Value,
    /// Truncation fact for the bounded `title` preview (cell 7).
    title_truncated: bool,
    title_bytes: Option<i64>,
}

impl HydratedRow {
    /// Same semantics as `RowExt::get_typed` on a fetched `Row`.
    pub(super) fn get_typed<T: coding_agent_search::franken_sync::compat::FromSqliteValue>(
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
pub(super) fn byte_len(value: Option<&str>) -> i64 {
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
pub(super) fn hydrate_rows(
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
        step_count: Option<i64>,
        artifact_count: Option<i64>,
        parent_conversation_id: Option<i64>,
        bag: facts_bag::SessionFactBag,
    }
    let mut facts: std::collections::BTreeMap<i64, Facts> = std::collections::BTreeMap::new();
    for row in run_in_probe(connection, HYDRATE_FACTS_SQL, ")", ids)? {
        let model_set_json: Option<String> = row.get_typed(14)?;
        let first_prompt_raw: Option<String> = row.get_typed(15)?;
        // Bounded previews: one oversized prompt/model set never refuses a
        // page; the bag carries the preview plus its truncation fact, and
        // the byte profile counts the emitted preview, not the raw cell.
        let (model_set, model_set_truncated, model_set_bytes) = facts_bag::bound_json_preview(
            model_set_json.clone(),
            facts_bag::decode_string_list(model_set_json.as_deref()),
        );
        let (first_prompt, first_prompt_truncated, first_prompt_bytes) =
            facts_bag::bound_text_preview(first_prompt_raw);
        facts.insert(
            row.get_typed(0)?,
            Facts {
                is_subagent: row.get_typed(1)?,
                task_category: row.get_typed(2)?,
                // Column 3 (the overall facts coverage_state) is deliberately
                // not read: classification coverage derives from the verdict
                // alone (facts_bag::classification_coverage).
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
                    model_set,
                    model_set_truncated,
                    model_set_bytes,
                    first_prompt,
                    first_prompt_truncated,
                    first_prompt_bytes,
                    ..Default::default()
                },
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
        bag: facts_bag::SessionFactBag,
    }
    let mut metrics: std::collections::BTreeMap<i64, Metrics> = std::collections::BTreeMap::new();
    if metrics_installed {
        for row in run_in_probe(connection, HYDRATE_METRICS_SQL, ")", ids)? {
            let tools_used_json: Option<String> = row.get_typed(6)?;
            let task_breakdown_json: Option<String> = row.get_typed(7)?;
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
            metrics.insert(
                row.get_typed(0)?,
                Metrics {
                    bag: facts_bag::SessionFactBag {
                        ttfe_seconds: row.get_typed(1)?,
                        one_shot_rate: row.get_typed(2)?,
                        retries_count: row.get_typed(3)?,
                        self_corrections_count: row.get_typed(4)?,
                        user_corrections_count: row.get_typed(5)?,
                        tools_used,
                        tools_used_truncated,
                        tools_used_bytes,
                        task_breakdown,
                        task_breakdown_truncated,
                        task_breakdown_bytes,
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
            bag.merge_metrics(&entry.bag);
        }
        let metrics_bag = bag.emit(fact.is_some());

        // The bounded `title` preview: cut on a UTF-8 char boundary with an
        // explicit truncation fact, so one oversized title never refuses a
        // page. Annotation/identity/category/model cells stay RAW-counted.
        let (title_preview, title_truncated, title_bytes) =
            facts_bag::bound_text_preview(conv.title.clone());

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
            + byte_len(fact.and_then(|f| f.task_category.as_deref()))
            + bag.preview_bytes();

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
            title_preview
                .clone()
                .map_or(SqliteValue::Null, |s| SqliteValue::Text(s.into())), // 7
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
            fact.map(|f| facts_bag::classification_coverage(f.is_subagent))
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
                title_truncated,
                title_bytes,
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
    item.insert("title_truncated".into(), json!(row.title_truncated));
    item.insert("title_bytes".into(), json!(row.title_bytes));
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
