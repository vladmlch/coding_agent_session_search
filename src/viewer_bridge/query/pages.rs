use anyhow::Result;
use coding_agent_search::franken_sync::{Connection, FrankenError, SqliteValue};
use serde_json::{Map, Value, json};

use super::super::annotations;
use super::super::identity;
use super::super::protocol::{ListFilters, RESPONSE_BYTES, refuse};
use super::super::revisions;
use super::filters::{
    ResolvedFilters, append_clauses, bind, build_conditions, resolve_filters, run_probes,
    run_tie_probe,
};
use super::hydrate::{HYDRATE_CONVERSATIONS_SQL, byte_len, decode_item, hydrate_rows};
use super::{CURSOR_MAX_BYTES, ROW_BYTES, SortKey, sort_plan};

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
    let tie_bind = bind(params, SqliteValue::Text(cursor.tie.clone()));
    let tie_bind2 = bind(params, SqliteValue::Text(cursor.tie.clone()));
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
        Value::String(text) => SqliteValue::Text(text.clone()),
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
/// classification_coverage, step_count, artifact_count, parent_conversation_id,
/// workspace_path*, byte_sum∥`
/// (`*` = TEXT_GUARD_BYTES-guarded, `†` = tail-state precedence,
/// `‡` = `lmca ?? ended ?? started`, `§` = absent annotation → 0,
/// `∥` = byte-length sum over the raw text cells).
///
/// Phase A — the index-ordered scan. One statement per pin partition
/// (exactly one when `pin_first` applies or a pinned filter already binds
/// the partition): `ORDER BY` mirrors the composite index's column order
/// verbatim so SQLite satisfies the sort from the index itself —
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
    let conds = build_conditions(resolved);
    let mut params = Vec::new();
    let mut clauses = Vec::new();
    append_clauses(&mut clauses, &mut params, &conds.base);
    append_clauses(&mut clauses, &mut params, &conds.activity);
    append_clauses(&mut clauses, &mut params, &conds.created);
    append_clauses(&mut clauses, &mut params, &conds.classification);
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
    // Column order MUST mirror `idx_viewer_sort_*` exactly: SQLite
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
    row: &rusqlite::Row<'_>,
    key: SortKey,
) -> std::result::Result<OrderRow, FrankenError> {
    let value: Option<Value> = if key.cursor_value_is_text() {
        row.get::<_, Option<String>>(3)?.map(Value::from)
    } else {
        row.get::<_, Option<i64>>(3)?.map(Value::from)
    };
    Ok(OrderRow {
        id: row.get(0)?,
        pin: row.get(1)?,
        known: row.get(2)?,
        value,
        tie: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
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
        let decoded = connection.query_map(&sql, &params, |row| decode_order_row(row, key))?;
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
#[allow(clippy::too_many_arguments)]
pub fn list_sessions(
    connection: &Connection,
    filters: &ListFilters,
    sort: Option<&str>,
    pin_first: Option<bool>,
    cursor: Option<&str>,
    limit: usize,
    identity_index: &mut Option<identity::ReverseIdentityIndex>,
    revision: &str,
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

    // Phase A: the persisted order-key scan, folded onto one row per
    // logical session. Codex continuation rollouts share their thread's
    // verified public id (the order tie key), so the scan over-fetches and
    // keeps only each public id's reverse-identity representative — the
    // lowest canonical id the archive verifies for it, the same row a
    // public-id detail lookup opens. That representative is a global
    // property of the public id, so no page repeats it and the choice never
    // depends on the page window. The reverse index is revision-bound and
    // cached across requests, so the per-row test is an in-memory lookup
    // once the queried source scope is filled.
    let mut ordered: Vec<OrderRow> = Vec::new();
    let mut fetch_cursor = cursor.clone();
    let fetch_size = limit.max(1);
    loop {
        let batch = run_order_streams(
            connection,
            &resolved,
            key,
            desc,
            pin_first,
            fetch_cursor.as_ref(),
            fetch_size,
        )?;
        let batch_len = batch.len();
        for row in batch {
            fetch_cursor = Some(cursor_tuple(&row, pin_first));
            let representative = if row.tie.is_empty() {
                true
            } else {
                identity::resolve_conversation_id_cached(
                    connection,
                    &row.tie,
                    identity_index,
                    revision,
                )?
                .is_none_or(|id| id == row.id)
            };
            if representative {
                ordered.push(row);
                if ordered.len() > limit {
                    break;
                }
            }
        }
        if ordered.len() > limit || batch_len <= fetch_size {
            break;
        }
    }

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

    // Resolve the identity/tag/parent overlay for the page's surviving rows
    // BEFORE the byte cut, so `push_bounded` measures exactly the bytes each
    // emitted item carries. Budgeting the decoded row first and enriching
    // afterwards let a page that fit the budget overflow once the overlay
    // filled `public_id`/`provider`/`tags`/`parent_public_id`, refusing the
    // whole response instead of cutting it. Rows that vanished between the
    // order scan and hydration are excluded here (the decode loop consumes
    // their position without emitting), keeping the racing-delete path
    // refusal-free.
    let present_ids: Vec<i64> = page_ids
        .iter()
        .copied()
        .filter(|id| by_id.contains_key(id))
        .collect();
    let public_ids = identity::resolve_public_ids(connection, &present_ids)?;
    let tags = annotations::tags_for(connection, &present_ids)?;
    // Parent identities resolve through the same canonical identity
    // machinery — one bounded primary-key probe per distinct parent id
    // (identity::resolve_public_id recomputes and verifies the public
    // id; an unresolvable/ambiguous parent stays typed NULL).
    let mut parent_ids: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    for row in by_id.values() {
        if let Some(parent_id) = row.parent_conversation_id {
            parent_ids.insert(parent_id);
        }
    }
    let mut parent_public_ids: std::collections::BTreeMap<i64, String> =
        std::collections::BTreeMap::new();
    for parent_id in parent_ids {
        if let Ok(identity) = identity::resolve_public_id(connection, parent_id) {
            parent_public_ids.insert(parent_id, identity.public_id);
        }
    }

    // Decode bounded; a page may serialize fewer rows than the order scan
    // returned when the response byte budget intervenes. Each item is
    // enriched before it is budgeted, so the cut reflects the emitted bytes
    // and a truncated page keeps a correct `next_cursor`.
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
        let total_bytes = row.total_bytes;
        if !(0..=ROW_BYTES).contains(&total_bytes) {
            return Err(refuse(
                "canonical_payload_too_large",
                "Canonical row exceeds the strict bridge byte profile",
            ));
        }
        let mut item = decode_item(row)?;
        let id = item["conversation_id"].as_i64().expect("decoded id");
        if let Some(identity) = public_ids.get(&id) {
            item["public_id"] = json!(identity.public_id);
            item["provider"] = json!(identity.provider);
        }
        if let Some(parent_id) = item["parent_conversation_id"].as_i64() {
            item["parent_public_id"] = json!(parent_public_ids.get(&parent_id));
        }
        item["tags"] = json!(tags.get(&id).cloned().unwrap_or_default());
        if !push_bounded(&mut items, item, &mut bytes)? {
            truncated = true;
            break;
        }
        last = Some(position);
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

/// `list_annotations(ListAnnotationsRequest{cursor, limit})` page body:
/// the annotations-only collection read. A keyset page over
/// `viewer_annotations` (one indexed `WHERE conversation_id > ? ORDER BY
/// conversation_id LIMIT ?` statement), with public identity and tags
/// resolved for the emitted rows only. It never hydrates session summary
/// text, so a corpus-wide metadata read does not depend on
/// `first_prompt`/`title` size. Annotation text is never truncated: a row
/// whose annotations alone exceed `ROW_BYTES` refuses typed
/// `canonical_payload_too_large`, exactly like the list lane. Rows whose
/// public identity cannot be verified emit `public_id: null` and are
/// counted in a `warnings` entry (a client keys records by public id, so
/// it skips them) — never a silent whole-page refusal.
pub fn list_annotations(
    connection: &Connection,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Value> {
    if !revisions::extensions_installed(connection)? {
        return Err(refuse(
            "viewer_extensions_missing",
            "The viewer extension profile is not installed; run --init-extensions first",
        ));
    }
    let after = match cursor {
        Some(raw) => decode_annotations_cursor(raw)?,
        None => 0,
    };
    let mut params = Vec::new();
    struct AnnotationPageRow {
        id: i64,
        is_pinned: i64,
        custom_title: Option<String>,
        notes: Option<String>,
        revision: i64,
    }
    let after_bind = bind(&mut params, SqliteValue::Integer(after));
    let limit_bind = bind(&mut params, SqliteValue::Integer((limit + 1) as i64));
    let rows = connection.query_map(
        &format!(
            "SELECT conversation_id, is_pinned, custom_title, notes, revision \
             FROM viewer_annotations WHERE conversation_id > {after_bind} \
             ORDER BY conversation_id LIMIT {limit_bind}"
        ),
        &params,
        |row| {
            Ok(AnnotationPageRow {
                id: row.get(0)?,
                is_pinned: row.get(1)?,
                custom_title: row.get(2)?,
                notes: row.get(3)?,
                revision: row.get(4)?,
            })
        },
    )?;
    let has_more = rows.len() > limit;
    let page_len = rows.len().min(limit);
    let ids: Vec<i64> = rows[..page_len].iter().map(|row| row.id).collect();
    let tags = annotations::tags_for(connection, &ids)?;
    let public_ids = identity::resolve_public_ids_tolerant(connection, &ids)?;
    let generation = revisions::generation(connection)?;

    let mut items = Vec::new();
    let mut bytes = 0_usize;
    let mut last: Option<i64> = None;
    let mut truncated = false;
    let mut unresolved = 0_usize;
    for row in rows.into_iter().take(limit) {
        let id = row.id;
        let is_pinned = row.is_pinned;
        let custom_title = row.custom_title;
        let notes = row.notes;
        let revision = row.revision;
        let row_tags = tags.get(&id).cloned().unwrap_or_default();
        // Annotations are never truncated: the row profile counts their RAW
        // bytes, so an annotation-only overflow keeps the strict refusal.
        let tag_bytes: i64 = row_tags.iter().map(|tag| tag.len() as i64).sum();
        let row_bytes = byte_len(custom_title.as_deref()) + byte_len(notes.as_deref()) + tag_bytes;
        if !(0..=ROW_BYTES).contains(&row_bytes) {
            return Err(refuse(
                "canonical_payload_too_large",
                "Canonical row exceeds the strict bridge byte profile",
            ));
        }
        let identity = public_ids.get(&id).and_then(|entry| entry.as_ref());
        if identity.is_none() {
            unresolved += 1;
        }
        let item = json!({
            "conversation_id": id,
            "public_id": identity.map(|i| i.public_id.clone()),
            "provider": identity.map(|i| i.provider.clone()),
            "is_pinned": is_pinned != 0,
            "custom_title": custom_title,
            "notes": notes,
            "tags": row_tags,
            "annotation_revision": revision,
        });
        if !push_bounded(&mut items, item, &mut bytes)? {
            truncated = true;
            break;
        }
        last = Some(id);
    }
    let next_cursor = if has_more || truncated {
        last.map(|id| Value::String(encode_annotations_cursor(id)))
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let mut warnings: Vec<String> = Vec::new();
    if unresolved > 0 {
        warnings.push(format!("unresolved_identity:{unresolved}"));
    }
    Ok(json!({
        "items": items,
        "next_cursor": next_cursor,
        "generation": generation,
        "complete": true,
        "warnings": warnings,
    }))
}

/// Opaque keyset cursor for `list_annotations`: the last emitted
/// `conversation_id`. Client-echoed state, bounded like every cursor.
fn encode_annotations_cursor(id: i64) -> String {
    format!("a:{id}")
}

fn decode_annotations_cursor(raw: &str) -> Result<i64> {
    if raw.len() > CURSOR_MAX_BYTES {
        return Err(refuse(
            "cursor_expired",
            "Annotations cursor exceeds its byte bound",
        ));
    }
    let id = raw
        .strip_prefix("a:")
        .and_then(|rest| rest.parse::<i64>().ok())
        .filter(|id| *id >= 0);
    id.ok_or_else(|| refuse("cursor_expired", "Malformed annotations cursor"))
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
