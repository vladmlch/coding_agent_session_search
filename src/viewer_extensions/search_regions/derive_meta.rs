use anyhow::Result;

use crate::franken_sync::compat::{ParamValue, RowExt};
use crate::viewer_extensions::facts::FactsDb;

use super::derive_helpers::bounded;
use super::{REGION_DOC_BASE, RegionRow, TEXT_SOURCE_META};

/// Metadata regions sourced from `viewer_annotations`/`viewer_tags` plus
/// the facts child links — appended LAST so targeted meta replacement never
/// renumbers the message-bound ordinals. Reads the canonical title itself
/// so callers without a packet (annotation edits, child-link fixups) can
/// re-derive identically.
pub fn derive_meta_regions(
    db: &impl FactsDb,
    conversation_id: i64,
    source_revision: i64,
    first_ordinal: i64,
) -> Result<Vec<RegionRow>> {
    let mut regions: Vec<RegionRow> = Vec::new();
    let canonical_title: Option<String> = db
        .facts_query_map(
            "SELECT title FROM conversations WHERE id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| row.get_typed::<Option<String>>(0),
        )?
        .into_iter()
        .next()
        .flatten();
    let annotation = db
        .facts_query_map(
            "SELECT is_pinned, custom_title, notes FROM viewer_annotations \
             WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| {
                Ok((
                    row.get_typed::<Option<String>>(1)?,
                    row.get_typed::<Option<String>>(2)?,
                ))
            },
        )?
        .into_iter()
        .next();
    let (custom_title, notes) = annotation.unwrap_or((None, None));
    let effective_title = custom_title.or(canonical_title);
    let mut next = first_ordinal;
    if let Some(title) = effective_title.filter(|t| !t.is_empty()) {
        regions.push(RegionRow {
            region_ordinal: next,
            region_key: "meta:title".to_owned(),
            scope: "title".to_owned(),
            scope_known: true,
            quill_idx: REGION_DOC_BASE as i64 + next,
            text_source: TEXT_SOURCE_META.to_owned(),
            indexed_text: Some(bounded(&title)),
            source_revision,
            ..RegionRow::default()
        });
        next += 1;
    }
    if let Some(notes) = notes.filter(|t| !t.is_empty()) {
        regions.push(RegionRow {
            region_ordinal: next,
            region_key: "meta:notes".to_owned(),
            scope: "chat".to_owned(),
            scope_known: true,
            quill_idx: REGION_DOC_BASE as i64 + next,
            text_source: TEXT_SOURCE_META.to_owned(),
            indexed_text: Some(bounded(&notes)),
            source_revision,
            ..RegionRow::default()
        });
        next += 1;
    }
    let tags = db.facts_query_map(
        "SELECT tag FROM viewer_tags WHERE conversation_id = ?1 ORDER BY tag",
        &[ParamValue::from(conversation_id)],
        |row| row.get_typed::<String>(0),
    )?;
    if !tags.is_empty() {
        regions.push(RegionRow {
            region_ordinal: next,
            region_key: "meta:tags".to_owned(),
            scope: "chat".to_owned(),
            scope_known: true,
            quill_idx: REGION_DOC_BASE as i64 + next,
            text_source: TEXT_SOURCE_META.to_owned(),
            indexed_text: Some(bounded(&tags.join(" "))),
            source_revision,
            ..RegionRow::default()
        });
        next += 1;
    }
    // Subagents scope: resolved child descriptors bound to this parent.
    // Split into an equality seek on the facts parent index plus one rowid
    // fetch per child, ordered and truncated in Rust rather than a one-shot
    // JOIN over facts and conversations. The row set is identical:
    // `conversations.id` is the unique INTEGER PRIMARY KEY, so
    // `ORDER BY c2.id` is a total order and `LIMIT 64` truncates the same
    // prefix; a facts row without a canonical conversation drops out here
    // exactly as the INNER JOIN dropped it.
    // Cost note: this fetches ALL children of the parent and truncates to
    // 64 in Rust — O(children) row fetches where the SQL JOIN stopped at
    // its O(64) LIMIT. Bounded in practice by archive shape (a parent
    // with thousands of direct children is pathological); revisit only
    // if a real archive shows one.
    type ChildDescriptor = (i64, Option<String>, Option<String>, Option<String>);
    let child_links: Vec<(i64, Option<String>)> = db.facts_query_map(
        "SELECT conversation_id, first_prompt FROM viewer_session_facts \
         WHERE parent_conversation_id = ?1",
        &[ParamValue::from(conversation_id)],
        |row| {
            Ok((
                row.get_typed::<i64>(0)?,
                row.get_typed::<Option<String>>(1)?,
            ))
        },
    )?;
    let mut children: Vec<ChildDescriptor> = Vec::with_capacity(child_links.len());
    for (child_id, first_prompt) in child_links {
        let Some((_, title, external_id)) = db
            .facts_query_map(
                "SELECT id, title, external_id FROM conversations WHERE id = ?1",
                &[ParamValue::from(child_id)],
                |row| {
                    Ok((
                        row.get_typed::<i64>(0)?,
                        row.get_typed::<Option<String>>(1)?,
                        row.get_typed::<Option<String>>(2)?,
                    ))
                },
            )?
            .into_iter()
            .next()
        else {
            continue;
        };
        children.push((child_id, title, external_id, first_prompt));
    }
    children.sort_by_key(|(child_id, _, _, _)| *child_id);
    children.truncate(64);
    for (child_id, title, external, first_prompt) in children {
        let text = [
            title.as_deref(),
            external.as_deref(),
            first_prompt.as_deref(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
        if text.is_empty() {
            continue;
        }
        regions.push(RegionRow {
            region_ordinal: next,
            region_key: format!("meta:child:{child_id}"),
            scope: "subagents".to_owned(),
            scope_known: true,
            quill_idx: REGION_DOC_BASE as i64 + next,
            text_source: TEXT_SOURCE_META.to_owned(),
            indexed_text: Some(bounded(&text)),
            source_revision,
            ..RegionRow::default()
        });
        next += 1;
    }
    Ok(regions)
}
