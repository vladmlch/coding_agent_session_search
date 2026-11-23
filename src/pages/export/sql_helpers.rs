use crate::franken_sync::compat::{RowExt, Transaction};
use crate::franken_sync::{Row as FrankenRow, params};
use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::path::Path;

pub(super) fn existing_regular_files_share_identity(first: &Path, second: &Path) -> Result<bool> {
    if !std::fs::metadata(first)
        .with_context(|| {
            format!(
                "Failed to inspect source identity probe {}",
                first.display()
            )
        })?
        .is_file()
    {
        return Ok(false);
    }
    if !std::fs::metadata(second)
        .with_context(|| {
            format!(
                "Failed to inspect export output identity probe {}",
                second.display()
            )
        })?
        .is_file()
    {
        return Ok(false);
    }

    let first_file = std::fs::File::open(first)
        .with_context(|| format!("Failed to open source identity probe {}", first.display()))?;
    if !first_file
        .metadata()
        .with_context(|| {
            format!(
                "Failed to inspect source identity probe {}",
                first.display()
            )
        })?
        .is_file()
    {
        return Ok(false);
    }
    let second_file = std::fs::File::open(second).with_context(|| {
        format!(
            "Failed to open export output identity probe {}",
            second.display()
        )
    })?;
    if !second_file
        .metadata()
        .with_context(|| {
            format!(
                "Failed to inspect export output identity probe {}",
                second.display()
            )
        })?
        .is_file()
    {
        return Ok(false);
    }

    let first_identity = crate::franken_sync::FileIdentity::from_file(&first_file)
        .context("Failed to identify source database filesystem object")?;
    let second_identity = crate::franken_sync::FileIdentity::from_file(&second_file)
        .context("Failed to identify export output filesystem object")?;
    Ok(first_identity.is_some() && first_identity == second_identity)
}

pub(super) type MessageExportRow = (
    i64,
    String,
    String,
    Option<i64>,
    i64,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
);

pub(super) fn table_columns_in_transaction(
    conn: &Transaction<'_>,
    table_name: &str,
) -> Result<Vec<String>> {
    let pragma = format!("PRAGMA table_info({table_name})");
    conn.query_map_collect(&pragma, params![], |row: &FrankenRow| {
        row.get_typed::<String>(1)
    })
    .context("Failed to inspect source table schema")
}

pub(super) fn table_exists_in_transaction(
    conn: &Transaction<'_>,
    table_name: &str,
) -> Result<bool> {
    if !table_name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        bail!("invalid SQLite table name: {table_name}");
    }

    table_columns_in_transaction(conn, table_name)
        .map(|columns| !columns.is_empty())
        .with_context(|| format!("Failed to inspect source table {table_name}"))
}

pub(super) fn build_message_export_query(columns: &[String]) -> String {
    let has_updated_at = columns.iter().any(|col| col == "updated_at");
    let has_model = columns.iter().any(|col| col == "model");
    let has_attachment_refs = columns.iter().any(|col| col == "attachment_refs");
    let has_extra_json = columns.iter().any(|col| col == "extra_json");

    format!(
        "SELECT id, role, content, created_at, idx, {}, {}, {}, {}
         FROM messages
         WHERE conversation_id = ?1
         ORDER BY idx ASC",
        if has_updated_at {
            "updated_at"
        } else {
            "NULL AS updated_at"
        },
        if has_model { "model" } else { "NULL AS model" },
        if has_attachment_refs {
            "attachment_refs"
        } else {
            "NULL AS attachment_refs"
        },
        if has_extra_json {
            "extra_json"
        } else {
            "NULL AS extra_json"
        }
    )
}

pub(super) fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value.and_then(|text| {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

pub(super) fn derive_message_model(extra_json: Option<&str>) -> Option<String> {
    let value: Value = serde_json::from_str(extra_json?).ok()?;

    [
        value.pointer("/model"),
        value.pointer("/cass/model"),
        value.pointer("/model_id"),
        value.pointer("/message/model"),
        value.pointer("/message/model_id"),
        value.pointer("/metadata/model"),
    ]
    .into_iter()
    .flatten()
    .find_map(|candidate| candidate.as_str())
    .map(str::trim)
    .filter(|candidate| !candidate.is_empty())
    .map(ToOwned::to_owned)
}

pub(super) fn derive_attachment_refs(extra_json: Option<&str>) -> Option<String> {
    let value: Value = serde_json::from_str(extra_json?).ok()?;

    [
        value.pointer("/attachment_refs"),
        value.pointer("/attachments"),
        value.pointer("/cass/attachment_refs"),
        value.pointer("/cass/attachments"),
        value.pointer("/attachmentRefs"),
        value.pointer("/message/attachment_refs"),
        value.pointer("/message/attachments"),
        value.pointer("/metadata/attachment_refs"),
        value.pointer("/metadata/attachments"),
    ]
    .into_iter()
    .flatten()
    .find_map(|candidate| {
        if candidate.is_null() {
            None
        } else {
            serde_json::to_string(candidate).ok()
        }
    })
}
