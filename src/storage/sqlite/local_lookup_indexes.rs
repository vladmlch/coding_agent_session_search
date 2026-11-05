//! Local fix (review 2026-11-02, S2).
//!
//! `snippets` is read and deleted by `message_id` (Codebuff revision paths,
//! orphan cleanup, Pages export, ON DELETE CASCADE from `messages`) but
//! has no index. The repair runs from `repair_missing_current_schema_objects`,
//! so both the normal and the fast current-schema opens get it, without a
//! schema-version bump. If a remote migration adds an equivalent index under
//! another name, this helper detects it and creates nothing.

use anyhow::{Context, Result};

use crate::franken_sync::{Connection, compat::RowExt};

pub(super) const SNIPPETS_MESSAGE_ID_INDEX: &str = "idx_snippets_message_id_local";

/// True when `create_sql` is a non-partial index on `snippets` whose first
/// key column is `message_id`.
pub(super) fn sql_indexes_snippets_message_id_first(create_sql: &str) -> bool {
    let normalized: String = create_sql
        .chars()
        .filter(|c| !c.is_whitespace() && !matches!(c, '"' | '`' | '[' | ']'))
        .flat_map(char::to_lowercase)
        .collect();
    normalized.contains("onsnippets(message_id") && !normalized.contains(")where")
}

pub(super) fn ensure_snippets_message_id_index(conn: &Connection) -> Result<()> {
    // Archives older than the `snippets` table's schema version have nothing
    // to index: a repair pass that still created the index here failed the
    // whole open with `no such table: main.snippets` (v13 fixture).
    let table_exists = conn
        .query(
            "SELECT 1 FROM sqlite_master \
             WHERE type = 'table' AND name = 'snippets' LIMIT 1;",
        )
        .with_context(|| "checking snippets table")?;
    if table_exists.is_empty() {
        return Ok(());
    }
    let rows = conn
        .query(
            "SELECT sql FROM sqlite_master \
             WHERE type = 'index' AND tbl_name = 'snippets' AND sql IS NOT NULL;",
        )
        .with_context(|| "listing snippets indexes")?;
    for row in &rows {
        let sql: String = row
            .get_typed(0)
            .with_context(|| "reading snippets index sql")?;
        if sql_indexes_snippets_message_id_first(&sql) {
            return Ok(());
        }
    }
    conn.execute(&format!(
        "CREATE INDEX IF NOT EXISTS {SNIPPETS_MESSAGE_ID_INDEX} ON snippets(message_id);"
    ))
    .with_context(|| format!("creating {SNIPPETS_MESSAGE_ID_INDEX}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_equivalent_index_shapes() {
        assert!(sql_indexes_snippets_message_id_first(
            "CREATE INDEX idx_snippets_message_id_local ON snippets(message_id)"
        ));
        assert!(sql_indexes_snippets_message_id_first(
            "CREATE INDEX \"remote_name\" ON \"snippets\" ( message_id , id )"
        ));
        assert!(!sql_indexes_snippets_message_id_first(
            "CREATE INDEX x ON snippets(id, message_id)"
        ));
        assert!(!sql_indexes_snippets_message_id_first(
            "CREATE INDEX x ON snippets(message_id) WHERE id > 0"
        ));
        assert!(!sql_indexes_snippets_message_id_first(
            "CREATE INDEX x ON messages(message_id)"
        ));
    }
}
