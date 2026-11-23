use std::io::BufRead;

use anyhow::{Result, anyhow, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use coding_agent_search::franken_sync::compat::RowExt;
use coding_agent_search::franken_sync::{Connection, SqliteValue};

use super::{
    ADDED_CANONICAL_TABLES, Input, Inspected, MAX_BATCH_BYTES, MAX_BATCH_RECORDS,
    MAX_TRIGGER_BYTES, META_TABLE, MIGRATIONS_TABLE, REVIEWED_TARGET_VERSION, SCHEMA_VERSION_KEY,
    SourceSchemaMarker, parse_source_version, require_reviewed_transition,
    table_added_after_source, target_version,
};
use crate::logical_archive::codec::{Cell, Record, Table, Validator};
use crate::logical_archive::{database_failure, export, import, integrity, integrity_unless_io};

pub(super) fn require_reviewed_table_layout(
    connection: &Connection,
    archived: &[Table],
    source_version: u32,
) -> Result<Vec<Table>> {
    let (added, carried): (Vec<Table>, Vec<Table>) = export::tables(connection)?
        .into_iter()
        .partition(|table| table_added_after_source(&table.name, source_version));
    ensure!(
        archived == carried,
        "reviewed migration to v{REVIEWED_TARGET_VERSION} requires canonical table/column/primary-key descriptors identical to the current ones, apart from tables added after v{source_version}; table drift is not authorized"
    );
    ensure!(
        added.len()
            == ADDED_CANONICAL_TABLES
                .iter()
                .filter(|(_, version)| *version > source_version)
                .count(),
        "current schema lacks a table the reviewed migration adds"
    );
    for table in &added {
        let sql = format!("SELECT 1 FROM {} LIMIT 1", export::quoted(&table.name)?);
        ensure!(
            connection
                .query(&sql)
                .map_err(|error| {
                    database_failure(
                        format!("cannot check newly added logical table {}", table.name),
                        error,
                    )
                })?
                .is_empty(),
            "table {} added since the archived schema must stay empty in a migrated archive",
            table.name
        );
    }
    Ok(carried)
}

fn values(cells: Vec<Cell>) -> Result<Vec<SqliteValue>> {
    cells
        .into_iter()
        .map(|cell| {
            cell.validate()?;
            Ok(match cell {
                Cell::Null => SqliteValue::Null,
                Cell::Integer(value) => SqliteValue::Integer(value),
                Cell::Real(bits) => SqliteValue::Float(f64::from_bits(
                    u64::from_str_radix(&bits, 16).map_err(|_| anyhow!("invalid REAL encoding"))?,
                )),
                Cell::Text(value) => SqliteValue::Text(value.into()),
                Cell::Blob(value) => SqliteValue::Blob(
                    STANDARD
                        .decode(value)
                        .map_err(|_| anyhow!("invalid BLOB encoding"))?
                        .into(),
                ),
            })
        })
        .collect()
}

fn insert_sql(table: &Table) -> Result<String> {
    table.validate()?;
    let columns = table
        .columns
        .iter()
        .map(|name| export::quoted(name))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let placeholders = vec!["?"; table.columns.len()].join(", ");
    Ok(format!(
        "INSERT INTO {} ({columns}) VALUES ({placeholders})",
        export::quoted(&table.name)?
    ))
}

pub(super) fn meta_schema_version_row(table: &Table, cells: &[Cell]) -> bool {
    if table.name != META_TABLE {
        return false;
    }
    table
        .columns
        .iter()
        .position(|column| column == "key")
        .and_then(|offset| cells.get(offset))
        .is_some_and(|cell| matches!(cell, Cell::Text(value) if value == SCHEMA_VERSION_KEY))
}

fn suspend_triggers(connection: &Connection) -> Result<Vec<String>> {
    let rows = connection
        .query(
            "SELECT name, substr(sql, 1, 65537) FROM sqlite_master WHERE type = 'trigger' ORDER BY name LIMIT 257",
        )
        .map_err(|error| database_failure("cannot read canonical migration triggers", error))?;
    ensure!(
        rows.len() <= 256,
        "canonical schema exceeds restore trigger limit"
    );
    let mut statements = Vec::new();
    let mut bytes = 0_usize;
    for row in rows {
        let name = row.get_typed::<String>(0).map_err(|error| {
            database_failure("cannot decode a canonical migration trigger name", error)
        })?;
        let sql = row.get_typed::<String>(1).map_err(|error| {
            database_failure(
                "cannot decode a canonical migration trigger definition",
                error,
            )
        })?;
        bytes = bytes.saturating_add(sql.len());
        ensure!(
            sql.len() <= 65_536 && bytes <= MAX_TRIGGER_BYTES,
            "canonical trigger definitions exceed restore budget"
        );
        connection
            .execute(&format!("DROP TRIGGER {}", export::quoted(&name)?))
            .map_err(|error| {
                database_failure("cannot suspend a canonical migration trigger", error)
            })?;
        statements.push(sql);
    }
    Ok(statements)
}

pub(super) fn verify_current_schema_authority(connection: &Connection) -> Result<()> {
    import::verify_schema_authority(connection, &target_version()?.to_string())
}

fn clear_archived_data(connection: &Connection, archived: &[Table]) -> Result<()> {
    for table in archived {
        if table.name == MIGRATIONS_TABLE {
            continue;
        }
        if table.name == META_TABLE {
            connection
                .execute("DELETE FROM \"meta\" WHERE \"key\" <> 'schema_version'")
                .map_err(|error| {
                    database_failure("cannot clear initializer rows in logical table meta", error)
                })?;
        } else {
            connection
                .execute(&format!("DELETE FROM {}", export::quoted(&table.name)?))
                .map_err(|error| {
                    database_failure(
                        format!(
                            "cannot clear initializer rows in logical table {}",
                            table.name
                        ),
                        error,
                    )
                })?;
        }
    }
    Ok(())
}

pub(super) fn restore_reviewed<R: BufRead>(
    connection: &Connection,
    input: &mut Input<R>,
    inspected: &Inspected,
) -> Result<()> {
    let source_version = parse_source_version(&inspected.header)?;
    let target = target_version()?;
    require_reviewed_transition(source_version, target)?;
    require_reviewed_table_layout(connection, &inspected.tables, source_version)?;
    verify_current_schema_authority(connection)?;

    let Some(Record::Header { header }) = input.record(1)? else {
        return Err(integrity("logical archive must begin with a header"));
    };
    ensure!(
        header == inspected.header,
        "logical archive changed between validation and replay"
    );
    let mut schema_marker = SourceSchemaMarker::new(&header);
    let mut validator = Validator::new(header).map_err(integrity_unless_io)?;
    connection
        .execute("PRAGMA foreign_keys = OFF")
        .map_err(|error| {
            database_failure("cannot suspend migration foreign-key enforcement", error)
        })?;
    connection.execute("BEGIN IMMEDIATE").map_err(|error| {
        database_failure("cannot begin the private migration transaction", error)
    })?;
    let triggers = suspend_triggers(connection)?;
    clear_archived_data(connection, &inspected.tables)?;

    let mut statement = None;
    let mut current_table: Option<Table> = None;
    let mut table_count = 0_usize;
    let mut table_row = 0_u64;
    let mut batch_records = 0_usize;
    let mut batch_bytes = 0_usize;
    let mut line = 2_u64;

    while let Some(record) = input.record(line)? {
        if batch_records == MAX_BATCH_RECORDS
            || input.record_bytes > MAX_BATCH_BYTES.saturating_sub(batch_bytes)
        {
            connection.execute("COMMIT").map_err(|error| {
                database_failure(
                    format!("cannot commit the private migration batch before record {line}"),
                    error,
                )
            })?;
            connection.execute("BEGIN IMMEDIATE").map_err(|error| {
                database_failure(
                    format!("cannot begin the private migration batch for record {line}"),
                    error,
                )
            })?;
            batch_records = 0;
            batch_bytes = 0;
        }
        validator
            .validate(&record)
            .map_err(|error| integrity(format!("record {line}: {error}")))?;
        schema_marker
            .observe(&record)
            .map_err(|error| integrity(format!("record {line}: {error}")))?;
        match record {
            Record::Table { table } => {
                ensure!(
                    inspected.tables.get(table_count) == Some(&table),
                    "record {line}: logical table descriptor changed after validation"
                );
                statement = if table.name == MIGRATIONS_TABLE {
                    None
                } else {
                    Some(connection.prepare(&insert_sql(&table)?).map_err(|error| {
                        database_failure(
                            format!("record {line}: cannot prepare logical table {} migration insertion", table.name),
                            error,
                        )
                    })?)
                };
                current_table = Some(table);
                table_count += 1;
                table_row = 0;
            }
            Record::Row { values: cells } => {
                let table = current_table
                    .as_ref()
                    .ok_or_else(|| anyhow!("record {line}: row precedes its table"))?;
                table_row = table_row
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("logical table row position overflow"))?;
                if table.name == MIGRATIONS_TABLE {
                    // Archived migration history is input evidence, never current authority.
                } else if meta_schema_version_row(table, &cells) {
                    // The observed source marker agrees with the admitted header;
                    // the initializer supplies the current target marker instead.
                } else {
                    let row = values(cells)?;
                    statement
                        .as_ref()
                        .ok_or_else(|| anyhow!("record {line}: no compatible insert target"))?
                        .execute_with_params(&row)
                        .map_err(|error| {
                            database_failure(
                                format!(
                                    "record {line}: {}, reviewed v{REVIEWED_TARGET_VERSION} row insertion failed; no destination was published",
                                    export::row_location(table, table_row, &row)
                                ),
                                error,
                            )
                        })?;
                }
            }
            Record::Completion { .. } => {}
            Record::Header { .. } => bail!("record {line}: duplicate archive header"),
        }
        batch_records += 1;
        batch_bytes += input.record_bytes;
        line = line
            .checked_add(1)
            .ok_or_else(|| anyhow!("logical record position overflow"))?;
    }

    let (header, completion) = validator.finish().map_err(integrity_unless_io)?;
    ensure!(
        (header, completion) == (inspected.header.clone(), inspected.completion.clone()),
        "logical archive changed during reviewed migration replay"
    );
    ensure!(
        table_count == inspected.tables.len(),
        "logical archive table set changed during reviewed migration replay"
    );
    schema_marker.finish().map_err(integrity_unless_io)?;

    drop(statement);
    import::restore_sequences(connection, &inspected.header, &inspected.tables)?;
    for trigger in triggers {
        connection.execute_batch(&trigger).map_err(|error| {
            database_failure("cannot reinstate canonical migration triggers", error)
        })?;
    }
    verify_current_schema_authority(connection)?;
    import::verify_database(connection)?;
    connection.execute("COMMIT").map_err(|error| {
        database_failure(
            "cannot commit the verified private migrated database",
            error,
        )
    })?;
    Ok(())
}
