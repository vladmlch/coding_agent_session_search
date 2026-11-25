use std::collections::BTreeMap;
use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use coding_agent_search::franken_sync::{Connection, FrankenError, SqliteValue, compat::RowExt};
use coding_agent_search::model::types::{Agent, AgentKind};
use coding_agent_search::storage::sqlite::SqliteStorage;

use super::projection::{ProjectionCursor, compare_table_rows, verify_projection_rows};
use super::replay::meta_schema_version_row;
use super::*;
use crate::logical_archive::export;

fn projection_header(format_version: u32, source_version: u32) -> Header {
    Header {
        format: codec::FORMAT.to_owned(),
        schema_version: format_version,
        archive_id: "projection-regression".into(),
        exported_at_ms: 1,
        storage_schema_version: source_version.to_string(),
        record_types: codec::record_types(format_version),
        contains_private_data: true,
        omissions: vec!["derived_search_assets".into()],
        sequences: (format_version == codec::SEQUENCE_VERSION).then(Vec::new),
    }
}

fn projection_records(marker: Option<Cell>, content: &str) -> Vec<Record> {
    let mut records = vec![
        Record::Table {
            table: Table {
                name: MIGRATIONS_TABLE.into(),
                columns: vec!["version".into(), "description".into()],
                primary_key: vec![0],
            },
        },
        Record::Row {
            values: vec![Cell::Integer(1), Cell::Text("PRIVATE-HISTORY".into())],
        },
        Record::Table {
            table: Table {
                name: "messages".into(),
                columns: vec!["id".into(), "content".into()],
                primary_key: vec![0],
            },
        },
        Record::Row {
            values: vec![Cell::Integer(7), Cell::Text(content.into())],
        },
        Record::Table {
            table: Table {
                name: META_TABLE.into(),
                columns: vec!["key".into(), "value".into()],
                primary_key: vec![0],
            },
        },
    ];
    if let Some(marker) = marker {
        records.push(Record::Row {
            values: vec![Cell::Text(SCHEMA_VERSION_KEY.into()), marker],
        });
    }
    records
}

fn archive_wire(header: Header, records: &[Record]) -> Result<(Vec<u8>, Inspected)> {
    let mut output = codec::encode(&Record::Header {
        header: header.clone(),
    })?;
    let mut validator = Validator::new(header.clone())?;
    let mut tables = Vec::new();
    for record in records {
        validator.prepare(record)?.write_to(&mut output)?;
        if let Record::Table { table } = record {
            tables.push(table.clone());
        }
    }
    let completion = validator.completion();
    validator
        .prepare(&Record::Completion {
            completion: completion.clone(),
        })?
        .write_to(&mut output)?;
    validator.finish()?;
    Ok((
        output,
        Inspected {
            header,
            completion,
            tables,
        },
    ))
}

/// Deliberately retain an earlier completion after changing compared or
/// skipped records. A projection that trusts its earlier verification pass
/// accepts some of these streams even though their digest is now false.
fn unchecked_projection_wire(inspected: &Inspected, records: &[Record]) -> Result<Vec<u8>> {
    let mut output = codec::encode(&Record::Header {
        header: inspected.header.clone(),
    })?;
    for record in records {
        serde_json::to_writer(&mut output, record)?;
        output.push(b'\n');
    }
    output.extend(codec::encode(&Record::Completion {
        completion: inspected.completion.clone(),
    })?);
    Ok(output)
}

fn projection_database(content: &str) -> Result<Connection> {
    let connection = Connection::open(":memory:")?;
    connection.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, content TEXT)")?;
    connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")?;
    connection.execute_with_params(
        "INSERT INTO messages VALUES (7, ?1)",
        &[SqliteValue::Text(content.into())],
    )?;
    connection.execute_with_params(
        "INSERT INTO meta VALUES ('schema_version', ?1)",
        &[SqliteValue::Text(
            REVIEWED_TARGET_VERSION.to_string().into(),
        )],
    )?;
    Ok(connection)
}

#[test]
fn projection_authenticates_the_rows_it_compares_after_prior_inspection() -> Result<()> {
    let connection = projection_database("PRIVATE-NEW-BODY")?;
    for version in [codec::VERSION, codec::CHUNKED_VERSION] {
        let original = projection_records(Some(Cell::Text("20".into())), "PRIVATE-OLD-BODY");
        let (wire, inspected) = archive_wire(projection_header(version, 20), &original)?;
        assert_eq!(
            codec::verify(&mut Cursor::new(wire))?.1,
            inspected.completion
        );

        let changed = projection_records(Some(Cell::Text("20".into())), "PRIVATE-NEW-BODY");
        // The second pass matches every persisted row, but still presents
        // the original completion. Old unvalidated projection accepted it.
        let forged = unchecked_projection_wire(&inspected, &changed)?;
        let error = verify_projection_rows(&connection, Cursor::new(forged), &inspected)
            .expect_err("a prior digest cannot authenticate newly compared rows");
        assert!(error.to_string().contains("digest mismatch"));
        assert!(!error.to_string().contains("PRIVATE-"));

        // Recomputing a valid digest for the replacement is not permission
        // to compare a different archive than the one already admitted.
        let (changed_wire, _) = archive_wire(inspected.header.clone(), &changed)?;
        assert!(
            verify_projection_rows(&connection, Cursor::new(changed_wire), &inspected).is_err()
        );
        assert_eq!(
            connection
                .query_row("SELECT content FROM messages WHERE id = 7")?
                .get_typed::<String>(0)?,
            "PRIVATE-NEW-BODY"
        );
    }
    connection.close()?;
    Ok(())
}

#[test]
fn projection_checks_skipped_history_identity_shape_and_content() -> Result<()> {
    let connection = projection_database("PRIVATE-BODY")?;
    let original = projection_records(Some(Cell::Text("21".into())), "PRIVATE-BODY");
    let (_, inspected) = archive_wire(projection_header(codec::CHUNKED_VERSION, 21), &original)?;
    for replacement in [
        vec![Cell::Null, Cell::Text("PRIVATE-HISTORY".into())],
        vec![Cell::Integer(1)],
        vec![
            Cell::Integer(1),
            Cell::Text("PRIVATE-CHANGED-HISTORY".into()),
        ],
    ] {
        let mut changed = original.clone();
        changed[1] = Record::Row {
            values: replacement,
        };
        let wire = unchecked_projection_wire(&inspected, &changed)?;
        let error = verify_projection_rows(&connection, Cursor::new(wire), &inspected)
            .expect_err("skipped migration history must still be authenticated");
        assert!(!error.to_string().contains("PRIVATE-"));
    }
    let mut duplicate = original.clone();
    duplicate.insert(2, original[1].clone());
    let wire = unchecked_projection_wire(&inspected, &duplicate)?;
    let error = verify_projection_rows(&connection, Cursor::new(wire), &inspected)
        .expect_err("skipped history cannot repeat a primary key");
    assert!(error.to_string().contains("duplicate or unordered"));
    connection.close()?;
    Ok(())
}

#[test]
fn projection_verifies_v1_v2_boundaries_and_current_or_reviewed_markers() -> Result<()> {
    let connection = projection_database("PRIVATE-BODY")?;
    for version in [codec::VERSION, codec::CHUNKED_VERSION] {
        for source in [20, 21, 22, REVIEWED_TARGET_VERSION] {
            let records = projection_records(Some(Cell::Text(source.to_string())), "PRIVATE-BODY");
            let (wire, inspected) = archive_wire(projection_header(version, source), &records)?;
            // Both nonempty table transitions go through put_back. Hashing
            // either boundary twice would reject this valid completion.
            verify_projection_rows(&connection, Cursor::new(&wire), &inspected)?;
            // EOF at a complete-record boundary is still an incomplete
            // archive, not a database conflict or a generic engine error.
            let mut boundary = 0;
            for record in wire.split_inclusive(|byte| *byte == b'\n') {
                let error =
                    verify_projection_rows(&connection, Cursor::new(&wire[..boundary]), &inspected)
                        .expect_err("a verified prefix must not replace completion");
                assert_eq!(
                    crate::logical_archive::classify_failure(&error),
                    (5, "logical-archive-integrity", false)
                );
                boundary += record.len();
            }
            assert!(
                verify_projection_rows(
                    &connection,
                    Cursor::new(&wire[..wire.len() - 1]),
                    &inspected
                )
                .is_err()
            );
            let mut trailing = wire;
            trailing.extend(codec::encode(&records[1])?);
            assert!(
                verify_projection_rows(&connection, Cursor::new(trailing), &inspected).is_err()
            );
        }
    }
    connection.close()?;
    Ok(())
}

#[test]
fn projection_cursor_authenticates_boundaries_once_and_refuses_incomplete_prefixes() -> Result<()> {
    for version in [codec::VERSION, codec::CHUNKED_VERSION] {
        let records = projection_records(Some(Cell::Text("20".into())), "PRIVATE-BODY");
        let (wire, inspected) = archive_wire(projection_header(version, 20), &records)?;
        let mut input = Cursor::new(&wire);
        let Some(Record::Header { header }) = codec::read_record(&mut input, 1)? else {
            bail!("fixture header missing");
        };
        let mut cursor = ProjectionCursor::new(input, header)?;
        let mut boundaries = 0;
        while let Some(record) = cursor.next()? {
            if matches!(record, Record::Table { .. } | Record::Completion { .. }) {
                let position = cursor.line;
                cursor.put_back(record.clone())?;
                assert_eq!(cursor.next()?, Some(record));
                assert_eq!(cursor.line, position);
                boundaries += 1;
            }
        }
        assert_eq!(boundaries, 4);
        assert_eq!(
            cursor.finish()?,
            (inspected.header.clone(), inspected.completion)
        );

        let header_end = wire.iter().position(|byte| *byte == b'\n').unwrap() + 1;
        let mut boundary = header_end;
        for record in wire[header_end..].split_inclusive(|byte| *byte == b'\n') {
            let mut input = Cursor::new(&wire[..boundary]);
            let Some(Record::Header { header }) = codec::read_record(&mut input, 1)? else {
                bail!("fixture header missing");
            };
            let mut cursor = ProjectionCursor::new(input, header)?;
            let error = loop {
                match cursor.next() {
                    Ok(Some(_)) => {}
                    Ok(None) => bail!("cursor accepted a prefix without completion"),
                    Err(error) => break error,
                }
            };
            assert_eq!(
                crate::logical_archive::classify_failure(&error),
                (5, "logical-archive-integrity", false)
            );
            assert!(!error.to_string().contains("PRIVATE-"));
            boundary += record.len();
        }
    }
    Ok(())
}

#[test]
fn projection_cursor_refuses_changed_rows_and_false_markers_after_inspection() -> Result<()> {
    let consume = |wire: Vec<u8>| -> Result<(Header, Completion)> {
        let mut input = Cursor::new(wire);
        let Some(Record::Header { header }) = codec::read_record(&mut input, 1)? else {
            bail!("fixture header missing");
        };
        let mut cursor = ProjectionCursor::new(input, header)?;
        while cursor.next()?.is_some() {}
        cursor.finish()
    };

    for version in [codec::VERSION, codec::CHUNKED_VERSION] {
        let records = projection_records(Some(Cell::Text("20".into())), "PRIVATE-BODY");
        let (wire, inspected) = archive_wire(projection_header(version, 20), &records)?;
        codec::verify(&mut Cursor::new(wire))?;
        for row in [1, 3] {
            let mut changed = records.clone();
            let Record::Row { values } = &mut changed[row] else {
                bail!("fixture row missing");
            };
            values[1] = Cell::Text("PRIVATE-CHANGED-AFTER-INSPECTION".into());
            let error = consume(unchecked_projection_wire(&inspected, &changed)?)
                .expect_err("compared and skipped rows both require the current digest");
            assert_eq!(
                crate::logical_archive::classify_failure(&error),
                (5, "logical-archive-integrity", false)
            );
            assert!(error.to_string().contains("digest mismatch"));
            assert!(!error.to_string().contains("PRIVATE-"));
        }
        for marker in [
            None,
            Some(Cell::Null),
            Some(Cell::Integer(20)),
            Some(Cell::Text("21".into())),
            Some(Cell::Text("PRIVATE-FALSE-VERSION".into())),
        ] {
            let records = projection_records(marker, "PRIVATE-BODY");
            let (wire, _) = archive_wire(projection_header(version, 20), &records)?;
            codec::verify(&mut Cursor::new(&wire))?;
            let error = consume(wire)
                .expect_err("a valid digest does not establish a truthful source schema marker");
            assert_eq!(
                crate::logical_archive::classify_failure(&error),
                (5, "logical-archive-integrity", false)
            );
            assert!(!error.to_string().contains("PRIVATE-"));
        }
    }
    Ok(())
}

#[test]
fn projection_preserves_a_reader_failure_after_the_valid_completion() -> Result<()> {
    struct FailedRead;
    impl Read for FailedRead {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "PRIVATE-READ-DETAIL",
            ))
        }
    }
    impl BufRead for FailedRead {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "PRIVATE-READ-DETAIL",
            ))
        }
        fn consume(&mut self, _: usize) {}
    }

    let records = projection_records(Some(Cell::Text("20".into())), "PRIVATE-BODY");
    let (wire, _) = archive_wire(projection_header(codec::CHUNKED_VERSION, 20), &records)?;
    let mut input = Cursor::new(wire).chain(FailedRead);
    let Some(Record::Header { header }) = codec::read_record(&mut input, 1)? else {
        bail!("fixture header missing");
    };
    let mut cursor = ProjectionCursor::new(input, header)?;
    let error = loop {
        match cursor.next() {
            Ok(Some(_)) => {}
            Ok(None) => bail!("completion hid a failed EOF read"),
            Err(error) => break error,
        }
    };
    assert_eq!(
        crate::logical_archive::classify_failure(&error),
        (14, "logical-archive-io", true)
    );
    assert_eq!(
        error.downcast_ref::<io::Error>().unwrap().kind(),
        io::ErrorKind::TimedOut
    );
    assert!(!error.to_string().contains("PRIVATE-"));
    Ok(())
}

#[test]
fn projection_rejects_false_or_missing_source_markers_even_with_a_valid_digest() -> Result<()> {
    let connection = projection_database("PRIVATE-BODY")?;
    for marker in [
        None,
        Some(Cell::Null),
        Some(Cell::Integer(20)),
        Some(Cell::Text("21".into())),
        Some(Cell::Text("PRIVATE-FALSE-VERSION".into())),
    ] {
        let records = projection_records(marker, "PRIVATE-BODY");
        let (wire, inspected) =
            archive_wire(projection_header(codec::CHUNKED_VERSION, 20), &records)?;
        codec::verify(&mut Cursor::new(&wire))?;
        let error = verify_projection_rows(&connection, Cursor::new(wire), &inspected)
            .expect_err("discarding an archived marker must not excuse a false source version");
        assert!(!error.to_string().contains("PRIVATE-"));
    }
    connection.close()?;
    Ok(())
}

#[test]
fn projection_database_read_failure_retains_its_safe_typed_cause() -> Result<()> {
    let connection = Connection::open(":memory:")?;
    let table = Table {
        name: "messages".into(),
        columns: vec!["id".into(), "content".into()],
        primary_key: vec![0],
    };
    let descriptor = Cursor::new(codec::encode(&Record::Table {
        table: table.clone(),
    })?);
    let mut cursor = ProjectionCursor::new(
        descriptor.chain(&b"PRIVATE-ARCHIVE-MUST-NOT-BE-READ"[..]),
        projection_header(codec::VERSION, 20),
    )?;
    assert!(matches!(cursor.next()?, Some(Record::Table { .. })));
    let error = compare_table_rows(&connection, &table, &mut cursor)
        .expect_err("a missing canonical table must fail database comparison");
    let message = error.to_string();
    assert!(message.contains("logical table messages before row 1, after 0 complete rows"));
    assert!(message.contains("missing table"));
    assert!(!message.contains("PRIVATE-ARCHIVE"));
    assert!(matches!(
        error.downcast_ref::<FrankenError>(),
        Some(FrankenError::NoSuchTable { .. })
    ));
    assert_eq!(
        cursor.line, 3,
        "database errors must not consume archive rows"
    );
    connection.close()?;
    Ok(())
}

#[test]
fn projection_cancellation_reports_the_completed_prefix_and_preserves_database() -> Result<()> {
    use coding_agent_search::franken_sync::CancellationHandle;
    use std::cell::Cell as Counter;

    struct CancelOnFirstRecord<'a, F> {
        remaining: &'a [u8],
        records: &'a Counter<u64>,
        cancel: Option<F>,
    }

    impl<F: FnOnce()> Read for CancelOnFirstRecord<'_, F> {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            let count = output.len().min(self.remaining.len());
            output[..count].copy_from_slice(&self.remaining[..count]);
            self.consume(count);
            Ok(count)
        }
    }

    impl<F: FnOnce()> BufRead for CancelOnFirstRecord<'_, F> {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            Ok(self.remaining)
        }

        fn consume(&mut self, count: usize) {
            for byte in &self.remaining[..count] {
                if *byte == b'\n' {
                    self.records.set(self.records.get() + 1);
                    if let Some(cancel) = self.cancel.take() {
                        cancel();
                    }
                }
            }
            self.remaining = &self.remaining[count..];
        }
    }

    let connection = Connection::open(":memory:")?;
    connection.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, content TEXT)")?;
    let table = Table {
        name: "messages".into(),
        columns: vec!["id".into(), "content".into()],
        primary_key: vec![0],
    };
    let mut encoded = Vec::new();
    connection.execute("BEGIN IMMEDIATE")?;
    let statement = connection.prepare("INSERT INTO messages VALUES (?, ?)")?;
    for id in 1..=2048 {
        statement.execute_with_params(&[
            SqliteValue::Integer(id),
            SqliteValue::Text("PRIVATE-PROJECTION-CONTENT".into()),
        ])?;
        encoded.extend(codec::encode(&Record::Row {
            values: vec![
                Cell::Integer(id),
                Cell::Text("PRIVATE-PROJECTION-CONTENT".into()),
            ],
        })?);
    }
    drop(statement);
    connection.execute("COMMIT")?;
    connection.execute("BEGIN")?;
    let cancellation = CancellationHandle::new();
    let cancel_handle = cancellation.clone();
    let records = Counter::new(0);
    let input = CancelOnFirstRecord {
        remaining: &encoded,
        records: &records,
        cancel: Some(move || cancel_handle.cancel()),
    };
    let descriptor = Cursor::new(codec::encode(&Record::Table {
        table: table.clone(),
    })?);
    let mut cursor = ProjectionCursor::new(
        descriptor.chain(input),
        projection_header(codec::VERSION, 20),
    )?;
    assert!(matches!(cursor.next()?, Some(Record::Table { .. })));
    let error = {
        let _binding = cancellation.enter();
        compare_table_rows(&connection, &table, &mut cursor)
            .expect_err("cancellation must stop a running database comparison")
    };
    assert!((1..2048).contains(&records.get()));
    let message = error.to_string();
    assert!(message.contains(&format!("after {} complete rows", records.get())));
    assert!(message.contains("FrankenSQLite Abort (code 4)"));
    assert!(!message.contains("PRIVATE-PROJECTION-CONTENT"));
    assert!(matches!(
        error.downcast_ref::<FrankenError>(),
        Some(FrankenError::Abort)
    ));
    connection.execute("ROLLBACK")?;
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM messages")?
            .get_typed::<i64>(0)?,
        2048
    );
    connection.close()?;
    Ok(())
}

fn database_files(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    ["", "-wal", "-shm", "-journal"]
        .into_iter()
        .filter_map(|suffix| {
            let mut name = path.as_os_str().to_os_string();
            name.push(suffix);
            let path = PathBuf::from(name);
            match fs::read(&path) {
                Ok(bytes) => Some((path, bytes)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => panic!("cannot inspect migrated fixture: {error}"),
            }
        })
        .collect()
}

/// A logical archive labelled `source_version`, exported from a current
/// database. Tables the target added after that version are left out,
/// as a real archive of that version lacks them, unless
/// `carry_added_tables` keeps them (a forged or drifted archive).
fn versioned_archive(
    root: &Path,
    source_version: u32,
    descriptor_drift: bool,
    carry_added_tables: bool,
) -> Result<PathBuf> {
    versioned_archive_with_format(
        root,
        source_version,
        descriptor_drift,
        carry_added_tables,
        codec::VERSION,
    )
}

fn versioned_archive_with_format(
    root: &Path,
    source_version: u32,
    descriptor_drift: bool,
    carry_added_tables: bool,
    format_version: u32,
) -> Result<PathBuf> {
    ensure!(
        target_version()? == REVIEWED_TARGET_VERSION,
        "reviewed migration tests require schema v{REVIEWED_TARGET_VERSION}; update policy before accepting a newer target"
    );
    let source = root.join(format!("source-{source_version}.db"));
    let storage = SqliteStorage::open(&source)?;
    storage.ensure_agent(&Agent {
        id: None,
        slug: "migration-fixture".into(),
        name: "Migration Fixture".into(),
        version: Some("v20-data".into()),
        kind: AgentKind::Cli,
    })?;
    if source_version >= 22 {
        storage.raw().execute(
            "INSERT INTO forgotten_sources(source_path, size_bytes, mtime_ms, forgotten_at_ms)
             VALUES('/fixture/retained-forget.jsonl', 12, 34, 56)",
        )?;
    }
    if source_version >= 23 {
        storage.raw().execute(
            "INSERT INTO forgotten_conversations
                (source_id, agent_slug, source_path, external_id, forgotten_at_ms)
             VALUES('local', 'opencode', '/fixture/store.db', 'forgotten-session', 78)",
        )?;
    }
    drop(storage);
    if format_version == codec::SEQUENCE_VERSION {
        let writer = Connection::open(export::path_text(&source)?)?;
        writer.execute_batch(
            "INSERT INTO embedding_jobs (id, db_path, model_id, status) VALUES
                (2, 'PRIVATE-RETAINED-JOB', 'fixture', 'completed'),
                (100, 'PRIVATE-DELETED-JOB', 'fixture', 'completed');
             DELETE FROM embedding_jobs WHERE id = 100;",
        )?;
        writer.close()?;
    }

    let connection = export::open_source(&source)?;
    let header = Header {
        format: codec::FORMAT.to_owned(),
        schema_version: format_version,
        archive_id: "reviewed-migration".to_owned(),
        exported_at_ms: 1,
        storage_schema_version: source_version.to_string(),
        record_types: codec::record_types(format_version),
        contains_private_data: true,
        omissions: vec!["derived_search_assets".to_owned()],
        sequences: if format_version == codec::SEQUENCE_VERSION {
            Some(export::sequences(
                &connection,
                &export::tables(&connection)?,
            )?)
        } else {
            None
        },
    };
    let mut validator = Validator::new(header.clone())?;
    let path = root.join(format!("schema-{source_version}.jsonl"));
    let mut output = File::create(&path)?;
    output.write_all(&codec::encode(&Record::Header {
        header: header.clone(),
    })?)?;

    for mut table in export::tables(&connection)? {
        if !carry_added_tables && table_added_after_source(&table.name, source_version) {
            continue;
        }
        if descriptor_drift && table.name == "agents" {
            let removed_offset = table
                .columns
                .iter()
                .position(|column| column == "version")
                .context("fixture agent version column missing")?;
            ensure!(!table.primary_key.contains(&removed_offset));
            table.columns.remove(removed_offset);
            for offset in &mut table.primary_key {
                if *offset > removed_offset {
                    *offset -= 1;
                }
            }
        }
        table.validate()?;
        output.write_all(&validator.push(&Record::Table {
            table: table.clone(),
        })?)?;
        let columns = table
            .columns
            .iter()
            .map(|column| export::quoted(column))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let order = table
            .primary_key
            .iter()
            .map(|&offset| {
                Ok(format!(
                    "{} COLLATE BINARY ASC",
                    export::quoted(&table.columns[offset])?
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let predicate = if table.name == MIGRATIONS_TABLE {
            format!(" WHERE version <= {source_version}")
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT {columns} FROM {}{predicate} ORDER BY {order}",
            export::quoted(&table.name)?
        );
        let mut failure = None;
        let streamed = connection.query_with_params_for_each(&sql, &[], |row| {
            let result = (|| -> Result<()> {
                let mut cells = export::cells(row.values())?;
                if table.name == META_TABLE {
                    let key = table
                        .columns
                        .iter()
                        .position(|column| column == "key")
                        .and_then(|offset| cells.get(offset));
                    if key.is_some_and(
                        |cell| matches!(cell, Cell::Text(value) if value == SCHEMA_VERSION_KEY),
                    ) && let Some(value_offset) =
                        table.columns.iter().position(|column| column == "value")
                    {
                        cells[value_offset] = Cell::Text(source_version.to_string());
                    }
                }
                output.write_all(&validator.push(&Record::Row { values: cells })?)?;
                Ok(())
            })();
            if let Err(error) = result {
                failure = Some(error);
                return Err(FrankenError::Internal(
                    "fixture archive writer aborted".into(),
                ));
            }
            Ok(())
        });
        if let Some(error) = failure {
            return Err(error);
        }
        streamed?;
    }
    let completion = validator.completion();
    output.write_all(&validator.push(&Record::Completion { completion })?)?;
    output.flush()?;
    validator.finish()?;
    connection.execute("ROLLBACK")?;
    connection.close_without_checkpoint()?;
    Ok(path)
}

#[test]
fn inspected_source_marker_must_match_before_a_migration_candidate_is_created() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = versioned_archive(root.path(), 20, false, false)?;
    let mut reader = BufReader::new(File::open(&input)?);
    let Some(Record::Header { header }) = codec::read_record(&mut reader, 1)? else {
        bail!("fixture header missing");
    };
    let mut records = Vec::new();
    let mut line = 2;
    let mut table = None;
    let mut marker_offset = None;
    while let Some(record) = codec::read_record(&mut reader, line)? {
        match &record {
            Record::Table { table: next } => table = Some(next.clone()),
            Record::Row { values }
                if table
                    .as_ref()
                    .is_some_and(|table| meta_schema_version_row(table, values)) =>
            {
                let value = table
                    .as_ref()
                    .and_then(|table| table.columns.iter().position(|name| name == "value"))
                    .context("fixture marker value column missing")?;
                marker_offset = Some((records.len(), value));
            }
            Record::Completion { .. } => break,
            Record::Row { .. } | Record::Header { .. } => {}
        }
        records.push(record);
        line += 1;
    }
    let (marker_row, marker_column) = marker_offset.context("fixture marker missing")?;
    for (case, replacement) in [
        None,
        Some(Cell::Null),
        Some(Cell::Integer(20)),
        Some(Cell::Text("21".into())),
        Some(Cell::Text("PRIVATE-FALSE-VERSION".into())),
    ]
    .into_iter()
    .enumerate()
    {
        let mut changed = records.clone();
        if let Some(replacement) = replacement {
            let Record::Row { values } = &mut changed[marker_row] else {
                bail!("fixture marker is not a row");
            };
            values[marker_column] = replacement;
        } else {
            changed.remove(marker_row);
        }
        let (wire, _) = archive_wire(header.clone(), &changed)?;
        codec::verify(&mut Cursor::new(&wire))?;
        let changed_input = root.path().join(format!("marker-case-{case}.jsonl"));
        fs::write(&changed_input, &wire)?;
        let destination = root.path().join(format!("refused-{case}.db"));
        let error = import_compatible(&changed_input, &destination, "reviewed-migration", false)
            .err()
            .context("a false source marker was accepted")?;
        assert!(
            error.to_string().contains("schema metadata disagree")
                || error
                    .to_string()
                    .contains("lacks the schema_version marker")
        );
        assert!(!error.to_string().contains("PRIVATE-"));
        assert!(!destination.exists());
        assert!(
            !root
                .path()
                .join(format!(".refused-{case}.db.logical-archive.lock"))
                .exists()
        );
        assert_eq!(fs::read(&changed_input)?, wire);
    }
    Ok(())
}

#[test]
fn reviewed_migrations_restore_rows_under_current_authority() -> Result<()> {
    for (source, mode) in [
        (20, "reviewed_v20_to_v23"),
        (21, "reviewed_v21_to_v23"),
        (22, "reviewed_v22_to_v23"),
    ] {
        let root = tempfile::tempdir()?;
        let input = versioned_archive(root.path(), source, false, false)?;
        let destination = root.path().join("restored.db");
        let outcome = import_compatible(&input, &destination, "reviewed-migration", false)?;
        let receipt = outcome.migration.context("migration receipt missing")?;
        assert_eq!(receipt.mode, mode);
        assert_eq!(receipt.from_storage_schema_version, source.to_string());
        assert_eq!(
            receipt.to_storage_schema_version,
            REVIEWED_TARGET_VERSION.to_string()
        );
        assert!(receipt.source_rows_verified);

        let storage = SqliteStorage::open_readonly(&destination)?;
        assert_eq!(
            u32::try_from(storage.schema_version()?)?,
            REVIEWED_TARGET_VERSION
        );
        let row = storage
            .raw()
            .query_row("SELECT slug, version FROM agents WHERE slug = 'migration-fixture'")?;
        assert_eq!(row.get_typed::<String>(0)?, "migration-fixture");
        assert_eq!(row.get_typed::<Option<String>>(1)?, Some("v20-data".into()));
        // A v22 source's existing tombstone is canonical data. The newly
        // introduced session table starts empty, as in an in-place upgrade.
        let forgotten = storage
            .raw()
            .query_row("SELECT COUNT(*) FROM forgotten_sources")?;
        assert_eq!(forgotten.get_typed::<i64>(0)?, i64::from(source >= 22));
        if source >= 22 {
            let stamp = storage.raw().query_row(
                "SELECT source_path, size_bytes, mtime_ms, forgotten_at_ms FROM forgotten_sources",
            )?;
            assert_eq!(
                stamp.get_typed::<String>(0)?,
                "/fixture/retained-forget.jsonl"
            );
            assert_eq!(stamp.get_typed::<i64>(1)?, 12);
            assert_eq!(stamp.get_typed::<i64>(2)?, 34);
            assert_eq!(stamp.get_typed::<i64>(3)?, 56);
        }
        assert_eq!(
            storage
                .raw()
                .query_row("SELECT COUNT(*) FROM forgotten_conversations")?
                .get_typed::<i64>(0)?,
            0
        );
    }
    Ok(())
}

#[test]
fn repeated_reviewed_migration_is_read_only_and_reports_unchanged() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = versioned_archive(root.path(), 21, false, false)?;
    let destination = root.path().join("restored.db");
    let created = import_compatible(&input, &destination, "reviewed-migration", false)?;
    assert!(created.created);
    let before = database_files(&destination);
    let repeated = import_compatible(&input, &destination, "reviewed-migration", true)?;
    assert!(!repeated.created);
    assert!(repeated.migration.is_some());
    assert_eq!(before, database_files(&destination));
    Ok(())
}

#[test]
fn exact_v23_restore_preserves_session_tombstones_and_retries_read_only() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = versioned_archive(root.path(), 23, false, false)?;
    let destination = root.path().join("restored.db");
    let outcome = import_compatible(&input, &destination, "reviewed-migration", false)?;
    assert!(outcome.migration.is_none());
    let storage = SqliteStorage::open_readonly(&destination)?;
    let tombstones = storage.forgotten_conversation_keys()?;
    assert_eq!(tombstones.len(), 1);
    let key = tombstones
        .iter()
        .next()
        .context("session tombstone missing")?;
    assert_eq!(key.source_id, "local");
    assert_eq!(key.agent_slug, "opencode");
    assert_eq!(key.source_path, "/fixture/store.db");
    assert_eq!(key.external_id, "forgotten-session");
    assert_eq!(
        storage
            .raw()
            .query_row("SELECT forgotten_at_ms FROM forgotten_conversations")?
            .get_typed::<i64>(0)?,
        78
    );
    drop(storage);
    let before = database_files(&destination);
    let repeated = import_compatible(&input, &destination, "reviewed-migration", true)?;
    assert!(!repeated.created);
    assert_eq!(before, database_files(&destination));
    Ok(())
}

#[test]
fn v22_retry_refuses_new_session_tombstones_and_preserves_them() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = versioned_archive(root.path(), 22, false, false)?;
    let destination = root.path().join("restored.db");
    import_compatible(&input, &destination, "reviewed-migration", false)?;
    let writer = Connection::open(export::path_text(&destination)?)?;
    writer.execute(
        "INSERT INTO forgotten_conversations
             (source_id, agent_slug, source_path, external_id, forgotten_at_ms)
         VALUES('local', 'devin', '/sessions.db/session', 'session', 1)",
    )?;
    writer.close()?;
    let before = database_files(&destination);
    assert!(import_compatible(&input, &destination, "reviewed-migration", true).is_err());
    assert_eq!(before, database_files(&destination));
    Ok(())
}

#[test]
fn reviewed_v3_migration_preserves_allocation_and_refuses_sequence_only_conflicts() -> Result<()> {
    for source_version in [20, 21, 22] {
        let root = tempfile::tempdir()?;
        let input = versioned_archive_with_format(
            root.path(),
            source_version,
            false,
            false,
            codec::SEQUENCE_VERSION,
        )?;
        let input_before = fs::read(&input)?;
        let destination = root.path().join("restored.db");
        let restored = import_compatible(&input, &destination, "reviewed-migration", false)?;
        assert!(restored.created);
        assert_eq!(restored.header.schema_version, codec::SEQUENCE_VERSION);
        assert_eq!(restored.completion, export::verify_file(&input)?.1);
        let before = database_files(&destination);
        let repeated = import_compatible(&input, &destination, "reviewed-migration", true)?;
        assert!(!repeated.created);
        assert_eq!(before, database_files(&destination));

        // No canonical row changed. A historical allocation change must
        // still invalidate the same authenticated migration projection.
        let writer = Connection::open(export::path_text(&destination)?)?;
        assert_eq!(
            writer
                .query_row("SELECT seq FROM sqlite_sequence WHERE name = 'embedding_jobs'")?
                .get_typed::<i64>(0)?,
            100
        );
        writer.execute("UPDATE sqlite_sequence SET seq = 200 WHERE name = 'embedding_jobs'")?;
        writer.close()?;
        let before_conflict = database_files(&destination);
        let error = import_compatible(&input, &destination, "reviewed-migration", true)
            .err()
            .context("sequence-only drift was accepted as an identical migrated archive")?;
        assert!(error.to_string().contains("allocation history differs"));
        assert!(!error.to_string().contains("PRIVATE-"));
        assert_eq!(before_conflict, database_files(&destination));
        assert_eq!(input_before, fs::read(&input)?);

        // Exercise the normal storage API on a separately restored image,
        // including opening it under current migration authority.
        let usable = root.path().join("usable.db");
        import_compatible(&input, &usable, "reviewed-migration", false)?;
        let storage = SqliteStorage::open(&usable)?;
        let next = storage.upsert_embedding_job("after-restore", "fixture", 1)?;
        assert_eq!(next, 101);
        storage.start_embedding_job(next)?;
        assert_eq!(
            storage
                .raw()
                .query_row("SELECT status FROM embedding_jobs WHERE id = 101")?
                .get_typed::<String>(0)?,
            "running"
        );
    }
    Ok(())
}

#[test]
fn reviewed_retry_rejects_sparse_current_history_even_when_projection_matches() -> Result<()> {
    for source in [20, 21, 22] {
        let root = tempfile::tempdir()?;
        let input = versioned_archive(root.path(), source, false, false)?;
        let destination = root.path().join("restored.db");
        import_compatible(&input, &destination, "reviewed-migration", false)?;
        let writer = Connection::open(export::path_text(&destination)?)?;
        writer.execute("DELETE FROM _schema_migrations WHERE version = 18")?;
        assert_eq!(
            writer
                .query_row("SELECT MAX(version) FROM _schema_migrations")?
                .get_typed::<i64>(0)?,
            i64::from(REVIEWED_TARGET_VERSION)
        );
        writer.close()?;
        let before = database_files(&destination);
        let input_before = fs::read(&input)?;

        // The authenticated projection intentionally ignores migration
        // history differences. It still matches this sparse target, so
        // the current-schema authority check must reject it separately.
        let mut file = crate::logical_archive::import::open_input(&input)?;
        let inspected = inspect(&mut file, "reviewed-migration")?;
        file.seek(SeekFrom::Start(0))?;
        let reader = export::open_source(&destination)?;
        verify_projection_rows(&reader, BufReader::new(file), &inspected)?;
        reader.execute("ROLLBACK")?;
        reader.close_without_checkpoint()?;

        let error = import_compatible(&input, &destination, "reviewed-migration", true)
            .err()
            .context("a matching projection must not authorize lower-version migration replay")?;
        assert!(error.to_string().contains("missing required migration v18"));
        assert_eq!(before, database_files(&destination));
        assert_eq!(input_before, fs::read(&input)?);
    }
    Ok(())
}

#[test]
fn changed_migrated_destination_is_a_conflict_not_an_overwrite() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = versioned_archive(root.path(), 20, false, false)?;
    let destination = root.path().join("restored.db");
    import_compatible(&input, &destination, "reviewed-migration", false)?;
    let writer = Connection::open(export::path_text(&destination)?)?;
    writer
        .execute("INSERT INTO meta (key, value) VALUES ('operator_note', 'keep migrated note')")?;
    writer.close()?;
    let before = database_files(&destination);
    assert!(import_compatible(&input, &destination, "reviewed-migration", true).is_err());
    assert_eq!(before, database_files(&destination));
    Ok(())
}

#[test]
fn a_row_in_an_added_table_makes_the_migrated_destination_a_conflict() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = versioned_archive(root.path(), 21, false, false)?;
    let destination = root.path().join("restored.db");
    import_compatible(&input, &destination, "reviewed-migration", false)?;
    let writer = Connection::open(export::path_text(&destination)?)?;
    writer.execute(
        "INSERT INTO forgotten_sources (source_path, size_bytes, mtime_ms, forgotten_at_ms) \
         VALUES ('/not/from/the/archive.jsonl', 1, 1, 1)",
    )?;
    writer.close()?;
    let before = database_files(&destination);
    assert!(import_compatible(&input, &destination, "reviewed-migration", true).is_err());
    assert_eq!(before, database_files(&destination));
    Ok(())
}

#[test]
fn reviewed_bridge_rejects_canonical_descriptor_drift() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = versioned_archive(root.path(), 20, true, false)?;
    let destination = root.path().join("restored.db");
    assert!(import_compatible(&input, &destination, "reviewed-migration", false).is_err());
    assert!(!destination.exists());
    Ok(())
}

#[test]
fn older_archive_carrying_a_table_added_later_is_refused() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = versioned_archive(root.path(), 21, false, true)?;
    let destination = root.path().join("restored.db");
    assert!(import_compatible(&input, &destination, "reviewed-migration", false).is_err());
    assert!(!destination.exists());
    Ok(())
}

#[test]
fn unreviewed_older_schema_is_refused_without_publication() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = versioned_archive(root.path(), 19, false, false)?;
    let destination = root.path().join("restored.db");
    assert!(import_compatible(&input, &destination, "reviewed-migration", false).is_err());
    assert!(!destination.exists());
    Ok(())
}

#[test]
fn newer_schema_is_refused_without_publication() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = versioned_archive(root.path(), REVIEWED_TARGET_VERSION + 1, false, false)?;
    let destination = root.path().join("restored.db");
    assert!(import_compatible(&input, &destination, "reviewed-migration", false).is_err());
    assert!(!destination.exists());
    Ok(())
}
