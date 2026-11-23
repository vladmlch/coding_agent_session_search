use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail, ensure};
use coding_agent_search::franken_sync::{Connection, FileIdentity, FrankenError};

use super::replay::{
    meta_schema_version_row, require_reviewed_table_layout, verify_current_schema_authority,
};
use super::{
    Inspected, MIGRATIONS_TABLE, SourceSchemaMarker, parse_source_version, target_version,
};
use crate::logical_archive::codec::{self, Cell, Completion, Header, Record, Table, Validator};
use crate::logical_archive::{database_failure, export, import, integrity, integrity_unless_io};

fn sidecars(path: &Path) -> [PathBuf; 3] {
    ["-wal", "-shm", "-journal"].map(|suffix| {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        PathBuf::from(name)
    })
}

fn require_absent(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        _ => bail!(
            "restore destination or SQLite sidecar already exists or cannot be inspected; nothing was replaced"
        ),
    }
}

pub(super) fn require_new_destination(path: &Path) -> Result<()> {
    require_absent(path)?;
    for sidecar in sidecars(path) {
        require_absent(&sidecar)?;
    }
    Ok(())
}

fn require_candidate_without_sidecars(candidate: &Path) -> Result<()> {
    for sidecar in sidecars(candidate) {
        require_absent(&sidecar)
            .context("restore publication image still depends on SQLite sidecars")?;
    }
    Ok(())
}

pub(super) fn materialize_candidate(connection: &Connection, candidate: &Path) -> Result<()> {
    require_new_destination(candidate)?;
    // Only the private, committed replay is checkpointed. The engine's page
    // backup avoids VACUUM INTO's whole-database hydration in the pinned engine.
    connection.backup_exact_to(candidate).map_err(|error| {
        database_failure(
            "cannot materialize a self-contained migrated publication image",
            error,
        )
    })?;
    require_candidate_without_sidecars(candidate)
}

pub(super) fn sync_candidate(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "migrated publication candidate must be a regular, non-symlink file"
    );
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options
        .open(path)?
        .sync_all()
        .context("cannot sync the verified migrated publication candidate")
}

pub(super) fn identity(path: &Path) -> Result<FileIdentity> {
    let file = import::open_input(path)?;
    FileIdentity::from_file(&file)?
        .ok_or_else(|| anyhow!("cannot prove the existing destination's file identity"))
}

fn require_same_file(connection: &Connection, path: &Path) -> Result<()> {
    let actual = identity(path)?;
    ensure!(
        connection.file_identity().map_err(|error| {
            database_failure(
                "cannot recheck the reviewed migration destination identity",
                error,
            )
        })? == Some(actual),
        "restore destination changed during reviewed migration comparison; retry without replacing it"
    );
    Ok(())
}

pub(super) struct ProjectionCursor<R> {
    reader: R,
    pub(super) line: u64,
    pending: Option<Record>,
    validator: Validator,
    schema_marker: SourceSchemaMarker,
    completed: bool,
    eof: bool,
}

impl<R: BufRead> ProjectionCursor<R> {
    pub(super) fn new(reader: R, header: Header) -> Result<Self> {
        let schema_marker = SourceSchemaMarker::new(&header);
        Ok(Self {
            reader,
            line: 2,
            pending: None,
            validator: Validator::new(header).map_err(integrity_unless_io)?,
            schema_marker,
            completed: false,
            eof: false,
        })
    }

    pub(super) fn next(&mut self) -> Result<Option<Record>> {
        if self.pending.is_some() {
            // A table boundary was already validated when first decoded.
            return Ok(self.pending.take());
        }
        if self.eof {
            return Ok(None);
        }
        let line = self.line;
        let record = codec::read_record(&mut self.reader, line)?;
        if let Some(record) = &record {
            self.validator
                .validate(record)
                .map_err(|error| integrity(format!("record {line}: {error}")))?;
            self.schema_marker
                .observe(record)
                .map_err(|error| integrity(format!("record {line}: {error}")))?;
            self.completed = matches!(record, Record::Completion { .. });
            self.line = self
                .line
                .checked_add(1)
                .ok_or_else(|| anyhow!("logical record position overflow"))?;
        } else {
            self.eof = true;
            if !self.completed {
                return Err(integrity(format!(
                    "logical archive ended before completion at record {line}"
                )));
            }
        }
        Ok(record)
    }

    pub(super) fn put_back(&mut self, record: Record) -> Result<()> {
        ensure!(
            self.pending.is_none(),
            "logical projection cursor already has a boundary"
        );
        self.pending = Some(record);
        Ok(())
    }

    pub(super) fn finish(self) -> Result<(Header, Completion)> {
        ensure!(
            self.eof && self.pending.is_none(),
            "logical projection must reach EOF before claiming verified source rows"
        );
        let verified = self.validator.finish().map_err(integrity_unless_io)?;
        self.schema_marker.finish().map_err(integrity_unless_io)?;
        Ok(verified)
    }
}

fn migrated_row_equal(table: &Table, archived: &[Cell], actual: &[Cell]) -> Result<bool> {
    if !meta_schema_version_row(table, archived) {
        return Ok(archived == actual);
    }
    ensure!(
        archived.len() == actual.len(),
        "migrated schema marker row changed shape"
    );
    let value_offset = table.columns.iter().position(|column| column == "value");
    let Some(value_offset) = value_offset else {
        return Ok(archived == actual);
    };
    for offset in 0..archived.len() {
        if offset == value_offset {
            ensure!(
                actual[offset] == Cell::Text(target_version()?.to_string()),
                "persisted schema marker is not current"
            );
        } else if archived[offset] != actual[offset] {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn compare_table_rows<R: BufRead>(
    connection: &Connection,
    table: &Table,
    cursor: &mut ProjectionCursor<R>,
) -> Result<()> {
    let sql = export::ordered_scan_sql(connection, table)?;

    let mut failure = None;
    let mut rows_compared = 0_u64;
    let streamed = connection.query_with_params_for_each(&sql, &[], |row| {
        let result = (|| -> Result<()> {
            let next_row = rows_compared
                .checked_add(1)
                .ok_or_else(|| anyhow!("logical table row position overflow"))?;
            let Some(record) = cursor.next()? else {
                bail!(
                    "persisted migrated table {} contains an extra row",
                    table.name
                );
            };
            let Record::Row { values: archived } = record else {
                bail!(
                    "persisted migrated table {} contains more rows than the archive",
                    table.name
                );
            };
            let actual = export::cells(row.values())?;
            ensure!(
                migrated_row_equal(table, &archived, &actual)?,
                "persisted migrated row differs from verified archive table {}",
                table.name
            );
            rows_compared = next_row;
            Ok(())
        })();
        if let Err(error) = result {
            failure = Some(error);
            return Err(FrankenError::Internal(
                "reviewed archive projection comparison aborted".to_owned(),
            ));
        }
        Ok(())
    });
    if let Some(error) = failure {
        return Err(error);
    }
    streamed.map_err(|error| {
        database_failure(
            format!(
                "cannot compare persisted migrated logical table {} before row {}, after {rows_compared} complete rows",
                table.name,
                rows_compared.saturating_add(1)
            ),
            error,
        )
    })?;

    if let Some(record) = cursor.next()? {
        if matches!(record, Record::Row { .. }) {
            bail!(
                "verified archive table {} contains a row missing from the migrated database",
                table.name
            );
        }
        cursor.put_back(record)?;
    }
    Ok(())
}

fn skip_archived_rows<R: BufRead>(cursor: &mut ProjectionCursor<R>) -> Result<()> {
    while let Some(record) = cursor.next()? {
        if matches!(record, Record::Row { .. }) {
            continue;
        }
        cursor.put_back(record)?;
        return Ok(());
    }
    bail!("logical archive ended before completion")
}

/// Authenticate the same input pass that is compared with persisted rows.
/// Even skipped migration-history rows advance the validator exactly once.
pub(super) fn verify_projection_rows(
    connection: &Connection,
    mut input: impl BufRead,
    inspected: &Inspected,
) -> Result<()> {
    let Some(Record::Header { header }) = codec::read_record(&mut input, 1)? else {
        return Err(integrity("logical archive must begin with a header"));
    };
    ensure!(header == inspected.header, "logical archive header changed");
    let mut cursor = ProjectionCursor::new(input, header)?;

    for expected in &inspected.tables {
        let Some(Record::Table { table }) = cursor.next()? else {
            bail!("logical archive table set changed during persisted verification");
        };
        ensure!(
            table == *expected,
            "logical archive table descriptor changed during persisted verification"
        );
        if table.name == MIGRATIONS_TABLE {
            skip_archived_rows(&mut cursor)?;
        } else {
            compare_table_rows(connection, &table, &mut cursor)?;
        }
    }

    let Some(Record::Completion { completion }) = cursor.next()? else {
        bail!("logical archive completion moved during persisted verification");
    };
    ensure!(
        completion == inspected.completion,
        "logical archive completion changed during persisted verification"
    );
    ensure!(
        cursor.next()?.is_none(),
        "records follow the archive completion during persisted verification"
    );
    ensure!(
        cursor.finish()? == (inspected.header.clone(), inspected.completion.clone()),
        "logical archive changed during persisted verification"
    );
    import::verify_sequences(connection, &inspected.header, &inspected.tables)?;
    Ok(())
}

pub(super) fn verify_persisted_projection(
    file: &mut File,
    candidate: &Path,
    inspected: &Inspected,
    expected_identity: Option<FileIdentity>,
    require_sidecar_free: bool,
) -> Result<()> {
    let reader = export::open_source(candidate)?;
    if let Some(expected_identity) = expected_identity {
        ensure!(
            reader.file_identity().map_err(|error| {
                database_failure(
                    "cannot read the reviewed migration destination identity",
                    error,
                )
            })? == Some(expected_identity),
            "restore destination changed before reviewed migration comparison; nothing was replaced"
        );
    }
    import::verify_database(&reader)?;
    verify_current_schema_authority(&reader)?;
    require_reviewed_table_layout(
        &reader,
        &inspected.tables,
        parse_source_version(&inspected.header)?,
    )?;

    file.seek(SeekFrom::Start(0))?;
    verify_projection_rows(&reader, BufReader::new(&mut *file), inspected)?;

    if expected_identity.is_some() {
        require_same_file(&reader, candidate)?;
    }
    reader.execute("ROLLBACK").map_err(|error| {
        database_failure(
            "cannot release the reviewed migration comparison snapshot",
            error,
        )
    })?;
    reader.close_without_checkpoint().map_err(|error| {
        database_failure(
            "cannot close the reviewed migration comparison reader",
            error,
        )
    })?;
    if require_sidecar_free {
        require_candidate_without_sidecars(candidate)?;
    }
    Ok(())
}
