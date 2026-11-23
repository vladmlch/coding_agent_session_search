//! Reviewed cross-version restoration for CASS logical archives.
//!
//! Exact-schema restoration remains the default. Cross-version restoration is
//! deliberately allowlisted rather than inferred from "compatible-looking" SQL
//! shapes: data backfills can be semantically required even when columns appear
//! additive. The reviewed bridges are storage schema v20, v21 and v22 -> v23.
//! Repository migration fixtures establish what each step adds. v21 adds the
//! conversation-context index. v22 adds the `forgotten_sources` table, the
//! `cass forget` source tombstones; v23 adds durable per-session tombstones in
//! `forgotten_conversations`. Tables introduced after the source version start
//! empty, as in an in-place upgrade. Existing source tombstones from v22 must
//! be preserved. Every other canonical table layout is unchanged.
//!
//! The current initializer remains the sole executable schema authority. Archived
//! `_schema_migrations` rows and `meta.schema_version` are verified as input but
//! never replayed as current authority. Every other archived row is replayed into
//! private staging and streamed back from the persisted publication image before
//! no-clobber publication. Identical retries use the same read-only projection.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};

use anyhow::{Context, Result, anyhow, ensure};
use coding_agent_search::storage::sqlite::CURRENT_SCHEMA_VERSION;

use super::codec::{self, Cell, Completion, Header, Record, Table, Validator};

mod entrypoint;
mod projection;
mod replay;
#[cfg(test)]
mod tests;

pub use entrypoint::import_compatible;

const MAX_BATCH_RECORDS: usize = 128;
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const MAX_TRIGGER_BYTES: usize = 1024 * 1024;
const MIGRATIONS_TABLE: &str = "_schema_migrations";
const META_TABLE: &str = "meta";
const SCHEMA_VERSION_KEY: &str = "schema_version";
/// Storage schema versions an archive may be migrated from, into
/// [`REVIEWED_TARGET_VERSION`] only.
const REVIEWED_SOURCE_VERSIONS: [u32; 3] = [20, 21, 22];
const REVIEWED_TARGET_VERSION: u32 = 23;
/// The introducing version is part of the bridge: a table already present in
/// the source must be replayed and compared, never silently replaced by empty.
const ADDED_CANONICAL_TABLES: [(&str, u32); 2] =
    [("forgotten_sources", 22), ("forgotten_conversations", 23)];

fn table_added_after_source(name: &str, source_version: u32) -> bool {
    ADDED_CANONICAL_TABLES
        .iter()
        .any(|&(table, version)| table == name && version > source_version)
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SchemaMigrationReceipt {
    pub mode: &'static str,
    pub from_storage_schema_version: String,
    pub to_storage_schema_version: String,
    pub schema_authority: &'static str,
    pub source_rows_verified: bool,
}

pub struct MigrationOutcome {
    pub header: Header,
    pub completion: Completion,
    pub created: bool,
    pub migration: Option<SchemaMigrationReceipt>,
}

struct Inspected {
    header: Header,
    completion: Completion,
    tables: Vec<Table>,
}

/// The reviewed bridge replaces this marker with current initializer authority,
/// but the archived marker must first agree with the admitted source version.
/// Every accepting pass checks it, including rows not replayed into the target.
struct SourceSchemaMarker {
    version: String,
    columns: Option<(usize, usize)>,
    seen: bool,
}

impl SourceSchemaMarker {
    fn new(header: &Header) -> Self {
        Self {
            version: header.storage_schema_version.clone(),
            columns: None,
            seen: false,
        }
    }

    fn observe(&mut self, record: &Record) -> Result<()> {
        match record {
            Record::Table { table } => {
                self.columns = if table.name == META_TABLE {
                    Some((
                        table
                            .columns
                            .iter()
                            .position(|name| name == "key")
                            .context(
                                "logical archive metadata lacks its schema marker key column",
                            )?,
                        table
                            .columns
                            .iter()
                            .position(|name| name == "value")
                            .context(
                                "logical archive metadata lacks its schema marker value column",
                            )?,
                    ))
                } else {
                    None
                };
            }
            Record::Row { values } => {
                if let Some((key, value)) = self.columns
                    && matches!(values.get(key), Some(Cell::Text(name)) if name == SCHEMA_VERSION_KEY)
                {
                    ensure!(
                        matches!(values.get(value), Some(Cell::Text(version)) if version == &self.version),
                        "logical archive header and canonical schema metadata disagree"
                    );
                    self.seen = true;
                }
            }
            Record::Header { .. } | Record::Completion { .. } => {}
        }
        Ok(())
    }

    fn finish(&self) -> Result<()> {
        ensure!(
            self.seen,
            "logical archive lacks the schema_version marker required for reviewed migration"
        );
        Ok(())
    }
}

struct Input<R> {
    inner: R,
    record_bytes: usize,
}

impl<R: BufRead> Input<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            record_bytes: 0,
        }
    }

    fn record(&mut self, line: u64) -> Result<Option<Record>> {
        self.record_bytes = 0;
        codec::read_record(self, line)
    }
}

impl<R: BufRead> Read for Input<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(bytes)?;
        self.record_bytes = self.record_bytes.saturating_add(count);
        Ok(count)
    }
}

impl<R: BufRead> BufRead for Input<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, count: usize) {
        self.inner.consume(count);
        self.record_bytes = self.record_bytes.saturating_add(count);
    }
}

fn target_version() -> Result<u32> {
    u32::try_from(CURRENT_SCHEMA_VERSION).context("current canonical schema version is invalid")
}

fn parse_source_version(header: &Header) -> Result<u32> {
    header
        .storage_schema_version
        .parse::<u32>()
        .context("logical archive storage schema version is invalid")
}

fn require_reviewed_transition(source: u32, target: u32) -> Result<()> {
    ensure!(
        REVIEWED_SOURCE_VERSIONS.contains(&source) && target == REVIEWED_TARGET_VERSION,
        "no reviewed logical-archive migration exists from storage schema {source} to {target}; exact restore or a version-specific migration is required"
    );
    Ok(())
}

fn inspect(file: &mut File, expected_archive_id: &str) -> Result<Inspected> {
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(file);
    let Some(Record::Header { header }) = codec::read_record(&mut reader, 1)? else {
        return Err(super::integrity("logical archive must begin with a header"));
    };
    header.validate().map_err(super::integrity_unless_io)?;
    ensure!(
        header.archive_id == expected_archive_id,
        "logical archive identity does not match --archive-id"
    );
    let mut validator = Validator::new(header.clone()).map_err(super::integrity_unless_io)?;
    let mut schema_marker = SourceSchemaMarker::new(&header);
    let mut tables = Vec::new();
    let mut line = 2_u64;
    while let Some(record) = codec::read_record(&mut reader, line)? {
        if let Record::Table { table } = &record {
            tables.push(table.clone());
        }
        validator
            .validate(&record)
            .map_err(|error| super::integrity(format!("record {line}: {error}")))?;
        schema_marker
            .observe(&record)
            .map_err(|error| super::integrity(format!("record {line}: {error}")))?;
        line = line
            .checked_add(1)
            .ok_or_else(|| anyhow!("logical record position overflow"))?;
    }
    let (verified_header, completion) = validator.finish().map_err(super::integrity_unless_io)?;
    schema_marker.finish().map_err(super::integrity_unless_io)?;
    ensure!(
        verified_header == header,
        "logical archive header changed during verification"
    );
    Ok(Inspected {
        header,
        completion,
        tables,
    })
}
