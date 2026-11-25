use std::fmt;
use std::path::{Path, PathBuf};

use rusqlite::types::{ToSql, ToSqlOutput, ValueRef};

// ---------------------------------------------------------------------------
// SqliteValue
// ---------------------------------------------------------------------------

/// SQLite value types exposed by the synchronous facade.
#[derive(Clone, Debug, PartialEq)]
pub enum SqliteValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl SqliteValue {
    /// SQLite storage-class name for the value (`null`, `integer`, `real`,
    /// `text`, `blob`).
    pub const fn typeof_str(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Integer(_) => "integer",
            Self::Float(_) => "real",
            Self::Text(_) => "text",
            Self::Blob(_) => "blob",
        }
    }

    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

impl ToSql for SqliteValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Float(value) => ValueRef::Real(*value),
            Self::Text(value) => ValueRef::Text(value.as_bytes()),
            Self::Blob(value) => ValueRef::Blob(value.as_slice()),
        }))
    }
}

macro_rules! impl_sqlite_value_from {
    ($($ty:ty => $ctor:expr_2021),+ $(,)?) => {
        $(
            impl From<$ty> for SqliteValue {
                fn from(value: $ty) -> Self {
                    ($ctor)(value)
                }
            }
        )+
    };
}

impl_sqlite_value_from! {
    i64 => SqliteValue::Integer,
    i32 => |v: i32| SqliteValue::Integer(i64::from(v)),
    i16 => |v: i16| SqliteValue::Integer(i64::from(v)),
    i8 => |v: i8| SqliteValue::Integer(i64::from(v)),
    u64 => |v: u64| SqliteValue::Integer(v.min(i64::MAX as u64) as i64),
    u32 => |v: u32| SqliteValue::Integer(i64::from(v)),
    u16 => |v: u16| SqliteValue::Integer(i64::from(v)),
    u8 => |v: u8| SqliteValue::Integer(i64::from(v)),
    usize => |v: usize| SqliteValue::Integer((v.min(i64::MAX as usize)) as i64),
    isize => |v: isize| SqliteValue::Integer(v as i64),
    f64 => SqliteValue::Float,
    f32 => |v: f32| SqliteValue::Float(f64::from(v)),
    bool => |v: bool| SqliteValue::Integer(i64::from(v)),
    String => SqliteValue::Text,
    &String => |v: &String| SqliteValue::Text(v.clone()),
    &str => |v: &str| SqliteValue::Text(v.to_owned()),
    Box<str> => |v: Box<str>| SqliteValue::Text(v.into_string()),
    Vec<u8> => SqliteValue::Blob,
    &Vec<u8> => |v: &Vec<u8>| SqliteValue::Blob(v.clone()),
    &[u8] => |v: &[u8]| SqliteValue::Blob(v.to_vec()),
    Box<[u8]> => |v: Box<[u8]>| SqliteValue::Blob(v.into_vec()),
}

impl From<std::borrow::Cow<'_, str>> for SqliteValue {
    fn from(value: std::borrow::Cow<'_, str>) -> Self {
        Self::Text(value.into_owned())
    }
}

impl<T: Into<SqliteValue>> From<Option<T>> for SqliteValue {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(inner) => inner.into(),
            None => Self::Null,
        }
    }
}

// ---------------------------------------------------------------------------
// Row (materialized)
// ---------------------------------------------------------------------------

/// A single materialized result row. Streaming paths hand out
/// `&rusqlite::Row<'_>` instead; this wrapper is the owned container kept for
/// compatibility with the old facade.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Row {
    values: Vec<SqliteValue>,
}

impl Row {
    pub fn new(values: Vec<SqliteValue>) -> Self {
        Self { values }
    }

    /// Returns the value at `index`, or `None` when out of range.
    pub fn get(&self, index: usize) -> Option<&SqliteValue> {
        self.values.get(index)
    }

    /// Borrowed slice view over all column values.
    pub fn values(&self) -> &[SqliteValue] {
        &self.values
    }

    /// Number of column values in this row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Alias for `len()` matching sqlite's column count wording.
    pub fn column_count(&self) -> usize {
        self.values.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &SqliteValue> {
        self.values.iter()
    }
}

impl From<Vec<SqliteValue>> for Row {
    fn from(values: Vec<SqliteValue>) -> Self {
        Self::new(values)
    }
}

impl std::ops::Index<usize> for Row {
    type Output = SqliteValue;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = &'a SqliteValue;
    type IntoIter = std::slice::Iter<'a, SqliteValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

impl IntoIterator for Row {
    type Item = SqliteValue;
    type IntoIter = std::vec::IntoIter<SqliteValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

pub(super) fn materialize_row(row: &rusqlite::Row<'_>, column_count: usize) -> Result<Row, FrankenError> {
    let mut values = Vec::with_capacity(column_count);
    for index in 0..column_count {
        let value = row
            .get_ref(index)
            .map_err(|error| FrankenError::from_rusqlite(error, None))?;
        values.push(sqlite_value_from_ref(value));
    }
    Ok(Row { values })
}

pub(super) fn sqlite_value_from_ref(value: ValueRef<'_>) -> SqliteValue {
    match value {
        ValueRef::Null => SqliteValue::Null,
        ValueRef::Integer(v) => SqliteValue::Integer(v),
        ValueRef::Real(v) => SqliteValue::Float(v),
        ValueRef::Text(v) => SqliteValue::Text(String::from_utf8_lossy(v).into_owned()),
        ValueRef::Blob(v) => SqliteValue::Blob(v.to_vec()),
    }
}

// ---------------------------------------------------------------------------
// ErrorCode + FrankenError
// ---------------------------------------------------------------------------

/// SQLite primary result-code names, mirroring the `SQLITE_*` extended result
/// code taxonomy so `{:?}` diagnostics keep their historical spelling
/// (`FrankenSQLite Abort (code 4)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
#[allow(dead_code)]
pub enum ErrorCode {
    Ok = 0,
    Error = 1,
    Internal = 2,
    Perm = 3,
    Abort = 4,
    Busy = 5,
    Locked = 6,
    NoMem = 7,
    ReadOnly = 8,
    Interrupt = 9,
    IoErr = 10,
    Corrupt = 11,
    NotFound = 12,
    Full = 13,
    CantOpen = 14,
    Protocol = 15,
    Empty = 16,
    Schema = 17,
    TooBig = 18,
    Constraint = 19,
    Mismatch = 20,
    Misuse = 21,
    NoLfs = 22,
    Auth = 23,
    Format = 24,
    Range = 25,
    NotADb = 26,
    Row = 100,
    Done = 101,
}

/// Error type used by the synchronous facade.
///
/// Variant coverage intentionally mirrors the historical FrankenSQLite error
/// surface so every downstream match site continues to compile. rusqlite
/// errors are mapped onto these variants by `FrankenError::from_rusqlite`.
#[derive(Debug)]
pub enum FrankenError {
    Internal(String),
    /// Operation cancelled (via `CancellationHandle`).
    Abort,
    /// Statement interrupted by `sqlite3_interrupt` (deadline or external
    /// interrupt handle) that was NOT triggered by the thread-local
    /// cancellation scope.
    Interrupted,
    Busy,
    /// Database needs recovery (e.g. strict read-only open refusing to
    /// rebuild the WAL index).
    BusyRecovery,
    /// Snapshot isolation conflict (SQLITE_BUSY_SNAPSHOT).
    BusySnapshot {
        conflicting_pages: String,
    },
    /// Database-level lock held (SQLITE_LOCKED).
    DatabaseLocked {
        path: PathBuf,
    },
    /// File-locking protocol failure.
    LockFailed {
        detail: String,
    },
    /// Write/write conflict detected on a snapshot write.
    WriteConflict {
        page: u64,
        holder: u64,
    },
    /// MVCC serialization failure.
    SerializationFailure {
        page: u64,
    },
    /// Snapshot aged out.
    SnapshotTooOld {
        txn_id: u64,
    },
    /// Attempted to write to a read-only database (SQLITE_READONLY).
    ReadOnly,
    /// I/O error (SQLITE_IOERR and host errors).
    Io(std::io::Error),
    /// I/O error reading a page.
    IoRead {
        page: u64,
    },
    /// I/O error writing a page.
    IoWrite {
        page: u64,
    },
    /// Short read while validating a page.
    ShortRead {
        expected: u64,
        actual: u64,
    },
    /// WAL checkpoint failed.
    CheckpointFailed {
        detail: String,
    },
    /// Database corruption detected (SQLITE_CORRUPT).
    DatabaseCorrupt {
        detail: String,
    },
    /// WAL file corruption.
    WalCorrupt {
        detail: String,
    },
    /// File is not a database (SQLITE_NOTADB).
    NotADatabase {
        path: PathBuf,
    },
    /// Cannot open database file (SQLITE_CANTOPEN).
    CannotOpen {
        path: PathBuf,
    },
    /// Table not found ("no such table: ...").
    NoSuchTable {
        name: String,
    },
    /// Column not found ("no such column: ...").
    NoSuchColumn {
        name: String,
    },
    /// Expected exactly one row, got none.
    QueryReturnedNoRows,
    /// Expected exactly one row, got more.
    QueryReturnedMultipleRows,
    /// Unique constraint violation (SQLITE_CONSTRAINT_UNIQUE/PRIMARYKEY/
    /// FOREIGNKEY). `columns` carries SQLite's diagnostic text.
    UniqueViolation {
        columns: String,
    },
    /// Database or disk is full (SQLITE_FULL).
    DatabaseFull,
    /// Type mismatch (SQLITE_MISMATCH / conversion failure).
    DatatypeMismatch,
    /// Out of memory (SQLITE_NOMEM). Renders as the literal "out of memory".
    OutOfMemory,
    /// Concurrency primitive unavailable in this build.
    ConcurrentUnavailable,
    /// Multi-process contract violation — maps to `ErrorCode::Busy`.
    MultiProcessContractViolation {
        detail: String,
    },
}

impl fmt::Display for FrankenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Internal(detail) => write!(f, "internal error: {detail}"),
            Self::Abort => write!(f, "operation aborted"),
            Self::Interrupted => write!(f, "interrupted"),
            Self::Busy => write!(f, "database is busy"),
            Self::BusyRecovery => write!(f, "database needs recovery"),
            Self::BusySnapshot { conflicting_pages } => write!(
                f,
                "database is busy (snapshot conflict: {conflicting_pages})"
            ),
            Self::DatabaseLocked { path } => {
                write!(f, "database is locked: {}", path.display())
            }
            Self::LockFailed { detail } => write!(f, "database lock failed: {detail}"),
            Self::WriteConflict { page, holder } => {
                write!(f, "write conflict on page {page} (holder {holder})")
            }
            Self::SerializationFailure { page } => {
                write!(f, "serialization failure on page {page}")
            }
            Self::SnapshotTooOld { txn_id } => {
                write!(f, "snapshot too old for transaction {txn_id}")
            }
            Self::ReadOnly => write!(f, "attempt to write a readonly database"),
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::IoRead { page } => write!(f, "I/O error reading page {page}"),
            Self::IoWrite { page } => write!(f, "I/O error writing page {page}"),
            Self::ShortRead { expected, actual } => {
                write!(f, "short read: expected {expected} bytes, got {actual}")
            }
            Self::CheckpointFailed { detail } => {
                write!(f, "checkpoint failed: {detail}")
            }
            Self::DatabaseCorrupt { detail } => {
                write!(f, "database corrupt: {detail}")
            }
            Self::WalCorrupt { detail } => write!(f, "WAL corrupt: {detail}"),
            Self::NotADatabase { path } => {
                write!(f, "not a database: {}", path.display())
            }
            Self::CannotOpen { path } => {
                write!(f, "cannot open: {}", path.display())
            }
            Self::NoSuchTable { name } => write!(f, "no such table: {name}"),
            Self::NoSuchColumn { name } => write!(f, "no such column: {name}"),
            Self::QueryReturnedNoRows => write!(f, "query returned no rows"),
            Self::QueryReturnedMultipleRows => {
                write!(f, "query returned more than one row")
            }
            Self::UniqueViolation { columns } => {
                write!(f, "UNIQUE constraint failed: {columns}")
            }
            Self::DatabaseFull => write!(f, "database or disk is full"),
            Self::DatatypeMismatch => write!(f, "datatype mismatch"),
            Self::OutOfMemory => write!(f, "out of memory"),
            Self::ConcurrentUnavailable => {
                write!(f, "concurrent primitives unavailable")
            }
            Self::MultiProcessContractViolation { detail } => {
                write!(f, "multi-process contract violation: {detail}")
            }
        }
    }
}

impl std::error::Error for FrankenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for FrankenError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<rusqlite::Error> for FrankenError {
    fn from(error: rusqlite::Error) -> Self {
        Self::from_rusqlite(error, None)
    }
}

/// Re-wrap a `FrankenError` as `rusqlite::Error` for callers whose `?` lands
/// in `rusqlite::Error`-converting error types (FAD `SqliteError::Sqlite`
/// via `#[from]`). The primary/extended code is preserved through
/// `ffi::Error::new(extended_error_code)` and the rendered detail through
/// the message channel.
pub(super) fn franken_error_to_rusqlite(error: FrankenError) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(error.extended_error_code()),
        Some(error.to_string()),
    )
}

impl FrankenError {
    /// The primary SQLite error code as a friendly enum (historical spelling
    /// preserved for `{:?}` diagnostics: `Abort`, `Busy`, `CantOpen`, ...).
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::Abort => ErrorCode::Abort,
            Self::Busy
            | Self::BusyRecovery
            | Self::BusySnapshot { .. }
            | Self::WriteConflict { .. }
            | Self::SerializationFailure { .. }
            | Self::SnapshotTooOld { .. }
            | Self::MultiProcessContractViolation { .. } => ErrorCode::Busy,
            Self::DatabaseLocked { .. } | Self::LockFailed { .. } => ErrorCode::Locked,
            Self::ReadOnly => ErrorCode::ReadOnly,
            Self::Interrupted => ErrorCode::Interrupt,
            Self::Io(_)
            | Self::IoRead { .. }
            | Self::IoWrite { .. }
            | Self::ShortRead { .. }
            | Self::CheckpointFailed { .. } => ErrorCode::IoErr,
            Self::DatabaseCorrupt { .. } | Self::WalCorrupt { .. } => ErrorCode::Corrupt,
            Self::NotADatabase { .. } => ErrorCode::NotADb,
            Self::CannotOpen { .. } => ErrorCode::CantOpen,
            Self::DatabaseFull => ErrorCode::Full,
            Self::OutOfMemory => ErrorCode::NoMem,
            Self::UniqueViolation { .. } => ErrorCode::Constraint,
            Self::DatatypeMismatch => ErrorCode::Mismatch,
            Self::Internal(_) => ErrorCode::Internal,
            Self::NoSuchTable { .. }
            | Self::NoSuchColumn { .. }
            | Self::QueryReturnedNoRows
            | Self::QueryReturnedMultipleRows
            | Self::ConcurrentUnavailable => ErrorCode::Error,
        }
    }

    /// Numeric extended result code (`sqlite3_extended_errcode` analogue).
    pub fn extended_error_code(&self) -> i32 {
        match self {
            Self::Abort => 4,                 // SQLITE_ABORT
            Self::Busy => 5,                  // SQLITE_BUSY
            Self::BusyRecovery => 261,        // SQLITE_BUSY_RECOVERY
            Self::BusySnapshot { .. } => 517, // SQLITE_BUSY_SNAPSHOT
            Self::DatabaseLocked { .. } | Self::LockFailed { .. } => 6,
            Self::WriteConflict { .. }
            | Self::SerializationFailure { .. }
            | Self::SnapshotTooOld { .. } => 517, // SQLITE_BUSY_SNAPSHOT
            Self::ConcurrentUnavailable => 1,
            Self::ReadOnly => 8,
            Self::Interrupted => 9,
            Self::Io(_)
            | Self::IoRead { .. }
            | Self::IoWrite { .. }
            | Self::ShortRead { .. }
            | Self::CheckpointFailed { .. } => 10,
            Self::DatabaseCorrupt { .. } | Self::WalCorrupt { .. } => 11,
            Self::DatabaseFull => 13,
            Self::CannotOpen { .. } => 14,
            Self::NoSuchTable { .. }
            | Self::NoSuchColumn { .. }
            | Self::QueryReturnedNoRows
            | Self::QueryReturnedMultipleRows => 1,
            Self::UniqueViolation { .. } => 2067, // SQLITE_CONSTRAINT_UNIQUE
            Self::DatatypeMismatch => 20,
            Self::OutOfMemory => 7,
            Self::NotADatabase { .. } => 26,
            Self::Internal(_) => 2,
            Self::MultiProcessContractViolation { .. } => 5,
        }
    }

    /// Map a rusqlite error onto the facade error surface. `path` fills in the
    /// path-carrying variants (`DatabaseLocked`, `CannotOpen`,
    /// `NotADatabase`).
    pub(super) fn from_rusqlite(error: rusqlite::Error, path: Option<&Path>) -> Self {
        use rusqlite::Error as R;
        let path_buf = path.map(Path::to_path_buf).unwrap_or_default();
        match error {
            R::SqliteFailure(ffi_error, msg) => Self::from_ffi_error(&ffi_error, msg, path_buf),
            R::SqliteSingleThreadedMode => Self::LockFailed {
                detail: "sqlite3 single-threaded mode".to_string(),
            },
            R::QueryReturnedNoRows => Self::QueryReturnedNoRows,
            R::QueryReturnedMoreThanOneRow => Self::QueryReturnedMultipleRows,
            R::InvalidColumnIndex(index) => Self::NoSuchColumn {
                name: format!("column index {index}"),
            },
            R::InvalidColumnName(name) => Self::NoSuchColumn { name },
            R::InvalidColumnType(_, _, _) => Self::DatatypeMismatch,
            R::InvalidParameterName(name) => {
                Self::Internal(format!("invalid parameter name: {name}"))
            }
            R::InvalidParameterCount(expected, actual) => Self::Internal(format!(
                "invalid parameter count: expected {expected}, got {actual}"
            )),
            R::InvalidPath(p) => Self::CannotOpen { path: p },
            R::ExecuteReturnedResults => Self::Internal("execute returned rows".to_string()),
            R::ToSqlConversionFailure(error) => Self::Internal(error.to_string()),
            R::FromSqlConversionFailure(_, _, error) => {
                let _ = error;
                Self::DatatypeMismatch
            }
            R::IntegralValueOutOfRange(_, _) => Self::DatatypeMismatch,
            R::Utf8Error(..) => Self::DatatypeMismatch,
            R::NulError(error) => Self::Internal(error.to_string()),
            R::InvalidQuery => Self::Internal("invalid query".to_string()),
            R::MultipleStatement => Self::Internal("multiple statements".to_string()),
            R::SqlInputError {
                error: ffi_error,
                msg,
                ..
            } => Self::from_ffi_error(&ffi_error, Some(msg), path_buf),
            R::UnwindingPanic => Self::Internal("unwinding panic".to_string()),
            R::StatementChangedRows(count) => {
                Self::Internal(format!("statement changed {count} rows"))
            }
            other => Self::Internal(other.to_string()),
        }
    }

    /// Map a `rusqlite::ffi::Error` (primary + extended result codes) onto the
    /// facade surface.
    fn from_ffi_error(
        error: &rusqlite::ffi::Error,
        message: Option<String>,
        path: PathBuf,
    ) -> Self {
        use rusqlite::ffi::ErrorCode as C;
        let detail = message.unwrap_or_else(|| error.to_string());
        match error.code {
            C::InternalMalfunction => Self::Internal(detail),
            C::PermissionDenied => Self::Internal(detail),
            C::OperationAborted => Self::Abort,
            C::DatabaseBusy => match error.extended_code {
                261 => Self::BusyRecovery, // SQLITE_BUSY_RECOVERY
                517 => Self::BusySnapshot {
                    conflicting_pages: detail,
                }, // SQLITE_BUSY_SNAPSHOT
                _ => Self::Busy,
            },
            C::DatabaseLocked => Self::DatabaseLocked { path },
            C::OutOfMemory => Self::OutOfMemory,
            C::ReadOnly => Self::ReadOnly,
            C::OperationInterrupted => Self::Interrupted,
            C::SystemIoFailure => Self::Io(std::io::Error::other(detail)),
            C::DatabaseCorrupt => Self::DatabaseCorrupt { detail },
            C::NotFound => Self::Internal(detail),
            C::DiskFull => Self::DatabaseFull,
            C::CannotOpen => Self::CannotOpen { path },
            C::FileLockingProtocolFailed => Self::LockFailed { detail },
            C::SchemaChanged => Self::Internal(detail),
            C::TooBig => Self::Internal(detail),
            C::ConstraintViolation => match error.extended_code {
                // SQLITE_CONSTRAINT_UNIQUE / _PRIMARYKEY / _FOREIGNKEY are
                // reported as UniqueViolation so downstream unique-probing
                // keeps working; other constraint classes land on Internal.
                2067 | 1555 | 787 => Self::UniqueViolation { columns: detail },
                _ => Self::Internal(detail),
            },
            C::TypeMismatch => Self::DatatypeMismatch,
            C::ApiMisuse => Self::Internal(detail),
            C::NoLargeFileSupport => Self::Internal(detail),
            C::AuthorizationForStatementDenied => Self::Internal(detail),
            C::ParameterOutOfRange => Self::Internal(detail),
            C::NotADatabase => Self::NotADatabase { path },
            _ => Self::classify_generic_error(detail, path),
        }
    }

    /// SQLITE_ERROR (generic) carries structure only in its message; peel the
    /// canonical `no such table:` / `no such column:` prefixes when present.
    fn classify_generic_error(detail: String, path: PathBuf) -> Self {
        if let Some(name) = detail.strip_prefix("no such table: ") {
            return Self::NoSuchTable {
                name: name.to_string(),
            };
        }
        if let Some(name) = detail.strip_prefix("no such column: ") {
            return Self::NoSuchColumn {
                name: name.to_string(),
            };
        }
        if detail.contains("file is not a database") {
            return Self::NotADatabase { path };
        }
        if detail.contains("interrupted") {
            return Self::Interrupted;
        }
        Self::Internal(detail)
    }

    pub(super) fn is_busy_class(&self) -> bool {
        matches!(
            self,
            Self::Busy
                | Self::BusyRecovery
                | Self::BusySnapshot { .. }
                | Self::DatabaseLocked { .. }
                | Self::LockFailed { .. }
        )
    }

    pub(super) fn is_interrupt_class(&self) -> bool {
        matches!(self, Self::Interrupted | Self::Abort)
    }
}

// ---------------------------------------------------------------------------
// FileIdentity
// ---------------------------------------------------------------------------

/// Filesystem identity used to detect whether two paths alias the same inode.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct FileIdentity {
    /// Device identifier (`st_dev` on Unix, volume serial on Windows).
    pub device: u64,
    /// Inode / file-index identifier.
    pub inode: u64,
}

impl FileIdentity {
    /// Read the descriptor-bound identity of `file`. Returns `Ok(None)` on
    /// platforms without a stable inode/first-class identity.
    pub fn from_file(file: &std::fs::File) -> std::io::Result<Option<FileIdentity>> {
        let metadata = file.metadata()?;
        Self::from_metadata(&metadata)
    }

    /// Read the filesystem identity of `path` without opening a descriptor on
    /// Unix (preserving in-process POSIX `fcntl` locks). On Windows, falls back
    /// to opening `path` because `volume_serial_number`/`file_index` require a
    /// handle.
    pub fn from_path(path: &Path) -> std::io::Result<Option<FileIdentity>> {
        #[cfg(unix)]
        {
            let metadata = std::fs::metadata(path)?;
            Self::from_metadata(&metadata)
        }
        #[cfg(windows)]
        {
            let file = std::fs::File::open(path)?;
            Self::from_file(&file)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let metadata = std::fs::metadata(path)?;
            Self::from_metadata(&metadata)
        }
    }

    fn from_metadata(metadata: &std::fs::Metadata) -> std::io::Result<Option<FileIdentity>> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(Some(FileIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
            }))
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            match (metadata.volume_serial_number(), metadata.file_index()) {
                (Some(volume), Some(index)) => Ok(Some(FileIdentity {
                    device: u64::from(volume),
                    inode: index,
                })),
                _ => Ok(None),
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = metadata;
            Ok(None)
        }
    }
}

