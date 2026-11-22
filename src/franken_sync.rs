//! CASS — synchronous SQLite facade backed by `rusqlite` (bundled C SQLite).
//!
//! The public surface mirrors the historical FrankenSQLite facade:
//! `Connection`, `PreparedStatement`, `compat::{ConnectionExt, Transaction,
//! TransactionExt}`, `Row`, `SqliteValue`, `FrankenError`, `FileIdentity`,
//! `params!`, cancellation scopes, `shutdown_driver`, and `BackupReport`.
//!
//! Underneath, `rusqlite::Connection` (bundled SQLite C engine) executes
//! every operation synchronously and directly on the calling thread — no
//! async runtime, no engine thread. Cancellation is therefore implemented
//! with real SQLite primitives: `sqlite3_interrupt` via `InterruptHandle`,
//! slice-bounded `busy_timeout` retry loops, and a shared deadline timer
//! thread that interrupts running statements when their deadline expires.
//!
//! Threading contract: `Connection` is `Send` but not `Sync`; keep each
//! `Connection` on one thread. `CancellationHandle` and `DeadlineGuard` are
//! `Send + Sync` and interrupt work from any thread. rusqlite's statement
//! cache makes `Connection` `!Sync`; cloning the handle keeps a cheap `Arc`.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::io::Read;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use rusqlite::config::DbConfig;
use rusqlite::types::{ToSql, ToSqlOutput, ValueRef};

/// Re-exported interrupt handle so hot paths can cancel work on other threads.
pub use rusqlite::InterruptHandle;
/// Typed transaction begin behavior (`Deferred`/`Immediate`/`Exclusive`).
pub use rusqlite::TransactionBehavior;

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

fn materialize_row(row: &rusqlite::Row<'_>, column_count: usize) -> Result<Row, FrankenError> {
    let mut values = Vec::with_capacity(column_count);
    for index in 0..column_count {
        let value = row
            .get_ref(index)
            .map_err(|error| FrankenError::from_rusqlite(error, None))?;
        values.push(sqlite_value_from_ref(value));
    }
    Ok(Row { values })
}

fn sqlite_value_from_ref(value: ValueRef<'_>) -> SqliteValue {
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
fn franken_error_to_rusqlite(error: FrankenError) -> rusqlite::Error {
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
    fn from_rusqlite(error: rusqlite::Error, path: Option<&Path>) -> Self {
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

    fn is_busy_class(&self) -> bool {
        matches!(
            self,
            Self::Busy
                | Self::BusyRecovery
                | Self::BusySnapshot { .. }
                | Self::DatabaseLocked { .. }
                | Self::LockFailed { .. }
        )
    }

    fn is_interrupt_class(&self) -> bool {
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

// ---------------------------------------------------------------------------
// params! macro
// ---------------------------------------------------------------------------

/// Positional `ParamValue` slice matching the rusqlite `params![...]` shape:
/// produces `&[compat::ParamValue]` (a slice — the `as` coercion keeps
/// `if`/`else` arms with different arities unifiable, as in `fparams!`).
#[macro_export]
macro_rules! __cass_franken_params {
    () => {
        &[] as &[$crate::franken_sync::compat::ParamValue]
    };
    ($($value:expr),+ $(,)?) => {
        &[$(
            $crate::franken_sync::compat::ParamValue::from($value)
        ),+] as &[$crate::franken_sync::compat::ParamValue]
    };
}
pub use crate::__cass_franken_params as params;

// ---------------------------------------------------------------------------
// Cancellation (TLS scope + interrupt-handle fan-out)
// ---------------------------------------------------------------------------

thread_local! {
    static CANCELLATION: RefCell<Option<CancellationHandle>> = const { RefCell::new(None) };
}

fn current_cancellation() -> Option<CancellationHandle> {
    CANCELLATION.with(|slot| slot.borrow().clone())
}

fn current_is_cancelled() -> bool {
    current_cancellation().is_some_and(|handle| handle.is_cancelled())
}

/// Check the thread-local cancellation scope; `Abort` when cancelled.
/// Open and operation entry points call this so a cancelled scope fails fast
/// even when no SQLite statement is in flight.
pub(crate) fn check_connection_admission() -> Result<(), FrankenError> {
    if current_is_cancelled() {
        return Err(FrankenError::Abort);
    }
    Ok(())
}

/// Cooperative cancellation handle. Holds every `InterruptHandle` registered
/// by in-flight operations on the owning thread so `cancel()` interrupts
/// active SQLite statements immediately, then latches `Abort`.
#[derive(Clone)]
pub(crate) struct CancellationHandle {
    state: Arc<CancellationState>,
}

struct CancellationState {
    cancelled: AtomicBool,
    /// Interrupt handles of in-flight operations registered on this scope,
    /// keyed by a monotonically increasing registration id.
    active: Mutex<Vec<(u64, rusqlite::InterruptHandle)>>,
    next_registration: AtomicU64,
}

impl fmt::Debug for CancellationHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationHandle")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl Default for CancellationHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationHandle {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                active: Mutex::new(Vec::new()),
                next_registration: AtomicU64::new(1),
            }),
        }
    }

    /// Cancel the scope: latch the flag, then interrupt every registered
    /// connection so in-flight SQLite calls return promptly.
    /// `InterruptHandle` is not `Clone` in rusqlite 0.40.2, so the
    /// interrupts are issued while holding the registration lock —
    /// `sqlite3_interrupt` is non-blocking, so this is safe. When ops are
    /// still in flight, a repeating 5 ms poke on the shared deadline timer
    /// re-interrupts them so a `cancel()` landing inside `conn.prepare`
    /// (`nVdbeActive == 0`, where `sqlite3_interrupt` is a no-op) is not
    /// lost once `sqlite3_step` begins.
    pub(crate) fn cancel(&self) {
        if self.state.cancelled.swap(true, AtomicOrdering::AcqRel) {
            return;
        }
        let has_active = {
            let active = lock_unpoisoned(&self.state.active);
            for (_, handle) in active.iter() {
                handle.interrupt();
            }
            !active.is_empty()
        };
        if has_active {
            deadline_timer().arm_poke(Arc::clone(&self.state), DEADLINE_POKE_INTERVAL);
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(AtomicOrdering::Acquire)
    }

    /// Install this scope on the calling thread until the returned scope
    /// token drops. Nested scopes restore their parent on drop.
    pub(crate) fn enter(&self) -> CancellationScope {
        let parent = CANCELLATION.with(|slot| slot.borrow_mut().replace(self.clone()));
        CancellationScope {
            parent,
            _thread_affine: PhantomData::<Rc<()>>,
        }
    }

    /// Attach a connection's interrupt handle to this scope. Returns
    /// `Err(FrankenError::Abort)` when the scope is already cancelled —
    /// checked under the registration lock so a `cancel()` racing between
    /// the caller's pre-check and the push can never leave an op running
    /// under a dead scope.
    fn register(
        &self,
        handle: rusqlite::InterruptHandle,
    ) -> Result<CancellationRegistration, FrankenError> {
        let id = self
            .state
            .next_registration
            .fetch_add(1, AtomicOrdering::Relaxed);
        let mut active = lock_unpoisoned(&self.state.active);
        if self.is_cancelled() {
            return Err(FrankenError::Abort);
        }
        active.push((id, handle));
        Ok(CancellationRegistration {
            state: Arc::downgrade(&self.state),
            id,
        })
    }
}

/// RAII token removing a registered interrupt handle when an op completes.
struct CancellationRegistration {
    state: std::sync::Weak<CancellationState>,
    id: u64,
}

impl Drop for CancellationRegistration {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let mut active = lock_unpoisoned(&state.active);
            active.retain(|(id, _)| *id != self.id);
        }
    }
}

/// RAII scope token restoring the previous cancellation scope on drop.
/// `!Send` on purpose: cancellation scopes are thread-local.
pub(crate) struct CancellationScope {
    parent: Option<CancellationHandle>,
    _thread_affine: PhantomData<Rc<()>>,
}

impl Drop for CancellationScope {
    fn drop(&mut self) {
        CANCELLATION.with(|slot| {
            *slot.borrow_mut() = self.parent.take();
        });
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

// ---------------------------------------------------------------------------
// Shared deadline timer (one worker thread, BinaryHeap + Condvar)
// ---------------------------------------------------------------------------

/// What an expired deadline pokes: a single statement's interrupt handle, or
/// every handle registered on a cancellation scope (re-poke while in-flight).
enum DeadlineFire {
    Interrupt(rusqlite::InterruptHandle),
    Cancellation(Arc<CancellationState>),
}

/// Mutable per-slot state guarded by its own mutex. Locking it on disarm
/// synchronizes `DeadlineGuard::drop` with an in-flight `interrupt()` so a
/// late fire can never land on the next query using the same connection.
struct DeadlineSlotState {
    disarmed: bool,
    fired: bool,
}

/// Shared state for one armed deadline.
struct DeadlineSlot {
    /// Per-slot lock serializing fire vs. disarm (see `deadline_worker`).
    state: Mutex<DeadlineSlotState>,
    /// Fire target: one connection's interrupt handle, or a whole
    /// cancellation scope's registered handles.
    fire: DeadlineFire,
}

/// Re-poke cadence for `nVdbeActive == 0` windows: `sqlite3_interrupt`
/// before `sqlite3_step` is a C SQLite no-op, so fired entries re-arm
/// themselves every 5 ms until their guard drops / scope drains.
const DEADLINE_POKE_INTERVAL: Duration = Duration::from_millis(5);

struct DeadlineEntry {
    deadline: Instant,
    id: u64,
    slot: Arc<DeadlineSlot>,
}

impl PartialEq for DeadlineEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.id == other.id
    }
}
impl Eq for DeadlineEntry {}
impl PartialOrd for DeadlineEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for DeadlineEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.id.cmp(&other.id))
    }
}

struct TimerState {
    heap: BinaryHeap<std::cmp::Reverse<DeadlineEntry>>,
    next_id: u64,
}

/// The process-wide deadline timer: one reusable worker thread that pops the
/// earliest deadline off a min-heap, sleeps on a `Condvar`, and interrupts
/// armed slots as they expire.
struct DeadlineTimer {
    state: Mutex<TimerState>,
    woken: Condvar,
}

impl DeadlineTimer {
    fn spawn() -> Arc<Self> {
        let timer = Arc::new(Self {
            state: Mutex::new(TimerState {
                heap: BinaryHeap::new(),
                next_id: 1,
            }),
            woken: Condvar::new(),
        });
        let worker = Arc::clone(&timer);
        let _ = std::thread::Builder::new()
            .name("cass-franken-sync-deadline".to_string())
            .spawn(move || deadline_worker(&worker));
        timer
    }

    fn arm(&self, interrupt: rusqlite::InterruptHandle, deadline: Instant) -> DeadlineGuard {
        let slot = Arc::new(DeadlineSlot {
            state: Mutex::new(DeadlineSlotState {
                disarmed: false,
                fired: false,
            }),
            fire: DeadlineFire::Interrupt(interrupt),
        });
        {
            let mut state = lock_unpoisoned(&self.state);
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.heap.push(std::cmp::Reverse(DeadlineEntry {
                deadline,
                id,
                slot: Arc::clone(&slot),
            }));
        }
        self.woken.notify_one();
        DeadlineGuard { slot }
    }

    /// Arm a repeating poke against every interrupt handle registered on a
    /// cancellation scope: fires `delay` from now, then re-arms every
    /// `DEADLINE_POKE_INTERVAL` while the scope still has in-flight
    /// operations (covers `nVdbeActive == 0` misses inside `prepare`).
    fn arm_poke(&self, cancel_state: Arc<CancellationState>, delay: Duration) {
        let slot = Arc::new(DeadlineSlot {
            state: Mutex::new(DeadlineSlotState {
                disarmed: false,
                fired: false,
            }),
            fire: DeadlineFire::Cancellation(cancel_state),
        });
        {
            let mut state = lock_unpoisoned(&self.state);
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.heap.push(std::cmp::Reverse(DeadlineEntry {
                deadline: Instant::now() + delay,
                id,
                slot,
            }));
        }
        self.woken.notify_one();
    }
}

static DEADLINE_TIMER: OnceLock<Arc<DeadlineTimer>> = OnceLock::new();

fn deadline_timer() -> &'static Arc<DeadlineTimer> {
    DEADLINE_TIMER.get_or_init(DeadlineTimer::spawn)
}

fn deadline_worker(timer: &Arc<DeadlineTimer>) {
    loop {
        // Pop every expired entry, then fire each under ITS OWN slot lock.
        // Lock order is strictly slot.state -> timer.state (disarm only
        // takes slot.state; arm only takes timer.state), so no cycle.
        let expired: Vec<DeadlineEntry> = {
            let mut state = lock_unpoisoned(&timer.state);
            let now = Instant::now();
            let mut expired = Vec::new();
            while let Some(std::cmp::Reverse(top)) = state.heap.peek() {
                if top.deadline > now {
                    break;
                }
                expired.push(state.heap.pop().expect("peeked entry").0);
            }
            expired
        };

        for entry in expired {
            let repush = {
                let mut slot_state = lock_unpoisoned(&entry.slot.state);
                if slot_state.disarmed {
                    false
                } else {
                    slot_state.fired = true;
                    let repush = match &entry.slot.fire {
                        DeadlineFire::Interrupt(handle) => {
                            handle.interrupt();
                            // Keep poking until the guard drops so a fire
                            // landing in the nVdbeActive == 0 window (inside
                            // prepare / between statements) is retried.
                            true
                        }
                        DeadlineFire::Cancellation(cancel_state) => {
                            let active = lock_unpoisoned(&cancel_state.active);
                            for (_, handle) in active.iter() {
                                handle.interrupt();
                            }
                            !active.is_empty()
                        }
                    };
                    if !repush {
                        slot_state.disarmed = true;
                    }
                    repush
                }
            };
            if repush {
                let mut state = lock_unpoisoned(&timer.state);
                state.heap.push(std::cmp::Reverse(DeadlineEntry {
                    deadline: Instant::now() + DEADLINE_POKE_INTERVAL,
                    id: entry.id,
                    slot: entry.slot,
                }));
            }
        }

        {
            let state = lock_unpoisoned(&timer.state);
            let wait_for = state
                .heap
                .peek()
                .map(|std::cmp::Reverse(entry)| entry.deadline);
            match wait_for {
                Some(deadline) => {
                    let delay = deadline.saturating_duration_since(Instant::now());
                    if delay.is_zero() {
                        continue;
                    }
                    let _ = timer
                        .woken
                        .wait_timeout(state, delay)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
                None => {
                    let _ = timer
                        .woken
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
        }
    }
}

/// RAII deadline token. While armed, expiry interrupts the owning connection
/// (re-poking every `DEADLINE_POKE_INTERVAL` so an expiry landing before
/// `sqlite3_step` is retried); `disarm()` (or `Drop`) synchronizes with any
/// in-flight interrupt through the per-slot lock, so a dropped guard can
/// never let a late fire hit the next query on that connection.
pub struct DeadlineGuard {
    slot: Arc<DeadlineSlot>,
}

impl DeadlineGuard {
    /// Arm a deadline against an interrupt handle directly.
    pub fn arm(interrupt: rusqlite::InterruptHandle, deadline: Instant) -> Self {
        deadline_timer().arm(interrupt, deadline)
    }

    /// `true` once the deadline actually expired and interrupted.
    pub fn fired(&self) -> bool {
        lock_unpoisoned(&self.slot.state).fired
    }

    /// `true` while the deadline entry is still armed.
    pub fn is_armed(&self) -> bool {
        !lock_unpoisoned(&self.slot.state).disarmed
    }

    /// Disarm the timer entry; expiry after this call cannot fire. The call
    /// blocks briefly if the worker is mid-`interrupt()` on this slot, so no
    /// late fire can ever land on a subsequent query.
    pub fn disarm(&self) {
        lock_unpoisoned(&self.slot.state).disarmed = true;
    }
}

impl Drop for DeadlineGuard {
    fn drop(&mut self) {
        self.disarm();
    }
}

// ---------------------------------------------------------------------------
// Operation helpers
// ---------------------------------------------------------------------------

/// `&[&dyn ToSql]` view over a `SqliteValue` slice for `stmt.query`/`execute`.
fn dynamic_params<'a>(values: &'a [SqliteValue]) -> Vec<&'a (dyn ToSql + 'a)> {
    values
        .iter()
        .map(|value| value as &(dyn ToSql + 'a))
        .collect()
}

/// Detect a lock-taking `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE`
/// (`TRANSACTION`/`TRAN` suffix optional, case-insensitive, whitespace
/// tolerant, one trailing `;` allowed) so the execute entry points can
/// route them through `begin_with_retry` — the slice-bounded busy-retry
/// path — instead of firing a single non-retried attempt. Plain `BEGIN`
/// (deferred) takes no lock and keeps the direct path; a `;` still
/// present after stripping the terminator means a multi-statement batch
/// and must stay on `execute_batch`.
fn lock_taking_begin_verb(sql: &str) -> Option<&'static str> {
    let trimmed = sql.trim();
    let body = trimmed.strip_suffix(';').unwrap_or(trimmed);
    if body.contains(';') {
        return None;
    }
    let mut words = body.split_whitespace();
    if !words
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("begin"))
    {
        return None;
    }
    let mode = words.next()?;
    let verb = if mode.eq_ignore_ascii_case("immediate") {
        "BEGIN IMMEDIATE"
    } else if mode.eq_ignore_ascii_case("exclusive") {
        "BEGIN EXCLUSIVE"
    } else {
        return None;
    };
    match words.next() {
        None => Some(verb),
        // Tolerate "BEGIN IMMEDIATE TRANSACTION" (SQLite grammar).
        Some(w)
            if w.eq_ignore_ascii_case("transaction") && words.next().is_none()
                || w.eq_ignore_ascii_case("tran") && words.next().is_none() =>
        {
            Some(verb)
        }
        Some(_) => None,
    }
}

/// Execute a statement to completion, discarding any rows it returns.
///
/// `PRAGMA journal_mode = WAL`, `BEGIN`, etc. all return rows under rusqlite's
/// strict `execute`, so the facade steps the statement manually instead.
/// Multi-statement SQL falls back to `execute_batch` (bound parameters apply
/// to the first statement only, which matches sqlite semantics anyway).
fn run_execute(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Result<usize, rusqlite::Error> {
    match conn.prepare(sql) {
        Ok(mut stmt) => {
            // Always step through `query` — even with zero params — because
            // `raw_execute` still fails with `ExecuteReturnedResults` on
            // row-returning statements (`PRAGMA journal_mode = WAL`,
            // `BEGIN`, `... RETURNING`). `dynamic_params(&[])` allocates
            // nothing and `query` drains `SQLITE_ROW` results cleanly.
            let refs = dynamic_params(params);
            let mut rows = stmt.query(&refs[..])?;
            while let Some(_row) = rows.next()? {}
            Ok(conn.changes() as usize)
        }
        Err(rusqlite::Error::MultipleStatement) => {
            conn.execute_batch(sql)?;
            Ok(conn.changes() as usize)
        }
        Err(error) => Err(error),
    }
}

/// Small deterministic jitter for busy-retry pacing: exponential base
/// (4 ms → 128 ms cap) plus a lightweight counter-derived jitter term so
/// contending waiters do not retry in lock-step. No RNG dependency.
fn retry_sleep(attempt: u32) -> Duration {
    static JITTER_SEED: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    let base_ms: u64 = (4u64 << attempt.min(5)).min(128);
    let jitter_ms = JITTER_SEED.fetch_add(0x9E37_79B9, AtomicOrdering::Relaxed) % 13;
    Duration::from_millis(base_ms + jitter_ms)
}

/// Read `PRAGMA busy_timeout` straight off the raw connection: an
/// uncancellable in-memory pragma read so the slice-bounded retry loops
/// capture `Some(original_ms)` for `BusyTimeoutRestore` even inside a
/// cancelled `CancellationScope` (`guarded` would return `Abort`).
fn read_busy_timeout_ms(conn: &rusqlite::Connection) -> Option<i64> {
    conn.query_row("PRAGMA busy_timeout", [], |r| r.get(0)).ok()
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Synchronous database connection backed directly by `rusqlite::Connection`.
///
/// `Send` but not `Sync` (rusqlite caches prepared statements in a
/// `RefCell`). All blocking work runs on the calling thread; cancellation is
/// delivered by `sqlite3_interrupt` through `CancellationHandle` /
/// `DeadlineGuard`.
pub struct Connection {
    inner: Option<rusqlite::Connection>,
    /// Filesystem path the connection was opened with (`:memory:` kept as-is
    /// for diagnostics).
    path: Option<PathBuf>,
    savepoint_seq: AtomicU64,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrankenSyncConnection")
            .field("path", &self.path)
            .field("closed", &self.inner.is_none())
            .finish()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close_best_effort_in_place();
    }
}

impl Connection {
    /// Open a database at `path` (`:memory:` supported).
    pub fn open(path: impl AsRef<str>) -> Result<Self, FrankenError> {
        check_connection_admission()?;
        let path = path.as_ref().to_owned();
        let inner = rusqlite::Connection::open(&path)
            .map_err(|error| FrankenError::from_rusqlite(error, Some(Path::new(&path))))?;
        Ok(Self {
            inner: Some(inner),
            path: Some(PathBuf::from(path)),
            savepoint_seq: AtomicU64::new(0),
        })
    }

    /// Open an existing database read/write without creating it.
    pub fn open_existing_schema_only(path: impl AsRef<str>) -> Result<Self, FrankenError> {
        Self::open_with_flags_inner(path.as_ref(), compat::OpenFlags::SQLITE_OPEN_READ_WRITE)
    }

    /// Alias kept for historical naming; identical to
    /// `open_existing_schema_only`.
    pub fn open_existing_schema_only_deferred_fts5(
        path: impl AsRef<str>,
    ) -> Result<Self, FrankenError> {
        Self::open_existing_schema_only(path)
    }

    /// Open an existing database read-only, permitting SQLite's built-in
    /// WAL-index recovery (heap walindex / shm rebuild as needed).
    pub fn open_schema_only_with_wal_index_recovery(
        path: impl AsRef<str>,
    ) -> Result<Self, FrankenError> {
        Self::open_with_flags_inner(path.as_ref(), compat::OpenFlags::SQLITE_OPEN_READ_ONLY)
    }

    /// Historical alias preserved for callers referencing the
    /// `open_existing_schema_only...` variants.
    pub fn open_existing(path: impl AsRef<str>) -> Result<Self, FrankenError> {
        Self::open_existing_schema_only(path)
    }

    /// Compatibility shim for the old `connection.as_async()` surface: the
    /// engine is synchronous now, so the "async view" only reports
    /// transaction state and exposes the raw connection.
    pub fn as_async(&self) -> AsyncView<'_> {
        AsyncView { inner: self.raw() }
    }

    /// Direct access to the underlying rusqlite connection (hot paths).
    ///
    /// # Panics
    /// Panics if the connection was already closed; use `try_raw` when the
    /// closed state is possible.
    pub fn raw(&self) -> &rusqlite::Connection {
        self.inner
            .as_ref()
            .expect("franken_sync::Connection used after close")
    }

    /// Mutable access to the underlying rusqlite connection.
    pub fn raw_mut(&mut self) -> &mut rusqlite::Connection {
        self.inner
            .as_mut()
            .expect("franken_sync::Connection used after close")
    }

    /// Fallible variant of `raw()` for close-sensitive paths.
    pub fn try_raw(&self) -> Result<&rusqlite::Connection, FrankenError> {
        self.inner
            .as_ref()
            .ok_or_else(|| FrankenError::Internal("connection closed".to_string()))
    }

    /// Zero-copy access to the underlying rusqlite connection for streaming
    /// call sites — no facade wrapping, no materialization.
    pub fn with_raw_rusqlite<R>(&self, f: impl FnOnce(&rusqlite::Connection) -> R) -> R {
        f(self.raw())
    }

    /// Mutable variant of `with_raw_rusqlite` (e.g. `conn.transaction()`).
    pub fn with_raw_rusqlite_mut<R>(
        &mut self,
        f: impl FnOnce(&mut rusqlite::Connection) -> R,
    ) -> R {
        f(self.raw_mut())
    }

    /// `true` while SQLite reports a live (non-autocommit) transaction.
    /// Returns `false` once the connection is closed.
    pub fn in_transaction(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| !inner.is_autocommit())
    }

    /// `true` while the connection is in autocommit mode.
    pub fn is_autocommit(&self) -> bool {
        !self.in_transaction()
    }

    /// Interrupt handle for `sqlite3_interrupt` from other threads.
    pub fn interrupt_handle(&self) -> rusqlite::InterruptHandle {
        self.raw().get_interrupt_handle()
    }

    /// Arm a deadline on the shared timer; expiry interrupts this connection.
    pub fn deadline_guard(&self, deadline: Instant) -> DeadlineGuard {
        DeadlineGuard::arm(self.raw().get_interrupt_handle(), deadline)
    }

    /// Arm a deadline `within` from now.
    pub fn deadline_guard_after(&self, within: Duration) -> DeadlineGuard {
        self.deadline_guard(Instant::now() + within)
    }

    /// Filesystem identity of the database file (`Ok(None)` for `:memory:`
    /// or platforms without inode identity).
    pub fn file_identity(&self) -> Result<Option<FileIdentity>, FrankenError> {
        let Some(path) = self.path.as_deref() else {
            return Ok(None);
        };
        if path.as_os_str() == ":memory:" {
            return Ok(None);
        }
        let file = std::fs::File::open(path)?;
        Ok(FileIdentity::from_file(&file)?)
    }

    /// Path the connection was opened with.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    // -- statement execution -------------------------------------------------

    /// Execute a statement to completion; rows it returns are discarded.
    /// Multi-statement SQL runs via `sqlite3_exec` semantics. Direct
    /// `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE` calls are routed through
    /// `begin_with_retry` so they inherit slice-bounded busy retry and
    /// cancellation checks exactly like `transaction_with_behavior`.
    pub fn execute(&self, sql: &str) -> Result<usize, FrankenError> {
        if let Some(verb) = lock_taking_begin_verb(sql) {
            self.begin_with_retry(verb, None)?;
            return Ok(0);
        }
        self.guarded(|conn| run_execute(conn, sql, &[]))
    }

    /// Execute with bound parameters. A parameterless lock-taking
    /// `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE` routes through
    /// `begin_with_retry` exactly like [`Self::execute`].
    pub fn execute_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError> {
        let begin_verb = if params.is_empty() {
            lock_taking_begin_verb(sql)
        } else {
            None
        };
        if let Some(verb) = begin_verb {
            self.begin_with_retry(verb, None)?;
            return Ok(0);
        }
        self.guarded(|conn| run_execute(conn, sql, params))
    }

    /// Execute a semicolon-separated script. A lone lock-taking
    /// `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE` (one optional trailing `;`)
    /// routes through `begin_with_retry`; multi-statement scripts keep
    /// `sqlite3_exec` semantics.
    pub fn execute_batch(&self, sql: &str) -> Result<(), FrankenError> {
        if let Some(verb) = lock_taking_begin_verb(sql) {
            return self.begin_with_retry(verb, None);
        }
        self.guarded(|conn| conn.execute_batch(sql))
    }

    /// Materialize every row of `sql`.
    pub fn query(&self, sql: &str) -> Result<Vec<Row>, FrankenError> {
        self.query_with_params(sql, &[])
    }

    /// Materialize every row of `sql` with bound parameters.
    pub fn query_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Vec<Row>, FrankenError> {
        let mut out = Vec::new();
        self.query_with_params_for_each(sql, params, |row| {
            out.push(row.clone());
            Ok(())
        })?;
        Ok(out)
    }

    /// Streaming per-row callback (no params): each materialized `Row` is
    /// handed to `f` as it is stepped; rows are not collected.
    pub fn query_for_each<F>(&self, sql: &str, f: F) -> Result<(), FrankenError>
    where
        F: FnMut(&Row) -> Result<(), FrankenError>,
    {
        self.query_with_params_for_each(sql, &[], f)
    }

    /// Streaming per-row callback with bound parameters.
    ///
    /// Unlike `query_with_params` this does not collect the result set; each
    /// row is materialized and passed to `f` one at a time.
    pub fn query_with_params_for_each<F>(
        &self,
        sql: &str,
        params: &[SqliteValue],
        mut f: F,
    ) -> Result<(), FrankenError>
    where
        F: FnMut(&Row) -> Result<(), FrankenError>,
    {
        self.guarded_result(|conn| {
            let refs = dynamic_params(params);
            let mut stmt = conn
                .prepare(sql)
                .map_err(|error| self.map_err_cancellable(error))?;
            let column_count = stmt.column_count();
            let mut rows = stmt
                .query(&refs[..])
                .map_err(|error| self.map_err_cancellable(error))?;
            // Post-prepare/pre-step re-check: an interrupt landing during
            // prepare (nVdbeActive == 0) is a C SQLite no-op.
            if current_is_cancelled() {
                return Err(FrankenError::Abort);
            }
            loop {
                match rows.next() {
                    Err(error) => return Err(self.map_err_cancellable(error)),
                    Ok(None) => return Ok(()),
                    Ok(Some(row)) => {
                        let materialized = materialize_row(row, column_count)?;
                        f(&materialized)?;
                    }
                }
            }
        })
    }

    /// Zero-copy streaming map: `f` borrows each `rusqlite::Row<'_>` directly
    /// (no `SqliteValue` materialization) and its results are collected.
    /// This is the hot-path API for callers that only need a few columns.
    pub fn query_map<T, F>(
        &self,
        sql: &str,
        params: &[SqliteValue],
        mut f: F,
    ) -> Result<Vec<T>, FrankenError>
    where
        F: FnMut(&rusqlite::Row<'_>) -> Result<T, FrankenError>,
    {
        self.guarded_result(|conn| {
            let refs = dynamic_params(params);
            let mut stmt = conn
                .prepare(sql)
                .map_err(|error| self.map_err_cancellable(error))?;
            let mut rows = stmt
                .query(&refs[..])
                .map_err(|error| self.map_err_cancellable(error))?;
            if current_is_cancelled() {
                return Err(FrankenError::Abort);
            }
            let mut out = Vec::new();
            loop {
                match rows.next() {
                    Err(error) => return Err(self.map_err_cancellable(error)),
                    Ok(None) => break,
                    Ok(Some(row)) => out.push(f(row)?),
                }
            }
            Ok(out)
        })
    }

    /// Execute `sql` expecting exactly one row.
    /// `QueryReturnedNoRows` on empty, `QueryReturnedMultipleRows` on >1.
    pub fn query_row(&self, sql: &str) -> Result<Row, FrankenError> {
        self.query_row_with_params(sql, &[])
    }

    /// `query_row` with bound parameters.
    pub fn query_row_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Row, FrankenError> {
        self.guarded_result(|conn| {
            let refs = dynamic_params(params);
            let mut stmt = conn
                .prepare(sql)
                .map_err(|error| self.map_err_cancellable(error))?;
            let column_count = stmt.column_count();
            let mut rows = stmt
                .query(&refs[..])
                .map_err(|error| self.map_err_cancellable(error))?;
            let first = match rows.next() {
                Err(error) => return Err(self.map_err_cancellable(error)),
                Ok(None) => return Err(FrankenError::QueryReturnedNoRows),
                Ok(Some(row)) => materialize_row(row, column_count)?,
            };
            match rows.next() {
                Err(error) => Err(self.map_err_cancellable(error)),
                Ok(None) => Ok(first),
                Ok(Some(_)) => Err(FrankenError::QueryReturnedMultipleRows),
            }
        })
    }

    /// Prepare a statement for repeated use.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement<'_>, FrankenError> {
        let stmt = self.guarded(|conn| conn.prepare(sql))?;
        Ok(PreparedStatement {
            stmt: RefCell::new(stmt),
            connection: self,
        })
    }

    /// Rowid of the most recent insert on this connection.
    /// Mirrors `sqlite3_last_insert_rowid`: infallible, `0` on a closed
    /// connection.
    pub fn last_insert_rowid(&self) -> i64 {
        if check_connection_admission().is_err() {
            return 0;
        }
        self.try_raw().map_or(0, |conn| conn.last_insert_rowid())
    }

    // -- engine-maintenance shims --------------------------------------------

    /// The rusqlite engine manages page freelists itself; orphaned-page
    /// repair is a no-op that reports zero pages repaired.
    pub fn repair_orphaned_pages(&self) -> Result<usize, FrankenError> {
        check_connection_admission()?;
        Ok(0)
    }

    /// Exact-content backup via `VACUUM INTO`, refused while a transaction is
    /// open (SQLite rejects `VACUUM` inside one; the old facade reported that
    /// as `Busy`).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn backup_exact_to(&self, target: impl AsRef<Path>) -> Result<BackupReport, FrankenError> {
        check_connection_admission()?;
        let target = target.as_ref();
        if self.in_transaction() {
            return Err(FrankenError::Busy);
        }
        if target.exists() {
            return Err(FrankenError::CannotOpen {
                path: target.to_path_buf(),
            });
        }
        let target_str = target.to_string_lossy().into_owned();
        self.execute_with_params("VACUUM INTO ?1", &[SqliteValue::Text(target_str)])?;
        read_backup_report(target)
    }

    /// Cancellable, slice-bounded `PRAGMA wal_checkpoint(TRUNCATE)` loop.
    ///
    /// Returns `(busy, log_frames, checkpointed_frames)` once `busy == 0`, or
    /// the last busy-class error once `deadline` is exhausted. The configured
    /// `PRAGMA busy_timeout` is sliced per attempt (≤250 ms) so `cancel()`
    /// lands promptly; when `deadline` is `None` the busy timeout itself is
    /// the retry budget (`busy_timeout=0` → a single attempt).
    pub fn wal_checkpoint_truncate(
        &self,
        deadline: Option<Instant>,
    ) -> Result<(i64, i64, i64), FrankenError> {
        const SLICE: Duration = Duration::from_millis(250);
        // Admission/open checks precede any busy_timeout rewrite: a
        // cancelled scope fails fast and a closed connection returns
        // Internal — never a raw() panic — so the sliced timeout cannot
        // leak past BusyTimeoutRestore.
        check_connection_admission()?;
        let raw = self.try_raw()?;
        let configured_ms = read_busy_timeout_ms(raw);
        let budget_deadline = deadline.or_else(|| {
            configured_ms
                .filter(|ms| *ms > 0)
                .map(|ms| Instant::now() + Duration::from_millis(ms.max(0) as u64))
        });
        let restore = BusyTimeoutRestore::arm(raw, configured_ms);
        let configured_ms = configured_ms.unwrap_or(0);
        let mut attempt = 0u32;
        let mut last_busy_error: Option<FrankenError> = None;
        let result = loop {
            if let Some(limit) = budget_deadline {
                let remaining = limit.saturating_duration_since(Instant::now());
                if remaining.is_zero() && attempt > 0 {
                    break Err(last_busy_error.take().unwrap_or(FrankenError::Busy));
                }
                let _ = raw.busy_timeout(remaining.min(SLICE).max(Duration::from_millis(1)));
            }
            match self.query_row("PRAGMA wal_checkpoint(TRUNCATE)") {
                Ok(row) => {
                    let busy = row_i64(&row, 0);
                    let log = row_i64(&row, 1);
                    let checkpointed = row_i64(&row, 2);
                    if busy == 0 {
                        break Ok((busy, log, checkpointed));
                    }
                    // Checkpoint returned busy: retry within the budget.
                    if let Some(limit) = budget_deadline {
                        if Instant::now() >= limit {
                            break Err(FrankenError::Busy);
                        }
                    } else if attempt > 0 || configured_ms <= 0 {
                        break Err(FrankenError::Busy);
                    }
                }
                // Busy-class *errors* from query_row retry exactly like a
                // busy=1 row does.
                Err(error) if error.is_busy_class() => {
                    if let Some(limit) = budget_deadline {
                        if Instant::now() >= limit {
                            break Err(error);
                        }
                    } else if attempt > 0 || configured_ms <= 0 {
                        break Err(error);
                    }
                    // Keep the specific busy-class error so an exhausted
                    // deadline reports it instead of plain Busy.
                    last_busy_error = Some(error);
                }
                Err(error) => break Err(error),
            }
            if let Err(error) = check_connection_admission() {
                break Err(error);
            }
            let sleep = budget_deadline.map_or_else(
                || retry_sleep(attempt),
                |limit| retry_sleep(attempt).min(limit.saturating_duration_since(Instant::now())),
            );
            std::thread::sleep(sleep);
            if let Err(error) = check_connection_admission() {
                break Err(error);
            }
            attempt = attempt.saturating_add(1);
        };
        drop(restore);
        result
    }

    // -- transactions ---------------------------------------------------------

    /// `BEGIN` with cancellable, slice-bounded busy retry. The configured
    /// `PRAGMA busy_timeout` is the overall budget; each attempt bounds the
    /// engine-internal wait to a ≤250 ms slice so `cancel()` lands promptly.
    pub fn transaction_with_behavior(
        &self,
        behavior: compat::TransactionBehavior,
    ) -> Result<compat::Transaction<'_>, FrankenError> {
        self.transaction_with_deadline(behavior, None)
    }

    /// `transaction_with_behavior` with an explicit deadline: retries stop at
    /// `deadline` and surface the last busy-class error.
    pub fn transaction_with_deadline(
        &self,
        behavior: compat::TransactionBehavior,
        deadline: Option<Instant>,
    ) -> Result<compat::Transaction<'_>, FrankenError> {
        let verb = match behavior {
            compat::TransactionBehavior::Immediate => "BEGIN IMMEDIATE",
            compat::TransactionBehavior::Exclusive => "BEGIN EXCLUSIVE",
            // Deferred plus any future non-exhaustive variants fall back to
            // plain BEGIN.
            _ => "BEGIN",
        };
        self.begin_with_retry(verb, deadline)?;
        Ok(compat::Transaction::active(self))
    }

    fn begin_with_retry(
        &self,
        verb: &'static str,
        deadline: Option<Instant>,
    ) -> Result<(), FrankenError> {
        const SLICE: Duration = Duration::from_millis(250);
        // Same ordering as wal_checkpoint_truncate: admission/open checks
        // before any busy_timeout rewrite so a cancelled scope aborts
        // fast and a closed connection returns Internal, keeping
        // BusyTimeoutRestore able to restore the real configured value.
        check_connection_admission()?;
        let raw = self.try_raw()?;
        let configured_ms = read_busy_timeout_ms(raw);
        let budget_deadline = deadline.or_else(|| {
            configured_ms
                .filter(|ms| *ms > 0)
                .map(|ms| Instant::now() + Duration::from_millis(ms.max(0) as u64))
        });
        let restore = BusyTimeoutRestore::arm(raw, configured_ms);
        let configured_ms = configured_ms.unwrap_or(0);
        let mut attempt = 0u32;
        let mut last_busy_error: Option<FrankenError> = None;
        let result = loop {
            if let Some(limit) = budget_deadline {
                let remaining = limit.saturating_duration_since(Instant::now());
                if remaining.is_zero() && attempt > 0 {
                    break Err(last_busy_error.take().unwrap_or(FrankenError::Busy));
                }
                let _ = raw.busy_timeout(remaining.min(SLICE).max(Duration::from_millis(1)));
            }
            match self.guarded(|conn| conn.execute(verb, [])) {
                Ok(_) => break Ok(()),
                Err(error) if error.is_busy_class() => {
                    if let Some(limit) = budget_deadline {
                        if Instant::now() >= limit {
                            break Err(error);
                        }
                    } else {
                        // No configured budget: a single attempt is sqlite's
                        // busy_timeout=0 contract.
                        if attempt > 0 || configured_ms <= 0 {
                            break Err(error);
                        }
                    }
                    // Keep the specific busy-class error so an exhausted
                    // deadline reports it instead of plain Busy.
                    last_busy_error = Some(error);
                    check_connection_admission()?;
                    // Clamp the inter-attempt sleep to the remaining budget
                    // so the loop cannot oversleep past `deadline`.
                    let sleep = budget_deadline.map_or_else(
                        || retry_sleep(attempt),
                        |limit| {
                            retry_sleep(attempt)
                                .min(limit.saturating_duration_since(Instant::now()))
                        },
                    );
                    std::thread::sleep(sleep);
                    // A cancel may have landed while we slept.
                    if let Err(error) = check_connection_admission() {
                        break Err(error);
                    }
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => break Err(error),
            }
        };
        drop(restore);
        result
    }

    /// Open a savepoint (`SAVEPOINT cass_sp_N`) usable inside or outside an
    /// outer transaction.
    pub fn savepoint(&self) -> Result<compat::Savepoint<'_>, FrankenError> {
        compat::Savepoint::open(self)
    }

    // -- close ----------------------------------------------------------------

    /// Close, rolling back any open transaction. `Ok(())` when already closed.
    pub fn close(mut self) -> Result<(), FrankenError> {
        self.close_in_place()
    }

    /// Close in place; `self` remains usable-for-drop but closed. Resets
    /// `SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE` first so a prior failed
    /// `close_without_checkpoint_in_place` cannot leak the flag into this
    /// checkpointing close.
    pub fn close_in_place(&mut self) -> Result<(), FrankenError> {
        let Some(conn) = self.inner.take() else {
            return Ok(());
        };
        let _ = conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, false);
        match conn.close() {
            Ok(()) => Ok(()),
            Err((conn, error)) => {
                self.inner = Some(conn);
                Err(self.map_err_cancellable(error))
            }
        }
    }

    /// Close without checkpointing the WAL on close
    /// (`SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE`). Preserves the `-wal` payload for
    /// later read-only recovery.
    pub fn close_without_checkpoint(mut self) -> Result<(), FrankenError> {
        self.close_without_checkpoint_in_place()
    }

    /// In-place variant of `close_without_checkpoint`.
    pub fn close_without_checkpoint_in_place(&mut self) -> Result<(), FrankenError> {
        let Some(conn) = self.inner.take() else {
            return Ok(());
        };
        // rusqlite 0.40.2 uses the two-argument setter; the one-argument
        // `db_config()` is the getter only.
        let _ = conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true);
        match conn.close() {
            Ok(()) => Ok(()),
            Err((conn, error)) => {
                self.inner = Some(conn);
                Err(self.map_err_cancellable(error))
            }
        }
    }

    /// Close best-effort, never erroring: skip the close-time checkpoint and
    /// discard the handle.
    pub fn close_best_effort_in_place(&mut self) {
        if let Some(conn) = self.inner.take() {
            let _ = conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true);
            match conn.close() {
                Ok(()) => {}
                Err((conn, _)) => drop(conn),
            }
        }
    }

    // -- internal -------------------------------------------------------------

    fn open_with_flags_inner(path: &str, flags: compat::OpenFlags) -> Result<Self, FrankenError> {
        check_connection_admission()?;
        if flags.contains(compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY) {
            return Self::open_strict_readonly(path, flags);
        }
        let rusqlite_flags = flags.to_rusqlite();
        let inner = rusqlite::Connection::open_with_flags(path, rusqlite_flags)
            .map_err(|error| FrankenError::from_rusqlite(error, Some(Path::new(path))))?;
        Ok(Self {
            inner: Some(inner),
            path: Some(PathBuf::from(path)),
            savepoint_seq: AtomicU64::new(0),
        })
    }

    /// Strict read-only open *in place*: not one byte, mtime, or sidecar of
    /// the db, `-wal`, `-shm`, or `-journal` may be created or modified.
    /// Mirrors `franken_agent_detection`'s proven
    /// `open_external_strict_in_place`:
    ///
    /// * **Mode 1** — non-WAL header (`header[18..20] != (2,_)`) and no
    ///   `-wal`/`-shm` sidecars: plain `READ_ONLY | NO_MUTEX` open; a
    ///   rollback-mode store creates no sidecar on a read-only open.
    /// * **Mode 2** (`#[cfg(unix)]`, `-wal` + `-shm` present): both are
    ///   validated byte-for-byte, then the store opens through a
    ///   `file:...?mode=ro&readonly_shm=1` URI so C SQLite mmaps the
    ///   wal-index read-only instead of `O_RDWR`/`PROT_WRITE` mutating it.
    /// * **Every other WAL/sidecar shape** — WAL header with missing or
    ///   invalid `-shm`, `-wal` without `-shm`, `-shm` without `-wal`, or
    ///   a live `-journal` — is refused with `BusyRecovery` *without* an
    ///   open on the original path.
    fn open_strict_readonly(path: &str, flags: compat::OpenFlags) -> Result<Self, FrankenError> {
        let db_path = Path::new(path);
        let meta = std::fs::metadata(db_path).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => FrankenError::CannotOpen {
                path: db_path.to_path_buf(),
            },
            _ => FrankenError::Io(error),
        })?;
        if !meta.is_file() {
            return Err(FrankenError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("SQLite path is not a regular file: {}", db_path.display()),
            )));
        }
        if meta.len() < SQLITE_HEADER_PROBE_SIZE as u64 {
            return Err(FrankenError::NotADatabase {
                path: db_path.to_path_buf(),
            });
        }
        let mut header = [0_u8; SQLITE_HEADER_PROBE_SIZE];
        std::fs::File::open(db_path)
            .and_then(|mut file| file.read_exact(&mut header))
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    FrankenError::NotADatabase {
                        path: db_path.to_path_buf(),
                    }
                } else {
                    FrankenError::Io(error)
                }
            })?;
        if header[..16] != SQLITE_HEADER_MAGIC {
            return Err(FrankenError::NotADatabase {
                path: db_path.to_path_buf(),
            });
        }

        // A live rollback journal means the store needs recovery a
        // read-only open must not perform.
        let journal_path = sidecar_path(db_path, "-journal");
        if std::fs::metadata(&journal_path).is_ok_and(|meta| meta.is_file() && meta.len() > 0) {
            let mut first = [0_u8; 1];
            match std::fs::File::open(&journal_path).and_then(|mut f| f.read_exact(&mut first)) {
                Ok(()) if first[0] != 0 => return Err(FrankenError::BusyRecovery),
                Ok(()) => {}
                Err(error) => return Err(FrankenError::Io(error)),
            }
        }

        let is_wal_header = header[18] == 2 || header[19] == 2;
        let wal_path = sidecar_path(db_path, "-wal");
        let shm_path = sidecar_path(db_path, "-shm");
        let wal_exists = sidecar_exists(&wal_path);
        let shm_exists = sidecar_exists(&shm_path);

        let mut open_flags = flags.to_rusqlite();
        // A strict read-only open strips any write/create bits the caller
        // may have combined — the engine must never create or mutate.
        open_flags.remove(
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        );
        open_flags.insert(
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        );

        // Mode 1 — pure rollback-mode store: no WAL marker anywhere, so a
        // read-only open cannot create `-wal` / `-shm`.
        if !is_wal_header && !wal_exists && !shm_exists {
            let inner = rusqlite::Connection::open_with_flags(path, open_flags)
                .map_err(|error| FrankenError::from_rusqlite(error, Some(db_path)))?;
            return Ok(Self {
                inner: Some(inner),
                path: Some(PathBuf::from(path)),
                savepoint_seq: AtomicU64::new(0),
            });
        }

        // Mode 2 (Unix only) — WAL store with both sidecars: validate each
        // byte-for-byte, then open through the `readonly_shm=1` URI.
        #[cfg(unix)]
        if wal_exists && shm_exists {
            return Self::open_wal_strict_in_place(db_path, path, &wal_path, &shm_path);
        }
        #[cfg(not(unix))]
        let _ = (wal_exists, shm_exists, open_flags);

        // Trap 1 (and every ambiguous sidecar shape): never let C SQLite
        // near the original path; callers snapshot-copy on BusyRecovery.
        Err(FrankenError::BusyRecovery)
    }

    /// Mode 2 of `open_strict_readonly`: a WAL store whose `-wal` and
    /// `-shm` both exist and validate is opened in place through a
    /// `mode=ro&readonly_shm=1` URI — C SQLite mmaps the wal-index
    /// read-only and takes read marks through locks instead of writing
    /// them: zero bytes and zero mtime changes on all three files.
    #[cfg(unix)]
    fn open_wal_strict_in_place(
        db_path: &Path,
        path: &str,
        wal_path: &Path,
        shm_path: &Path,
    ) -> Result<Self, FrankenError> {
        if !wal_file_is_valid(wal_path)? || !shm_file_is_valid(shm_path)? {
            return Err(FrankenError::BusyRecovery);
        }
        let uri = read_only_uri(db_path);
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_URI
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let inner = match rusqlite::Connection::open_with_flags(uri.as_str(), flags) {
            Ok(inner) => inner,
            Err(error) if wal_index_open_failure(&error) => {
                return Err(FrankenError::BusyRecovery);
            }
            Err(error) => {
                return Err(FrankenError::from_rusqlite(error, Some(db_path)));
            }
        };
        // The schema read forces C SQLite to attach the wal-index now, so
        // a store that only fails on first access reports `BusyRecovery`
        // here instead of mid-scan.
        match inner.query_row("PRAGMA schema_version;", [], |row| row.get::<usize, i64>(0)) {
            Ok(_) => Ok(Self {
                inner: Some(inner),
                path: Some(PathBuf::from(path)),
                savepoint_seq: AtomicU64::new(0),
            }),
            Err(error) if wal_index_open_failure(&error) => {
                drop(inner);
                Err(FrankenError::BusyRecovery)
            }
            Err(error) => Err(FrankenError::from_rusqlite(error, Some(db_path))),
        }
    }

    /// Cancellation-checked operation runner: pre-checks the TLS scope,
    /// registers this connection's interrupt handle so `cancel()` lands in
    /// `sqlite3_interrupt`, then maps errors (Interrupted → Abort when the
    /// scope latched).
    fn guarded<'s, T>(
        &'s self,
        op: impl FnOnce(&'s rusqlite::Connection) -> Result<T, rusqlite::Error>,
    ) -> Result<T, FrankenError> {
        let cancellation = current_cancellation();
        if cancellation.as_ref().is_some_and(|c| c.is_cancelled()) {
            return Err(FrankenError::Abort);
        }
        let conn = self.try_raw()?;
        let _registration = cancellation
            .as_ref()
            .map(|c| c.register(conn.get_interrupt_handle()))
            .transpose()?;
        // Re-check AFTER registering: a cancel() landing between the
        // pre-check and the push would interrupt() while
        // nVdbeActive == 0 (a no-op) — bail before op() starts so the
        // pre-step interrupt is never silently lost.
        if cancellation.as_ref().is_some_and(|c| c.is_cancelled()) {
            return Err(FrankenError::Abort);
        }
        match op(conn) {
            Ok(value) => Ok(value),
            Err(error) => Err(self.map_err_cancellable(error)),
        }
    }

    /// Same wrapper for ops whose inner error channel is already
    /// `FrankenError` (e.g. per-row callbacks); rusqlite errors inside must
    /// still flow through `map_err_cancellable`.
    fn guarded_result<'s, T>(
        &'s self,
        op: impl FnOnce(&'s rusqlite::Connection) -> Result<T, FrankenError>,
    ) -> Result<T, FrankenError> {
        let cancellation = current_cancellation();
        if cancellation.as_ref().is_some_and(|c| c.is_cancelled()) {
            return Err(FrankenError::Abort);
        }
        let conn = self.try_raw()?;
        let _registration = cancellation
            .as_ref()
            .map(|c| c.register(conn.get_interrupt_handle()))
            .transpose()?;
        if cancellation.as_ref().is_some_and(|c| c.is_cancelled()) {
            return Err(FrankenError::Abort);
        }
        op(conn)
    }

    /// Map a rusqlite error; `Interrupted`/`Abort` become `Abort` when the
    /// TLS cancellation scope is latched.
    fn map_err_cancellable(&self, error: rusqlite::Error) -> FrankenError {
        let mapped = FrankenError::from_rusqlite(error, self.path.as_deref());
        if mapped.is_interrupt_class() && current_is_cancelled() {
            return FrankenError::Abort;
        }
        mapped
    }

    /// Direct (uncancellable) batch execution — used for mandatory rollback
    /// paths that must run even inside a cancelled scope, and safe on a
    /// closed connection (returns `Internal`, never panics).
    fn execute_direct(&self, sql: &str) -> Result<(), FrankenError> {
        let conn = self.try_raw()?;
        conn.execute_batch(sql)
            .map_err(|error| FrankenError::from_rusqlite(error, self.path.as_deref()))
    }
}

/// Narrow compatibility view returned by `Connection::as_async()`.
///
/// The engine is synchronous now; this only preserves the
/// `.as_async().in_transaction()` shape used by semantic replay and archive
/// code.
pub struct AsyncView<'conn> {
    inner: &'conn rusqlite::Connection,
}

impl AsyncView<'_> {
    /// `true` while SQLite reports a live (non-autocommit) transaction.
    pub fn in_transaction(&self) -> bool {
        !self.inner.is_autocommit()
    }

    /// `true` while the connection is in autocommit mode.
    pub fn is_autocommit(&self) -> bool {
        self.inner.is_autocommit()
    }

    /// Historical in-memory-hydration counter. Under rusqlite every query
    /// materializes through real SQLite rows, so the count is reported as
    /// saturated (`usize::MAX` = "always hydrated").
    pub fn memdb_row_hydration_count(&self) -> usize {
        usize::MAX
    }

    /// Direct access to the underlying rusqlite connection.
    pub fn raw(&self) -> &rusqlite::Connection {
        self.inner
    }
}

impl Clone for AsyncView<'_> {
    fn clone(&self) -> Self {
        *self
    }
}
impl Copy for AsyncView<'_> {}

fn row_i64(row: &Row, index: usize) -> i64 {
    match row.get(index) {
        Some(SqliteValue::Integer(value)) => *value,
        Some(SqliteValue::Float(value)) => *value as i64,
        _ => 0,
    }
}

/// RAII helper restoring the original `PRAGMA busy_timeout` after a
/// slice-bounded retry loop rewrote it (`None` = pragma unreadable → do not
/// restore).
struct BusyTimeoutRestore<'c> {
    conn: &'c rusqlite::Connection,
    original_ms: Option<i64>,
}

impl<'c> BusyTimeoutRestore<'c> {
    fn arm(conn: &'c rusqlite::Connection, original_ms: Option<i64>) -> Self {
        Self { conn, original_ms }
    }
}

impl Drop for BusyTimeoutRestore<'_> {
    fn drop(&mut self) {
        if let Some(original_ms) = self.original_ms {
            let _ = self
                .conn
                .busy_timeout(Duration::from_millis(original_ms.max(0) as u64));
        }
    }
}

// ---------------------------------------------------------------------------
// Strict read-only sidecar validation (ported verbatim from
// franken_agent_detection::connectors::sqlite_sync — proven in production)
// ---------------------------------------------------------------------------

/// The SQLite database file magic: bytes 0..16 of a database header
/// (`SQLite format 3\0`). A file without it is not a database, however an
/// engine build might otherwise treat it.
const SQLITE_HEADER_MAGIC: [u8; 16] = *b"SQLite format 3\0";

/// Length of the standard SQLite database file header in bytes.
const SQLITE_HEADER_PROBE_SIZE: usize = 100;

/// Length of the standard SQLite WAL header in bytes.
///
/// A `-wal` file at or below this length holds no transaction frames
/// (read-only clients commonly create a 0-byte or 32-byte frameless `-wal`).
const SQLITE_WAL_HEADER_SIZE: usize = 32;

/// Documented SQLite WAL header magic values (little-endian / big-endian
/// checksum byte order) and format version `3007000`.
const SQLITE_WAL_MAGIC_LE: u32 = 0x377f_0682;
const SQLITE_WAL_MAGIC_BE: u32 = 0x377f_0683;
const SQLITE_WAL_FORMAT_VERSION: u32 = 3_007_000;

/// Length of the SQLite wal-index header in bytes.
///
/// The `-shm` file starts with two 48-byte copies of the header, so the
/// file must hold at least those 96 bytes; real wal-indexes additionally
/// carry at least one checkpoint-block region, and a file under the
/// documented minimum of 136 bytes can never be a usable wal-index.
const SQLITE_SHM_MIN_SIZE: u64 = 136;
const SQLITE_SHM_HEADER_PAIR: usize = 96;

fn sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = db_path.as_os_str().to_os_string();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

/// Whether the sidecar exists at all (any filesystem object).
fn sidecar_exists(path: &Path) -> bool {
    path.symlink_metadata().is_ok()
}

#[cfg(unix)]
const fn read_be_u32(buf: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ])
}

/// Whether the `-wal` file's header is internally sane. A 0-byte `-wal` is
/// valid (a cleanly checkpointed store); anything shorter than the 32-byte
/// header, or a header whose magic, format version, or page size is wrong,
/// means the wal-index cannot be trusted and the store must not be opened
/// in place.
#[cfg(unix)]
fn wal_file_is_valid(wal_path: &Path) -> Result<bool, FrankenError> {
    let meta = match std::fs::metadata(wal_path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(FrankenError::Io(error)),
    };
    if !meta.is_file() {
        return Ok(false);
    }
    let len = meta.len();
    if len == 0 {
        return Ok(true);
    }
    if len < SQLITE_WAL_HEADER_SIZE as u64 {
        return Ok(false);
    }
    let mut header = [0_u8; SQLITE_WAL_HEADER_SIZE];
    let mut file = match std::fs::File::open(wal_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(FrankenError::Io(error)),
    };
    file.read_exact(&mut header).map_err(FrankenError::Io)?;
    let magic = read_be_u32(&header, 0);
    if magic != SQLITE_WAL_MAGIC_LE && magic != SQLITE_WAL_MAGIC_BE {
        return Ok(false);
    }
    if read_be_u32(&header, 4) != SQLITE_WAL_FORMAT_VERSION {
        return Ok(false);
    }
    let page_size = read_be_u32(&header, 8);
    Ok(page_size == 65_536 || (512..65_536).contains(&page_size) && page_size.is_power_of_two())
}

/// The SQLite wal-index header checksum: five pairs of native-endian
/// `u32`s over the header's first 40 bytes.
#[cfg(unix)]
fn wal_index_checksum(header_40: &[u8]) -> (u32, u32) {
    let mut s0 = 0_u32;
    let mut s1 = 0_u32;
    for pair in header_40.chunks_exact(8) {
        let x0 = u32::from_ne_bytes([pair[0], pair[1], pair[2], pair[3]]);
        let x1 = u32::from_ne_bytes([pair[4], pair[5], pair[6], pair[7]]);
        s0 = s0.wrapping_add(x0).wrapping_add(s1);
        s1 = s1.wrapping_add(x1).wrapping_add(s0);
    }
    (s0, s1)
}

/// Whether the `-shm` file is a coherent wal-index: a regular file at
/// least [`SQLITE_SHM_MIN_SIZE`] bytes, whose two 48-byte header copies
/// agree and whose first header carries format version `3007000`, the
/// `isInit` marker, a plausible `bigEndCksum` flag, a valid `szPage`, and a
/// matching header checksum.
#[cfg(unix)]
fn shm_file_is_valid(shm_path: &Path) -> Result<bool, FrankenError> {
    let meta = match std::fs::metadata(shm_path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(FrankenError::Io(error)),
    };
    if !meta.is_file() || meta.len() < SQLITE_SHM_MIN_SIZE {
        return Ok(false);
    }
    let mut buf = [0_u8; SQLITE_SHM_HEADER_PAIR];
    let mut file = std::fs::File::open(shm_path).map_err(FrankenError::Io)?;
    file.read_exact(&mut buf).map_err(FrankenError::Io)?;
    let (h0, h1) = buf.split_at(48);
    if h0 != h1 {
        return Ok(false);
    }
    if u32::from_ne_bytes([h0[0], h0[1], h0[2], h0[3]]) != SQLITE_WAL_FORMAT_VERSION {
        return Ok(false);
    }
    if h0[12] != 1 || h0[13] > 1 {
        return Ok(false);
    }
    let sz_page = u16::from_ne_bytes([h0[14], h0[15]]);
    if !(sz_page == 1 || (sz_page >= 512 && sz_page.is_power_of_two())) {
        return Ok(false);
    }
    let (s0, s1) = wal_index_checksum(&h0[..40]);
    Ok(s0 == u32::from_ne_bytes([h0[40], h0[41], h0[42], h0[43]])
        && s1 == u32::from_ne_bytes([h0[44], h0[45], h0[46], h0[47]]))
}

/// Build a `file:` URI for `db_path` in pure `std` (no `urlencoding`
/// crate): iterate the raw path bytes so non-UTF-8 names survive,
/// collapse consecutive `/` into one, keep `/` itself and every other
/// printable ASCII byte unescaped, and percent-encode `%`, `?`, `#`,
/// every byte `<= 0x20`, and every non-ASCII byte `>= 0x80` —
/// `sqlite3ParseUri` decodes each `%XX` back into the exact raw byte.
#[cfg(unix)]
fn read_only_uri(db_path: &Path) -> String {
    use std::os::unix::ffi::OsStrExt as _;

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let absolute = std::fs::canonicalize(db_path)
        .unwrap_or_else(|_| std::path::absolute(db_path).unwrap_or_else(|_| db_path.to_path_buf()));
    let mut encoded = String::with_capacity(absolute.as_os_str().len() + 16);
    let mut prev_was_slash = false;
    for &byte in absolute.as_os_str().as_bytes() {
        if byte == b'/' {
            if !prev_was_slash {
                encoded.push('/');
            }
            prev_was_slash = true;
            continue;
        }
        prev_was_slash = false;
        if byte <= b' ' || matches!(byte, b'%' | b'?' | b'#') || byte >= 0x80 {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        } else {
            encoded.push(char::from(byte));
        }
    }
    format!("file:{encoded}?mode=ro&readonly_shm=1")
}

/// Whether an open or first-read failure is the WAL-index class a snapshot
/// copy resolves (busy, cannot-open, or read-only), as opposed to the
/// caller's own problem (SQL errors, missing file, ...).
#[cfg(unix)]
const fn wal_index_open_failure(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::DatabaseBusy
                    | rusqlite::ErrorCode::CannotOpen
                    | rusqlite::ErrorCode::ReadOnly,
                ..
            },
            ..
        )
    )
}

// ---------------------------------------------------------------------------
// PreparedStatement
// ---------------------------------------------------------------------------

/// Prepared statement wrapper holding the rusqlite statement under a
/// `RefCell` so all operations take `&self` (facade contract).
pub struct PreparedStatement<'conn> {
    stmt: RefCell<rusqlite::Statement<'conn>>,
    connection: &'conn Connection,
}

impl<'conn> PreparedStatement<'conn> {
    /// Prepare `sql` against `connection`.
    pub fn new(connection: &'conn Connection, sql: &str) -> Result<Self, FrankenError> {
        connection.prepare(sql)
    }

    /// Materialize every row.
    pub fn query(&self) -> Result<Vec<Row>, FrankenError> {
        self.query_with_params(&[])
    }

    /// Materialize every row with bound parameters.
    pub fn query_with_params(&self, params: &[SqliteValue]) -> Result<Vec<Row>, FrankenError> {
        let mut out = Vec::new();
        self.query_with_params_for_each(params, |row| {
            out.push(row.clone());
            Ok(())
        })?;
        Ok(out)
    }

    /// Streaming per-row callback with bound parameters.
    pub fn query_with_params_for_each<F>(
        &self,
        params: &[SqliteValue],
        mut f: F,
    ) -> Result<(), FrankenError>
    where
        F: FnMut(&Row) -> Result<(), FrankenError>,
    {
        let conn = self.connection.try_raw()?;
        let _registration = current_cancellation()
            .map(|c| c.register(conn.get_interrupt_handle()))
            .transpose()?;
        check_connection_admission()?;
        if current_is_cancelled() {
            return Err(FrankenError::Abort);
        }
        let refs = dynamic_params(params);
        let mut stmt = self.stmt.borrow_mut();
        let column_count = stmt.column_count();
        let mut rows = stmt
            .query(&refs[..])
            .map_err(|error| self.connection.map_err_cancellable(error))?;
        if current_is_cancelled() {
            return Err(FrankenError::Abort);
        }
        loop {
            match rows.next() {
                Err(error) => return Err(self.connection.map_err_cancellable(error)),
                Ok(None) => return Ok(()),
                Ok(Some(row)) => {
                    let materialized = materialize_row(row, column_count)?;
                    f(&materialized)?;
                }
            }
        }
    }

    /// Streaming per-row callback (no params).
    pub fn query_for_each<F>(&self, f: F) -> Result<(), FrankenError>
    where
        F: FnMut(&Row) -> Result<(), FrankenError>,
    {
        self.query_with_params_for_each(&[], f)
    }

    /// Execute the prepared statement, discarding any returned rows.
    pub fn execute(&self) -> Result<usize, FrankenError> {
        self.execute_with_params(&[])
    }

    /// Execute with bound parameters. `stmt.query` is used unconditionally
    /// (`raw_execute` still fails with `ExecuteReturnedResults` on
    /// row-returning statements AND leaves prior parameter bindings in
    /// place on the cached statement).
    pub fn execute_with_params(&self, params: &[SqliteValue]) -> Result<usize, FrankenError> {
        let conn = self.connection.try_raw()?;
        let _registration = current_cancellation()
            .map(|c| c.register(conn.get_interrupt_handle()))
            .transpose()?;
        check_connection_admission()?;
        if current_is_cancelled() {
            return Err(FrankenError::Abort);
        }
        let refs = dynamic_params(params);
        let mut stmt = self.stmt.borrow_mut();
        stmt.query(&refs[..])
            .and_then(|mut rows| {
                while let Some(_row) = rows.next()? {}
                Ok(())
            })
            .map_err(|error| self.connection.map_err_cancellable(error))?;
        Ok(conn.changes() as usize)
    }

    /// Execute expecting exactly one row.
    pub fn query_row(&self) -> Result<Row, FrankenError> {
        self.query_row_with_params(&[])
    }

    /// `query_row` with bound parameters.
    pub fn query_row_with_params(&self, params: &[SqliteValue]) -> Result<Row, FrankenError> {
        let conn = self.connection.try_raw()?;
        let _registration = current_cancellation()
            .map(|c| c.register(conn.get_interrupt_handle()))
            .transpose()?;
        check_connection_admission()?;
        if current_is_cancelled() {
            return Err(FrankenError::Abort);
        }
        let refs = dynamic_params(params);
        let mut stmt = self.stmt.borrow_mut();
        let column_count = stmt.column_count();
        let mut rows = stmt
            .query(&refs[..])
            .map_err(|error| self.connection.map_err_cancellable(error))?;
        let first = match rows.next() {
            Err(error) => return Err(self.connection.map_err_cancellable(error)),
            Ok(None) => return Err(FrankenError::QueryReturnedNoRows),
            Ok(Some(row)) => materialize_row(row, column_count)?,
        };
        match rows.next() {
            Err(error) => Err(self.connection.map_err_cancellable(error)),
            Ok(None) => Ok(first),
            Ok(Some(_)) => Err(FrankenError::QueryReturnedMultipleRows),
        }
    }
}

// ---------------------------------------------------------------------------
// compat module — rusqlite-shaped API surface
// ---------------------------------------------------------------------------

pub mod compat {
    //! Compatibility surface mirroring the rusqlite-style API the old facade
    //! exposed (`open_with_flags`, `ParamValue`, `params!` consumers,
    //! `FromSqliteValue`, `RowExt`, `OptionalExtension`, `ConnectionExt`,
    //! `Transaction`, `TransactionExt`, `TransactionBehavior`, `Savepoint`).

    use super::{Connection, FrankenError, Row, SqliteValue};
    use std::fmt;
    use std::ops::{
        BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, Deref, Not, Sub, SubAssign,
    };

    pub use super::params;
    pub use rusqlite::TransactionBehavior;

    // -- OpenFlags ------------------------------------------------------------

    /// `SQLITE_OPEN_*` bitflags, mirroring the C constants plus the facade's
    /// custom `SQLITE_OPEN_STRICT_READ_ONLY` (never creates/mutates `-wal` or
    /// `-shm`; refuses with `BusyRecovery` when WAL-index recovery is needed).
    #[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct OpenFlags {
        bits: u32,
    }

    impl fmt::Debug for OpenFlags {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "OpenFlags({:#x})", self.bits)
        }
    }

    impl OpenFlags {
        pub const SQLITE_OPEN_READ_ONLY: Self = Self::from_bits_retain(0x0000_0001);
        pub const SQLITE_OPEN_READ_WRITE: Self = Self::from_bits_retain(0x0000_0002);
        pub const SQLITE_OPEN_CREATE: Self = Self::from_bits_retain(0x0000_0004);
        pub const SQLITE_OPEN_DELETE_ON_CLOSE: Self = Self::from_bits_retain(0x0000_0008);
        pub const SQLITE_OPEN_EXCLUSIVE: Self = Self::from_bits_retain(0x0000_0010);
        pub const SQLITE_OPEN_AUTO_PROXY: Self = Self::from_bits_retain(0x0000_0020);
        pub const SQLITE_OPEN_URI: Self = Self::from_bits_retain(0x0000_0040);
        pub const SQLITE_OPEN_MEMORY: Self = Self::from_bits_retain(0x0000_0080);
        pub const SQLITE_OPEN_MAIN_DB: Self = Self::from_bits_retain(0x0000_0100);
        pub const SQLITE_OPEN_TEMP_DB: Self = Self::from_bits_retain(0x0000_0200);
        pub const SQLITE_OPEN_TRANSIENT_DB: Self = Self::from_bits_retain(0x0000_0400);
        pub const SQLITE_OPEN_MAIN_JOURNAL: Self = Self::from_bits_retain(0x0000_0800);
        pub const SQLITE_OPEN_TEMP_JOURNAL: Self = Self::from_bits_retain(0x0000_1000);
        pub const SQLITE_OPEN_TRANSIENT_JOURNAL: Self = Self::from_bits_retain(0x0000_2000);
        pub const SQLITE_OPEN_SUBJOURNAL: Self = Self::from_bits_retain(0x0000_4000);
        pub const SQLITE_OPEN_SUPER_JOURNAL: Self = Self::from_bits_retain(0x0000_8000);
        pub const SQLITE_OPEN_NO_MUTEX: Self = Self::from_bits_retain(0x0001_0000);
        pub const SQLITE_OPEN_FULL_MUTEX: Self = Self::from_bits_retain(0x0002_0000);
        pub const SQLITE_OPEN_SHARED_CACHE: Self = Self::from_bits_retain(0x0004_0000);
        pub const SQLITE_OPEN_PRIVATE_CACHE: Self = Self::from_bits_retain(0x0008_0000);
        pub const SQLITE_OPEN_WAL: Self = Self::from_bits_retain(0x0010_0000);
        pub const SQLITE_OPEN_NOFOLLOW: Self = Self::from_bits_retain(0x0100_0000);
        /// Facade-private flag (never passed to SQLite): refuse any open that
        /// would need to create, rebuild, or mutate the `-wal`/`-shm`
        /// sidecars.
        pub const SQLITE_OPEN_STRICT_READ_ONLY: Self = Self::from_bits_retain(0x4000_0000);

        pub const fn empty() -> Self {
            Self { bits: 0 }
        }

        pub const fn all() -> Self {
            Self { bits: u32::MAX }
        }

        pub const fn from_bits(bits: u32) -> Option<Self> {
            Some(Self { bits })
        }

        pub const fn from_bits_truncate(bits: u32) -> Self {
            Self { bits }
        }

        pub const fn from_bits_retain(bits: u32) -> Self {
            Self { bits }
        }

        pub const fn bits(&self) -> u32 {
            self.bits
        }

        pub const fn is_empty(&self) -> bool {
            self.bits == 0
        }

        pub const fn contains(&self, other: Self) -> bool {
            (self.bits & other.bits) == other.bits
        }

        pub const fn intersects(&self, other: Self) -> bool {
            (self.bits & other.bits) != 0
        }

        pub fn insert(&mut self, other: Self) {
            self.bits |= other.bits;
        }

        pub fn remove(&mut self, other: Self) {
            self.bits &= !other.bits;
        }

        pub fn toggle(&mut self, other: Self) {
            self.bits ^= other.bits;
        }

        /// Convert to rusqlite's `OpenFlags` (i32 bits), stripping
        /// facade-private bits.
        pub(crate) fn to_rusqlite(self) -> rusqlite::OpenFlags {
            rusqlite::OpenFlags::from_bits_truncate(
                (self.bits & !Self::SQLITE_OPEN_STRICT_READ_ONLY.bits) as i32,
            )
        }
    }

    impl BitOr for OpenFlags {
        type Output = Self;
        fn bitor(self, rhs: Self) -> Self {
            Self::from_bits_retain(self.bits | rhs.bits)
        }
    }
    impl BitOrAssign for OpenFlags {
        fn bitor_assign(&mut self, rhs: Self) {
            self.insert(rhs);
        }
    }
    impl BitAnd for OpenFlags {
        type Output = Self;
        fn bitand(self, rhs: Self) -> Self {
            Self::from_bits_retain(self.bits & rhs.bits)
        }
    }
    impl BitAndAssign for OpenFlags {
        fn bitand_assign(&mut self, rhs: Self) {
            self.bits &= rhs.bits;
        }
    }
    impl BitXor for OpenFlags {
        type Output = Self;
        fn bitxor(self, rhs: Self) -> Self {
            Self::from_bits_retain(self.bits ^ rhs.bits)
        }
    }
    impl BitXorAssign for OpenFlags {
        fn bitxor_assign(&mut self, rhs: Self) {
            self.bits ^= rhs.bits;
        }
    }
    impl Sub for OpenFlags {
        type Output = Self;
        fn sub(self, rhs: Self) -> Self {
            Self::from_bits_retain(self.bits & !rhs.bits)
        }
    }
    impl SubAssign for OpenFlags {
        fn sub_assign(&mut self, rhs: Self) {
            self.remove(rhs);
        }
    }
    impl Not for OpenFlags {
        type Output = Self;
        fn not(self) -> Self {
            Self::from_bits_retain(!self.bits)
        }
    }

    // -- ParamValue ------------------------------------------------------------

    /// Owned parameter value mirroring the rusqlite `params!` element type.
    #[derive(Clone, Debug, PartialEq)]
    pub struct ParamValue {
        value: SqliteValue,
    }

    impl ParamValue {
        pub fn new<T: Into<SqliteValue>>(value: T) -> Self {
            Self {
                value: value.into(),
            }
        }

        pub fn value(&self) -> &SqliteValue {
            &self.value
        }

        pub fn into_value(self) -> SqliteValue {
            self.value
        }
    }

    impl<T: Into<SqliteValue>> From<T> for ParamValue {
        fn from(value: T) -> Self {
            Self::new(value)
        }
    }

    /// `&[ParamValue]` → `Vec<SqliteValue>` bridge used by `query_params` /
    /// `execute_compat` call sites.
    pub fn param_slice_to_values(params: &[ParamValue]) -> Vec<SqliteValue> {
        params.iter().map(|param| param.value.clone()).collect()
    }

    /// Iterator → `Vec<SqliteValue>` bridge (`params_from_iter([ParamValue,
    /// ...])` call sites).
    pub fn params_from_iter<I, T>(iter: I) -> Vec<SqliteValue>
    where
        I: IntoIterator<Item = T>,
        T: Into<ParamValue>,
    {
        iter.into_iter()
            .map(|item| item.into().into_value())
            .collect()
    }

    /// Open `path` with explicit `OpenFlags`. Honors the facade-private
    /// `SQLITE_OPEN_STRICT_READ_ONLY` bit.
    pub fn open_with_flags(
        path: impl AsRef<str>,
        flags: OpenFlags,
    ) -> Result<Connection, FrankenError> {
        Connection::open_with_flags_inner(path.as_ref(), flags)
    }

    // -- FromSqliteValue -------------------------------------------------------

    /// Typed cell extraction from `SqliteValue` (`RowExt::get_typed`).
    pub trait FromSqliteValue: Sized {
        fn from_sqlite_value(value: &SqliteValue) -> Result<Self, FrankenError>;
    }

    fn type_error(expected: &'static str, value: &SqliteValue) -> FrankenError {
        let _ = expected;
        let _ = value;
        FrankenError::DatatypeMismatch
    }

    macro_rules! impl_from_sqlite_value_int {
        ($($ty:ty),+ $(,)?) => {
            $(
                impl FromSqliteValue for $ty {
                    fn from_sqlite_value(value: &SqliteValue) -> Result<Self, FrankenError> {
                        match value {
                            SqliteValue::Integer(v) => <$ty>::try_from(*v)
                                .map_err(|_| FrankenError::DatatypeMismatch),
                            SqliteValue::Float(v) => Ok(*v as $ty),
                            _ => Err(type_error("integer", value)),
                        }
                    }
                }
            )+
        };
    }

    impl_from_sqlite_value_int!(i64, i32, i16, i8, u64, u32, u16, u8, isize, usize);

    impl FromSqliteValue for f64 {
        fn from_sqlite_value(value: &SqliteValue) -> Result<Self, FrankenError> {
            match value {
                SqliteValue::Integer(v) => Ok(*v as f64),
                SqliteValue::Float(v) => Ok(*v),
                _ => Err(type_error("real", value)),
            }
        }
    }

    impl FromSqliteValue for f32 {
        fn from_sqlite_value(value: &SqliteValue) -> Result<Self, FrankenError> {
            match value {
                SqliteValue::Integer(v) => Ok(*v as f32),
                SqliteValue::Float(v) => Ok(*v as f32),
                _ => Err(type_error("real", value)),
            }
        }
    }

    impl FromSqliteValue for bool {
        fn from_sqlite_value(value: &SqliteValue) -> Result<Self, FrankenError> {
            match value {
                SqliteValue::Integer(v) => Ok(*v != 0),
                _ => Err(type_error("integer", value)),
            }
        }
    }

    impl FromSqliteValue for String {
        fn from_sqlite_value(value: &SqliteValue) -> Result<Self, FrankenError> {
            match value {
                SqliteValue::Text(v) => Ok(v.clone()),
                _ => Err(type_error("text", value)),
            }
        }
    }

    impl FromSqliteValue for Box<str> {
        fn from_sqlite_value(value: &SqliteValue) -> Result<Self, FrankenError> {
            match value {
                SqliteValue::Text(v) => Ok(v.clone().into_boxed_str()),
                _ => Err(type_error("text", value)),
            }
        }
    }

    impl FromSqliteValue for Vec<u8> {
        fn from_sqlite_value(value: &SqliteValue) -> Result<Self, FrankenError> {
            match value {
                SqliteValue::Blob(v) => Ok(v.clone()),
                _ => Err(type_error("blob", value)),
            }
        }
    }

    impl FromSqliteValue for Box<[u8]> {
        fn from_sqlite_value(value: &SqliteValue) -> Result<Self, FrankenError> {
            match value {
                SqliteValue::Blob(v) => Ok(v.clone().into_boxed_slice()),
                _ => Err(type_error("blob", value)),
            }
        }
    }

    impl FromSqliteValue for SqliteValue {
        fn from_sqlite_value(value: &SqliteValue) -> Result<Self, FrankenError> {
            Ok(value.clone())
        }
    }

    impl<T: FromSqliteValue> FromSqliteValue for Option<T> {
        fn from_sqlite_value(value: &SqliteValue) -> Result<Self, FrankenError> {
            match value {
                SqliteValue::Null => Ok(None),
                other => T::from_sqlite_value(other).map(Some),
            }
        }
    }

    /// `get_typed` for materialized `Row` and borrowed `rusqlite::Row`.
    /// Connector tests import THIS trait for `rusqlite::Row` mapping
    /// closures; `?` on `FrankenError` lands in FAD `SqliteError` through
    /// the `From<FrankenError> for SqliteError` bridge below.
    pub trait RowExt {
        fn get_typed<T: FromSqliteValue>(&self, index: usize) -> Result<T, FrankenError>;
    }

    impl RowExt for Row {
        fn get_typed<T: FromSqliteValue>(&self, index: usize) -> Result<T, FrankenError> {
            let value = self.get(index).ok_or_else(|| FrankenError::NoSuchColumn {
                name: format!("column index {index}"),
            })?;
            T::from_sqlite_value(value)
        }
    }

    impl RowExt for rusqlite::Row<'_> {
        fn get_typed<T: FromSqliteValue>(&self, index: usize) -> Result<T, FrankenError> {
            let value = self
                .get_ref(index)
                .map_err(|error| FrankenError::from_rusqlite(error, None))?;
            let owned = super::sqlite_value_from_ref(value);
            T::from_sqlite_value(&owned)
        }
    }

    /// `?`-bridge so `FrankenError` lands in FAD's connector error type.
    /// Orphan-legal: the `From` parameter (`FrankenError`) is a local type.
    /// The `Sqlite` variant preserves the primary/extended result code and
    /// rendered detail text.
    impl From<FrankenError> for franken_agent_detection::connectors::sqlite_sync::SqliteError {
        fn from(error: FrankenError) -> Self {
            Self::Sqlite(super::franken_error_to_rusqlite(error))
        }
    }

    /// rusqlite `OptionalExtension`: `QueryReturnedNoRows` → `None`.
    pub trait OptionalExtension<T> {
        fn optional(self) -> Result<Option<T>, FrankenError>;
    }

    impl<T> OptionalExtension<T> for Result<T, FrankenError> {
        fn optional(self) -> Result<Option<T>, FrankenError> {
            match self {
                Ok(value) => Ok(Some(value)),
                Err(FrankenError::QueryReturnedNoRows) => Ok(None),
                Err(error) => Err(error),
            }
        }
    }

    // -- ConnectionExt -----------------------------------------------------------

    /// Compatibility trait for rusqlite-shaped helper methods.
    pub trait ConnectionExt {
        /// First row mapped via `f`; `QueryReturnedNoRows` when empty
        /// (rusqlite `query_row` semantics — lenient about extra rows).
        fn query_row_map<T, F>(
            &self,
            sql: &str,
            params: &[ParamValue],
            f: F,
        ) -> Result<T, FrankenError>
        where
            F: FnOnce(&Row) -> Result<T, FrankenError>;

        /// Collect mapped rows (materialized).
        fn query_map_collect<T, F>(
            &self,
            sql: &str,
            params: &[ParamValue],
            f: F,
        ) -> Result<Vec<T>, FrankenError>
        where
            F: FnMut(&Row) -> Result<T, FrankenError>;

        /// Execute with `ParamValue` parameters.
        fn execute_compat(&self, sql: &str, params: &[ParamValue]) -> Result<usize, FrankenError>;
    }

    impl ConnectionExt for Connection {
        fn query_row_map<T, F>(
            &self,
            sql: &str,
            params: &[ParamValue],
            f: F,
        ) -> Result<T, FrankenError>
        where
            F: FnOnce(&Row) -> Result<T, FrankenError>,
        {
            // Legacy parity: exactly-one semantics (max 2 steps), so
            // `QueryReturnedMultipleRows` still surfaces instead of being
            // silently drained away by a for_each that keeps stepping.
            let values = param_slice_to_values(params);
            let row = self.query_row_with_params(sql, &values)?;
            f(&row)
        }

        fn query_map_collect<T, F>(
            &self,
            sql: &str,
            params: &[ParamValue],
            f: F,
        ) -> Result<Vec<T>, FrankenError>
        where
            F: FnMut(&Row) -> Result<T, FrankenError>,
        {
            let values = param_slice_to_values(params);
            let mut f = f;
            let mut out = Vec::new();
            self.query_with_params_for_each(sql, &values, |row| {
                out.push(f(row)?);
                Ok(())
            })?;
            Ok(out)
        }

        fn execute_compat(&self, sql: &str, params: &[ParamValue]) -> Result<usize, FrankenError> {
            self.execute_with_params(sql, &param_slice_to_values(params))
        }
    }

    // -- Transaction / Savepoint ------------------------------------------------

    enum TxKind {
        Root,
        Savepoint(String),
    }

    /// Root transaction wrapper. `Drop` rolls back when not finished
    /// (`RELEASE`/`COMMIT` for savepoints via `Savepoint`).
    pub struct Transaction<'conn> {
        connection: &'conn Connection,
        kind: TxKind,
        finished: bool,
    }

    impl<'conn> Transaction<'conn> {
        /// Internal constructor used by `Connection::transaction_with_*`.
        pub(crate) fn active(connection: &'conn Connection) -> Self {
            Self {
                connection,
                kind: TxKind::Root,
                finished: false,
            }
        }

        fn savepoint_name(connection: &Connection) -> String {
            let seq = connection
                .savepoint_seq
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            format!("cass_sp_{seq}")
        }

        /// Create a nested `SAVEPOINT` inside this transaction.
        pub fn savepoint(&self) -> Result<Savepoint<'conn>, FrankenError> {
            Savepoint::open(self.connection)
        }

        /// Commit the transaction (`COMMIT` / `RELEASE` for savepoints).
        /// Idempotent: committing an already-finished transaction is a no-op.
        pub fn commit(&mut self) -> Result<(), FrankenError> {
            if self.finished {
                return Ok(());
            }
            let sql = match &self.kind {
                TxKind::Root => "COMMIT".to_string(),
                TxKind::Savepoint(name) => format!("RELEASE \"{name}\""),
            };
            self.connection.execute_direct(&sql)?;
            self.finished = true;
            Ok(())
        }

        /// Roll back (`ROLLBACK` / `ROLLBACK TO + RELEASE`). Mandatory path:
        /// bypasses cancellation so a cancelled scope cannot strand the
        /// transaction.
        pub fn rollback(&mut self) -> Result<(), FrankenError> {
            if self.finished {
                return Ok(());
            }
            let sql = match &self.kind {
                TxKind::Root => "ROLLBACK".to_string(),
                TxKind::Savepoint(name) => {
                    format!("ROLLBACK TO \"{name}\"; RELEASE \"{name}\"")
                }
            };
            self.connection.execute_direct(&sql)?;
            self.finished = true;
            Ok(())
        }

        /// Execute a statement inside the transaction.
        pub fn execute(&self, sql: &str) -> Result<usize, FrankenError> {
            self.connection.execute(sql)
        }

        /// Execute with bound parameters inside the transaction.
        pub fn execute_with_params(
            &self,
            sql: &str,
            params: &[SqliteValue],
        ) -> Result<usize, FrankenError> {
            self.connection.execute_with_params(sql, params)
        }

        /// Execute with `SqliteValue` parameters inside the transaction.
        /// Identical to `execute_with_params` — the name is kept for
        /// source compatibility (legacy call sites pass `&Vec<SqliteValue>`).
        pub fn execute_with_params_skip_statement_savepoint(
            &self,
            sql: &str,
            params: &[SqliteValue],
        ) -> Result<usize, FrankenError> {
            self.connection.execute_with_params(sql, params)
        }

        /// `ParamValue` alias of `execute_with_params`.
        pub fn execute_compat(
            &self,
            sql: &str,
            params: &[ParamValue],
        ) -> Result<usize, FrankenError> {
            self.connection
                .execute_with_params(sql, &param_slice_to_values(params))
        }

        /// Multi-statement script inside the transaction.
        pub fn execute_batch(&self, sql: &str) -> Result<(), FrankenError> {
            self.connection.execute_batch(sql)
        }

        /// Materialize all rows inside the transaction.
        pub fn query(&self, sql: &str) -> Result<Vec<Row>, FrankenError> {
            self.connection.query(sql)
        }

        /// Materialize all rows with bound parameters.
        pub fn query_with_params(
            &self,
            sql: &str,
            params: &[SqliteValue],
        ) -> Result<Vec<Row>, FrankenError> {
            self.connection.query_with_params(sql, params)
        }

        /// `ParamValue` alias of `query_with_params`.
        pub fn query_params(
            &self,
            sql: &str,
            params: &[ParamValue],
        ) -> Result<Vec<Row>, FrankenError> {
            self.connection
                .query_with_params(sql, &param_slice_to_values(params))
        }

        /// Exactly-one-row query inside the transaction.
        pub fn query_row(&self, sql: &str) -> Result<Row, FrankenError> {
            self.connection.query_row(sql)
        }

        /// Exactly-one-row query with bound parameters.
        pub fn query_row_with_params(
            &self,
            sql: &str,
            params: &[SqliteValue],
        ) -> Result<Row, FrankenError> {
            self.connection.query_row_with_params(sql, params)
        }

        /// First-row map (rusqlite `query_row` semantics — exactly-one
        /// parity with `Connection::query_row_with_params`: max 2 steps,
        /// `QueryReturnedMultipleRows` preserved).
        pub fn query_row_map<T, F>(
            &self,
            sql: &str,
            params: &[ParamValue],
            f: F,
        ) -> Result<T, FrankenError>
        where
            F: FnOnce(&Row) -> Result<T, FrankenError>,
        {
            let values = param_slice_to_values(params);
            let row = self.query_row_with_params(sql, &values)?;
            f(&row)
        }

        /// Collect mapped rows inside the transaction.
        pub fn query_map_collect<T, F>(
            &self,
            sql: &str,
            params: &[ParamValue],
            mut f: F,
        ) -> Result<Vec<T>, FrankenError>
        where
            F: FnMut(&Row) -> Result<T, FrankenError>,
        {
            let values = param_slice_to_values(params);
            let mut out = Vec::new();
            self.connection
                .query_with_params_for_each(sql, &values, |row| {
                    out.push(f(row)?);
                    Ok(())
                })?;
            Ok(out)
        }

        /// Rowid of the most recent insert inside the transaction. Kept as
        /// `Result` — legacy callers chain `.ok()` (e.g.
        /// `franken_last_rowid` in storage/sqlite.rs).
        pub fn last_insert_rowid(&self) -> Result<i64, FrankenError> {
            Ok(self.connection.last_insert_rowid())
        }
    }

    impl Drop for Transaction<'_> {
        fn drop(&mut self) {
            if self.finished {
                return;
            }
            let _ = self.rollback();
        }
    }

    /// `SAVEPOINT` wrapper — separate type per the facade contract. On drop,
    /// rolls back to the savepoint and releases it.
    pub struct Savepoint<'conn> {
        tx: Transaction<'conn>,
    }

    impl<'conn> Savepoint<'conn> {
        /// Open `SAVEPOINT cass_sp_N` on `connection`. Goes through
        /// `connection.execute` (not `execute_direct`) so the TLS
        /// cancellation scope is honored.
        pub(crate) fn open(connection: &'conn Connection) -> Result<Self, FrankenError> {
            let name = Transaction::savepoint_name(connection);
            connection.execute(&format!("SAVEPOINT \"{name}\""))?;
            Ok(Self {
                tx: Transaction {
                    connection,
                    kind: TxKind::Savepoint(name),
                    finished: false,
                },
            })
        }

        /// `RELEASE` the savepoint (commits its changes into the outer
        /// transaction).
        pub fn commit(&mut self) -> Result<(), FrankenError> {
            self.tx.commit()
        }

        /// `ROLLBACK TO` + `RELEASE` the savepoint.
        pub fn rollback(&mut self) -> Result<(), FrankenError> {
            self.tx.rollback()
        }
    }

    impl<'conn> Deref for Savepoint<'conn> {
        type Target = Transaction<'conn>;
        fn deref(&self) -> &Self::Target {
            &self.tx
        }
    }

    // -- TransactionExt ----------------------------------------------------------

    /// Compatibility trait exposing `conn.transaction()` /
    /// `conn.transaction_with_behavior(...)`.
    pub trait TransactionExt {
        fn transaction(&self) -> Result<Transaction<'_>, FrankenError>;
        fn transaction_with_behavior(
            &self,
            behavior: TransactionBehavior,
        ) -> Result<Transaction<'_>, FrankenError>;
    }

    impl TransactionExt for Connection {
        fn transaction(&self) -> Result<Transaction<'_>, FrankenError> {
            self.transaction_with_behavior(TransactionBehavior::Deferred)
        }

        fn transaction_with_behavior(
            &self,
            behavior: TransactionBehavior,
        ) -> Result<Transaction<'_>, FrankenError> {
            Connection::transaction_with_behavior(self, behavior)
        }
    }

    impl ConnectionExt for Transaction<'_> {
        fn query_row_map<T, F>(
            &self,
            sql: &str,
            params: &[ParamValue],
            f: F,
        ) -> Result<T, FrankenError>
        where
            F: FnOnce(&Row) -> Result<T, FrankenError>,
        {
            Transaction::query_row_map(self, sql, params, f)
        }

        fn query_map_collect<T, F>(
            &self,
            sql: &str,
            params: &[ParamValue],
            f: F,
        ) -> Result<Vec<T>, FrankenError>
        where
            F: FnMut(&Row) -> Result<T, FrankenError>,
        {
            Transaction::query_map_collect(self, sql, params, f)
        }

        fn execute_compat(&self, sql: &str, params: &[ParamValue]) -> Result<usize, FrankenError> {
            Transaction::execute_compat(self, sql, params)
        }
    }
}

// ---------------------------------------------------------------------------
// migrate module — shared schema-migration runner
// ---------------------------------------------------------------------------

pub mod migrate {
    //! Shared schema-migration runner: applies ordered `(version, name, sql)`
    //! steps into `_schema_migrations`, each in its own `BEGIN IMMEDIATE`
    //! transaction (one transaction per migration step).

    use super::compat::{RowExt, TransactionBehavior};
    use super::{Connection, FrankenError, SqliteValue};

    /// One schema migration step.
    #[derive(Clone, Debug)]
    pub struct Migration {
        pub version: i64,
        pub name: String,
        pub sql: String,
    }

    impl Migration {
        pub fn new(version: i64, name: impl Into<String>, sql: impl Into<String>) -> Self {
            Self {
                version,
                name: name.into(),
                sql: sql.into(),
            }
        }
    }

    /// Outcome of `MigrationRunner::run`.
    #[derive(Clone, Debug, Default)]
    pub struct MigrationResult {
        /// Versions applied by this run, in applied order.
        pub applied: Vec<i64>,
        /// `true` when the database had no recorded schema-migration ledger
        /// rows before this run ("first pass over an empty database is
        /// fresh" — same contract the combined-runner form reported).
        pub was_fresh: bool,
    }

    /// Ordered migration runner: keeps an internal `Vec<Migration>` sorted by
    /// version at `run` time.
    #[derive(Default)]
    pub struct MigrationRunner {
        migrations: Vec<Migration>,
    }

    impl MigrationRunner {
        pub fn new() -> Self {
            Self::default()
        }

        /// Builder-style step registration.
        pub fn add(mut self, version: i64, name: &str, sql: &str) -> Self {
            self.migrations.push(Migration::new(version, name, sql));
            self
        }

        /// Register a step in place.
        pub fn add_step(&mut self, migration: Migration) {
            self.migrations.push(migration);
        }

        /// Run all pending steps, each in its own `BEGIN IMMEDIATE`
        /// transaction (cancel-safe via `transaction_with_behavior`). The
        /// ledger bootstrap DDL itself also runs inside `BEGIN IMMEDIATE`
        /// so it blocks on writer locks only within the slice-bounded,
        /// cancellable retry path rather than uninterruptibly in
        /// autocommit for the full `PRAGMA busy_timeout`.
        pub fn run(&self, conn: &Connection) -> Result<MigrationResult, FrankenError> {
            let ledger_exists = !conn
                .query(
                    "SELECT 1 FROM sqlite_master \
                     WHERE type = 'table' AND name = '_schema_migrations' LIMIT 1",
                )?
                .is_empty();
            let mut was_fresh = true;
            let mut applied_versions = std::collections::BTreeSet::new();
            if ledger_exists {
                for row in conn.query("SELECT version FROM _schema_migrations")? {
                    applied_versions.insert(row.get_typed::<i64>(0)?);
                }
                if !applied_versions.is_empty() {
                    was_fresh = false;
                }
            } else {
                // Ledger bootstrap inside BEGIN IMMEDIATE: serialized with
                // other writers under the retry/cancel path, and the table
                // creation + initial state read are atomic.
                let mut tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                tx.execute_batch(
                    "CREATE TABLE IF NOT EXISTS _schema_migrations (\n                     version INTEGER PRIMARY KEY,\n                     name TEXT NOT NULL,\n                     applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))\n                 )",
                )?;
                tx.commit()?;
            }
            let mut steps = self.migrations.clone();
            steps.sort_by_key(|migration| migration.version);
            let mut applied = Vec::new();
            for step in steps {
                if applied_versions.contains(&step.version) {
                    continue;
                }
                let mut tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                // Re-check inside the immediate transaction: a concurrent
                // runner may have applied this version between our outer
                // read and acquiring the write lock.
                let already = !tx
                    .query_with_params(
                        "SELECT 1 FROM _schema_migrations WHERE version = ?1 LIMIT 1",
                        &[SqliteValue::Integer(step.version)],
                    )?
                    .is_empty();
                if already {
                    tx.rollback()?;
                    continue;
                }
                tx.execute_batch(&step.sql)?;
                tx.execute_with_params(
                    "INSERT INTO _schema_migrations (version, name) VALUES (?1, ?2)",
                    &[
                        SqliteValue::Integer(step.version),
                        SqliteValue::Text(step.name.clone()),
                    ],
                )?;
                tx.commit()?;
                applied.push(step.version);
            }
            Ok(MigrationResult { applied, was_fresh })
        }
    }
}

// ---------------------------------------------------------------------------
// BackupReport
// ---------------------------------------------------------------------------

/// Report produced by `Connection::backup_exact_to` (`VACUUM INTO` snapshot).
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub struct BackupReport {
    /// Byte length of the backup file.
    pub byte_len: u64,
    /// `header[16..18]` page-size field of the backup (1 means 65536).
    pub page_size: u32,
    /// Backup page count (`byte_len / page_size`).
    pub page_count: u32,
}

#[cfg(not(target_arch = "wasm32"))]
fn read_backup_report(path: &Path) -> Result<BackupReport, FrankenError> {
    let mut file = std::fs::File::open(path)?;
    let byte_len = file.metadata()?.len();
    let mut header = [0u8; 100];
    let read = file.read(&mut header)?;
    if read < 100 || !header.starts_with(b"SQLite format 3\0") {
        return Err(FrankenError::Internal(format!(
            "backup target {} is not a database image",
            path.display()
        )));
    }
    let raw_size = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if raw_size == 1 {
        65_536u32
    } else {
        u32::from(raw_size)
    };
    Ok(BackupReport {
        byte_len,
        page_size,
        page_count: if page_size == 0 {
            0
        } else {
            (byte_len / u64::from(page_size)) as u32
        },
    })
}

// ---------------------------------------------------------------------------
// shutdown_driver (compatibility no-op)
// ---------------------------------------------------------------------------

/// Compatibility hook: the async driver is gone, so shutdown is a no-op.
/// Kept because `src/lib.rs` and several test harnesses still call it.
pub(crate) fn shutdown_driver() -> bool {
    true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::compat::{
        self, param_slice_to_values, params_from_iter, ConnectionExt, OptionalExtension,
        ParamValue, RowExt, TransactionExt,
    };
    use super::*;
    use rusqlite::types::ValueRef;
    use std::time::{Duration, Instant};

    fn conn() -> Connection {
        Connection::open(":memory:").expect("open :memory:")
    }

    fn seed_files(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE files(path TEXT NOT NULL, title TEXT NOT NULL);
             INSERT INTO files(path, title) VALUES
                ('/tmp/a.txt', 'alpha'),
                ('/tmp/b.txt', 'beta');",
        )
        .expect("seed");
    }

    #[test]
    fn interrupt_handle_cancels_active_recursive_query_promptly() {
        let conn = conn();
        let handle = conn.interrupt_handle();
        let interrupter = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            handle.interrupt();
        });
        let started = Instant::now();
        let result = conn.query(
            "WITH RECURSIVE cnt(x) AS (
                 SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 1000000000
             ) SELECT COUNT(x) FROM cnt",
        );
        interrupter.join().expect("interrupter thread");
        let err = result.expect_err("slow query must be interrupted");
        assert!(
            matches!(err, FrankenError::Interrupted | FrankenError::Abort),
            "expected interrupt-class error, got {err:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "interrupt should land promptly"
        );
    }

    #[test]
    fn shared_deadline_timer_fires_and_disarms_across_many_guards() {
        let conn = conn();
        // 1) A short deadline must fire mid-query and mark fired().
        let guard = conn.deadline_guard_after(Duration::from_millis(120));
        let result = conn.query(
            "WITH RECURSIVE cnt(x) AS (
                 SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 1000000000
             ) SELECT COUNT(x) FROM cnt",
        );
        let err = result.expect_err("deadline should interrupt the query");
        assert!(err.is_interrupt_class(), "got {err:?}");
        assert!(guard.fired(), "deadline slot should record the fire");

        // 2) 60 guards armed then disarmed: none may fire, all on the same
        //    reusable timer thread.
        let mut guards = Vec::new();
        for _ in 0..60 {
            guards.push(conn.deadline_guard_after(Duration::from_millis(40)));
        }
        for guard in &guards {
            guard.disarm();
        }
        std::thread::sleep(Duration::from_millis(150));
        for guard in &guards {
            assert!(!guard.fired(), "disarmed deadline must never fire");
        }
        drop(guards);
    }

    #[test]
    fn cancellable_begin_immediate_retry_honors_cancellation_handle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("contended.db");
        let path_str = path.to_string_lossy().into_owned();

        let blocker = Connection::open(&path_str).expect("blocker open");
        blocker.execute("CREATE TABLE t(x)").expect("create");
        blocker.execute_batch("BEGIN IMMEDIATE").expect("lock");

        let cancellation = CancellationHandle::new();
        let cancel = cancellation.clone();
        let started = Instant::now();
        let worker = std::thread::spawn(move || {
            let conn = Connection::open(&path_str).expect("worker open");
            conn.execute("PRAGMA busy_timeout = 5000")
                .expect("busy timeout");
            let _scope = cancel.enter();
            conn.transaction_with_behavior(compat::TransactionBehavior::Immediate)
                .map(|_| ())
        });
        std::thread::sleep(Duration::from_millis(300));
        cancellation.cancel();
        let result = worker.join().expect("worker join");
        assert!(
            matches!(result, Err(FrankenError::Abort)),
            "cancelled BEGIN IMMEDIATE should Abort, got {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "cancellation should land promptly (elapsed {:?})",
            started.elapsed()
        );
        blocker.execute_batch("ROLLBACK").expect("release lock");
    }

    #[test]
    fn lock_taking_begin_verb_strips_trailing_semicolon() {
        assert_eq!(
            lock_taking_begin_verb("BEGIN IMMEDIATE;"),
            Some("BEGIN IMMEDIATE")
        );
        assert_eq!(
            lock_taking_begin_verb("BEGIN IMMEDIATE TRANSACTION;"),
            Some("BEGIN IMMEDIATE")
        );
        assert_eq!(
            lock_taking_begin_verb("  begin   exclusive   tran ;  "),
            Some("BEGIN EXCLUSIVE")
        );
        assert_eq!(
            lock_taking_begin_verb("BEGIN IMMEDIATE"),
            Some("BEGIN IMMEDIATE")
        );
        // Deferred BEGIN takes no lock; a `;` left inside the body means
        // a multi-statement batch that must stay on execute_batch.
        assert_eq!(lock_taking_begin_verb("BEGIN;"), None);
        assert_eq!(lock_taking_begin_verb("BEGIN TRANSACTION;"), None);
        assert_eq!(lock_taking_begin_verb("BEGIN IMMEDIATE; SELECT 1;"), None);
        assert_eq!(lock_taking_begin_verb("SELECT 1;"), None);
        assert_eq!(lock_taking_begin_verb(""), None);
    }

    #[test]
    fn semicolon_begin_immediate_routes_through_execute_entry_points() {
        let conn = conn();
        conn.execute("BEGIN IMMEDIATE;").expect("execute");
        assert!(conn.in_transaction());
        conn.execute_batch("ROLLBACK").expect("rollback");
        assert!(conn.is_autocommit());

        conn.execute_with_params("BEGIN IMMEDIATE;", &[])
            .expect("execute_with_params");
        assert!(conn.in_transaction());
        conn.execute_batch("ROLLBACK").expect("rollback");

        conn.execute_batch("BEGIN IMMEDIATE TRANSACTION;")
            .expect("execute_batch");
        assert!(conn.in_transaction());
        conn.execute_batch("ROLLBACK").expect("rollback");
        assert!(conn.is_autocommit());
    }

    #[test]
    fn begin_immediate_semicolon_restores_busy_timeout() {
        let conn = conn();
        conn.execute("PRAGMA busy_timeout = 4321").expect("set");
        conn.execute("BEGIN IMMEDIATE;").expect("begin");
        conn.execute_batch("ROLLBACK").expect("rollback");
        let row = conn.query_row("PRAGMA busy_timeout").expect("read");
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 4321);
    }

    #[test]
    fn cancelled_scope_leaves_busy_timeout_untouched() {
        let conn = conn();
        conn.execute("PRAGMA busy_timeout = 4321").expect("set");
        let cancellation = CancellationHandle::new();
        {
            let _scope = cancellation.enter();
            cancellation.cancel();
            let err = conn
                .wal_checkpoint_truncate(Some(Instant::now() + Duration::from_secs(5)))
                .expect_err("cancelled scope must abort");
            assert!(matches!(err, FrankenError::Abort), "got {err:?}");
            let err = conn
                .transaction_with_deadline(compat::TransactionBehavior::Immediate, None)
                .expect_err("cancelled begin must abort");
            assert!(matches!(err, FrankenError::Abort), "got {err:?}");
        }
        // The sliced busy_timeout must never leak past BusyTimeoutRestore.
        let row = conn.query_row("PRAGMA busy_timeout").expect("read");
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 4321);
    }

    #[test]
    fn busy_retry_paths_fail_cleanly_on_closed_connection() {
        let mut conn = conn();
        conn.close_in_place().expect("close");
        let err = conn
            .execute("BEGIN IMMEDIATE;")
            .expect_err("must not panic");
        assert!(matches!(err, FrankenError::Internal(_)), "got {err:?}");
        let err = conn
            .wal_checkpoint_truncate(None)
            .expect_err("must not panic");
        assert!(matches!(err, FrankenError::Internal(_)), "got {err:?}");
    }

    #[test]
    fn close_without_checkpoint_preserves_wal_for_later_recovery() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal.db");
        let path_str = path.to_string_lossy().into_owned();

        let conn = Connection::open(&path_str).expect("open");
        conn.execute("PRAGMA journal_mode = WAL").expect("wal mode");
        conn.execute("PRAGMA wal_autocheckpoint = 0")
            .expect("no auto ckpt");
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
            .expect("write");
        conn.close_without_checkpoint().expect("close w/o ckpt");

        let wal = sidecar_path(&path, "-wal");
        assert!(wal.exists(), "-wal must survive no-checkpoint close");
        let wal_len = std::fs::metadata(&wal).expect("wal meta").len();
        assert!(wal_len > 0, "-wal must retain frames");

        // A subsequent open still sees the WAL content (recovery).
        let reopened = Connection::open(&path_str).expect("reopen");
        let row = reopened.query_row("SELECT COUNT(*) FROM t").expect("count");
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 1);
    }

    #[test]
    fn strict_readonly_refuses_damaged_or_absent_shm_without_mutation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("strict.db");
        let path_str = path.to_string_lossy().into_owned();

        // Seed a WAL database and close WITHOUT checkpoint so -wal/-shm stay.
        let conn = Connection::open(&path_str).expect("open");
        conn.execute("PRAGMA journal_mode = WAL").expect("wal");
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (7);")
            .expect("write");
        conn.close_without_checkpoint().expect("close");

        let shm = sidecar_path(&path, "-shm");
        let wal = sidecar_path(&path, "-wal");
        assert!(shm.exists(), "-shm must persist after no-checkpoint close");
        let wal_before = std::fs::read(&wal).expect("wal bytes");
        let strict = compat::OpenFlags::SQLITE_OPEN_READ_ONLY
            | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY;

        // Case A: valid shm -> strict open succeeds, reads work, no mutation.
        let c = compat::open_with_flags(&path_str, strict).expect("strict open");
        let row = c.query_row("SELECT x FROM t").expect("row");
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 7);
        c.close_without_checkpoint().expect("close ro");
        assert_eq!(std::fs::read(&wal).expect("wal after"), wal_before);

        // Case B: absent -shm -> BusyRecovery, and no sidecar is recreated.
        let bak = sidecar_path(&path, "-shm.keep");
        std::fs::rename(&shm, &bak).expect("hide shm");
        let err = compat::open_with_flags(&path_str, strict).expect_err("must refuse");
        assert!(
            matches!(err, FrankenError::BusyRecovery),
            "absent shm should be BusyRecovery, got {err:?}"
        );
        assert!(!shm.exists(), "strict open must not recreate -shm");
        assert_eq!(std::fs::read(&wal).expect("wal bytes"), wal_before);
        std::fs::rename(&bak, &shm).expect("restore shm");

        // Case C: damaged -shm -> BusyRecovery.
        std::fs::write(&shm, vec![0xABu8; 256]).expect("corrupt shm");
        let err = compat::open_with_flags(&path_str, strict).expect_err("must refuse");
        assert!(
            matches!(err, FrankenError::BusyRecovery),
            "damaged shm should be BusyRecovery, got {err:?}"
        );
        assert_eq!(std::fs::read(&wal).expect("wal bytes"), wal_before);
    }

    #[test]
    fn strict_readonly_plain_journal_needs_no_shm() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("legacy.db");
        let path_str = path.to_string_lossy().into_owned();
        let conn = Connection::open(&path_str).expect("open");
        conn.execute("PRAGMA journal_mode = DELETE")
            .expect("legacy mode");
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (3);")
            .expect("write");
        conn.close().expect("close");
        let strict = compat::OpenFlags::SQLITE_OPEN_READ_ONLY
            | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY;
        let c = compat::open_with_flags(&path_str, strict).expect("strict open");
        let row = c.query_row("SELECT x FROM t").expect("row");
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 3);
    }

    #[test]
    fn query_row_enforces_exactly_one_row() {
        let conn = conn();
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1), (2);")
            .expect("seed");
        let err = conn.query_row("SELECT x FROM t WHERE x = 9").unwrap_err();
        assert!(matches!(err, FrankenError::QueryReturnedNoRows));
        let err = conn.query_row("SELECT x FROM t").unwrap_err();
        assert!(matches!(err, FrankenError::QueryReturnedMultipleRows));
        let row = conn.query_row("SELECT x FROM t WHERE x = 1").unwrap();
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 1);
    }

    #[test]
    fn cancellation_scope_is_latched_and_restored() {
        let conn = conn();
        let cancellation = CancellationHandle::new();
        {
            let _scope = cancellation.enter();
            cancellation.cancel();
            assert!(cancellation.is_cancelled());
            let err = conn.query("SELECT 1").unwrap_err();
            assert!(matches!(err, FrankenError::Abort));
        }
        // Scope dropped: ops run again.
        let rows = conn.query("SELECT 1 AS one").expect("query");
        assert_eq!(rows[0].get_typed::<i64>(0).unwrap(), 1);
    }

    #[test]
    fn row_callback_error_propagates_once() {
        let conn = conn();
        seed_files(&conn);
        let mut calls = 0usize;
        let err = conn
            .query_for_each("SELECT path, title FROM files", |row| {
                calls += 1;
                if calls > 1 {
                    panic!("query must stop after callback error");
                }
                let _ = row.get_typed::<String>(1)?;
                Err(FrankenError::BusyRecovery)
            })
            .unwrap_err();
        assert!(matches!(err, FrankenError::BusyRecovery));
        assert_eq!(calls, 1);
    }

    #[test]
    fn multi_statement_execute_preserves_order() {
        let conn = conn();
        conn.execute(
            "CREATE TABLE side_effect_log(seq INTEGER PRIMARY KEY, tag TEXT);\n             INSERT INTO side_effect_log(tag) VALUES ('before');\n             THIS IS NOT VALID SQL;\n             INSERT INTO side_effect_log(tag) VALUES ('after');",
        )
        .expect_err("multi-statement execute must fail");
        let rows = conn
            .query("SELECT tag FROM side_effect_log ORDER BY seq")
            .expect("log");
        assert_eq!(
            rows.iter()
                .map(|row| row.get_typed::<String>(0).unwrap())
                .collect::<Vec<_>>(),
            vec!["before".to_string()],
            "no replay of prior statements on failure"
        );
    }

    #[test]
    fn transaction_and_savepoint_roundtrip() {
        let conn = conn();
        conn.execute("CREATE TABLE t(x)").expect("create");
        {
            let mut tx = conn.transaction().expect("begin");
            tx.execute("INSERT INTO t VALUES (1)").expect("insert");
            {
                let mut sp = tx.savepoint().expect("savepoint");
                sp.execute("INSERT INTO t VALUES (2)").expect("sp insert");
                sp.rollback().expect("sp rollback");
            }
            tx.commit().expect("commit");
        }
        let row = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 1);
        assert!(conn.as_async().is_autocommit());
    }

    #[test]
    fn compat_params_and_optional_extension_work() {
        let conn = conn();
        seed_files(&conn);
        let count: i64 = conn
            .query_row_map(
                "SELECT COUNT(*) FROM files WHERE title != ?1",
                params!["missing"],
                |row| row.get_typed::<i64>(0),
            )
            .expect("count");
        assert_eq!(count, 2);
        let none: Option<Row> = conn
            .query_row("SELECT path FROM files WHERE title = 'none'")
            .optional()
            .expect("optional");
        assert!(none.is_none());
        let mapped: Vec<String> = conn
            .query_map_collect("SELECT title FROM files ORDER BY title", params![], |row| {
                row.get_typed::<String>(0)
            })
            .expect("collect");
        assert_eq!(mapped, vec!["alpha".to_string(), "beta".to_string()]);

        let params = param_slice_to_values(params![1i64, "x", Option::<i64>::None]);
        assert_eq!(params.len(), 3);
        assert!(matches!(params[2], SqliteValue::Null));
        let from_iter = params_from_iter([ParamValue::from(7i64)]);
        assert_eq!(from_iter, vec![SqliteValue::Integer(7)]);
    }

    #[test]
    fn query_map_borrows_raw_rows() {
        let conn = conn();
        seed_files(&conn);
        let titles = conn
            .query_map("SELECT title FROM files ORDER BY title", &[], |row| {
                row.get_ref(0)
                    .map(|v| match v {
                        ValueRef::Text(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                        _ => String::new(),
                    })
                    .map_err(FrankenError::from)
            })
            .expect("map");
        assert_eq!(titles, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn backup_exact_to_vacuum_into_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src.db");
        let dst = dir.path().join("dst.db");
        let conn = Connection::open(src.to_string_lossy()).expect("open");
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (11);")
            .expect("seed");
        let report = conn.backup_exact_to(&dst).expect("backup");
        assert!(report.byte_len > 0);
        assert!(report.page_size >= 512);
        assert!(report.page_count > 0);
        // Refuses overwrite.
        assert!(matches!(
            conn.backup_exact_to(&dst),
            Err(FrankenError::CannotOpen { .. })
        ));
        let check = Connection::open(dst.to_string_lossy()).expect("open dst");
        let row = check.query_row("SELECT x FROM t").unwrap();
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 11);
    }

    #[test]
    fn migration_runner_applies_ordered_steps() {
        let conn = conn();
        let runner = migrate::MigrationRunner::new()
            .add(1, "initial", "CREATE TABLE a(x);")
            .add(2, "second", "CREATE TABLE b(y);");
        let result = runner.run(&conn).expect("first run");
        assert_eq!(result.applied, vec![1, 2]);
        assert!(result.was_fresh, "virgin ledger should report fresh");
        let result = runner.run(&conn).expect("second run");
        assert!(result.applied.is_empty());
        assert!(!result.was_fresh);
        let row = conn
            .query_row("SELECT COUNT(*) FROM _schema_migrations")
            .unwrap();
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 2);
    }

    #[test]
    fn franken_error_code_and_extended_codes_match() {
        let err = FrankenError::Abort;
        assert_eq!(err.extended_error_code(), 4);
        assert_eq!(format!("{:?}", err.error_code()), "Abort");
        let err = FrankenError::BusyRecovery;
        assert_eq!(err.extended_error_code(), 261);
        let err = FrankenError::OutOfMemory;
        assert_eq!(err.to_string(), "out of memory");
        let err = FrankenError::from_rusqlite(
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(1),
                Some("no such table: sessions".to_string()),
            ),
            None,
        );
        assert!(matches!(
            err,
            FrankenError::NoSuchTable { ref name } if name == "sessions"
        ));
    }

    #[test]
    fn shutdown_driver_is_a_noop_success() {
        assert!(shutdown_driver());
    }
}
