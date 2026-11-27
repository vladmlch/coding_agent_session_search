#[cfg(not(target_arch = "wasm32"))]
use std::io::Read;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

use super::value_and_error::{franken_error_to_rusqlite, sqlite_value_from_ref};
use super::{Connection, FrankenError, Row, SqliteValue};

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

    pub use crate::__cass_franken_params as params;
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

    #[derive(Debug)]
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

    impl fmt::Debug for Transaction<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Transaction")
                .field("kind", &self.kind)
                .field("finished", &self.finished)
                .finish()
        }
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
pub(super) fn read_backup_report(path: &Path) -> Result<BackupReport, FrankenError> {
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
#[cfg(test)]
pub(crate) fn shutdown_driver() -> bool {
    true
}
