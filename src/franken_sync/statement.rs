use std::cell::RefCell;

use super::connection::dynamic_params;
use super::deadline_and_cancel::{current_cancellation, current_is_cancelled};
use super::value_and_error::materialize_row;
use super::{Connection, FrankenError, Row, SqliteValue, check_connection_admission};

// ---------------------------------------------------------------------------
// PreparedStatement
// ---------------------------------------------------------------------------

/// Prepared statement wrapper holding the rusqlite statement under a
/// `RefCell` so all operations take `&self` (facade contract).
pub struct PreparedStatement<'conn> {
    pub(super) stmt: RefCell<rusqlite::Statement<'conn>>,
    pub(super) connection: &'conn Connection,
    pub(super) refreshes_wal: bool,
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
        {
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
                    Ok(None) => break,
                    Ok(Some(row)) => {
                        let materialized = materialize_row(row, column_count)?;
                        f(&materialized)?;
                    }
                }
            }
        }
        if self.refreshes_wal {
            self.connection.refresh_wal_state_now();
        }
        Ok(())
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
        {
            let mut stmt = self.stmt.borrow_mut();
            stmt.query(&refs[..])
                .and_then(|mut rows| {
                    while let Some(_row) = rows.next()? {}
                    Ok(())
                })
                .map_err(|error| self.connection.map_err_cancellable(error))?;
        }
        if self.refreshes_wal {
            self.connection.refresh_wal_state_now();
        }
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
        let first = {
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
                Err(error) => return Err(self.connection.map_err_cancellable(error)),
                Ok(None) => first,
                Ok(Some(_)) => return Err(FrankenError::QueryReturnedMultipleRows),
            }
        };
        if self.refreshes_wal {
            self.connection.refresh_wal_state_now();
        }
        Ok(first)
    }
}
