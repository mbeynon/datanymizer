use super::{
    connector, SchemaInspector,
};
use crate::postgres::{table::PgTable, column::PgColumn};
use anyhow::Result;
use postgres::types::Type;

#[derive(Clone)]
pub struct SqlTextSchemaInspector;

impl SchemaInspector for SqlTextSchemaInspector {
    type Type = Type;
    type Connection = connector::SqlTextConnection;
    type Table = PgTable;
    type Column = PgColumn;

    // Get all tables in the database
    fn get_tables(&self, _connection: &mut Self::Connection) -> Result<Vec<Self::Table>> {
        let items: Vec<Self::Table> = Vec::new();
        Ok(items)
    }

    /// Get table size
    fn get_table_size(
        &self,
        _connection: &mut Self::Connection,
        _table: &Self::Table,
    ) -> Result<i64> {
        let size: i64 = 0;
        Ok(size)
    }

    // Get all dependencies (by FK) for `table` in database
    fn get_dependencies(
        &self,
        _connection: &mut Self::Connection,
        _table: &Self::Table,
    ) -> Result<Vec<Self::Table>> {
        let tables: Vec<Self::Table> = Vec::new();
        Ok(tables)
    }

    /// Get columns for table
    fn get_columns(
        &self,
        _connection: &mut Self::Connection,
        _table: &Self::Table,
    ) -> Result<Vec<Self::Column>> {
        let items: Vec<Self::Column> = Vec::new();
        Ok(items)
    }
}

impl SqlTextSchemaInspector {
}
