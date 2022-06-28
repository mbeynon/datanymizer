use super::{
    connector, schema_inspector::SqlTextSchemaInspector,
};
use crate::postgres::{row::PgRow, column::PgColumn, table::PgTable};
use crate::{indicator::Indicator, Dumper, Table};
use anyhow::{Result,Error};
use datanymizer_engine::{Engine, Settings};
#[cfg(test)]
use datanymizer_engine::{Filter, TableList};
use regex::Regex;
use std::{
    io::{prelude::*},
    process::{self},
    time::Instant,
};

enum ParseState {
    Passthrough,
    TableDefinition,
    TableData,
}

pub struct SqlTextDumper<W: Write + Send, I: Indicator + Send> {
    schema_inspector: SqlTextSchemaInspector,
    engine: Engine,
    dump_writer: W,
    indicator: I,
    state: ParseState,
    tables: Vec<PgTable>,
}

impl<W: 'static + Write + Send, I: 'static + Indicator + Send> SqlTextDumper<W, I> {
    pub fn new(
        engine: Engine,
        dump_writer: W,
        indicator: I,
    ) -> Result<Self> {
        Ok(Self {
            engine,
            dump_writer,
            indicator,
            schema_inspector: SqlTextSchemaInspector {},
            state: ParseState::Passthrough,
            tables: Vec::new(),
        })
    }

    // This stage handles scanning the entire stream for DDL and Data loading
    // statements.  We assume the DDL for a table appears before the copy.
    fn dump_stream(&mut self, connection: &mut connector::SqlTextConnection) -> Result<()> {
        let re_create_table = Regex::new(r"(?xi)
            ^CREATE\s+TABLE\s+
            \x22?(?P<schema>\w+)\x22?      # schema name, may be quoted
            \.
            \x22?(?P<table>\w+)\x22?       # table name, may be quoted
            \s+\(\s*$")?;
        let re_create_table_col = Regex::new(r"(?xi)
            ^\s+
            \x22?(?P<id>\w+)\x22?\s+       # identifier name
            (?P<type>[^,]+)                # type name (includes NOT NULL)
            ")?;
        let re_copy_from = Regex::new(r"(?xi)
            ^COPY\s+
            \x22?(?P<schema>\w+)\x22?      # schema name, may be quoted
            \.
            \x22?(?P<table>\w+)\x22?       # table name, may be quoted
            \s+\(\s*(?P<cols>.*)\s*\)\s+   # comma sep list of col names (may be quoted) in parens
            FROM\s+(?:STDIN)\s*;\s*$")?;
        let re_copy_identifiers = Regex::new(r"(\x22??P<col>\x22?[^,\s]+)")?;

        self.state = ParseState::Passthrough;
        let mut table: PgTable = PgTable::new(String::from(""), String::from(""));
        let mut columns: Vec<PgColumn> = Vec::new();
        let mut col_position = 0i32;
        //let mut cfg: &PgTable;
        let mut started = Instant::now();
        let mut tot_tables_transform = 0;
        let mut cur_table_transform = 0;
        let mut num_rows = 0;

        for maybe_line in connection.reader().lines() {
            let line = maybe_line?;
            match self.state {
                ParseState::Passthrough => {
                    if line.starts_with("CREATE TABLE ") {
                        if let Some(caps) = re_create_table.captures(line.as_str()) {
                            let schema_name = caps.name("schema").map_or("", |m| m.as_str());
                            let table_name = caps.name("table").map_or("", |m| m.as_str());
                            table = PgTable::new(table_name.to_string(), schema_name.to_string());
                            if self.engine.settings.find_table(&table.get_names()).is_some() {
                                self.state = ParseState::TableDefinition;
                                columns.clear();
                                col_position = 0i32;
                                tot_tables_transform += 1;
                            }
                        }
                    } else if line.starts_with("COPY ") {
                        if let Some(caps) = re_copy_from.captures(line.as_str()) {
                            let schema_name = caps.name("schema").map_or("", |m| m.as_str());
                            let table_name = caps.name("table").map_or("", |m| m.as_str());
    
                            // check if matches a handled table, and anonymize it
                            let table_test = PgTable::new(table_name.to_string(), schema_name.to_string());
                            if let Some(table_found) = self.tables.iter().find(|&t| t == &table_test) {
                                table = (*table_found).clone();
                                // cfg = self.engine.settings.find_table(&table.get_names()).unwrap();  // must exist
                                let copy_cols: Vec<String> = re_copy_identifiers
                                    .captures_iter(caps.name("cols").map_or("", |m| m.as_str()))
                                    .map(|c| String::from(c.name("col").unwrap().as_str()))
                                    .collect();

                                // verify COPY cols match the earlier CREATE TABLE cols
                                if copy_cols.iter()
                                    .zip(table.get_columns_names().iter())
                                    .all(|(a, b)| a.eq(b)) {

                                    cur_table_transform += 1;
                                    num_rows = 0;

                                    started = Instant::now();
                                    self.indicator
                                        .start_pb_stream(cur_table_transform,tot_tables_transform, &table.get_full_name(), "starting ...");
            
                                    // start transforming the table data
                                    self.state = ParseState::TableData;
                                    self.write_log(format!("pg_datanymizer: ANON TABLE; Name: {}; Schema: {}", table_name, schema_name))?;
                                } else {
                                    eprintln!(
                                        "SqlTextDumper error: fields mismatch count/order:\n\tCREATE TABLE: {}\n\tCOPY INTO:    {}",
                                        table.get_columns_names().join(", "),
                                        copy_cols.join(", "),
                                    );
                                    process::exit(1);  // or Result<Err> ?
                                }
                            }
                        }
                    }
                },
                ParseState::TableDefinition => {
                    if line.len() == 2 && line[..2].eq(");") {
                        table.set_columns(columns);
                        self.tables.push(table);
                        columns = Vec::new();
                        table = PgTable::new(String::from(""), String::from(""));
                        self.state = ParseState::Passthrough;
                    } else if let Some(caps) = re_create_table_col.captures(line.as_str()) {
                        let id_name = caps.name("id").map_or("", |m| m.as_str());
                        let type_name = caps.name("type").map_or("", |m| m.as_str());
                        columns.push(PgColumn {
                            position: col_position,
                            name: String::from(id_name),
                            data_type: String::from(type_name),
                            inner_type: Some(0),
                        });
                        col_position += 1i32;
                    }
                },
                ParseState::TableData => {
                    if line.len() == 2 && line[..2].eq("\\.") {
                        let finished = started.elapsed();
                        self.indicator
                            .finish_pb_stream(num_rows, finished);

                        self.state = ParseState::Passthrough;
                    } else {
                        // TODO: do this find_table() once -- was getting type errors (Table trait vs PgTable type)
                        if let Some(cfg) = self.engine.settings.find_table(&table.get_names()) {
                            let row = PgRow::from_string_row(line, table.clone());
                            let transformed = row.transform(&self.engine, cfg.name.as_str())?;

                            self.dump_writer_all(&transformed)?;
                            num_rows += 1;
                            self.indicator.inc_pb_stream(num_rows);
                            continue
                        }
                    }
                },
            };
            self.dump_writer_all(&line)?;
        }

        Ok(())
    }

    fn dump_writer_all(&mut self, line: &str) -> Result<(), Error> {
        self.dump_writer.write_all(line.as_bytes())
            .map_err::<Error,_> (|e| e.into())?;
        self.dump_writer.write_all(b"\n")
            .map_err::<Error,_> (|e| e.into())?;
        Ok(())
    }
}

impl<W: 'static + Write + Send, I: 'static + Indicator + Send> Dumper for SqlTextDumper<W, I> {
    type Table = PgTable;
    type Connection = connector::SqlTextConnection;
    type SchemaInspector = SqlTextSchemaInspector;

    // This stage is meant to introspect the schema, but this dumper is stream
    // based, and can only examine DDL as it goes by
    fn pre_data(&mut self, _connection: &mut Self::Connection) -> Result<()> {
        self.write_log("pg_datanymizer anonymized database dump".into())?;

        self.debug("No pre-data schema analysis required...".into());
        Ok(())
    }
  
    // This stage handles scanning the entire stream for DDL and Data loading
    // statements.  We assume the DDL for a table appears before the copy.
    fn data(&mut self, connection: &mut Self::Connection) -> Result<()> {
        self.debug("Start filtering .sql text stream...".into());

        self.dump_stream(connection)?;
        Ok(())
    }

    // This stage is meant to dump foreign keys, indices, and other.  Since this
    // dumper is stream based, we just passthrough this section during self.data().
    fn post_data(&mut self, _connection: &mut Self::Connection) -> Result<()> {
        self.debug("No post-data schema analysis required...".into());
        Ok(())
    }

    fn schema_inspector(&self) -> Self::SchemaInspector {
        self.schema_inspector.clone()
    }

    fn settings(&self) -> &Settings {
        &self.engine.settings
    }

    fn write_log(&mut self, message: String) -> Result<()> {
        self.dump_writer
            .write_all(format!("\n---\n--- {}\n---\n", message).as_bytes())
            .map_err(|e| e.into())
    }

    fn debug(&self, message: String) {
        self.indicator.debug_msg(message.as_str());
    }
}

#[cfg(test)]
fn table_args(filter: &Option<Filter>) -> Result<Vec<String>> {
    let mut args = vec![];
    if let Some(f) = filter {
        let list = f.schema_match_list();
        let flag = match list {
            TableList::Only(_) => "-t",
            TableList::Except(_) => "-T",
        };
        for table in list.tables() {
            args.push(String::from(flag));
            args.push(PgTable::quote_table_name(table.as_str())?);
        }
    }

    Ok(args)
}

#[cfg(test)]
fn sort_tables(tables: &mut Vec<(PgTable, i32)>, order: &[String]) {
    tables.sort_by_cached_key(|(tbl, weight)| {
        let position = order.iter().position(|i| tbl.get_names().contains(i));
        (position, -weight)
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_args() {
        let tables = vec![String::from("table1"), String::from("table2")];

        let empty: Vec<String> = vec![];
        assert_eq!(table_args(&None).unwrap(), empty);

        let mut filter = Filter::new(
            TableList::Except(vec![String::from("table1")]),
            TableList::default(),
        );
        filter.load_tables(tables.clone());
        assert_eq!(
            table_args(&Some(filter)).unwrap(),
            vec![String::from("-T"), String::from("\"table1\"")]
        );

        let mut filter = Filter::new(
            TableList::default(),
            TableList::Except(vec![String::from("table1")]),
        );
        filter.load_tables(tables.clone());
        assert_eq!(table_args(&Some(filter)).unwrap(), empty);

        let mut filter = Filter::new(
            TableList::Only(vec![String::from("table1"), String::from("table2")]),
            TableList::default(),
        );
        filter.load_tables(tables.clone());
        assert_eq!(
            table_args(&Some(filter)).unwrap(),
            vec![
                String::from("-t"),
                String::from("\"table1\""),
                String::from("-t"),
                String::from("\"table2\"")
            ]
        );

        let mut filter = Filter::new(
            TableList::Only(vec![String::from("table*")]),
            TableList::default(),
        );
        filter.load_tables(tables);
        assert_eq!(
            table_args(&Some(filter)).unwrap(),
            vec![
                String::from("-t"),
                String::from("\"table1\""),
                String::from("-t"),
                String::from("\"table2\"")
            ]
        );
    }

    #[test]
    fn test_sort_tables() {
        let order = vec!["table2".to_string(), "public.table1".to_string()];

        let mut tables = vec![
            (PgTable::new("table1".to_string(), "public".to_string()), 0),
            (PgTable::new("table2".to_string(), "public".to_string()), 1),
            (PgTable::new("table3".to_string(), "public".to_string()), 2),
            (PgTable::new("table4".to_string(), "public".to_string()), 3),
            (PgTable::new("table1".to_string(), "other".to_string()), 4),
            (PgTable::new("table2".to_string(), "other".to_string()), 5),
        ];

        sort_tables(&mut tables, &order);

        let ordered_names: Vec<_> = tables
            .iter()
            .map(|(t, w)| (t.get_full_name(), *w))
            .collect();
        assert_eq!(
            ordered_names,
            vec![
                ("other.table1".to_string(), 4),
                ("public.table4".to_string(), 3),
                ("public.table3".to_string(), 2),
                ("other.table2".to_string(), 5),
                ("public.table2".to_string(), 1),
                ("public.table1".to_string(), 0),
            ]
        )
    }
}
