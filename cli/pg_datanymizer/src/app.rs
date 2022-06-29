use anyhow::Result;
use std::{fs::File, io};
use url::Url;

use crate::options::{Options, TransactionConfig};

use datanymizer_dumper::{
    indicator::{ConsoleIndicator, SilentIndicator},
    postgres::{connector::Connector, dumper::PgDumper, IsolationLevel},
    sqltext::{connector::SqlTextConnector, dumper::SqlTextDumper},
    Dumper,
};
use datanymizer_engine::{Engine, Settings};

pub struct App {
    options: Options,
    database_url: Url,
}

impl App {
    pub fn from_options(options: Options) -> Result<Self> {
        let database_url = options.database_url()?;

        Ok(App {
            options,
            database_url,
        })
    }

    pub fn run(&self) -> Result<()> {
        match &self.options.input_file {
            Some(x) if x.eq("-") => self.run_sqltext_dumper(true, String::from("")),
            Some(x)              => self.run_sqltext_dumper(false, x.clone()),
            None                 => self.run_postgres_dumper()
        }
    }

    fn run_postgres_dumper(&self) -> Result<()> {
        // db connection mode with a live db to introspect for schema and pull data
        let engine = self.engine()?;
        let mut connection = Connector::new(
            self.database_url.clone(),
            self.options.accept_invalid_hostnames,
            self.options.accept_invalid_certs,
        ).connect()?;
        match &self.options.file {  // output file
            Some(filename) => PgDumper::new(
                engine,
                self.dump_isolation_level(),
                self.options.pg_dump_location.clone(),
                File::create(filename)?,
                ConsoleIndicator::new(),
                self.options.pg_dump_args.clone(),
            )?
            .dump(&mut connection),

            None => PgDumper::new(
                engine,
                self.dump_isolation_level(),
                self.options.pg_dump_location.clone(),
                io::stdout(),
                SilentIndicator,
                self.options.pg_dump_args.clone(),
            )?
            .dump(&mut connection),
        }
    }

    fn run_sqltext_dumper(&self, is_stdin: bool, infile: String) -> Result<()> {
        // streaming mode reading from stdin or a file with .SQL dump data
        let engine = self.engine()?;
        let mut connection = SqlTextConnector::new(
            is_stdin,
            infile,
        ).connect()?;
        match &self.options.file {  // output file
            Some(filename) => SqlTextDumper::new(
                engine,
                File::create(filename)?,                    
                ConsoleIndicator::new(),
            )?
            .dump(&mut connection),

            None => SqlTextDumper::new(
                engine,
                io::stdout(),
                SilentIndicator,
            )?
            .dump(&mut connection),
        }
    }

    fn engine(&self) -> Result<Engine> {
        let settings = Settings::new(self.options.config.clone())?;
        Ok(Engine::new(settings))
    }

    fn dump_isolation_level(&self) -> Option<IsolationLevel> {
        match self.options.dump_transaction {
            TransactionConfig::NoTransaction => None,
            TransactionConfig::ReadUncommitted => Some(IsolationLevel::ReadUncommitted),
            TransactionConfig::ReadCommitted => Some(IsolationLevel::ReadCommitted),
            TransactionConfig::RepeatableRead => Some(IsolationLevel::RepeatableRead),
            TransactionConfig::Serializable => Some(IsolationLevel::Serializable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use structopt::StructOpt;

    mod isolation_level {
        use super::*;

        #[test]
        fn default() {
            let options =
                Options::from_iter(vec!["DBNAME", "postgres://postgres@localhost/dbname"]);
            let level = App::from_options(options).unwrap().dump_isolation_level();
            assert!(matches!(level, Some(IsolationLevel::ReadCommitted)));
        }

        fn level(dt: &str) -> Option<IsolationLevel> {
            let options = Options::from_iter(vec![
                "DBNAME",
                "postgres://postgres@localhost/dbname",
                "--dump-transaction",
                dt,
            ]);
            App::from_options(options).unwrap().dump_isolation_level()
        }

        #[test]
        fn no_transaction() {
            let level = level("NoTransaction");
            assert!(level.is_none());
        }

        #[test]
        fn repeatable_read() {
            let level = level("RepeatableRead");
            assert!(matches!(level, Some(IsolationLevel::RepeatableRead)));
        }
    }
}
