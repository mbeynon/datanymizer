use anyhow::Result;
use std::fs;
use std::io;

pub struct SqlTextConnection {
    reader: Box<dyn io::BufRead>,
}

impl SqlTextConnection {
    pub fn new(reader: Box<dyn io::BufRead>) -> Self {
        Self { reader }
    }
    pub fn reader(&mut self) -> &mut Box<dyn io::BufRead> {
        &mut self.reader
    }
}

pub struct SqlTextConnector {
    pub is_stdin: bool,
    pub file_input: String,
}

impl SqlTextConnector {
    pub fn new(is_stdin: bool, file_input: String) -> Self {
        Self { is_stdin, file_input }
    }

    pub fn connect(&self) -> Result<SqlTextConnection> {
        let rdr: Box<dyn io::BufRead> = match self.is_stdin {
            true => Box::new(io::BufReader::new(io::stdin())),
            _    => Box::new(io::BufReader::new(fs::File::open(&self.file_input).unwrap()))
        };
        Ok(SqlTextConnection::new(rdr))
    }
}

#[cfg(test)]
mod tests {
    // use super::*;

    // mod tls_connector {
    //     use super::*;

    //     #[test]
    //     fn default() {
    //         // TODO: 
    //         // let tls_connector = tls_connector("postgres://postgres@localhost/dbname");
    //         // assert!(tls_connector.is_none());
    //     }
    // }
}
