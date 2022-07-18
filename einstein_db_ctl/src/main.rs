use std::collections::binary_heap::Iter;
use std::collections::hash_map::{IterMut, Keys};
///! Copyright (c) EinsteinDB. All rights reserved.
/// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
///
/// # einstein_db_ctl
///
use std::env;
use std::error::Error;
use std::process;
use std::path::Path;
use std::path::PathBuf;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, ErrorKind};
use std::io::BufWriter;
use std::io::Write;
use std::io::Read;
use std::iter::Peekable;
use std::net::TcpStream;

mod violetabft;



use crate::einsteindb::einstein_db_ctl::*;
use crate::einsteindb::einstein_db_ctl::einstein_db_ctl::*;

use crate::einsteindb::einstein_ml::*;
use crate::einsteindb::einstein_ml::einstein_ml::*;

use crate::einsteindb::einsteindb_server::*;
use crate::einsteindb::einsteindb_server::einsteindb_server::*;





/// EinsteinDB command line interface.
/// 
/// # Examples
/// 
/// ```
///     
///    # use einstein_db::prelude::*;
///   # let mut db = Database::new();
///  # let mut table = db.create_table("table");
/// # let mut table = table.unwrap();
/// # let mut table = table.as_mut();
///     
///   # let mut iter = table.iter();
/// 
///  # let mut iter = iter.peekable();


//extern crate einstein_db;
use einstein_db::prelude::*;
//extern crate futures;
use futures::prelude::*;
//extern crate tokio;
use tokio::prelude::*;



use structopt::StructOpt;
use einstein_db::prelude::*;
use futures::{Future, Stream};
use tokio::prelude::*;
use tokio_codec::{Framed, LinesCodec};
use tokio_io::io;

use crate ::einsteindb::*;




#[assert(assertion)]
#[cfg(assertion)]
fn assert(assertion: bool) -> Result<(), Box<dyn Error>> {
    if !assertion {
        Err(Box::new(Error::new("assertion failed")))
    } else {
        Ok(())
    }
}




#[derive(StructOpt, Debug, Clone, PartialEq, Eq)]
pub struct EinsteinDB {
    #[structopt(subcommand)]
    pub subcommand: Subcommand,

    pub db_name: String,


    pub table_name: String,


    pub column_name: String,

    pub table_path: String,

    pub table_type: String,


    pub violetabft_path: String,

    pub fidelity: String,

    pub table_size: String,

    pub table_key_type: String,

    pub table_value_type: String,

    pub table_key_size: String,

    pub table_value_size: String,


    pub table_key_path: String,

    pub table_value_path: String,

    pub table_key_type_path: String,



    pub table_value_type_path: String,

    pub table_key_size_path: String,
}




#[derive(StructOpt, Debug, Clone, PartialEq, Eq)]
pub fn fidelgrcpio_column_name(column_name: String) -> String where {
    column_name
}

pub trait ColumnName {
    fn column_name(&self) -> String;
}

pub fn column_name(column_name: String) -> String {
    for c in column_name.chars() {
        if c.is_alphabetic() || c.is_numeric()|| c.is_whitespace() && c != '_' {

            continue;
        } else {
            panic!("column name must be alphabetic or numeric or underscore");

        }
    }
    column_name
}



/*
    pub db: Database,

    pub table: Table,

    //pub iter: Iter<T>,
    pub iter: Peekable<Iter<T>>,

    pub iter_mut: IterMut<K, V>,

    pub iter_peekable: Peekable<Iter<T>>,

    pub iter_peekable_mut: Peekable<IterMut<K, V>>,

}
*/

#[derive(StructOpt, Debug, Clone, PartialEq, Eq)]
 pub fn sync(
    db: Database,
    table: Table,
    mut iter: Peekable<Iter<T>>,
    iter_mut: IterMut<K, V>) {
    let mut iter = iter;
    let mut table = table;

    if iter.peek().is_some() {
        println!("iter.peek().is_some()");
    } else {
        println!("iter.peek().is_none()");
    }
        let mut iter_mut = table.iter_mut();
        let mut iter_peekable = table.iter();
        table.iter();
        table.iter_mut();
        let mut iter_peekable_mut = table.iter_mut();
    table.iter();

        if iter_mut.is_empty() {
            println!("iter_mut.is_empty()");

        } else {
            println!("iter_mut.is_not_empty()");

        }
        if iter_peekable.is_empty() {
            println!("iter_peekable.is_empty()");
        } else {
            println!("iter_peekable.is_not_empty()");
        }

        if iter_peekable_mut.is_empty() {
            println!("iter_peekable_mut.is_empty()");
        } else {
            println!("iter_peekable_mut.is_not_empty()");
        }
    }




    //let mut iter_peekable_mut = iter_mut.peekable();





#[derive(StructOpt, Debug, Clone, PartialEq, Eq)]
pub enum Subcommand {
    #[structopt(name = "create-table")]
    CreateTable {
        #[structopt(name = "table-name")]
        table_name: String,
    },
    #[structopt(name = "drop-table")]
    DropTable {
        #[structopt(name = "table-name")]
        table_name: String,
    },
    #[structopt(name = "insert")]
    Insert {
        #[structopt(name = "table-name")]
        table_name: String,
        #[structopt(name = "key")]
        key: String,
        #[structopt(name = "value")]
        value: String,
    },
    #[structopt(name = "delete")]
    Delete {
        #[structopt(name = "table-name")]
        table_name: String,
        #[structopt(name = "key")]
        key: String,
    },
    #[structopt(name = "get")]
    Get {
        #[structopt(name = "table-name")]
        table_name: String,
        #[structopt(name = "key")]
        key: String,
    },
    #[structopt(name = "scan")]
    Scan {
        #[structopt(name = "table-name")]
        table_name: String,
        #[structopt(name = "start-key")]
        start_key: String,
        #[structopt(name = "end-key")]
        end_key: String,
    },
    #[structopt(name = "scan-range")]
    ScanRange {
        #[structopt(name = "table-name")]
        table_name: String,
        #[structopt(name = "start-key")]
        start_key: String,
        #[structopt(name = "end-key")]
        end_key: String,
    },
    #[structopt(name = "scan-range-reverse")]
    ScanRangeReverse {
        #[structopt(name = "table-name")]
        table_name: String,
        #[structopt(name = "start-key")]
        start_key: String,
        #[structopt(name = "end-key")]
        end_key: String,
    },
    #[structopt(name = "scan-range-reverse-reverse")]
    ScanRangeReverseReverse {
        #[structopt(name = "table-name")]
        table_name: String,
        #[structopt(name = "start-key")]
        start_key: String,
        #[structopt(name = "end-key")]
        end_key: String,
    },
}

//commands
//create table
//create index
//insert
//delete
//update
//select
//select all
//select where
//select where all







#[derive(StructOpt, Debug)]
#[structopt(name = "einstein_db_ctl")]
struct Opt {
    #[structopt(short = "p", long = "port", default_value = "8080")]
    port: u16,
}




/// Commands
/// 
/// ```
/// # use einstein_db::prelude::*;
/// # let mut db = Database::new();
/// # let mut table = db.create_table("table");
/// # let mut table = table.unwrap();
/// # let mut table = table.as_mut();
/// # let mut iter = table.iter();
/// # let mut iter = iter.peekable();

#[derive(StructOpt, Debug)]
enum Command {
    #[structopt(name = "create")]
    Create {
        #[structopt(name = "table")]
        table: String,
    },
    #[structopt(name = "drop")]
    Drop {
        #[structopt(name = "table")]
        table: String,
    },
    #[structopt(name = "insert")]
    Insert {
        #[structopt(name = "table")]
        table: String,
        #[structopt(name = "key")]
        key: String,
        #[structopt(name = "value")]
        value: String,
    },
    #[structopt(name = "get")]
    Get {
        #[structopt(name = "table")]
        table: String,
        #[structopt(name = "key")]
        key: String,
    },
    #[structopt(name = "delete")]
    Delete {
        #[structopt(name = "table")]
        table: String,
        #[structopt(name = "key")]
        key: String,
    },
    #[structopt(name = "list")]
    List {
        #[structopt(name = "table")]
        table: String,
    },
    #[structopt(name = "iter")]
    Iter {
        #[structopt(name = "table")]
        table: String,
    },
    #[structopt(name = "peek")]
    Peek {
        #[structopt(name = "table")]
        table: String,
    },
    #[structopt(name = "peek_back")]
    PeekBack {
        #[structopt(name = "table")]
        table: String,
    },
    #[structopt(name = "peek_nth")]
    PeekNth {
        #[structopt(name = "table")]
        table: String,
        #[structopt(name = "n")]
        n: usize
    },
    #[structopt(name = "peek_nth_back")]
    PeekNthBack {
        #[structopt(name = "table")]
        table: String,
        #[structopt(name = "n")]
        n: usize
    },
    #[structopt(name = "peek_range")]
    PeekRange {
        #[structopt(name = "table")]
        table: String,
        #[structopt(name = "start")]
        start: String,
        #[structopt(name = "end")]
        end: String,
    },
    #[structopt(name = "peek_range_back")]
    PeekRangeBack {
        #[structopt(name = "table")]
        table: String,
        #[structopt(name = "start")]
        start: String,
        #[structopt(name = "end")]
        end: String,
    },
    //time travel commands
    #[structopt(name = "time_travel")]
    TimeTravel {
        #[structopt(name = "table")]
        table: String,
        #[structopt(name = "key")]
        key: String,
        #[structopt(name = "time")]
        time: u64,
    },
    #[structopt(name = "time_travel_range")]
    TimeTravelRange {
        #[structopt(name = "table")]
        table: String,
        #[structopt(name = "start")]
        start: String,
        #[structopt(name = "end")]
        end: String,
        #[structopt(name = "time")]
        time: u64,
    },
    #[structopt(name = "time_travel_nth")]
    TimeTravelNth {
        #[structopt(name = "table")]
        table: String,
        #[structopt(name = "n")]
        n: usize,
        #[structopt(name = "time")]
        time: u64,
    }
}



#[derive(StructOpt, Debug)]
pub fn einstein_db_ctl(&mut db: &mut Database, opt: Opt) {
    let mut server = Server::new(opt.port);
    server.run().await;
}


#[derive(StructOpt, Debug)]
pub enum EinsteinDbCtlCommand {
    #[structopt(name = "ctl")]
    Ctl {
        #[structopt(subcommand)]
        command: Command,
    },
}


#[derive(StructOpt, Debug)]
pub enum EinsteinDbCtlSubCommand {
    #[structopt(name = "ctl")]
    Ctl {
        #[structopt(subcommand)]
        command: Command,
    },
}


#[derive(StructOpt, Debug)]
pub enum EinsteinDbCtlSubCommandSubCommand {
    #[structopt(name = "ctl")]
    Ctl {
        #[structopt(subcommand)]
        command: Command,
    },
}
impl EinsteinDB {


    pub fn run(&mut self) -> String {
        let opt = Opt::from_args();
        einstein_db_ctl(self, opt);
        String::from("")

        }

    pub fn run_command(&mut self, command: Command) -> String {
        let opt = Opt::from_args();
        einstein_db_ctl(self, opt);
        String::from("")

        }



    pub fn new() -> EinsteinDB {
        let einstein_db = EinsteinDB {
            subcommand: subcommand::SubCommand::from_args(),
            db_name: "".to_string(),
            table_name: "".to_string(),
            column_name: "".to_string(),
            table_path: "".to_string(),
            table_type: "".to_string(),
            violetabft_path: "".to_string(),
            fidelity: "".to_string(),
            table_size: "".to_string(),
            table_key_type: "".to_string(),
            table_value_type: "".to_string(),

            table_key_size: "".to_string(),
            table_value_size: "".to_string(),
            table_key_path: "".to_string(),
            table_value_path: "".to_string(),
            table_key_type_path: "".to_string(),
            table_value_type_path: "".to_string(),
            table_key_size_path: "".to_string()
        };
        einstein_db

        /// ```
        /// # use einstein_db::prelude::*;
        /// # let mut db = Database::new();
        /// # let mut table = db.create_table("table");
        /// # let mut table = table.unwrap();
        /// # let mut table = table.as_mut();
        /// # let mut iter = table.iter();
        /// # let mut iter = iter.peekable();
        /// ```








    }
}
impl EinsteinDB {
    pub fn create_table(&mut self, name: &str) -> Result<&mut Table, Error> {
        self.db.create_table(name)
    }


}


impl EinsteinDB {
    pub fn drop_table(&mut self, name: &str) -> Result<&mut Table, Error> {
        self.db.drop_table(name)
    }
}


impl EinsteinDB {
    pub fn insert(&mut self, key: &str, value: &str) -> Result<&mut Table, Error> {
        self.table.insert(key, value)
    }
}




impl EinsteinDB {
    pub fn get(&mut self, key: &str) -> Result<&mut Table, Error> {
        self.table.get(key)
    }

    //interlock commands_mutex
    pub fn get_mutex(&mut self, key: &str) -> Result<&mut Table, Error> {
        self.table.get_mutex(key)
    }
}


impl EinsteinDB {

    pub fn delete(&mut self, key: &str) -> Result<&mut Table, Error> {
        self.table.delete(key)
    }

    //interlock commands_mutex with retractions in spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions
    pub fn delete_mutex(&mut self, key: &str) -> Result<&mut Table, Error> {
        self.table.delete_mutex(key)
    }
}


impl EinsteinDB {
    pub fn list(&mut self) -> Result<&mut Table, Error> {
        self.table.list()
    }
}

    fn handle_command<'a>(command: Box< dyn Error + Send + SyncResult>, db: &'a mut Database) {
        match command {
            Box::new(Error::Command(Command::CreateTable { table })) => {
                let mut table = db.create_table(&table).unwrap();
                Box::new(Ok(table))
            },
            Box::new(Error::Command(Command::DropTable { table })) => {
                let mut table = db.drop_table(&table).unwrap();
                Box::new(Ok(table))
            },
            Box::new(Error::Command(Command::Insert { table, key, value })) => {
                let mut table = db.get_mutex(&table).unwrap();
                let mut table = table.insert(&key, &value).unwrap();
                Box::new(Ok(table))
            },
            Box::new(Error::Command(Command::Get { table, key })) => {
                let mut table = db.get_mutex(&table).unwrap();
                let mut table = table.get(&key).unwrap();
                Box::new(Ok(table))
            },
            Box::new(Error::Command(Command::Delete { table, key })) => {
                let mut table = db.get_mutex(&table).unwrap();
                let mut table = table.delete(&key).unwrap();
                Box::new(Ok(table))
            },
            Box::new(Error::Command(Command::List { table })) => {
                let mut table = db.get_mutex(&table).unwrap();
                let mut table = table.list().unwrap();
                Box::new(Ok(table))
            },
            Box::new(Error::Command(Command::Peek { table, })) => {
                let mut table = db.get_mutex(&table).unwrap();
                let mut table = table.peek(&key).unwrap();
                Box::new(Ok(table))
            },
            Box::new(Error::Command(Command::PeekRange { table, start, end })) => {
                let mut table = db.get_mutex(&table).unwrap();
                let mut table = table.peek_range(&start, &end).unwrap();
            }
        }



        fn handle_command_mutex<'a>(command: Box<dyn Error + Send + SyncResult>, db: &'a mut Database) -> Result<(), Error> {
            match command {
                switch_table(table_name) => {
                    db.switch_table(table_name);
                    Ok(())
                }
                create_table(table_name) => {
                    db.create_table(table_name);
                    Ok(())
                }
                drop_table(table_name) => {
                    db.drop_table(table_name);
                    Ok(())
                }
                insert(key, value) => {
                    db.insert(key, value);
                    Ok(())
                }
                get(key) => {
                    db.get(key);
                    Ok(())
                }
                get_mutex(key) => {
                    db.get_mutex(key);
                    Ok(())
                }
                delete(key) => {
                    db.delete(key);
                    Ok(())
                }
                delete_mutex(key) => {
                    db.delete_mutex(key);
                    Ok(())
                }
                list() => {
                    db.list();
                    Ok(())
                }
                _ => {
                    Err(Error::new(ErrorKind::Other, "Unknown command"))
                }
            }
        }

        impl EinsteinDB {
            pub fn run(&mut self) -> Result<(), Error> {
                let mut table = db.get_mutex(&key).unwrap();
                table.insert(&key, &value).unwrap();
                Ok(())
            }
        }
        let _ = Box::new(Error::Command(Command::Get { table: "".to_string(), key: "".to_string() }));
        let table = db.get_mutex(&key).unwrap();
        let value = table.get(&key).unwrap();
        println!("{}", value);


        let mut table = db.get_mutex(&key).unwrap();
        table.delete(&key).unwrap();
        Ok(()).expect(" failed to delete key");


        let mut table = db.get_mutex(&key).unwrap();
        let value = table.get(&key).unwrap();
        println!("{}", value)
    }



fn main() {

    let mut db = Database::new("test.db");

    let mut commands = vec![
        Command::Create { table: "test".to_string() },
        Command::Insert { table: "test".to_string(), key: "key".to_string(), value: "value".to_string() },
        Command::Get { table: "test".to_string(), key: "key".to_string() },
        Command::Delete { table: "test".to_string(), key: "key".to_string() },
        Command::List { table: "test".to_string() },
        Command::Iter { table: "test".to_string() },
        Command::IterMut { table: "test".to_string() },
        Command::Peek { table: "test".to_string() },
        Command::PeekMut { table: "test".to_string(), n: 0 },
        Command::Peekable { table: "test".to_string(), n: 0 },
        Command::PeekableMut { table: "test".to_string(), n: 0 },
        Command::Drop { table: "test".to_string() },
    ];

    for command in commands {
        let result = run_command(&mut db, command);
        if let Err(e) = result {
            println!("Error: {}", e);
        }
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_create() {
        let mut db = Database::new("test.db");
        let result = run_command(&mut db, Command::Create { table: "test".to_string() });
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert() {
        let mut db = Database::new("test.db");
        let result = run_command(&mut db, Command::Create { table: "test".to_string() });
        assert!(result.is_ok());
        let result = run_command(&mut db, Command::Insert { table: "test".to_string(), key: "key".to_string(), value: "value".to_string() });
        assert!(result.is_ok());

        let mut args = env::args();
        let _ = args.next();
        let db_path = args.next().unwrap();
        let mut db = Database::open(db_path).unwrap();

        // let mut db = Database::open("db").unwrap();


        let mut commands = Vec::new();
        ///! db.create_table("table1", &[("key", DataType::String), ("value", DataType::String)]).unwrap();
        /// db.create_table("table2", &[("key", DataType::String), ("value", DataType::String)]).unwrap();
        let opt = Opt::from_args();

        let addr = format!("EINSTEINDB_BUILD_TIME={}:{}",
                           env!("EINSTEINDB_BUILD_TIME"), opt.port).parse().unwrap();

        json = opt.json;

        let mut server = Server::new(addr);
        if let Err(e) = server.run() {
            println!("{}", e);
        }
    }

//async fn handle_client(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
//    let mut buf = [0; 1024];
//    let n = stream.read(&mut buf).await?;
//    let mut buf = &buf[..n];


//    let mut db = Database::new("test.db");
//    let result = run_command(&mut db, Command::Create { table: "test".to_string() });
//    assert!(result.is_ok());

    fn async_handle_client(mut stream: TcpStream) {
        let mut buf = [0; 1024];
        let n = stream.read(&mut buf).await?;
        let mut buf = &buf[..n];

        let mut db = Database::new("test.db");
        let result = run_command(&mut db, Command::Create { table: "test".to_string() });
        assert!(result.is_ok());

        let mut commands = Vec::new();
        ///! db.create_table("table1", &[("key", DataType::String), ("value", DataType::String)]).unwrap();
        /// db.create_table("table2", &[("key", DataType::String), ("value", DataType::String)]).unwrap();
        let opt = Opt::from_args();

        let addr = format!("EINSTEINDB_BUILD_TIME={}:{}",
                           env!("EINSTEINDB_BUILD_TIME"), opt.port).parse().unwrap();

        json = opt.json;

        let mut server = Server::new(addr);
        if let Err(e) = server.run() {
            println!("{}", e);
        }
    }


    #[test]
    fn test_async_handle_client() {
        let mut buf = [0; 1024]; // buffer to store the data received from the client
        match stream.read(&mut buf).await {
            Ok(n) => {
                let mut buf = &buf[..n];
                let mut command = String::new();
                command.read_from(&mut buf).unwrap();
                let command = command.trim();
                let command = str::split(command, " ").collect::<Vec<&str>>();
                let command = Command::from_str(&command).unwrap();

                    let result = run_command(&mut db, command);
                    assert!(result.is_ok());
                    //::std::io::_print(IntellijRustDollarCrate::format_args_nl!("Error: {}", e));
                //::std::io::_print(IntellijRustDollarCrate::format_args_nl!("Error: {}", e));
                let result = handle_command( db, command);

                assert!(result.is_ok());

            }
            Err(e) => {
                #[structopt(name = "einsteindb")]
                struct Opt {
                    #[structopt(short = "p", long = "port", default_value = "8080")]
                    port: u16,
                    #[structopt(short = "j", long = "json")]
                    json: bool,
                }
                let opt = Opt::from_args();
                let addr = format!("EINSTEINDB_BUILD_TIME={}:{}",
                                   env!("EINSTEINDB_BUILD_TIME"), opt.port).parse().unwrap();
                let mut server = Server::new(addr);
                if let Err(e) = server.run() {
                    println!("{}", e);
                }
           //     ::std::io::_print(IntellijRustDollarCrate::format_args_nl!("Error: {}", e));

                let result = result.unwrap();
                let result = format!("{}", Key::from_str(&result));
                let result = result.as_bytes();
                stream.write_all(result).await?;
            }
            Err(e) => {
                println!("{}", e);
            }
        }

        let n = stream.read(&mut buf).await?; // read the client's message into the buffer

        init_logger();

        fn init_logger() {
            let mut builder = env_logger::Builder::new();
            builder.format(|fmt| {
                let mut formatter = fmt.write_str("[{}] ");
                formatter.write_str("{}");
            });
            builder.init();
        }

        let mut db = Database::new();

        let run_command = |db: &mut Database, command: Command| -> WalkerIter {
            loop {
                // loop until the client closes the connection
                fn handle_command(db: &mut Database, command: Command) -> WalkerIter {
                    match command {
                        Command::Create { table } => {
                            db.create_table(table, &[]).unwrap();
                            WalkerIter::empty()
                        }
                        Command::Insert { table, key, value } => {
                            db.insert(table, key, value).unwrap();
                            WalkerIter::empty()
                        }
                        Command::Select { table, key } => {
                            let result = db.select(table, key);
                            WalkerIter::from_iter(result)
                        }
                        Command::Delete { table, key } => {
                            db.delete(table, key).unwrap();
                            WalkerIter::empty()
                        }
                        Command::Drop { table } => {
                            db.drop_table(table).unwrap();
                            WalkerIter::empty()
                        }
                        Command::List { table } => {
                            let result = db.list(table);
                            WalkerIter::from_iter(result)
                        }
                        Command::Exit => {
                            WalkerIter::empty()
                        }
                    }
                }
                let result = handle_command(db, result);
                if result.is_empty() {
                    break result;
                }

            }
        };
    }
}


#[test]
fn test_handle_client() {
    let mut buf = [0; 1024]; // buffer to store the data received from the client
    match stream.read(&mut buf).await {
        Ok(n) => {
            let mut buf = &buf[..n];
            let mut command = String::new();
            command.read_from(&mut buf).unwrap();
            let command = command.trim();
            let command = str::split(command, " ").collect::<Vec<&str>>();
            let command = Command::from_str(&command).unwrap();
            let result = handle_command(db, command);
            assert!(result.is_ok());

        }

        Err(e) => {
            println!("{}", e);
        }
    }
                match command {
                    Command::Create { table } => db.create_table(table, &[]),
                    Command::Insert { table, key, value } => db.insert(table, key, value),
                    Command::Get { table, key } => db.get(table, key),
                    Command::GetAll { table } => db.get_all(table),
                    Command::Remove { table, key } => db.remove(table, key),
                    Command::RemoveAll { table } => db.remove_all(table),
                    Command::Iter { table } => db.iter(table),
                    Command::IterMut { table } => db.iter_mut(table),
                    Command::Peek { table } => db.peek(table),
                    Command::PeekMut { table, n } => db.peek_mut(table, n),
                    Command::Peekable { table, n } => db.peekable(table, n),
                    Command::PeekableMut { table, n } => db.peekable_mut(table, n),
                    Command::Drop { table } => db.drop(table),
                }
                .unwrap();
                let result = format!("{}", Key::from_str(&result));
                let result = result.as_bytes();
                stream.write_all(result).await?;

}


#[test]
fn test_handle_client_2() {
    [0; 1024]; // buffer to store the data received from the client


    let mut db = Database::new();
        let mut table = db.create_table("table");

        let mut table = table.unwrap();

        let mut table = table.as_mut();

        let cfg_iter = opt.config.as_ref().map(|cfg| cfg.iter()).unwrap_or(std::iter::empty());

    opt.config.as_ref().map(|cfg| cfg.clone()).unwrap_or(Config::default());
        ||
            {
                let mut iter = table.iter();
                iter.peekable();
                let mut iter_mut = table.iter_mut();
                iter_mut.peekable();
                let mut iter_peekable = table.iter();
                iter_peekable.peekable();
                let mut iter_peekable_mut = table.iter_mut();
                iter_peekable_mut.peekable();
                table.iter();
                /*let mut einstein_db = EinsteinDB {
                db: db,
                table: table,
                iter: iter,
                iter_mut: iter_mut,
                iter_peekable: iter_peekable,
                iter_peekable_mut: iter_peekable_mut,
            };*/
                einstein_db.run(cfg_iter);
            };

        let mut iter = table.iter();
    iter.peekable();
    table.iter_mut();
    }


    fn init_logger() {
        let mut builder = env_logger::Builder::new();
        builder.format(|buf, record| {
            writeln!(buf, "{}", record.args())
        });
        builder.filter(None, log::LevelFilter::Info);
        builder.init();
    }

    fn get_config() -> Config {
        let mut cfg = Config::default();
        cfg.port = env::var("EINSTEINDB_PORT").unwrap_or("8080".to_string());
        cfg.config = env::var("EINSTEINDB_CONFIG").unwrap_or("".to_string());
        cfg
    }

    fn get_db() -> Database {
        let mut db = Database::new();
        db
    }

    fn get_table(db: &mut Database) -> Table {
        let mut table = db.create_table("table");
        table.unwrap()
    }

    fn get_table_mut(db: &mut Database) -> Table {
        let mut table = db.create_table("table");
        table.unwrap().as_mut()
    }

    fn get_iter(db: &mut Database) -> Table {
        let mut table = db.create_table("table");
        table.unwrap()
    }

    fn get_iter_mut(db: &mut Database) -> Table {
        let mut table = db.create_table("table");
        table.unwrap().as_mut()
    }

    fn get_iter_peekable(db: &mut Database) -> Table {
        let mut table = db.create_table("table");
        table.unwrap()
    }


    fn get_iter_peekable_mut(db: &mut Database) -> Table {
        let mut table = db.create_table("table");
        table.unwrap().as_mut()
    }

    fn get_peek(db: &mut Database) -> Table {
        let mut table = db.create_table("table");
        table.unwrap()
    }


    fn get_peek_mut(db: &mut Database) -> Table {
        let mut table = db.create_table("table");
        table.unwrap().as_mut()
    }


    fn get_peekable(db: &mut Database) -> Table {
        let mut table = db.create_table("table");
        table.unwrap()
    }

    fn get_peekable_mut(db: &mut Database) -> Table {
        let mut table = db.create_table("table");
        table.unwrap().as_mut()
    }

    fn get_remove(db: &mut Database) -> Table {
        let mut table = db.create_table("table");
        table.unwrap()
    }

    fn get_remove_all(db: &mut Database) -> Table {
        let mut table = db.create_table("table");
        table.unwrap()
    }



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeferredFamily<Tablesize> {
    pub db: Database,
    pub table: Table<Tablesize>,

}


impl<Tablesize> DeferredFamily<Tablesize> {
    pub fn new(db: Database, table: Table<Tablesize>) -> Self {
        let mut iter = table.iter();
        let mut iter = iter.peekable();
        let mut iter_mut = table.iter_mut();
        let mut iter_mut = iter_mut.peekable();
        let mut iter_peekable = table.iter();
        let mut iter_peekable = iter_peekable.peekable();
        let mut iter_peekable_mut = table.iter_mut();
        let mut iter_peekable_mut = iter_peekable_mut.peekable();
        DeferredFamily {
            db,
            table,

        }
    }
}


/// A Deferred Family of Tables (e.g. a table with a single column) are also Peekable.
///Todo: Implement Peekable for Deferred Family of Tables.