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

extern crate structopt;
extern crate einstein_db;
extern crate futures;
extern crate tokio;
extern crate tokio_codec;
extern crate tokio_io;
extern crate tokio_tcp;
extern crate tokio_timer;


use structopt::StructOpt;
use einstein_db::prelude::*;
use futures::{Future, Stream};
use tokio::prelude::*;
use tokio_codec::{Framed, LinesCodec};
use tokio_io::io;





pub struct EinsteinDB {

    pub db: Database,


    pub table: Table,

    pub iter: Iter,

    pub iter_mut: IterMut,


    pub iter_peekable: Peekable<Iter>,


    pub iter_peekable_mut: Peekable<IterMut>,


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

fn main(){

    let status = std::process::exit(match Opt::from_args() {
        Opt { port } => {
            let mut db = Database::new();
            let mut table = db.create_table(table).unwrap();
            let mut table = table.as_mut();
            println!("Table {} created", table.name());
            0
        }
        _ => 1
    });
    std::process::exit(status);
}




impl EinsteinDB {
    pub fn new() -> EinsteinDB {
        EinsteinDB {
            db: Database::new(),
            table: Table::new(),
            iter: Iter::new(),
            iter_mut: IterMut::new(),
            iter_peekable: Peekable::new(),
            iter_peekable_mut: Peekable::new(),
        }
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
}


impl EinsteinDB {
    pub fn delete(&mut self, key: &str) -> Result<&mut Table, Error> {
        self.table.delete(key)
    }
}


impl EinsteinDB {
    pub fn list(&mut self) -> Result<&mut Table, Error> {
        self.table.list()
    }
}

fn handle_command(command: Command, db: &mut Database) -> Result<(), Box<dyn std::error::Error>> {

   /// create table
   /// drop table
   /// insert key value
   /// get key
    /// delete key
   ///






    match command {
        Command::Create { table } => {
            let mut table = db.create_table(table).unwrap();
            let mut table = table.as_mut();
            println!("Table {} created", table.name());
            let mut db = Database::new();
            let mut table = db.create_table(table).unwrap();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.drop();
            println!("Table {} dropped", table.name());
            0
        }

        Command::Drop { table } => {
            let mut table = db.drop_table(table).unwrap();
            let mut table = table.as_mut();
            println!("Table {} dropped", table.name());
            0
        }

        Command::Insert { table, key, value } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.insert(key, value).unwrap();
            println!("Inserted {}", key);
            0
        }

        Command::Get { table, key } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let value = table.get(key).unwrap();
            println!("{}", value);
            0
        }

        Command::Delete { table, key } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.delete(key).unwrap();
            println!("Deleted {}", key);
            0
        }

        Command::List { table } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.list().unwrap();
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Command::Peek { table} => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.peek(n).unwrap();
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Command::PeekMut { table, n } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.peek_mut(n).unwrap();
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Command::Peekable { table, n } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.peekable(n).unwrap();
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Command::PeekableMut { table, n } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.peekable_mut(n).unwrap();
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }, // end of peekable mut

        Command::Drop { table } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.drop();
            println!("Table {} dropped", table.name());
            0
        }
        Command::Insert { table, key, value } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.insert(key, value);
            println!("Inserted {}: {}", key, value);
            0
        }

        Command::Get { table, key } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let value = table.get(key);
            println!("{}: {}", key, value);
            0
        }

        Command::Delete { table, key } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.delete(key);
            println!("Deleted {}", key);
            0
        }

        Command::List { table } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.list();
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Command::Iter { table } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.iter();
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Command::IterMut { table } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let value = table.peek(key);
            println!("{}: {}", key, value);
            0
        }

        Command::Peek { table} => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.peek(n);
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Command::PeekMut { table, n } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.peek_mut(n);
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Command::Peekable { table, n } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.peekable(n);
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Command::PeekableMut { table, n } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.peekable_mut(n);
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Command::Drop { table } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.drop();
            println!("Table {} dropped", table.name());
            0
        }

        Command::Insert { table, key, value } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.insert(key, value);
            println!("Inserted {}: {}", key, value);
            0
        }

        Command::Get { table, key } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let value = table.get(key);
            println!("{}: {}", key, value);
            0
        }

        Command::Delete { table, key } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.delete(key);
            println!("Deleted {}", key);
            0
        }

        Command::List { table } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.list();
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Command::Iter { table } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.iter();
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Command::IterMut { table } => {
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let value = table.peek(key);
            println!("{}: {}", key, value);
            0
        }
    }

    Ok(())
}


