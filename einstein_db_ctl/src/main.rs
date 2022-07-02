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


#[derive(StructOpt, Debug)]
#[structopt(name = "einstein_db_ctl")]
struct Opt {
    #[structopt(subcommand)]
    cmd: Command,
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
        #[structopt(name = "end")
        end: String,
        #[structopt(name = "time")]
        time: u64,
    },
    #[structopt(name = "time_travel_nth")],
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
        Opt { cmd: Command::Create { table } } => {
            let mut db = Database::new();
            let mut table = db.create_table(table).unwrap();
            let mut table = table.as_mut();
            println!("Table {} created", table.name());
            0
        }
        Opt { cmd: Command::Drop { table } } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.drop();
            println!("Table {} dropped", table.name());
            0
        }
        Opt { cmd: Command::Insert { table, key, value } } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.insert(key, value).unwrap();
            println!("Inserted {}:{}", key, value);
            0
        }
        Opt { cmd: Command::Get { table, key } } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let value = table.get(key).unwrap();
            println!("{}:{}", key, value);
            0
        }
        Opt { cmd: Command::Delete { table, key } } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.delete(key).unwrap();
            println!("Deleted {}", key);
            0
        }
        Opt { cmd: Command::List { table } } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.iter();
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }
        Opt { cmd: Command::Iter { table } } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.iter();
            while let Some(key) = iter.next() {
                println!("{}", key);
            }
            0
        }

        Opt { cmd: Command::Peek { table, key } } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let value = table.peek(key).unwrap();
            println!("{}:{}", key, value);
            0
        }

        Opt { cmd: Command::PeekBack { table, key } } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let value = table.peek_back(key).unwrap();
            println!("{}:{}", key, value);
            0
        }

        Opt { cmd: Command::PeekNth { table, n } } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let value = table.peek_nth(n).unwrap();
            println!("{}", value);
            0
        }
    let opt = Opt::from_args();
    
    match opt.cmd {
        Command::Create { table } => {
            let mut db = Database::new();
            let mut table = db.create_table(table).unwrap();
            let mut table = table.as_mut();
            println!("Table created");
        }
        Command::Drop { table } => {
            let mut db = Database::new();
            let mut table = db.drop_table(table).unwrap();
            println!("Table dropped");
        }
        Command::Insert { table, key, value } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.insert(key, value).unwrap();
            println!("Inserted");
        }
        Command::Get { table, key } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let value = table.get(key).unwrap();
            println!("{}", value);
        }
        Command::Delete { table, key } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            table.delete(key).unwrap();
            println!("Deleted");
        }
        Command::List { table } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.iter();
            while let Some(key) = iter.next().unwrap() {
                println!("{}", key);
            }
        }
        Command::Iter { table } => {
            let mut db = Database::new();
            let mut table = db.get_table(table).unwrap();
            let mut table = table.as_mut();
            let mut iter = table.iter();
            while let Some(key) = iter.next().unwrap() {
                println!("{}", key);
            }
        }

    }
}





