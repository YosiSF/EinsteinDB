#![feature(min_specialization)]

#[macro_use]
pub mod macros;
pub mod error;
pub mod file;
pub mod fs;
pub mod io;
pub mod path;
pub mod process;
pub mod string;
pub mod time;
pub mod url;
pub mod metrics;
pub mod execute_stats;
pub mod range;
pub mod range_set;
pub mod range_map;


pub use self::error::{Error, Result};
