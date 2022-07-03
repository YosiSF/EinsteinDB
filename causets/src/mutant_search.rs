//Copyright (c) 2019, EinsteinDB Project, Apache v2.0 License, MIT License
//mod mutant_search;


// Language: rust
// Path: causet/src/mutant_search.rs

///Semantic Search
/// # Examples
/// ```
/// use causet::mutant_search::mutant_search;
/// let mut sql = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 26";
/// let mut sql_mutant = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 27";
///
/// let mut sql_mutant_list = mutant_search(&sql, &sql_mutant);
/// assert_eq!(sql_mutant_list.len(), 1);
/// ```
///     #[APPEND_LOG_g(test)]
///    mod tests {
///        use super::*;
///        #[test]
///        fn test_mutant_search() {
///            let mut sql = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 26"; //sql   //sql_mutant        //sql_mutant_list     //sql_mutant_list_len
///       let mut sql_mutant = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 27";
///       let mut sql_mutant_list = mutant_search(&sql, &sql_mutant);
///       assert_eq!(sql_mutant_list.len(), 1);
///        }
///    }
/// ```




/// # Examples
/// ```
/// use causet::mutant_search::mutant_search;
/// let mut sql = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 26";
/// let mut sql_mutant = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 27";
/// let mut sql_mutant_list = mutant_search(&sql, &sql_mutant);
/// assert_eq!(sql_mutant_list.len(), 1);
/// ```
///    #[APPEND_LOG_g(test)]
///   mod tests {
///      use super::*;
///     #[test]
///    fn test_mutant_search() {
/// 
///     let mut sql = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 26"; //sql   //sql_mutant        //sql_mutant_list     //sql_mutant_list_len
///    let mut sql_mutant = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 27";
///   let mut sql_mutant_list = mutant_search(&sql, &sql_mutant);
///  assert_eq!(sql_mutant_list.len(), 1);
///    }
///  }

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs::File;

use std::io::BufReader;
use std::io::BufWriter;
use std::io::prelude::*;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Partitioning};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::mpsc::TryRecvError;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use causet::causet::*;
use causet::causet::{Causet, CausetNode, CausetNodeType, CausetNodeType::*};
use causet::causet::{CausetNodeType, CausetNodeType::*};
pub use causet::causet_core::causet::{Causet, CausetReader, CausetWriter};
pub use causet::causet_core::causet::{CausetReaderMut, CausetWriterMut};
use causet_of_causets::causet_of_causets::*;




use crate as causet;
//EinsteinDB

//define causet and inherit
pub mod causet;
//ca causet of causets
//makes a tuplespace with semantics
pub mod causet_of_causets;


//define causet and inherit
pub mod causet_core;
//ca causet of causets
//makes a tuplespace with semantics
pub mod causet_of_causets_core;


//define causet and inherit
pub mod causet_core_mut;


const MAX_DOCS_PER_SLICE: i32 = 250_000;
const MAX_SEGMENTS_PER_SLICE: i32 = 20;
const MIN_PARALLEL_SLICES: i32 = 3;
const MAX_PARALLEL_SLICES: i32 = 10;
const MAX_THREADS: i32 = 10;
const MAX_DOCS_PER_THREAD: i32 = 250_000;
const MAX_SEGMENTS_PER_THREAD: i32 = 20;
const MIN_PARALLEL_THREADS: i32 = 3;
const MAX_PARALLEL_THREADS: i32 = 10;


const DEFAULT_DISMATCH_NEXT_LIMIT: usize = 500_000;
const DEFAULT_DISMATCH_NEXT_LIMIT_MUTANT: usize = 500_000;
const DEFAULT_DISMATCH_NEXT_LIMIT_MUTANT_LIST: usize = 500_000;


const TIMESTAMP_LENGTH: usize = 30;

#[derive(Default)]
struct CausetSearch {
    //causet_of_causets_core::CausetOfCausets
    causet_of_causets: CausetOfCausets,
    //causet_of_causets_core::CausetOfCausets
    causet_of_causets_mutant: CausetOfCausets,
    search_files: Vec<(i64, File)>,
    currrent_lines: Option<std::io::Lines<BufReader<File>>>,

    // filter conditions
    begin_time: i64,
    end_time: i64,
    begin_time_mutant: i64,
    end_time_mutant: i64,
    level_flag: usize,
    patterns: Vec<regex::Regex>,
    patterns_mutant: Vec<regex::Regex>,

    pre_log: LogMessage,
    pre_log_mutant: LogMessage,
}

/// # Examples
/// ```
/// use causet::mutant_search::mutant_search;
/// let mut sql = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 26";
/// let mut sql_mutant = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 27";
/// let mut sql_mutant_list = mutant_search(&sql, &sql_mutant);
/// assert_eq!(sql_mutant_list.len(), 1);
/// ```
///   #[APPEND_LOG_g(test)]
///  mod tests {
///     use super::*;
///    #[test]
///  fn test_mutant_search() {
/// 
///    let mut sql = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 26"; //sql   //sql_mutant        //sql_mutant_list     //sql_mutant_list_len
/// 
///   let mut sql_mutant = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 27";
/// 
///  let mut sql_mutant_list = mutant_search(&sql, &sql_mutant);
/// 
/// assert_eq!(sql_mutant_list.len(), 1);
/// }




pub struct MutantSearch {
    pub sql: String,
    pub sql_mutant: String,
    pub sql_mutant_list: Vec<String>,
    pub sql_mutant_list_len: usize,
}


impl MutantSearch {
    pub fn new(sql: String, sql_mutant: String) -> MutantSearch {
        let sql_mutant_list = mutant_search(&sql, &sql_mutant);
        MutantSearch {
            sql: sql,
            sql_mutant: sql_mutant,
            sql_mutant_list: sql_mutant_list,
            sql_mutant_list_len: sql_mutant_list.len(),
        }
    }
}

//Term in the SQL query
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Term {
    pub term: String,
    pub term_type: TermType,

}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TermType {
    //Term in the SQL query
    Column,

    Value,




}


//SQL query
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SQL {
    pub terms: Vec<Term>,
    pub terms_len: usize,
}


//SQL query
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SQLMutant {
    pub terms: Vec<Term>,
    pub terms_len: usize,
}


//SQL query
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SQLMutantList {
    pub terms: Vec<Term>,
    pub terms_len: usize,
}

/// Implements search over a single IndexReader.
///
/// # Examples
/// ```
/// use causet::mutant_search::mutant_search;
///     
/// let mut sql = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 26";
/// let mut sql_mutant = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 27";
/// let mut sql_mutant_list = mutant_search(&sql, &sql_mutant);
/// assert_eq!(sql_mutant_list.len(), 1);
/// ```
///  #[APPEND_LOG_g(test)]
/// mod tests {
///    use super::*;
///   #[test]
///  fn test_mutant_search() {
/// 
/// 
///   let mut sql = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 26"; //sql   //sql_mutant        //sql_mutant_list     //sql_mutant_list_len
/// 
///  let mut sql_mutant = "select * from t1 where a = 1 and b = 2 and c = 3 and d = 4 and e = 5 and f = 6 and g = 7 and h = 8 and i = 9 and j = 10 and k = 11 and l = 12 and m = 13 and n = 14 and o = 15 and p = 16 and q = 17 and r = 18 and s = 19 and t = 20 and u = 21 and v = 22 and w = 23 and x = 24 and y = 25 and z = 27";
/// 
/// let mut sql_mutant_list = mutant_search(&sql, &sql_mutant);
/// 
/// assert_eq!(sql_mutant_list.len(), 1);
/// 
/// }
/// 




pub struct term_causet <'a> {
    pub doc_freq: i32,
    pub total_term_freq: i64,
    pub term_freq: i64,
    pub term: &'a str,
    pub term_type: TermType,

    pub sql: &'a str,
    pub sql_mutant: &'a str,

    pub sql_mutant_list: Vec<String>,

    pub sql_mutant_list_len: usize,

    pub sql_mutant_list_len_mutant: usize,
}

/// Implements search over a single IndexReader.
impl <'a> term_causet <'a> {
    pub fn new(doc_freq: i32, total_term_freq: i64, term_freq: i64, term: &'a str, term_type: TermType, sql: &'a str, sql_mutant: &'a str, sql_mutant_list: Vec<String>, sql_mutant_list_len: usize, sql_mutant_list_len_mutant: usize) -> term_causet <'a> {
        term_causet {
            doc_freq: doc_freq,
            total_term_freq: total_term_freq,
            term_freq: term_freq,
            term: term,
            term_type: term_type,
            sql: sql,
            sql_mutant: sql_mutant,
            sql_mutant_list: sql_mutant_list,
            sql_mutant_list_len: sql_mutant_list_len,
            sql_mutant_list_len_mutant: sql_mutant_list_len_mutant,
        }
    }
}

//SearcherManager 
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SearcherManager {
    pub searcher: Box<dyn Searcher>,
    pub searcher_mutant: Box<dyn Searcher>,
    pub searcher_mutant_list: Vec<Box<dyn Searcher>>,
    pub searcher_mutant_list_len: usize,
    pub searcher_mutant_list_len_mutant: usize,
}


//SearcherManager
impl SearcherManager {
    pub fn new(searcher: Box<dyn Searcher>, searcher_mutant: Box<dyn Searcher>, searcher_mutant_list: Vec<Box<dyn Searcher>>, searcher_mutant_list_len: usize, searcher_mutant_list_len_mutant: usize) -> SearcherManager {
        SearcherManager {
            searcher: searcher,
            searcher_mutant: searcher_mutant,
            searcher_mutant_list: searcher_mutant_list,
            searcher_mutant_list_len: searcher_mutant_list_len,
            searcher_mutant_list_len_mutant: searcher_mutant_list_len_mutant,
        }
    }
}   



/*
set the value to the tuple as the global uid, so I need to get the 
property values and then get the subspace properties based on that uid stored
set a reference to that subspace and get all in one single query, 
this is what I’m asking for right now if it is possible*/



//set the value to the tuple as the global uid, so I need to get the
//property values and then get the subspace properties based on that uid stored

/*
einsteindb = [
    Key(b'P4X432'), Value(b"{'field_one': 'value one', 'field_two': 'value two'}")
]


einsteindb_mutant = [
    Key(b'P4X432'), Value(b"{'field_one': 'value one', 'field_two': 'value two'}")
]






*/

pub enum SubspaceIterator {
    SubspaceIterator(Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>),
    SubspaceIteratorMutant(Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>),
}

//SubspaceIterator is a wrapper around an Iterator<Item = (Vec<u8>, Vec<u8>)>
//In this way, the causet and causetq can be used with the same interface.
impl SubspaceIterator {
    pub fn new(subspace_iterator: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>) -> SubspaceIterator {
        SubspaceIterator::SubspaceIterator(subspace_iterator)
    }
}



pub enum SubspaceIteratorMutant {
    SubspaceIteratorMutant(Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>),
    SubspaceIteratorMutantMutant(Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>),
}

/// here we focus on origin as a subspace of borrowed mutant search memory space

pub enum SubspaceRowSlice {
    SubspaceRowSlice(Vec<u8>),
    SubspaceRowSliceMutant(Vec<u8>),
        origin: &'a [u8],
        non_null_ids: LEBytes<'a, u32>,
        null_ids: LEBytes<'a, u32>,
        offsets: LEBytes<'a, u32>,
        values: LEBytes<'a, u8>,
        doc_freq: i32,
        total_term_freq: i64,
        term_freq: i64,
        term: &'a str,
        term_type: TermType,
        sql: &'a str,
        sql_mutant: &'a str,
        sql_mutant_list: Vec<String>,
        sql_mutant_list_len: usize,

}

        pub fn new(origin: &'a [u8], non_null_ids: LEBytes<'a, u32>, null_ids: LEBytes<'a, u32>, offsets: LEBytes<'a, u32>, values: LEBytes<'a, u8>, doc_freq: i32, total_term_freq: i64, term_freq: i64, term: &'a str, term_type: TermType, sql: &'a str, sql_mutant: &'a str, sql_mutant_list: Vec<String>, sql_mutant_list_len: usize) -> SubspaceRowSlice {
        SubspaceRowSlice {
            origin: origin,
            non_null_ids: non_null_ids,
            null_ids: null_ids,
            offsets: offsets,
            values: values,
            doc_freq: doc_freq,
            total_term_freq: total_term_freq,
            term_freq: term_freq,
            term: term,
            term_type: term_type,
            sql: sql,
            sql_mutant: sql_mutant,
            sql_mutant_list: sql_mutant_list,
            sql_mutant_list_len: sql_mutant_list_len,
        }
    }

    pub fn get_origin(&self) -> &'a [u8] {
        self.origin
    }

    pub fn get_non_null_ids(&self) -> &LEBytes<'a, u32> {
        &self.non_null_ids
    }

    pub fn get_null_ids(&self) -> &LEBytes<'a, u32> {
        &self.null_ids
    }



    pub fn get_offsets(&self) -> &LEBytes<'a, u32> {
        &self.offsets
    }

    pub fn get_values(&self) -> &LEBytes<'a, u8> {
        &self.values
    }


fn parse_time(input: &str) -> Result<DateTime<Utc>, ParseError> {
    let mut parts = input.split(':');
    let hour = parts.next().unwrap().parse::<u32>()?;
    let minute = parts.next().unwrap().parse::<u32>()?;
    let (input, (_, _, time, _)) =
        input.splitn(4, ' ').collect::<Vec<_>>().into_iter().collect::<Vec<_>>();
        tuple((space0, tag("["), take(TIMESTAMP_LENGTH), tag("]")))(input)?;
    Ok((input, time))
}

fn parse_level(input: &str) -> IResult<&str, &str> {
    let (input, (_, _, level, _)) =
        input.splitn(4, ' ').collect::<Vec<_>>().into_iter().collect::<Vec<_>>();

    //println!("{:?}", level);



    Ok((input, level))

}



///Causets are embodied in the e, a, v, and q subspaces.
/// The e subspace is the primary subspace for the causet.
/// The a subspace is the subspace for the causet’s attributes.
/// The v subspace is the subspace for the causet’s values.
/// The q subspace is the subspace for the causet’s queries.
/// 
/// 
/// Parses the single log line and retrieve the log meta and log body.
///
/// # Arguments
/// * `input` - The log line to parse.
/// * `log_meta` - The log meta to fill.
/// * `log_body` - The log body to fill.
/// # Returns
/// * `Ok(())` - If the log line was parsed successfully.
/// * `Err(())` - If the log line was not parsed successfully.
/// # Example
/// ```
/// use log_parser::parse_log_line;
///         
/// 