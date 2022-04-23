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
use causet_of_causets::causet_of_causets::{CausetOfCausets, CausetOfCausetsNode, CausetOfCausetsNodeType, CausetOfCausetsNodeType::*};
use causet_of_causets::causet_of_causets::{CausetOfCausetsNodeType, CausetOfCausetsNodeType::*};

//EinsteinDB

//define causet and inherit
pub mod causet;
//ca causet of causets
//makes a tuplespace with semantics
pub mod causet_of_causets;
