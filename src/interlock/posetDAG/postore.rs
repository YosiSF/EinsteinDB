//Copyright 2019 EinsteinDB Venire Labs Inc Apache 2.0 License

use einstein_query::storage::{IntervalRange, OwnedKvPair, PointRange, Result as QEResult, Storage};

use crate::interlock::Error;
use crate::storage::Statistics;
use crate::storage::{Key, Scanner, Store};

pub struct EinsteinDBStorage<S: Store> {
  pub fn new (store: S) -> Self {
  
  
  }
}

impl<S: Store> From<S> for EinsteinDBStorage<S> {
//Ensure typesafety of metric collection
//so as to remain integer-based.

  type: Statistics = Statistics;
  
  fn begin_branescan(
    &mut self,
    is_backward_scan: bool,
    is_key_only: bool,
    range: IntervalRange,
    
  
  )
}
