// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.
// mod for einsteindb_macro_test
//
// mod for einsteindb_macro_impl_test
//
// mod for einsteindb_macro_test {
//    #[macro_use]
//   extern crate einsteindb_macro;
//
//  use einsteindb_macro::*;
//  use std::collections::HashMap;
//
// #[macro_use]
//
// mod for einsteindb_macro_test {
//   #[macro_use]}

use std::ffi::CString;
use std::os::raw::c_char;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::{thread, time};


use std::sync::mpsc::{RecvError, RecvTimeoutError};
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::TrySendError;
use std::sync::mpsc::SendError;


use std::sync::mpsc::{RecvError, RecvTimeoutError};
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::TrySendError;


use std::sync::mpsc::{RecvError, RecvTimeoutError};





use soliton_panic::*;
use soliton::*;
use causetq::*;
use causetq::{AllegroPoset, Poset};
use causetq::{PosetError, PosetErrorKind};
use causetq::{PosetNode, PosetNodeId, PosetNodeData};
use causetq::{Sync};
use causetq::{SyncError, SyncErrorKind};
use causetq::{SyncNode, SyncNodeId, SyncNodeData};

use causet::*;
use causet::{Causet};
use causet::{CausetError, CausetErrorKind};
use causet::{CausetNode, CausetNodeId, CausetNodeData};

use einstein_ml::*;
use einstein_ml::{EinsteinML};
use einstein_ml::{EinsteinMLError, EinsteinMLErrorKind};
use einstein_ml::{EinsteinMLNode, EinsteinMLNodeId, EinsteinMLNodeData};


use einstein_db::*;
use einstein_db::{EinsteinDB};
use einstein_db::{EinsteinDBError, EinsteinDBErrorKind};
use einstein_db::{EinsteinDBNode, EinsteinDBNodeId, EinsteinDBNodeData};

use berolinasql::*;
use berolinasql::{BerolinaSQL};
use berolinasql::{BerolinaSQLError, BerolinaSQLErrorKind};
use berolinasql::{BerolinaSQLNode, BerolinaSQLNodeId, BerolinaSQLNodeData};

use einsteindb_server::*;
use einsteindb_server::{EinsteinDBServer};
use einsteindb_server::{EinsteinDBServerError, EinsteinDBServerErrorKind};


use soliton_panic::*;
use soliton::*;
use causetq::*;




#[derive(Serialize, Deserialize)]
pub struct SyncConfig {
    pub name: String,
    pub thread_count: usize,
}





#[derive(Debug)]
pub struct ErrorImpl {
    pub kind: ErrorKind,
}


#[derive(Debug)]
pub enum ErrorKind {
    /// The underlying `AllegroPoset` has failed.
    /// This error is returned when the underlying `AllegroPoset` fails.
    Poset(PosetError),
    /// The underlying `Causet` has failed.
    /// This error is returned when the underlying `Causet` fails.
    Causet(CausetError),
    /// The underlying `EinsteinML` has failed.
    /// This error is returned when the underlying `EinsteinML` fails.
    EinsteinML(EinsteinMLError),
    /// The underlying `EinsteinDB` has failed.
    /// This error is returned when the underlying `EinsteinDB` fails.
    EinsteinDB(EinsteinDBError),
    /// The underlying `BerolinaSQL` has failed.
    /// This error is returned when the underlying `BerolinaSQL` fails.
    BerolinaSQL(BerolinaSQLError),
    /// The underlying `EinsteinDBServer` has failed.
    /// This error is returned when the underlying `EinsteinDBServer` fails.
    EinsteinDBServer(EinsteinDBServerError),
    /// The underlying `Sync` has failed.
    /// This error is returned when the underlying `Sync` fails.
    Sync(SyncError),
    /// The underlying `Soliton` has failed.
    /// This error is returned when the underlying `Soliton` fails.
    Soliton(SolitonError),
    /// The underlying `SolitonPanic` has failed.
    /// This error is returned when the underlying `SolitonPanic` fails.
    SolitonPanic(SolitonPanicError),
    /// The underlying `CausetQ` has failed.
    /// This error is returned when the underlying `CausetQ` fails.
    CausetQ(CausetQError),
    /// The underlying `CausetQPanic` has failed.
    /// This error is returned when the underlying `CausetQPanic` fails.
    CausetQPanic(CausetQPanicError),
    /// The underlying `BerolinaSQLPanic` has failed.
    /// This error is returned when the underlying `BerolinaSQLPanic` fails.
    /// This error is returned when the underlying `BerolinaSQLPanic` fails.

    BerolinaSQLPanic(BerolinaSQLPanicError),
    /// The underlying `EinsteinDBPanic` has failed.
}




impl ErrorImpl {
    pub fn new(kind: ErrorKind) -> Self {
        ErrorImpl { kind }
    }
}



///! The `Error` type for the `einstein_db` crate.
/// This type is used to represent errors that can occur when using the `einstein_db` crate.
/// It is a wrapper around the `ErrorImpl` type.
/// It is used to provide a more user-friendly error message.
/// It is also used to provide a more user-friendly error message.
///
///
/// # Examples
/// ```
/// use einstein_db::*;
/// use einstein_db::Error;
///
/// let error = Error::new(ErrorKind::Poset(PosetError::new(PosetErrorKind::NodeNotFound)));
/// println!("{:?}", error);
/// ```
/// use einstein_db::*;
/// use einstein_db::Error;
/// use einstein_db::ErrorKind;
///
/// let error = Error::new(ErrorKind::Poset(PosetError::new(PosetErrorKind::NodeNotFound)));
/// println!("{:?}", error);
///





#[derive(Debug)]
pub struct Error {
    pub error: ErrorImpl,
}


impl Error {
    pub fn new(error: ErrorImpl) -> Self {
        Error { error }
    }
}






#[no_mangle]
#[allow(unused_variables)]
pub extern "C" fn new_sync(config: &SyncConfig) -> Result<Arc<Sync>, ErrorImpl> {
    let poset = new_sync(config)?;
    let sync = Sync::new(poset);
    Ok(sync)
}


#[no_mangle]
#[allow(unused_variables)]
pub extern "C" fn new_sync_with_db(config: &SyncConfig, db: &Arc<EinsteinDB>) -> Result<Arc<Sync>, ErrorImpl> {
    let poset = new_sync_with_db(config, db)?;
    let sync = Sync::new(poset);
    Ok(sync)
}


#[no_mangle]
#[allow(unused_variables)]
pub extern "C" fn new_sync_with_ml(config: &SyncConfig, ml: &Arc<EinsteinML>) -> Result<Arc<Sync>, ErrorImpl> {
    let poset = new_sync_with_ml(config, ml)?;
    let sync = Sync::new(poset);
    Ok(sync)
}


#[no_mangle]
#[allow(unused_variables)]
pub extern "C" fn new_sync_with_sql(config: &SyncConfig, sql: &Arc<BerolinaSQL>) -> Result<Arc<Sync>, ErrorImpl> {
    let poset = new_sync_with_sql(config, sql)?;
    let sync = Sync::new(poset);
    Ok(sync)
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CausetPartitionerRequest<'a> {
    pub prev_user_soliton_id: &'a [u8],
    pub current_user_soliton_id: &'a [u8],
    pub current_output_file_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CausetPartitionerResult {
    NotRequired,
    Required,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CausetPartitionerContext<'a> {
    pub is_full_jet_bundle: bool,
    pub is_manual_jet_bundle: bool,
    pub output_l_naught: i32,
    pub smallest_soliton_id: &'a [u8],
    pub largest_soliton_id: &'a [u8],
    pub current_user_soliton_id: &'a [u8],
    pub current_output_file_size: u64,

}



impl CausetPartitionerContext<'a> {
pub fn new(
        is_full_jet_bundle: bool,
        is_manual_jet_bundle: bool,
        output_l_naught: i32,
        smallest_soliton_id: &'a [u8],
        largest_soliton_id: &'a [u8],
        current_user_soliton_id: &'a [u8],
        current_output_file_size: u64,
    ) -> Self {
        Self {
            is_full_jet_bundle,
            is_manual_jet_bundle,
            output_l_naught,
            smallest_soliton_id,
            largest_soliton_id,
            current_user_soliton_id,
            current_output_file_size,
        }
    }


}



pub trait CausetPartitioner {
    fn partitioner_name(&self) -> &str;
    fn partitioner_name_cstr(&self) -> CString;
    fn partitioner_name_cstr_mut(&mut self) -> &mut CString;
    fn partitioner_name_cstr_mut_ref(&mut self) -> &mut CString;
    fn partitioner_name_cstr_ref(&self) -> &CString;
    fn partitioner_name_cstr_ref_mut(&mut self) -> &mut CString;
    fn partitioner_name_cstr_mut_ref_ref(&mut self) -> &mut CString;

    fn should_partition(&mut self, req: &CausetPartitionerRequest<'_>) -> CausetPartitionerResult;
    fn can_do_trivial_move(&mut self, smallest_soliton_id: &[u8], largest_soliton_id: &[u8]) -> bool;
    fn partitioner_context(&mut self, req: &CausetPartitionerRequest<'_>) -> CausetPartitionerContext<'_>;

}

pub trait CausetPartitionerFactory: Sync + Send {

// end trait CausetPartitionerFactory

    fn name(&self) -> &CString;
    fn name_mut(&mut self) -> &mut CString;
    fn name_mut_ref(&mut self) -> &mut CString;
    fn name_ref(&self) -> &CString;
    fn name_mut_ref_ref(&mut self) -> &mut CString;

    fn should_partition(&mut self, req: &CausetPartitionerRequest<'_>) -> CausetPartitionerResult;
    fn can_do_trivial_move(&mut self, smallest_soliton_id: &[u8], largest_soliton_id: &[u8]) -> bool;
    fn partitioner_context(&mut self, req: &CausetPartitionerRequest<'_>) -> CausetPartitionerContext<'_>;

    fn create_partitioner(&self, context: &CausetPartitionerContext<'_>) -> Option<Self::Partitioner>;
    // end trait CausetPartitionerFactory
}// end trait CausetPartitionerFactory

fn main() {

    let mut soliton_panic = SolitonPanic::new();
    soliton_panic.run();
}
