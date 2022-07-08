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


use soliton_panic::*;
use soliton::*;
use causal_set::CausalSet;




#[no_mangle]
#[allow(unused_variables)]



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
