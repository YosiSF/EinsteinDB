// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;

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

