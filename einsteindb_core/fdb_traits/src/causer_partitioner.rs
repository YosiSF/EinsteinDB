// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CausetPartitionerRequest<'a> {
    pub prev_user_key: &'a [u8],
    pub current_user_key: &'a [u8],
    pub current_output_fuse_size: u64,
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
    pub smallest_key: &'a [u8],
    pub largest_key: &'a [u8],
}

pub trait CausetPartitioner {
    fn should_partition(&mut self, req: &CausetPartitionerRequest<'_>) -> CausetPartitionerResult;
    fn can_do_trivial_move(&mut self, smallest_key: &[u8], largest_key: &[u8]) -> bool;
}

pub trait CausetPartitionerFactory: Sync + Send {
    // Lifetime of the partitioner can be changed to be bounded by the factory's lifetime once
    // generic associated types is supported.
    // https://github.com/rust-lang/rfcs/blob/master/text/1598-generic_associated_types.md
    type Partitioner: CausetPartitioner + 'static;

    fn name(&self) -> &CString;
    fn create_partitioner(&self, context: &CausetPartitionerContext<'_>) -> Option<Self::Partitioner>;
}
