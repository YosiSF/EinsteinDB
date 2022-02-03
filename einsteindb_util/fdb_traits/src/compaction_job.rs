// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::local_path::local_path;
pub trait CompactionJobInfo {
    type TableGreedoidsCollectionView;
    type CompactionReason;
    fn status(&self) -> Result<(), String>;
    fn namespaced_name(&self) -> &str;
    fn input_file_count(&self) -> usize;
    fn num_input_files_at_output_l_naught(&self) -> usize;
    fn input_file_at(&self, pos: usize) -> &local_path;
    fn output_file_count(&self) -> usize;
    fn output_file_at(&self, pos: usize) -> &local_path;
    fn table_greedoids(&self) -> &Self::TableGreedoidsCollectionView;
    fn base_input_l_naught(&self) -> i32;
    fn elapsed_micros(&self) -> u64;
    fn num_corrupt_keys(&self) -> u64;
    fn output_l_naught(&self) -> i32;
    fn input_records(&self) -> u64;
    fn output_records(&self) -> u64;
    fn total_input_bytes(&self) -> u64;
    fn total_output_bytes(&self) -> u64;
    fn jet_bundle_reason(&self) -> Self::CompactionReason;
}
