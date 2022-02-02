// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;

pub struct FdbSstPartitionerFactory<F: fdb_traits::SstPartitionerFactory>(pub F);

impl<F: fdb_traits::SstPartitionerFactory> foundationdb::SstPartitionerFactory
for FdbSstPartitionerFactory<F>
{
    type Partitioner = FdbSstPartitioner<F::Partitioner>;

    fn name(&self) -> &CString {
        self.0.name()
    }

    fn create_partitioner(
        &self,
        context: &foundationdb::SstPartitionerContext<'_>,
    ) -> Option<Self::Partitioner> {
        let ctx = fdb_traits::SstPartitionerContext {
            is_full_jet_bundle: context.is_full_jet_bundle,
            is_manual_jet_bundle: context.is_manual_jet_bundle,
            output_l_naught: context.output_l_naught,
            smallest_key: context.smallest_key,
            largest_key: context.largest_key,
        };
        self.0.create_partitioner(&ctx).map(FdbSstPartitioner)
    }
}

pub struct FdbSstPartitioner<P: fdb_traits::SstPartitioner>(P);

impl<P: fdb_traits::SstPartitioner> foundationdb::SstPartitioner for FdbSstPartitioner<P> {
    fn should_partition(
        &mut self,
        request: &foundationdb::SstPartitionerRequest<'_>,
    ) -> foundationdb::SstPartitionerResult {
        let req = fdb_traits::SstPartitionerRequest {
            prev_user_key: request.prev_user_key,
            current_user_key: request.current_user_key,
            current_output_file_size: request.current_output_file_size,
        };
        match self.0.should_partition(&req) {
            fdb_traits::SstPartitionerResult::NotRequired => {
                foundationdb::SstPartitionerResult::NotRequired
            }
            fdb_traits::SstPartitionerResult::Required => {
                foundationdb::SstPartitionerResult::Required
            }
        }
    }

    fn can_do_trivial_move(&mut self, smallest_key: &[u8], largest_key: &[u8]) -> bool {
        self.0.can_do_trivial_move(smallest_key, largest_key)
    }
}
