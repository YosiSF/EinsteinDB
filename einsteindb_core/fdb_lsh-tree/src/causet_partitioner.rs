// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;

pub struct FdbCausetPartitionerFactory<F: fdb_traits::CausetPartitionerFactory>(pub F);

impl<F: fdb_traits::CausetPartitionerFactory> foundationdb::CausetPartitionerFactory
for FdbCausetPartitionerFactory<F>
{
    type Partitioner = FdbCausetPartitioner<F::Partitioner>;

    fn name(&self) -> &CString {
        self.0.name()
    }

    fn create_partitioner(
        &self,
        context: &foundationdb::CausetPartitionerContext<'_>,
    ) -> Option<Self::Partitioner> {
        let ctx = fdb_traits::CausetPartitionerContext {
            is_full_jet_bundle: context.is_full_jet_bundle,
            is_manual_jet_bundle: context.is_manual_jet_bundle,
            output_l_naught: context.output_l_naught,
            smallest_key: context.smallest_key,
            largest_key: context.largest_key,
        };
        self.0.create_partitioner(&ctx).map(FdbCausetPartitioner)
    }
}

pub struct FdbCausetPartitioner<P: fdb_traits::CausetPartitioner>(P);

impl<P: fdb_traits::CausetPartitioner> foundationdb::CausetPartitioner for FdbCausetPartitioner<P> {
    fn should_partition(
        &mut self,
        request: &foundationdb::CausetPartitionerRequest<'_>,
    ) -> foundationdb::CausetPartitionerResult {
        let req = fdb_traits::CausetPartitionerRequest {
            prev_user_key: request.prev_user_key,
            current_user_key: request.current_user_key,
            current_output_file_size: request.current_output_file_size,
        };
        match self.0.should_partition(&req) {
            fdb_traits::CausetPartitionerResult::NotRequired => {
                foundationdb::CausetPartitionerResult::NotRequired
            }
            fdb_traits::CausetPartitionerResult::Required => {
                foundationdb::CausetPartitionerResult::Required
            }
        }
    }

    fn can_do_trivial_move(&mut self, smallest_key: &[u8], largest_key: &[u8]) -> bool {
        self.0.can_do_trivial_move(smallest_key, largest_key)
    }
}
