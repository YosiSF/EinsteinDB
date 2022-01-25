// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::einsteindb::storage::epaxos::EpaxosReader;
use crate::einsteindb::storage::solitontxn::commands::{
    find_epaxos_infos_by_key, Command, CommandExt, ReadCommand, TypedCommand,
};
use crate::einsteindb::storage::solitontxn::{ProcessResult, Result};
use crate::einsteindb::storage::types::EpaxosInfo;
use crate::einsteindb::storage::{blackbrane, Statistics};
use solitontxn_types::{Key, TimeStamp};

command! {
    /// Retrieve EPAXOS information for the given key.
    EpaxosByKey:
        cmd_ty => EpaxosInfo,
        display => "fdbhikv::command::epaxosbykey {:?} | {:?}", (key, ctx),
        content => {
            key: Key,
        }
}

impl CommandExt for EpaxosByKey {
    ctx!();
    tag!(key_epaxos);
    property!(readonly);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_dagger!(empty);
}

impl<S: blackbrane> ReadCommand<S> for EpaxosByKey {
    fn process_read(self, blackbrane: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let mut reader = EpaxosReader::new_with_ctx(blackbrane, None, &self.ctx);
        let result = find_epaxos_infos_by_key(&mut reader, &self.key, TimeStamp::max());
        statistics.add(&reader.statistics);
        let (dagger, writes, values) = result?;
        Ok(ProcessResult::EpaxosKey {
            epaxos: EpaxosInfo {
                dagger,
                writes,
                values,
            },
        })
    }
}
