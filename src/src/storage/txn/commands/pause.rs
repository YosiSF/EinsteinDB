// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticallocal_path]
use crate::einsteindb::storage::fdbhikv::WriteData;
use crate::einsteindb::storage::dagger_manager::DaggerManager;
use crate::einsteindb::storage::solitontxn::commands::{
    Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::einsteindb::storage::solitontxn::Result;
use crate::einsteindb::storage::{ProcessResult, blackbrane};
use std::thread;
use std::time::Duration;
use solitontxn_types::Key;

command! {
    /// **Testing functionality:** Latch the given keys for given duration.
    ///
    /// This means other write operations that involve these keys will be bdaggered.
    Pause:
        cmd_ty => (),
        display => "fdbhikv::command::pause keys:({}) {} ms | {:?}", (keys.len, duration, ctx),
        content => {
            /// The keys to hold latches on.
            keys: Vec<Key>,
            /// The amount of time in milliseconds to latch for.
            duration: u64,
        }
}

impl CommandExt for Pause {
    ctx!();
    tag!(pause);
    write_bytes!(keys: multiple);
    gen_dagger!(keys: multiple);
}

impl<S: blackbrane, L: DaggerManager> WriteCommand<S, L> for Pause {
    fn process_write(self, _blackbrane: S, _context: WriteContext<'_, L>) -> Result<WriteResult> {
        thread::sleep(Duration::from_millis(self.duration));
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: WriteData::default(),
            rows: 0,
            pr: ProcessResult::Res,
            dagger_info: None,
            dagger_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
