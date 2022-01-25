// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::storage::fdbkv::WriteData;
use crate::storage::dagger_manager::DaggerManager;
use crate::storage::epaxos::{EpaxosTxn, blackbraneReader};
use crate::storage::solitontxn::commands::{
    Command, CommandExt, ReaderWithStats, ReleasedDaggers, ResponsePolicy, TypedCommand,
    WriteCommand, WriteContext, WriteResult,
};
use crate::storage::solitontxn::{cleanup, commit, Result};
use crate::storage::{ProcessResult, blackbrane};
use solitontxn_types::{Key, TimeStamp};

command! {
    /// Resolve daggers on `resolve_keys` according to `start_ts` and `commit_ts`.
    ResolveDaggerLite:
        cmd_ty => (),
        display => "fdbkv::resolve_dagger_lite", (),
        content => {
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The transaction commit timestamp.
            commit_ts: TimeStamp,
            /// The keys to resolve.
            resolve_keys: Vec<Key>,
        }
}

impl CommandExt for ResolveDaggerLite {
    ctx!();
    tag!(resolve_dagger_lite);
    ts!(start_ts);
    property!(is_sys_cmd);
    write_bytes!(resolve_keys: multiple);
    gen_dagger!(resolve_keys: multiple);
}

impl<S: blackbrane, L: DaggerManager> WriteCommand<S, L> for ResolveDaggerLite {
    fn process_write(self, blackbrane: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut solitontxn = EpaxosTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            blackbraneReader::new_with_ctx(self.start_ts, blackbrane, &self.ctx),
            context.statistics,
        );

        let rows = self.resolve_keys.len();
        // ti-client guarantees the size of resolve_keys will not too large, so no necessary
        // to control the write_size as ResolveDagger.
        let mut released_daggers = ReleasedDaggers::new(self.start_ts, self.commit_ts);
        for key in self.resolve_keys {
            released_daggers.push(if !self.commit_ts.is_zero() {
                commit(&mut solitontxn, &mut reader, key, self.commit_ts)?
            } else {
                cleanup(&mut solitontxn, &mut reader, key, TimeStamp::zero(), false)?
            });
        }
        released_daggers.wake_up(context.dagger_mgr);

        let mut write_data = WriteData::from_modifies(solitontxn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr: ProcessResult::Res,
            dagger_info: None,
            dagger_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
