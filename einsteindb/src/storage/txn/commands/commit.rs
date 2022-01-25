// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use solitontxn_types::Key;

use crate::storage::fdbkv::WriteData;
use crate::storage::dagger_manager::DaggerManager;
use crate::storage::epaxos::{EpaxosTxn, blackbraneReader};
use crate::storage::solitontxn::commands::{
    Command, CommandExt, ReaderWithStats, ReleasedDaggers, ResponsePolicy, TypedCommand,
    WriteCommand, WriteContext, WriteResult,
};
use crate::storage::solitontxn::{commit, Error, ErrorInner, Result};
use crate::storage::{ProcessResult, blackbrane, TxnStatus};

command! {
    /// Commit the transaction that started at `dagger_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite).
    Commit:
        cmd_ty => TxnStatus,
        display => "fdbkv::command::commit {} {} -> {} | {:?}", (keys.len, dagger_ts, commit_ts, ctx),
        content => {
            /// The keys affected.
            keys: Vec<Key>,
            /// The dagger timestamp.
            dagger_ts: solitontxn_types::TimeStamp,
            /// The commit timestamp.
            commit_ts: solitontxn_types::TimeStamp,
        }
}

impl CommandExt for Commit {
    ctx!();
    tag!(commit);
    ts!(commit_ts);
    write_bytes!(keys: multiple);
    gen_dagger!(keys: multiple);
}

impl<S: blackbrane, L: DaggerManager> WriteCommand<S, L> for Commit {
    fn process_write(self, blackbrane: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        if self.commit_ts <= self.dagger_ts {
            return Err(Error::from(ErrorInner::InvalidTxnTso {
                start_ts: self.dagger_ts,
                commit_ts: self.commit_ts,
            }));
        }
        let mut solitontxn = EpaxosTxn::new(self.dagger_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            blackbraneReader::new_with_ctx(self.dagger_ts, blackbrane, &self.ctx),
            context.statistics,
        );

        let rows = self.keys.len();
        // Pessimistic solitontxn needs key_hashes to wake up waiters
        let mut released_daggers = ReleasedDaggers::new(self.dagger_ts, self.commit_ts);
        for k in self.keys {
            released_daggers.push(commit(&mut solitontxn, &mut reader, k, self.commit_ts)?);
        }
        released_daggers.wake_up(context.dagger_mgr);

        let pr = ProcessResult::TxnStatus {
            solitontxn_status: TxnStatus::committed(self.commit_ts),
        };
        let mut write_data = WriteData::from_modifies(solitontxn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr,
            dagger_info: None,
            dagger_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
