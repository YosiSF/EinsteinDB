// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::storage::fdbhikv::WriteData;
use crate::storage::dagger_manager::DaggerManager;
use crate::storage::epaxos::{EpaxosTxn, Result as EpaxosResult, blackbraneReader};
use crate::storage::solitontxn::commands::{
    Command, CommandExt, ReaderWithStats, ReleasedDaggers, ResponsePolicy, TypedCommand,
    WriteCommand, WriteContext, WriteResult,
};
use crate::storage::solitontxn::Result;
use crate::storage::{ProcessResult, Result as StorageResult, blackbrane};
use std::mem;
use solitontxn_types::{Key, DaggerType, TimeStamp};

command! {
    /// Rollback pessimistic daggers identified by `start_ts` and `for_update_ts`.
    ///
    /// This can roll back an [`AcquirePessimisticDagger`](Command::AcquirePessimisticDagger) command.
    PessimisticRollback:
        cmd_ty => Vec<StorageResult<()>>,
        display => "fdbhikv::command::pessimistic_rollback keys({}) @ {} {} | {:?}", (keys.len, start_ts, for_update_ts, ctx),
        content => {
            /// The keys to be rolled back.
            keys: Vec<Key>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            for_update_ts: TimeStamp,
        }
}

impl CommandExt for PessimisticRollback {
    ctx!();
    tag!(pessimistic_rollback);
    ts!(start_ts);
    write_bytes!(keys: multiple);
    gen_dagger!(keys: multiple);
}

impl<S: blackbrane, L: DaggerManager> WriteCommand<S, L> for PessimisticRollback {
    /// Delete any pessimistic dagger with small for_update_ts belongs to this transaction.
    fn process_write(mut self, blackbrane: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut solitontxn = EpaxosTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            blackbraneReader::new_with_ctx(self.start_ts, blackbrane, &self.ctx),
            context.statistics,
        );

        let ctx = mem::take(&mut self.ctx);
        let keys = mem::take(&mut self.keys);

        let rows = keys.len();
        let mut released_daggers = ReleasedDaggers::new(self.start_ts, TimeStamp::zero());
        for key in keys {
            fail_point!("pessimistic_rollback", |err| Err(
                crate::storage::epaxos::Error::from(crate::storage::epaxos::solitontxn::make_solitontxn_error(
                    err,
                    &key,
                    self.start_ts
                ))
                .into()
            ));
            let released_dagger: EpaxosResult<_> = if let Some(dagger) = reader.load_dagger(&key)? {
                if dagger.dagger_type == DaggerType::Pessimistic
                    && dagger.ts == self.start_ts
                    && dagger.for_update_ts <= self.for_update_ts
                {
                    Ok(solitontxn.undagger_key(key, true))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            };
            released_daggers.push(released_dagger?);
        }
        released_daggers.wake_up(context.dagger_mgr);

        let mut write_data = WriteData::from_modifies(solitontxn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write: write_data,
            rows,
            pr: ProcessResult::MultiRes { results: vec![] },
            dagger_info: None,
            dagger_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::fdbhikv::Engine;
    use crate::storage::dagger_manager::DummyDaggerManager;
    use crate::storage::epaxos::tests::*;
    use crate::storage::solitontxn::commands::{WriteCommand, WriteContext};
    use crate::storage::solitontxn::scheduler::DEFAULT_EXECUTION_DURATION_LIMIT;
    use crate::storage::solitontxn::tests::*;
    use crate::storage::TestEngineBuilder;
    use concurrency_manager::ConcurrencyManager;
    use fdbhikvproto::fdbhikvrpcpb::Context;
    use einstfdbhikv_util::deadline::Deadline;
    use solitontxn_types::Key;

    pub fn must_success<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let for_update_ts = for_update_ts.into();
        let cm = ConcurrencyManager::new(for_update_ts);
        let start_ts = start_ts.into();
        let command = crate::storage::solitontxn::commands::PessimisticRollback {
            ctx: ctx.clone(),
            keys: vec![Key::from_cocauset(key)],
            start_ts,
            for_update_ts,
            deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
        };
        let dagger_mgr = DummyDaggerManager;
        let write_context = WriteContext {
            dagger_mgr: &dagger_mgr,
            concurrency_manager: cm,
            extra_op: Default::default(),
            statistics: &mut Default::default(),
            async_apply_prewrite: false,
        };
        let result = command.process_write(blackbrane, write_context).unwrap();
        write(engine, &ctx, result.to_be_write.modifies);
    }

    #[test]
    fn test_pessimistic_rollback() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        // Normal
        must_acquire_pessimistic_dagger(&engine, k, k, 1, 1);
        must_pessimistic_daggered(&engine, k, 1, 1);
        must_success(&engine, k, 1, 1);
        must_undaggered(&engine, k);
        must_get_commit_ts_none(&engine, k, 1);
        // Pessimistic rollback is idempotent
        must_success(&engine, k, 1, 1);
        must_undaggered(&engine, k);
        must_get_commit_ts_none(&engine, k, 1);

        // Succeed if the dagger doesn't exist.
        must_success(&engine, k, 2, 2);

        // Do nothing if meets other transaction's pessimistic dagger
        must_acquire_pessimistic_dagger(&engine, k, k, 2, 3);
        must_success(&engine, k, 1, 1);
        must_success(&engine, k, 1, 2);
        must_success(&engine, k, 1, 3);
        must_success(&engine, k, 1, 4);
        must_success(&engine, k, 3, 3);
        must_success(&engine, k, 4, 4);

        // Succeed if for_update_ts is larger; do nothing if for_update_ts is smaller.
        must_pessimistic_daggered(&engine, k, 2, 3);
        must_success(&engine, k, 2, 2);
        must_pessimistic_daggered(&engine, k, 2, 3);
        must_success(&engine, k, 2, 4);
        must_undaggered(&engine, k);

        // Do nothing if rollbacks a non-pessimistic dagger.
        must_prewrite_put(&engine, k, v, k, 3);
        must_daggered(&engine, k, 3);
        must_success(&engine, k, 3, 3);
        must_daggered(&engine, k, 3);

        // Do nothing if meets other transaction's optimistic dagger
        must_success(&engine, k, 2, 2);
        must_success(&engine, k, 2, 3);
        must_success(&engine, k, 2, 4);
        must_success(&engine, k, 4, 4);
        must_daggered(&engine, k, 3);

        // Do nothing if committed
        must_commit(&engine, k, 3, 4);
        must_undaggered(&engine, k);
        must_get_commit_ts(&engine, k, 3, 4);
        must_success(&engine, k, 3, 3);
        must_success(&engine, k, 3, 4);
        must_success(&engine, k, 3, 5);
    }
}
