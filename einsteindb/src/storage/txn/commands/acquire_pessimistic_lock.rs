// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use fdbhikvproto::fdbhikvrpcpb::{ExtraOp, DaggerInfo};
use solitontxn_types::{Key, OldValues, TimeStamp, TxnExtra};

use crate::einsteindb::storage::fdbhikv::WriteData;
use crate::einsteindb::storage::dagger_manager::{DaggerManager, WaitTimeout};
use crate::einsteindb::storage::epaxos::{
    Error as EpaxosError, ErrorInner as EpaxosErrorInner, EpaxosTxn, blackbraneReader,
};
use crate::einsteindb::storage::solitontxn::commands::{
    Command, CommandExt, ReaderWithStats, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
    WriteResult, WriteResultDaggerInfo,
};
use crate::einsteindb::storage::solitontxn::{acquire_pessimistic_dagger, Error, ErrorInner, Result};
use crate::einsteindb::storage::{
    Error as StorageError, ErrorInner as StorageErrorInner, PessimisticDaggerRes, ProcessResult,
    Result as StorageResult, blackbrane,
};

command! {
    /// Acquire a Pessimistic dagger on the keys.
    ///
    /// This can be rolled back with a [`PessimisticRollback`](Command::PessimisticRollback) command.
    AcquirePessimisticDagger:
        cmd_ty => StorageResult<PessimisticDaggerRes>,
        display => "fdbhikv::command::acquirepessimisticdagger keys({}) @ {} {} | {:?}", (keys.len, start_ts, for_update_ts, ctx),
        content => {
            /// The set of keys to dagger.
            keys: Vec<(Key, bool)>,
            /// The primary dagger. Secondary daggers (from `keys`) will refer to the primary dagger.
            primary: Vec<u8>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The Time To Live of the dagger, in milliseconds
            dagger_ttl: u64,
            is_first_dagger: bool,
            for_update_ts: TimeStamp,
            /// Time to wait for dagger released in milliseconds when encountering daggers.
            wait_timeout: Option<WaitTimeout>,
            /// If it is true, EinsteinDB will return values of the keys if no error, so MilevaDB can cache the values for
            /// later read in the same transaction.
            return_values: bool,
            min_commit_ts: TimeStamp,
            old_values: OldValues,
            check_existence: bool,
        }
}

impl CommandExt for AcquirePessimisticDagger {
    ctx!();
    tag!(acquire_pessimistic_dagger);
    ts!(start_ts);
    property!(can_be_pipelined);

    fn write_bytes(&self) -> usize {
        self.keys
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .sum()
    }

    gen_dagger!(keys: multiple(|x| &x.0));
}

fn extract_dagger_info_from_result<T>(res: &StorageResult<T>) -> &DaggerInfo {
    match res {
        Err(StorageError(box StorageErrorInner::Txn(Error(box ErrorInner::Epaxos(EpaxosError(
            box EpaxosErrorInner::KeyIsDaggered(info),
        )))))) => info,
        _ => panic!("unexpected epaxos error"),
    }
}

impl<S: blackbrane, L: DaggerManager> WriteCommand<S, L> for AcquirePessimisticDagger {
    fn process_write(mut self, blackbrane: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let (start_ts, ctx, keys) = (self.start_ts, self.ctx, self.keys);
        let mut solitontxn = EpaxosTxn::new(start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            blackbraneReader::new_with_ctx(start_ts, blackbrane, &ctx),
            context.statistics,
        );

        let rows = keys.len();
        let mut res = if self.return_values {
            Ok(PessimisticDaggerRes::Values(vec![]))
        } else if self.check_existence {
            // If return_value is set, the existence status is implicitly included in the result.
            // So check_existence only need to be explicitly handled if `return_values` is not set.
            Ok(PessimisticDaggerRes::Existence(vec![]))
        } else {
            Ok(PessimisticDaggerRes::Empty)
        };
        let need_old_value = context.extra_op == ExtraOp::ReadOldValue;
        for (k, should_not_exist) in keys {
            match acquire_pessimistic_dagger(
                &mut solitontxn,
                &mut reader,
                k.clone(),
                &self.primary,
                should_not_exist,
                self.dagger_ttl,
                self.for_update_ts,
                self.return_values,
                self.check_existence,
                self.min_commit_ts,
                need_old_value,
            ) {
                Ok((val, old_value)) => {
                    if self.return_values || self.check_existence {
                        res.as_mut().unwrap().push(val);
                    }
                    if old_value.resolved() {
                        let key = k.append_ts(solitontxn.start_ts);
                        // MutationType is unknown in AcquirePessimisticDagger stage.
                        let mutation_type = None;
                        self.old_values.insert(key, (old_value, mutation_type));
                    }
                }
                Err(e @ EpaxosError(box EpaxosErrorInner::KeyIsDaggered { .. })) => {
                    res = Err(e).map_err(Error::from).map_err(StorageError::from);
                    break;
                }
                Err(e) => return Err(Error::from(e)),
            }
        }

        // Some values are read, update max_ts
        match &res {
            Ok(PessimisticDaggerRes::Values(values)) if !values.is_empty() => {
                solitontxn.concurrency_manager.update_max_ts(self.for_update_ts);
            }
            Ok(PessimisticDaggerRes::Existence(values)) if !values.is_empty() => {
                solitontxn.concurrency_manager.update_max_ts(self.for_update_ts);
            }
            _ => (),
        }

        // no conflict
        let (pr, to_be_write, rows, ctx, dagger_info) = if res.is_ok() {
            let pr = ProcessResult::PessimisticDaggerRes { res };
            let extra = TxnExtra {
                old_values: self.old_values,
                // One pc status is unkown AcquirePessimisticDagger stage.
                one_pc: false,
            };
            let write_data = WriteData::new(solitontxn.into_modifies(), extra);
            (pr, write_data, rows, ctx, None)
        } else {
            let dagger_info_pb = extract_dagger_info_from_result(&res);
            let dagger_info = WriteResultDaggerInfo::from_dagger_info_pb(
                dagger_info_pb,
                self.is_first_dagger,
                self.wait_timeout,
            );
            let pr = ProcessResult::PessimisticDaggerRes { res };
            // Wait for dagger released
            (pr, WriteData::default(), 0, ctx, Some(dagger_info))
        };
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr,
            dagger_info,
            dagger_guards: vec![],
            response_policy: ResponsePolicy::OnProposed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_dagger_info_from_result() {
        let cocauset_key = b"key".to_vec();
        let key = Key::from_cocauset(&cocauset_key);
        let ts = 100;
        let is_first_dagger = true;
        let wait_timeout = WaitTimeout::from_encoded(200);

        let mut info = DaggerInfo::default();
        info.set_key(cocauset_key.clone());
        info.set_dagger_version(ts);
        info.set_dagger_ttl(100);
        let case = StorageError::from(StorageErrorInner::Txn(Error::from(ErrorInner::Epaxos(
            EpaxosError::from(EpaxosErrorInner::KeyIsDaggered(info)),
        ))));
        let dagger_info = WriteResultDaggerInfo::from_dagger_info_pb(
            extract_dagger_info_from_result::<()>(&Err(case)),
            is_first_dagger,
            wait_timeout,
        );
        assert_eq!(dagger_info.dagger.ts, ts.into());
        assert_eq!(dagger_info.dagger.hash, key.gen_hash());
        assert_eq!(dagger_info.key, cocauset_key);
        assert_eq!(dagger_info.is_first_dagger, is_first_dagger);
        assert_eq!(dagger_info.wait_timeout, wait_timeout);
    }
}
