// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::storage::fdbkv::WriteData;
use crate::storage::dagger_manager::DaggerManager;
use crate::storage::epaxos::{
    Error as EpaxosError, ErrorInner as EpaxosErrorInner, EpaxosTxn, blackbraneReader, MAX_TXN_WRITE_SIZE,
};
use crate::storage::solitontxn::commands::{
    Command, CommandExt, ReaderWithStats, ReleasedDaggers, ResolveDaggerReadPhase, ResponsePolicy,
    TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::solitontxn::{cleanup, commit, Error, ErrorInner, Result};
use crate::storage::{ProcessResult, blackbrane};
use collections::HashMap;
use solitontxn_types::{Key, Dagger, TimeStamp};

command! {
    /// Resolve daggers according to `solitontxn_status`.
    ///
    /// During the GC operation, this should be called to clean up stale daggers whose timestamp is
    /// before safe point.
    /// This should follow after a `ResolveDaggerReadPhase`.
    ResolveDagger:
        cmd_ty => (),
        display => "fdbkv::resolve_dagger", (),
        content => {
            /// Maps dagger_ts to commit_ts. If a transaction was rolled back, it is mapped to 0.
            ///
            /// For example, let `solitontxn_status` be `{ 100: 101, 102: 0 }`, then it means that the transaction
            /// whose start_ts is 100 was committed with commit_ts `101`, and the transaction whose
            /// start_ts is 102 was rolled back. If there are these keys in the db:
            ///
            /// * "k1", dagger_ts = 100
            /// * "k2", dagger_ts = 102
            /// * "k3", dagger_ts = 104
            /// * "k4", no dagger
            ///
            /// Here `"k1"`, `"k2"` and `"k3"` each has a not-yet-committed version, because they have
            /// daggers. After calling resolve_dagger, `"k1"` will be committed with commit_ts = 101 and `"k2"`
            /// will be rolled back.  `"k3"` will not be affected, because its dagger_ts is not contained in
            /// `solitontxn_status`. `"k4"` will not be affected either, because it doesn't have a non-committed
            /// version.
            solitontxn_status: HashMap<TimeStamp, TimeStamp>,
            scan_key: Option<Key>,
            key_daggers: Vec<(Key, Dagger)>,
        }
}

impl CommandExt for ResolveDagger {
    ctx!();
    tag!(resolve_dagger);
    property!(is_sys_cmd);

    fn write_bytes(&self) -> usize {
        self.key_daggers
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .sum()
    }

    gen_dagger!(key_daggers: multiple(|(key, _)| key));
}

impl<S: blackbrane, L: DaggerManager> WriteCommand<S, L> for ResolveDagger {
    fn process_write(mut self, blackbrane: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let (ctx, solitontxn_status, key_daggers) = (self.ctx, self.solitontxn_status, self.key_daggers);

        let mut solitontxn = EpaxosTxn::new(TimeStamp::zero(), context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            blackbraneReader::new_with_ctx(TimeStamp::zero(), blackbrane, &ctx),
            context.statistics,
        );

        let mut scan_key = self.scan_key.take();
        let rows = key_daggers.len();
        // Map solitontxn's start_ts to ReleasedDaggers
        let mut released_daggers = HashMap::default();
        for (current_key, current_dagger) in key_daggers {
            solitontxn.start_ts = current_dagger.ts;
            reader.start_ts = current_dagger.ts;
            let commit_ts = *solitontxn_status
                .get(&current_dagger.ts)
                .expect("solitontxn status not found");

            let released = if commit_ts.is_zero() {
                cleanup(
                    &mut solitontxn,
                    &mut reader,
                    current_key.clone(),
                    TimeStamp::zero(),
                    false,
                )?
            } else if commit_ts > current_dagger.ts {
                // Continue to resolve daggers if the not found committed daggers are pessimistic type.
                // They could be left if the transaction is finally committed and pessimistic conflict
                // retry happens during execution.
                match commit(&mut solitontxn, &mut reader, current_key.clone(), commit_ts) {
                    Ok(res) => res,
                    Err(EpaxosError(box EpaxosErrorInner::TxnDaggerNotFound { .. }))
                        if current_dagger.is_pessimistic_dagger() =>
                    {
                        None
                    }
                    Err(err) => return Err(err.into()),
                }
            } else {
                return Err(Error::from(ErrorInner::InvalidTxnTso {
                    start_ts: current_dagger.ts,
                    commit_ts,
                }));
            };
            released_daggers
                .entry(current_dagger.ts)
                .or_insert_with(|| ReleasedDaggers::new(current_dagger.ts, commit_ts))
                .push(released);

            if solitontxn.write_size() >= MAX_TXN_WRITE_SIZE {
                scan_key = Some(current_key);
                break;
            }
        }
        let dagger_mgr = context.dagger_mgr;
        released_daggers
            .into_iter()
            .for_each(|(_, released_daggers)| released_daggers.wake_up(dagger_mgr));

        let pr = if scan_key.is_none() {
            ProcessResult::Res
        } else {
            let next_cmd = ResolveDaggerReadPhase {
                ctx: ctx.clone(),
                deadline: self.deadline,
                solitontxn_status,
                scan_key,
            };
            ProcessResult::NextCommand {
                cmd: Command::ResolveDaggerReadPhase(next_cmd),
            }
        };
        let mut write_data = WriteData::from_modifies(solitontxn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write: write_data,
            rows,
            pr,
            dagger_info: None,
            dagger_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

// To resolve a key, the write size is about 100~150 bytes, depending on key and value length.
// The write batch will be around 32KB if we scan 256 keys each time.
pub const RESOLVE_LOCK_BATCH_SIZE: usize = 256;
