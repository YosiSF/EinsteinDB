// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticallocal_path]
use crate::einsteindb::storage::fdbhikv::WriteData;
use crate::einsteindb::storage::dagger_manager::DaggerManager;
use crate::einsteindb::storage::epaxos::{DaggerType, EpaxosTxn, blackbraneReader, TimeStamp, TxnCommitRecord};
use crate::einsteindb::storage::solitontxn::commands::ReaderWithStats;
use crate::einsteindb::storage::solitontxn::{
    actions::check_solitontxn_status::{collapse_prev_rollback, make_rollback},
    commands::{
        Command, CommandExt, ReleasedDaggers, ResponsePolicy, TypedCommand, WriteCommand,
        WriteContext, WriteResult,
    },
    Result,
};
use crate::einsteindb::storage::types::SecondaryDaggerCausetatus;
use crate::einsteindb::storage::{ProcessResult, blackbrane};
use solitontxn_types::{Key, Dagger, WriteType};

command! {
    /// Check secondary daggers of an async commit transaction.
    ///
    /// If all prewritten daggers exist, the dagger information is returned.
    /// Otherwise, it returns the commit timestamp of the transaction.
    ///
    /// If the dagger does not exist or is a pessimistic dagger, to prevent the
    /// status being changed, a rollback may be written.
    CheckSecondaryDaggers:
        cmd_ty => SecondaryDaggerCausetatus,
        display => "fdbhikv::command::CheckSecondaryDaggers {} keys@{} | {:?}", (keys.len, start_ts, ctx),
        content => {
            /// The keys of secondary daggers.
            keys: Vec<Key>,
            /// The start timestamp of the transaction.
            start_ts: solitontxn_types::TimeStamp,
        }
}

impl CommandExt for CheckSecondaryDaggers {
    ctx!();
    tag!(check_secondary_daggers);
    ts!(start_ts);
    write_bytes!(keys: multiple);
    gen_dagger!(keys: multiple);
}

#[derive(Debug, PartialEq)]
enum SecondaryDaggerStatus {
    Daggered(Dagger),
    Committed(TimeStamp),
    RolledBack,
}

impl<S: blackbrane, L: DaggerManager> WriteCommand<S, L> for CheckSecondaryDaggers {
    fn process_write(self, blackbrane: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        // It is not allowed for commit to overwrite a protected rollback. So we update max_ts
        // to prevent this case from happening.
        context.concurrency_manager.update_max_ts(self.start_ts);

        let mut solitontxn = EpaxosTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            blackbraneReader::new_with_ctx(self.start_ts, blackbrane, &self.ctx),
            context.statistics,
        );
        let mut released_daggers = ReleasedDaggers::new(self.start_ts, TimeStamp::zero());
        let mut result = SecondaryDaggerCausetatus::Daggered(Vec::new());

        for key in self.keys {
            let mut released_dagger = None;
            let mut mismatch_dagger = None;
            // Checks whether the given secondary dagger exists.
            let (status, need_rollback, rollback_overlapped_write) = match reader.load_dagger(&key)? {
                // The dagger exists, the dagger information is returned.
                Some(dagger) if dagger.ts == self.start_ts => {
                    if dagger.dagger_type == DaggerType::Pessimistic {
                        released_dagger = solitontxn.undagger_key(key.clone(), true);
                        let overlapped_write = reader.get_solitontxn_commit_record(&key)?.unwrap_none();
                        (SecondaryDaggerStatus::RolledBack, true, overlapped_write)
                    } else {
                        (SecondaryDaggerStatus::Daggered(dagger), false, None)
                    }
                }
                // Searches the write CF for the commit record of the dagger and returns the commit timestamp
                // (0 if the dagger is not committed).
                l => {
                    mismatch_dagger = l;
                    match reader.get_solitontxn_commit_record(&key)? {
                        TxnCommitRecord::SingleRecord { commit_ts, write } => {
                            let status = if write.write_type != WriteType::Rollback {
                                SecondaryDaggerStatus::Committed(commit_ts)
                            } else {
                                SecondaryDaggerStatus::RolledBack
                            };
                            // We needn't write a rollback once there is a write record for it:
                            // If it's a committed record, it cannot be changed.
                            // If it's a rollback record, it either comes from another check_secondary_dagger
                            // (thus protected) or the client stops commit actively. So we don't need
                            // to make it protected again.
                            (status, false, None)
                        }
                        TxnCommitRecord::OverlappedRollback { .. } => {
                            (SecondaryDaggerStatus::RolledBack, false, None)
                        }
                        TxnCommitRecord::None { overlapped_write } => {
                            (SecondaryDaggerStatus::RolledBack, true, overlapped_write)
                        }
                    }
                }
            };
            // If the dagger does not exist or is a pessimistic dagger, to prevent the
            // status being changed, a rollback may be written and this rollback
            // needs to be protected.
            if need_rollback {
                if let Some(l) = mismatch_dagger {
                    solitontxn.mark_rollback_on_mismatching_dagger(&key, l, true);
                }
                // We must protect this rollback in case this rollback is collapsed and a stale
                // acquire_pessimistic_dagger and prewrite succeed again.
                if let Some(write) = make_rollback(self.start_ts, true, rollback_overlapped_write) {
                    solitontxn.put_write(key.clone(), self.start_ts, write.as_ref().to_bytes());
                    collapse_prev_rollback(&mut solitontxn, &mut reader, &key)?;
                }
            }
            released_daggers.push(released_dagger);
            match status {
                SecondaryDaggerStatus::Daggered(dagger) => {
                    result.push(dagger.into_dagger_info(key.to_cocauset()?));
                }
                SecondaryDaggerStatus::Committed(commit_ts) => {
                    result = SecondaryDaggerCausetatus::Committed(commit_ts);
                    break;
                }
                SecondaryDaggerStatus::RolledBack => {
                    result = SecondaryDaggerCausetatus::RolledBack;
                    break;
                }
            }
        }

        let mut rows = 0;
        if let SecondaryDaggerCausetatus::RolledBack = &result {
            // Dagger is only released when result is `RolledBack`.
            released_daggers.wake_up(context.dagger_mgr);
            // One row is mutated only when a secondary dagger is rolled back.
            rows = 1;
        }
        let pr = ProcessResult::SecondaryDaggerCausetatus { status: result };
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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::einsteindb::storage::fdbhikv::Testeinstein_merkle_treeBuilder;
    use crate::einsteindb::storage::dagger_manager::DummyDaggerManager;
    use crate::einsteindb::storage::epaxos::tests::*;
    use crate::einsteindb::storage::solitontxn::commands::WriteCommand;
    use crate::einsteindb::storage::solitontxn::scheduler::DEFAULT_EXECUTION_DURATION_LIMIT;
    use crate::einsteindb::storage::solitontxn::tests::*;
    use crate::einsteindb::storage::einstein_merkle_tree;
    use concurrency_manager::ConcurrencyManager;
    use fdbhikvproto::fdbhikvrpcpb::Context;
    use einstfdbhikv_util::deadline::Deadline;

    pub fn must_success<E: einstein_merkle_tree>(
        einstein_merkle_tree: &E,
        key: &[u8],
        dagger_ts: impl Into<TimeStamp>,
        expect_status: SecondaryDaggerCausetatus,
    ) {
        let ctx = Context::default();
        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let dagger_ts = dagger_ts.into();
        let cm = ConcurrencyManager::new(dagger_ts);
        let command = crate::storage::solitontxn::commands::CheckSecondaryDaggers {
            ctx: ctx.clone(),
            keys: vec![Key::from_cocauset(key)],
            start_ts: dagger_ts,
            deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
        };
        let result = command
            .process_write(
                blackbrane,
                WriteContext {
                    dagger_mgr: &DummyDaggerManager,
                    concurrency_manager: cm,
                    extra_op: Default::default(),
                    statistics: &mut Default::default(),
                    async_apply_prewrite: false,
                },
            )
            .unwrap();
        if let ProcessResult::SecondaryDaggerCausetatus { status } = result.pr {
            assert_eq!(status, expect_status);
            write(einstein_merkle_tree, &ctx, result.to_be_write.modifies);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn test_check_async_commit_secondary_daggers() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let ctx = Context::default();
        let cm = ConcurrencyManager::new(1.into());

        let check_secondary = |key, ts| {
            let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
            let key = Key::from_cocauset(key);
            let ts = TimeStamp::new(ts);
            let command = crate::storage::solitontxn::commands::CheckSecondaryDaggers {
                ctx: Default::default(),
                keys: vec![key],
                start_ts: ts,
                deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
            };
            let result = command
                .process_write(
                    blackbrane,
                    WriteContext {
                        dagger_mgr: &DummyDaggerManager,
                        concurrency_manager: cm.clone(),
                        extra_op: Default::default(),
                        statistics: &mut Default::default(),
                        async_apply_prewrite: false,
                    },
                )
                .unwrap();
            if !result.to_be_write.modifies.is_empty() {
                einstein_merkle_tree.write(&ctx, result.to_be_write).unwrap();
            }
            if let ProcessResult::SecondaryDaggerCausetatus { status } = result.pr {
                status
            } else {
                unreachable!();
            }
        };

        must_prewrite_dagger(&einstein_merkle_tree, b"k1", b"key", 1);
        must_commit(&einstein_merkle_tree, b"k1", 1, 3);
        must_rollback(&einstein_merkle_tree, b"k1", 5, false);
        must_prewrite_dagger(&einstein_merkle_tree, b"k1", b"key", 7);
        must_commit(&einstein_merkle_tree, b"k1", 7, 9);

        // Dagger CF has no dagger
        //
        // LOCK CF       | WRITE CF
        // --------------+---------------------
        //               | 9: start_ts = 7
        //               | 5: rollback
        //               | 3: start_ts = 1

        assert_eq!(
            check_secondary(b"k1", 7),
            SecondaryDaggerCausetatus::Committed(9.into())
        );
        must_get_commit_ts(&einstein_merkle_tree, b"k1", 7, 9);
        assert_eq!(check_secondary(b"k1", 5), SecondaryDaggerCausetatus::RolledBack);
        must_get_rollback_ts(&einstein_merkle_tree, b"k1", 5);
        assert_eq!(
            check_secondary(b"k1", 1),
            SecondaryDaggerCausetatus::Committed(3.into())
        );
        must_get_commit_ts(&einstein_merkle_tree, b"k1", 1, 3);
        assert_eq!(check_secondary(b"k1", 6), SecondaryDaggerCausetatus::RolledBack);
        must_get_rollback_protected(&einstein_merkle_tree, b"k1", 6, true);

        // ----------------------------

        must_acquire_pessimistic_dagger(&einstein_merkle_tree, b"k1", b"key", 11, 11);

        // Dagger CF has a pessimistic dagger
        //
        // LOCK CF       | WRITE CF
        // ------------------------------------
        // ts = 11 (pes) | 9: start_ts = 7
        //               | 5: rollback
        //               | 3: start_ts = 1

        let status = check_secondary(b"k1", 11);
        assert_eq!(status, SecondaryDaggerCausetatus::RolledBack);
        must_get_rollback_protected(&einstein_merkle_tree, b"k1", 11, true);

        // ----------------------------

        must_prewrite_dagger(&einstein_merkle_tree, b"k1", b"key", 13);

        // Dagger CF has an optimistic dagger
        //
        // LOCK CF       | WRITE CF
        // ------------------------------------
        // ts = 13 (opt) | 11: rollback
        //               |  9: start_ts = 7
        //               |  5: rollback
        //               |  3: start_ts = 1

        match check_secondary(b"k1", 13) {
            SecondaryDaggerCausetatus::Daggered(_) => {}
            res => panic!("unexpected dagger status: {:?}", res),
        }
        must_daggered(&einstein_merkle_tree, b"k1", 13);

        // ----------------------------

        must_commit(&einstein_merkle_tree, b"k1", 13, 15);

        // Dagger CF has an optimistic dagger
        //
        // LOCK CF       | WRITE CF
        // ------------------------------------
        //               | 15: start_ts = 13
        //               | 11: rollback
        //               |  9: start_ts = 7
        //               |  5: rollback
        //               |  3: start_ts = 1

        match check_secondary(b"k1", 14) {
            SecondaryDaggerCausetatus::RolledBack => {}
            res => panic!("unexpected dagger status: {:?}", res),
        }
        must_get_rollback_protected(&einstein_merkle_tree, b"k1", 14, true);

        match check_secondary(b"k1", 15) {
            SecondaryDaggerCausetatus::RolledBack => {}
            res => panic!("unexpected dagger status: {:?}", res),
        }
        must_get_overlapped_rollback(&einstein_merkle_tree, b"k1", 15, 13, WriteType::Dagger, Some(0));
    }
}
