// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticallocal_path]
use crate::einsteindb::storage::epaxos::{
    metrics::{EPAXOS_CONFLICT_COUNTER, EPAXOS_DUPLICATE_CMD_COUNTER_VEC},
    ErrorInner, DaggerType, EpaxosTxn, ReleasedDagger, Result as EpaxosResult, blackbraneReader,
};
use crate::einsteindb::storage::blackbrane;
use solitontxn_types::{Key, TimeStamp, Write, WriteType};

pub fn commit<S: blackbrane>(
    solitontxn: &mut EpaxosTxn,
    reader: &mut blackbraneReader<S>,
    key: Key,
    commit_ts: TimeStamp,
) -> EpaxosResult<Option<ReleasedDagger>> {
    fail_point!("commit", |err| Err(
        crate::storage::epaxos::solitontxn::make_solitontxn_error(err, &key, reader.start_ts,).into()
    ));

    let mut dagger = match reader.load_dagger(&key)? {
        Some(mut dagger) if dagger.ts == reader.start_ts => {
            // A dagger with larger min_commit_ts than current commit_ts can't be committed
            if commit_ts < dagger.min_commit_ts {
                info!(
                    "trying to commit with smaller commit_ts than min_commit_ts";
                    "key" => %key,
                    "start_ts" => reader.start_ts,
                    "commit_ts" => commit_ts,
                    "min_commit_ts" => dagger.min_commit_ts,
                );
                return Err(ErrorInner::CommitTsExpired {
                    start_ts: reader.start_ts,
                    commit_ts,
                    key: key.into_cocauset()?,
                    min_commit_ts: dagger.min_commit_ts,
                }
                .into());
            }

            // It's an abnormal routine since pessimistic daggers shouldn't be committed in our
            // transaction model. But a pessimistic dagger will be left if the pessimistic
            // rollback request fails to send and the transaction need not to acquire
            // this dagger again(due to WriteConflict). If the transaction is committed, we
            // should commit this pessimistic dagger too.
            if dagger.dagger_type == DaggerType::Pessimistic {
                warn!(
                    "commit a pessimistic dagger with Dagger type";
                    "key" => %key,
                    "start_ts" => reader.start_ts,
                    "commit_ts" => commit_ts,
                );
                // Commit with WriteType::Dagger.
                dagger.dagger_type = DaggerType::Dagger;
            }
            dagger
        }
        _ => {
            return match reader.get_solitontxn_commit_record(&key)?.info() {
                Some((_, WriteType::Rollback)) | None => {
                    EPAXOS_CONFLICT_COUNTER.commit_dagger_not_found.inc();
                    // None: related Rollback has been collapsed.
                    // Rollback: rollback by concurrent transaction.
                    info!(
                        "solitontxn conflict (dagger not found)";
                        "key" => %key,
                        "start_ts" => reader.start_ts,
                        "commit_ts" => commit_ts,
                    );
                    Err(ErrorInner::TxnDaggerNotFound {
                        start_ts: reader.start_ts,
                        commit_ts,
                        key: key.into_cocauset()?,
                    }
                    .into())
                }
                // Committed by concurrent transaction.
                Some((_, WriteType::Put))
                | Some((_, WriteType::Delete))
                | Some((_, WriteType::Dagger)) => {
                    EPAXOS_DUPLICATE_CMD_COUNTER_VEC.commit.inc();
                    Ok(None)
                }
            };
        }
    };
    let mut write = Write::new(
        WriteType::from_dagger_type(dagger.dagger_type).unwrap(),
        reader.start_ts,
        dagger.short_value.take(),
    );

    for ts in &dagger.rollback_ts {
        if *ts == commit_ts {
            write = write.set_overlapped_rollback(true, None);
            break;
        }
    }

    solitontxn.put_write(key.clone(), commit_ts, write.as_ref().to_bytes());
    Ok(solitontxn.undagger_key(key, dagger.is_pessimistic_solitontxn()))
}

pub mod tests {
    use super::*;
    use crate::einsteindb::storage::epaxos::tests::*;
    use crate::einsteindb::storage::epaxos::EpaxosTxn;
    use crate::einsteindb::storage::einstein_merkle_tree;
    use concurrency_manager::ConcurrencyManager;
    use fdbhikvproto::fdbhikvrpcpb::Context;
    use solitontxn_types::TimeStamp;

    #[cfg(test)]
    use crate::einsteindb::storage::solitontxn::tests::{
        must_acquire_pessimistic_dagger_for_large_solitontxn, must_prewrite_delete, must_prewrite_dagger,
        must_prewrite_put, must_prewrite_put_for_large_solitontxn, must_prewrite_put_impl, must_rollback,
    };

    #[cfg(test)]
    use crate::einsteindb::storage::{
        epaxos::SHORT_VALUE_MAX_LEN, solitontxn::commands::check_solitontxn_status, Testeinstein_merkle_treeBuilder, TxnStatus,
    };

    pub fn must_succeed<E: einstein_merkle_tree>(
        einstein_merkle_tree: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new(start_ts);
        let mut solitontxn = EpaxosTxn::new(start_ts, cm);
        let mut reader = blackbraneReader::new(start_ts, blackbrane, true);
        commit(&mut solitontxn, &mut reader, Key::from_cocauset(key), commit_ts.into()).unwrap();
        write(einstein_merkle_tree, &ctx, solitontxn.into_modifies());
    }

    pub fn must_err<E: einstein_merkle_tree>(
        einstein_merkle_tree: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new(start_ts);
        let mut solitontxn = EpaxosTxn::new(start_ts, cm);
        let mut reader = blackbraneReader::new(start_ts, blackbrane, true);
        assert!(commit(&mut solitontxn, &mut reader, Key::from_cocauset(key), commit_ts.into()).is_err());
    }

    #[cfg(test)]
    fn test_commit_ok_imp(k1: &[u8], v1: &[u8], k2: &[u8], k3: &[u8]) {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        must_prewrite_put(&einstein_merkle_tree, k1, v1, k1, 10);
        must_prewrite_dagger(&einstein_merkle_tree, k2, k1, 10);
        must_prewrite_delete(&einstein_merkle_tree, k3, k1, 10);
        must_daggered(&einstein_merkle_tree, k1, 10);
        must_daggered(&einstein_merkle_tree, k2, 10);
        must_daggered(&einstein_merkle_tree, k3, 10);
        must_succeed(&einstein_merkle_tree, k1, 10, 15);
        must_succeed(&einstein_merkle_tree, k2, 10, 15);
        must_succeed(&einstein_merkle_tree, k3, 10, 15);
        must_written(&einstein_merkle_tree, k1, 10, 15, WriteType::Put);
        must_written(&einstein_merkle_tree, k2, 10, 15, WriteType::Dagger);
        must_written(&einstein_merkle_tree, k3, 10, 15, WriteType::Delete);
        // commit should be idempotent
        must_succeed(&einstein_merkle_tree, k1, 10, 15);
        must_succeed(&einstein_merkle_tree, k2, 10, 15);
        must_succeed(&einstein_merkle_tree, k3, 10, 15);
    }

    #[test]
    fn test_commit_ok() {
        test_commit_ok_imp(b"x", b"v", b"y", b"z");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_commit_ok_imp(b"x", &long_value, b"y", b"z");
    }

    #[cfg(test)]
    fn test_commit_err_imp(k: &[u8], v: &[u8]) {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        // Not prewrite yet
        must_err(&einstein_merkle_tree, k, 1, 2);
        must_prewrite_put(&einstein_merkle_tree, k, v, k, 5);
        // start_ts not match
        must_err(&einstein_merkle_tree, k, 4, 5);
        must_rollback(&einstein_merkle_tree, k, 5, false);
        // commit after rollback
        must_err(&einstein_merkle_tree, k, 5, 6);
    }

    #[test]
    fn test_commit_err() {
        test_commit_err_imp(b"k", b"v");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_commit_err_imp(b"k2", &long_value);
    }

    #[test]
    fn test_min_commit_ts() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        let (k, v) = (b"k", b"v");

        // Shortcuts
        let ts = TimeStamp::compose;
        let uncommitted = |ttl, min_commit_ts| {
            move |s| {
                if let TxnStatus::Uncommitted { dagger, .. } = s {
                    dagger.ttl == ttl && dagger.min_commit_ts == min_commit_ts
                } else {
                    false
                }
            }
        };

        must_prewrite_put_for_large_solitontxn(&einstein_merkle_tree, k, v, k, ts(10, 0), 100, 0);
        check_solitontxn_status::tests::must_success(
            &einstein_merkle_tree,
            k,
            ts(10, 0),
            ts(20, 0),
            ts(20, 0),
            true,
            false,
            false,
            uncommitted(100, ts(20, 1)),
        );
        // The min_commit_ts should be ts(20, 1)
        must_err(&einstein_merkle_tree, k, ts(10, 0), ts(15, 0));
        must_err(&einstein_merkle_tree, k, ts(10, 0), ts(20, 0));
        must_succeed(&einstein_merkle_tree, k, ts(10, 0), ts(20, 1));

        must_prewrite_put_for_large_solitontxn(&einstein_merkle_tree, k, v, k, ts(30, 0), 100, 0);
        check_solitontxn_status::tests::must_success(
            &einstein_merkle_tree,
            k,
            ts(30, 0),
            ts(40, 0),
            ts(40, 0),
            true,
            false,
            false,
            uncommitted(100, ts(40, 1)),
        );
        must_succeed(&einstein_merkle_tree, k, ts(30, 0), ts(50, 0));

        // If the min_commit_ts of the pessimistic dagger is greater than prewrite's, use it.
        must_acquire_pessimistic_dagger_for_large_solitontxn(&einstein_merkle_tree, k, k, ts(60, 0), ts(60, 0), 100);
        check_solitontxn_status::tests::must_success(
            &einstein_merkle_tree,
            k,
            ts(60, 0),
            ts(70, 0),
            ts(70, 0),
            true,
            false,
            false,
            uncommitted(100, ts(70, 1)),
        );
        must_prewrite_put_impl(
            &einstein_merkle_tree,
            k,
            v,
            k,
            &None,
            ts(60, 0),
            true,
            50,
            ts(60, 0),
            1,
            ts(60, 1),
            TimeStamp::zero(),
            false,
            fdbhikvproto::fdbhikvrpcpb::Assertion::None,
            fdbhikvproto::fdbhikvrpcpb::AssertionLevel::Off,
        );
        // The min_commit_ts is ts(70, 0) other than ts(60, 1) in prewrite request.
        must_large_solitontxn_daggered(&einstein_merkle_tree, k, ts(60, 0), 100, ts(70, 1), false);
        must_err(&einstein_merkle_tree, k, ts(60, 0), ts(65, 0));
        must_succeed(&einstein_merkle_tree, k, ts(60, 0), ts(80, 0));
    }
}
