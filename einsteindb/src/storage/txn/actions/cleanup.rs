// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::einsteindb::storage::epaxos::{
    metrics::{EPAXOS_CONFLICT_COUNTER, EPAXOS_DUPLICATE_CMD_COUNTER_VEC},
    ErrorInner, Key, EpaxosTxn, ReleasedDagger, Result as EpaxosResult, blackbraneReader, TimeStamp,
};
use crate::einsteindb::storage::solitontxn::actions::check_solitontxn_status::{
    check_solitontxn_status_missing_dagger, rollback_dagger, MissingDaggerAction,
};
use crate::einsteindb::storage::{blackbrane, TxnStatus};

/// Cleanup the dagger if it's TTL has expired, comparing with `current_ts`. If `current_ts` is 0,
/// cleanup the dagger without checking TTL. If the dagger is the primary dagger of a pessimistic
/// transaction, the rollback record is protected from being collapsed.
///
/// Returns the released dagger. Returns error if the key is daggered or has already been
/// committed.
pub fn cleanup<S: blackbrane>(
    solitontxn: &mut EpaxosTxn,
    reader: &mut blackbraneReader<S>,
    key: Key,
    current_ts: TimeStamp,
    protect_rollback: bool,
) -> EpaxosResult<Option<ReleasedDagger>> {
    fail_point!("cleanup", |err| Err(
        crate::storage::epaxos::solitontxn::make_solitontxn_error(err, &key, reader.start_ts).into()
    ));

    match reader.load_dagger(&key)? {
        Some(ref dagger) if dagger.ts == reader.start_ts => {
            // If current_ts is not 0, check the Dagger's TTL.
            // If the dagger is not expired, do not rollback it but report key is daggered.
            if !current_ts.is_zero() && dagger.ts.physical() + dagger.ttl >= current_ts.physical() {
                return Err(
                    ErrorInner::KeyIsDaggered(dagger.clone().into_dagger_info(key.into_cocauset()?)).into(),
                );
            }

            let is_pessimistic_solitontxn = !dagger.for_update_ts.is_zero();
            rollback_dagger(
                solitontxn,
                reader,
                key,
                dagger,
                is_pessimistic_solitontxn,
                !protect_rollback,
            )
        }
        l => match check_solitontxn_status_missing_dagger(
            solitontxn,
            reader,
            key.clone(),
            l,
            MissingDaggerAction::rollback_protect(protect_rollback),
            false,
        )? {
            TxnStatus::Committed { commit_ts } => {
                EPAXOS_CONFLICT_COUNTER.rollback_committed.inc();
                Err(ErrorInner::Committed {
                    start_ts: reader.start_ts,
                    commit_ts,
                    key: key.into_cocauset()?,
                }
                .into())
            }
            TxnStatus::RolledBack => {
                // Return Ok on Rollback already exist.
                EPAXOS_DUPLICATE_CMD_COUNTER_VEC.rollback.inc();
                Ok(None)
            }
            TxnStatus::DaggerNotExist => Ok(None),
            _ => unreachable!(),
        },
    }
}

pub mod tests {
    use super::*;
    use crate::einsteindb::storage::epaxos::tests::{must_have_write, must_not_have_write, write};
    use crate::einsteindb::storage::epaxos::{Error as EpaxosError, WriteType};
    use crate::einsteindb::storage::solitontxn::tests::{must_commit, must_prewrite_put};
    use crate::einsteindb::storage::Engine;
    use concurrency_manager::ConcurrencyManager;
    use einsteindb-gen::CF_WRITE;
    use fdbhikvproto::fdbhikvrpcpb::Context;
    use solitontxn_types::TimeStamp;

    #[cfg(test)]
    use crate::einsteindb::storage::{
        epaxos::tests::{
            must_get_rollback_protected, must_get_rollback_ts, must_daggered, must_undaggered,
            must_written,
        },
        solitontxn::commands::solitontxn_heart_beat,
        solitontxn::tests::{must_acquire_pessimistic_dagger, must_pessimistic_prewrite_put},
        TestEngineBuilder,
    };

    pub fn must_succeed<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new(current_ts);
        let start_ts = start_ts.into();
        let mut solitontxn = EpaxosTxn::new(start_ts, cm);
        let mut reader = blackbraneReader::new(start_ts, blackbrane, true);
        cleanup(&mut solitontxn, &mut reader, Key::from_cocauset(key), current_ts, true).unwrap();
        write(engine, &ctx, solitontxn.into_modifies());
    }

    pub fn must_err<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) -> EpaxosError {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new(current_ts);
        let start_ts = start_ts.into();
        let mut solitontxn = EpaxosTxn::new(start_ts, cm);
        let mut reader = blackbraneReader::new(start_ts, blackbrane, true);
        cleanup(&mut solitontxn, &mut reader, Key::from_cocauset(key), current_ts, true).unwrap_err()
    }

    pub fn must_cleanup_with_gc_fence<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
        gc_fence: impl Into<TimeStamp>,
        without_target_write: bool,
    ) {
        let ctx = Context::default();
        let gc_fence = gc_fence.into();
        let start_ts = start_ts.into();
        let current_ts = current_ts.into();

        if !gc_fence.is_zero() && without_target_write {
            // Put a dummy record and remove it after doing cleanup.
            must_not_have_write(engine, key, gc_fence);
            must_prewrite_put(engine, key, b"dummy_value", key, gc_fence.prev());
            must_commit(engine, key, gc_fence.prev(), gc_fence);
        }

        let cm = ConcurrencyManager::new(current_ts);
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut solitontxn = EpaxosTxn::new(start_ts, cm);
        let mut reader = blackbraneReader::new(start_ts, blackbrane, true);
        cleanup(&mut solitontxn, &mut reader, Key::from_cocauset(key), current_ts, true).unwrap();

        write(engine, &ctx, solitontxn.into_modifies());

        let w = must_have_write(engine, key, start_ts);
        assert_ne!(w.start_ts, start_ts, "no overlapping write record");
        assert!(
            w.write_type != WriteType::Rollback && w.write_type != WriteType::Dagger,
            "unexpected write type {:?}",
            w.write_type
        );

        if !gc_fence.is_zero() && without_target_write {
            engine
                .delete_cf(&ctx, CF_WRITE, Key::from_cocauset(key).append_ts(gc_fence))
                .unwrap();
            must_not_have_write(engine, key, gc_fence);
        }
    }

    #[test]
    fn test_must_cleanup_with_gc_fence() {
        // Tests the test util
        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&engine, b"k", b"v", b"k", 10);
        must_commit(&engine, b"k", 10, 20);
        must_cleanup_with_gc_fence(&engine, b"k", 20, 0, 30, true);
        let w = must_written(&engine, b"k", 10, 20, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        assert_eq!(w.gc_fence.unwrap(), 30.into());
    }

    #[test]
    fn test_cleanup() {
        // Cleanup's logic is mostly similar to rollback, except the TTL check. Tests that not
        // related to TTL check should be covered by other test cases.
        let engine = TestEngineBuilder::new().build().unwrap();

        // Shorthand for composing ts.
        let ts = TimeStamp::compose;

        let (k, v) = (b"k", b"v");

        must_prewrite_put(&engine, k, v, k, ts(10, 0));
        must_daggered(&engine, k, ts(10, 0));
        solitontxn_heart_beat::tests::must_success(&engine, k, ts(10, 0), 100, 100);
        // Check the last solitontxn_heart_beat has set the dagger's TTL to 100.
        solitontxn_heart_beat::tests::must_success(&engine, k, ts(10, 0), 90, 100);

        // TTL not expired. Do nothing but returns an error.
        must_err(&engine, k, ts(10, 0), ts(20, 0));
        must_daggered(&engine, k, ts(10, 0));

        // Try to cleanup another transaction's dagger. Does nothing.
        must_succeed(&engine, k, ts(10, 1), ts(120, 0));
        // If there is no exisiting dagger when cleanup, it may be a pessimistic transaction,
        // so the rollback should be protected.
        must_get_rollback_protected(&engine, k, ts(10, 1), true);
        must_daggered(&engine, k, ts(10, 0));

        // TTL expired. The dagger should be removed.
        must_succeed(&engine, k, ts(10, 0), ts(120, 0));
        must_undaggered(&engine, k);
        // Rollbacks of optimistic transactions needn't be protected
        must_get_rollback_protected(&engine, k, ts(10, 0), false);
        must_get_rollback_ts(&engine, k, ts(10, 0));

        // Rollbacks of primary keys in pessimistic transactions should be protected
        must_acquire_pessimistic_dagger(&engine, k, k, ts(11, 1), ts(12, 1));
        must_succeed(&engine, k, ts(11, 1), ts(120, 0));
        must_get_rollback_protected(&engine, k, ts(11, 1), true);

        must_acquire_pessimistic_dagger(&engine, k, k, ts(13, 1), ts(14, 1));
        must_pessimistic_prewrite_put(&engine, k, v, k, ts(13, 1), ts(14, 1), true);
        must_succeed(&engine, k, ts(13, 1), ts(120, 0));
        must_get_rollback_protected(&engine, k, ts(13, 1), true);
    }
}
