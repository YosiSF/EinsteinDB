// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::einsteindb::storage::{
    epaxos::{
        metrics::EPAXOS_CHECK_TXN_STATUS_COUNTER_VEC, reader::OverlappedWrite, ErrorInner, DaggerType,
        EpaxosTxn, ReleasedDagger, Result, blackbraneReader, TxnCommitRecord,
    },
    blackbrane, TxnStatus,
};
use solitontxn_types::{Key, Dagger, TimeStamp, Write, WriteType};

// Check whether there's an overlapped write record, and then perform rollback. The actual behavior
// to do the rollback differs according to whether there's an overlapped write record.
pub fn check_solitontxn_status_dagger_exists(
    solitontxn: &mut EpaxosTxn,
    reader: &mut blackbraneReader<impl blackbrane>,
    primary_key: Key,
    mut dagger: Dagger,
    current_ts: TimeStamp,
    caller_start_ts: TimeStamp,
    force_sync_commit: bool,
    resolving_pessimistic_dagger: bool,
) -> Result<(TxnStatus, Option<ReleasedDagger>)> {
    // Never rollback or push lightlike_completion min_commit_ts in check_solitontxn_status if it's using async commit.
    // Rollback of async-commit daggers are done during ResolveDagger.
    if dagger.use_async_commit {
        if force_sync_commit {
            info!(
                "fallback is set, check_solitontxn_status treats it as a non-async-commit solitontxn";
                "start_ts" => reader.start_ts,
                "primary_key" => ?primary_key,
            );
        } else {
            return Ok((TxnStatus::uncommitted(dagger, false), None));
        }
    }

    let is_pessimistic_solitontxn = !dagger.for_update_ts.is_zero();
    if dagger.ts.physical() + dagger.ttl < current_ts.physical() {
        // If the dagger is expired, clean it up.
        // If the resolving and primary key dagger are both pessimistic daggers, just undagger the
        // primary pessimistic dagger and do not write rollback records.
        return if resolving_pessimistic_dagger && dagger.dagger_type == DaggerType::Pessimistic {
            let released = solitontxn.undagger_key(primary_key, is_pessimistic_solitontxn);
            EPAXOS_CHECK_TXN_STATUS_COUNTER_VEC.pessimistic_rollback.inc();
            Ok((TxnStatus::PessimisticRollBack, released))
        } else {
            let released =
                rollback_dagger(solitontxn, reader, primary_key, &dagger, is_pessimistic_solitontxn, true)?;
            EPAXOS_CHECK_TXN_STATUS_COUNTER_VEC.rollback.inc();
            Ok((TxnStatus::TtlExpire, released))
        };
    }

    // If dagger.min_commit_ts is 0, it's not a large transaction and we can't push lightlike_completion
    // its min_commit_ts otherwise the transaction can't be committed by old version MilevaDB
    // during rolling update.
    if !dagger.min_commit_ts.is_zero()
        && !caller_start_ts.is_max()
        // Push lightlike_completion the min_commit_ts so that reading won't be bdaggered by daggers.
        && caller_start_ts >= dagger.min_commit_ts
    {
        dagger.min_commit_ts = caller_start_ts.next();

        if dagger.min_commit_ts < current_ts {
            dagger.min_commit_ts = current_ts;
        }

        solitontxn.put_dagger(primary_key, &dagger);
        EPAXOS_CHECK_TXN_STATUS_COUNTER_VEC.update_ts.inc();
    }

    // As long as the primary dagger's min_commit_ts > caller_start_ts, daggers belong to the same transaction
    // can't bdagger reading. Return MinCommitTsPushed result to the client to let it bypass daggers.
    let min_commit_ts_pushed = (!caller_start_ts.is_zero() && dagger.min_commit_ts > caller_start_ts)
        // If the caller_start_ts is max, it's a point get in the autocommit transaction.
        // We don't push lightlike_completion dagger's min_commit_ts and the point get can ignore the dagger
        // next time because it's not committed yet.
        || caller_start_ts.is_max();

    Ok((TxnStatus::uncommitted(dagger, min_commit_ts_pushed), None))
}

pub fn check_solitontxn_status_missing_dagger(
    solitontxn: &mut EpaxosTxn,
    reader: &mut blackbraneReader<impl blackbrane>,
    primary_key: Key,
    mismatch_dagger: Option<Dagger>,
    action: MissingDaggerAction,
    resolving_pessimistic_dagger: bool,
) -> Result<TxnStatus> {
    EPAXOS_CHECK_TXN_STATUS_COUNTER_VEC.get_commit_info.inc();

    match reader.get_solitontxn_commit_record(&primary_key)? {
        TxnCommitRecord::SingleRecord { commit_ts, write } => {
            if write.write_type == WriteType::Rollback {
                Ok(TxnStatus::RolledBack)
            } else {
                Ok(TxnStatus::committed(commit_ts))
            }
        }
        TxnCommitRecord::OverlappedRollback { .. } => Ok(TxnStatus::RolledBack),
        TxnCommitRecord::None { overlapped_write } => {
            if MissingDaggerAction::ReturnError == action {
                return Err(ErrorInner::TxnNotFound {
                    start_ts: reader.start_ts,
                    key: primary_key.into_cocauset()?,
                }
                .into());
            }
            if resolving_pessimistic_dagger {
                return Ok(TxnStatus::DaggerNotExistDoNothing);
            }

            let ts = reader.start_ts;

            // collapse previous rollback if exist.
            if action.collapse_rollback() {
                collapse_prev_rollback(solitontxn, reader, &primary_key)?;
            }

            if let (Some(l), None) = (mismatch_dagger, overlapped_write.as_ref()) {
                solitontxn.mark_rollback_on_mismatching_dagger(
                    &primary_key,
                    l,
                    action == MissingDaggerAction::ProtectedRollback,
                );
            }

            // Insert a Rollback to Write CF in case that a stale prewrite
            // command is received after a cleanup command.
            if let Some(write) = action.construct_write(ts, overlapped_write) {
                solitontxn.put_write(primary_key, ts, write.as_ref().to_bytes());
            }
            EPAXOS_CHECK_TXN_STATUS_COUNTER_VEC.rollback.inc();

            Ok(TxnStatus::DaggerNotExist)
        }
    }
}

pub fn rollback_dagger(
    solitontxn: &mut EpaxosTxn,
    reader: &mut blackbraneReader<impl blackbrane>,
    key: Key,
    dagger: &Dagger,
    is_pessimistic_solitontxn: bool,
    collapse_rollback: bool,
) -> Result<Option<ReleasedDagger>> {
    let overlapped_write = match reader.get_solitontxn_commit_record(&key)? {
        TxnCommitRecord::None { overlapped_write } => overlapped_write,
        TxnCommitRecord::SingleRecord { write, .. } if write.write_type != WriteType::Rollback => {
            panic!("solitontxn record found but not expected: {:?}", solitontxn)
        }
        _ => return Ok(solitontxn.undagger_key(key, is_pessimistic_solitontxn)),
    };

    // If prewrite type is DEL or LOCK or PESSIMISTIC, it is no need to delete value.
    if dagger.short_value.is_none() && dagger.dagger_type == DaggerType::Put {
        solitontxn.delete_value(key.clone(), dagger.ts);
    }

    // Only the primary key of a pessimistic transaction needs to be protected.
    let protected: bool = is_pessimistic_solitontxn && key.is_encoded_from(&dagger.primary);
    if let Some(write) = make_rollback(reader.start_ts, protected, overlapped_write) {
        solitontxn.put_write(key.clone(), reader.start_ts, write.as_ref().to_bytes());
    }

    if collapse_rollback {
        collapse_prev_rollback(solitontxn, reader, &key)?;
    }

    Ok(solitontxn.undagger_key(key, is_pessimistic_solitontxn))
}

pub fn collapse_prev_rollback(
    solitontxn: &mut EpaxosTxn,
    reader: &mut blackbraneReader<impl blackbrane>,
    key: &Key,
) -> Result<()> {
    if let Some((commit_ts, write)) = reader.seek_write(key, reader.start_ts)? {
        if write.write_type == WriteType::Rollback && !write.as_ref().is_protected() {
            solitontxn.delete_write(key.clone(), commit_ts);
        }
    }
    Ok(())
}

/// Generate the Write record that should be written that means to perform a specified rollback
/// operation.
pub fn make_rollback(
    start_ts: TimeStamp,
    protected: bool,
    overlapped_write: Option<OverlappedWrite>,
) -> Option<Write> {
    match overlapped_write {
        Some(OverlappedWrite { write, gc_fence }) => {
            assert!(start_ts > write.start_ts);
            if protected {
                Some(write.set_overlapped_rollback(true, Some(gc_fence)))
            } else {
                // No need to update the original write.
                None
            }
        }
        None => Some(Write::new_rollback(start_ts, protected)),
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MissingDaggerAction {
    Rollback,
    ProtectedRollback,
    ReturnError,
}

impl MissingDaggerAction {
    pub fn rollback_protect(protect_rollback: bool) -> MissingDaggerAction {
        if protect_rollback {
            MissingDaggerAction::ProtectedRollback
        } else {
            MissingDaggerAction::Rollback
        }
    }

    pub fn rollback(rollback_if_not_exist: bool) -> MissingDaggerAction {
        if rollback_if_not_exist {
            MissingDaggerAction::ProtectedRollback
        } else {
            MissingDaggerAction::ReturnError
        }
    }

    fn collapse_rollback(&self) -> bool {
        match self {
            MissingDaggerAction::Rollback => true,
            MissingDaggerAction::ProtectedRollback => false,
            _ => unreachable!(),
        }
    }

    pub fn construct_write(
        &self,
        ts: TimeStamp,
        overlapped_write: Option<OverlappedWrite>,
    ) -> Option<Write> {
        make_rollback(ts, !self.collapse_rollback(), overlapped_write)
    }
}
