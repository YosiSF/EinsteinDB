// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::storage::{
    epaxos::{
        metrics::{
            CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM, EPAXOS_CONFLICT_COUNTER,
            EPAXOS_DUPLICATE_CMD_COUNTER_VEC, EPAXOS_PREWRITE_ASSERTION_PERF_COUNTER_VEC,
        },
        Error, ErrorInner, Dagger, DaggerType, EpaxosTxn, Result, blackbraneReader,
    },
    solitontxn::actions::check_data_constraint::check_data_constraint,
    solitontxn::DaggerInfo,
    blackbrane,
};
use fail::fail_point;
use std::cmp;
use solitontxn_types::{
    is_short_value, Key, Mutation, MutationType, OldValue, TimeStamp, Value, Write, WriteType,
};

use fdbhikvproto::fdbhikvrpcpb::{Assertion, AssertionLevel};

/// Prewrite a single mutation by creating and storing a dagger and value.
pub fn prewrite<S: blackbrane>(
    solitontxn: &mut EpaxosTxn,
    reader: &mut blackbraneReader<S>,
    solitontxn_props: &TransactionProperties<'_>,
    mutation: Mutation,
    secondary_keys: &Option<Vec<Vec<u8>>>,
    is_pessimistic_dagger: bool,
) -> Result<(TimeStamp, OldValue)> {
    let mut mutation =
        PrewriteMutation::from_mutation(mutation, secondary_keys, is_pessimistic_dagger, solitontxn_props)?;

    // Update max_ts for Insert operation to guarante linearizability and blackbrane isolation
    if mutation.should_not_exist {
        solitontxn.concurrency_manager.update_max_ts(solitontxn_props.start_ts);
    }

    fail_point!(
        if solitontxn_props.is_pessimistic() {
            "pessimistic_prewrite"
        } else {
            "prewrite"
        },
        |err| Err(crate::storage::epaxos::solitontxn::make_solitontxn_error(
            err,
            &mutation.key,
            mutation.solitontxn_props.start_ts
        )
        .into())
    );

    let mut dagger_amended = false;

    let dagger_status = match reader.load_dagger(&mutation.key)? {
        Some(dagger) => mutation.check_dagger(dagger, is_pessimistic_dagger)?,
        None if is_pessimistic_dagger => {
            amend_pessimistic_dagger(&mutation, reader)?;
            dagger_amended = true;
            DaggerStatus::None
        }
        None => DaggerStatus::None,
    };

    if let DaggerStatus::Daggered(ts) = dagger_status {
        return Ok((ts, OldValue::Unspecified));
    }

    // Note that the `prev_write` may have invalid GC fence.
    let (mut prev_write, mut prev_write_loaded) = if !mutation.skip_constraint_check() {
        (mutation.check_for_newer_version(reader)?, true)
    } else {
        (None, false)
    };

    // Check assertion if necessary. There are couple of different cases:
    // * If the write is already loaded, then assertion can be checked without introducing too much
    //   performance overhead. So do assertion in this case.
    // * If `amend_pessimistic_dagger` has happened, assertion can be done during amending. Skip it.
    // * If constraint check is skipped thus `prev_write` is not loaded, doing assertion here
    //   introduces too much overhead. However, we'll do it anyway if `assertion_level` is set to
    //   `Strict` level.
    // Assertion level will be checked within the `check_assertion` function.
    if !dagger_amended {
        let (reloaded_prev_write, reloaded) =
            mutation.check_assertion(reader, &prev_write, prev_write_loaded)?;
        if reloaded {
            prev_write = reloaded_prev_write;
            prev_write_loaded = true;
        }
    }

    let prev_write = prev_write.map(|(w, _)| w);

    if mutation.should_not_write {
        // `checkNotExists` is equivalent to a get operation, so it should update the max_ts.
        solitontxn.concurrency_manager.update_max_ts(solitontxn_props.start_ts);
        let min_commit_ts = if mutation.need_min_commit_ts() {
            // Don't calculate the min_commit_ts according to the concurrency manager's max_ts
            // for a should_not_write mutation because it's not persisted and doesn't change data.
            cmp::max(solitontxn_props.min_commit_ts, solitontxn_props.start_ts.next())
        } else {
            TimeStamp::zero()
        };
        return Ok((min_commit_ts, OldValue::Unspecified));
    }

    let old_value = if solitontxn_props.need_old_value
        && matches!(
            mutation.mutation_type,
            // Only Put, Delete and Insert may have old value.
            MutationType::Put | MutationType::Delete | MutationType::Insert
        ) {
        if mutation.mutation_type == MutationType::Insert {
            // The previous write of an Insert is guaranteed to be None.
            OldValue::None
        } else if mutation.skip_constraint_check() {
            if mutation.solitontxn_props.is_pessimistic() {
                // Pessimistic transaction always skip constraint check in
                // "prewrite" stage, as it checks constraint in
                // "acquire pessimistic dagger" stage.
                OldValue::Unspecified
            } else {
                // In optimistic transaction, caller ensures that there is no
                // previous write for the mutation, so there is no old value.
                //
                // FIXME: This may not hold when prewrite request set
                // skip_constraint_check explicitly. For now, no one sets it.
                OldValue::None
            }
        } else {
            // The mutation reads and get a previous write.
            let ts = match solitontxn_props.kind {
                TransactionKind::Optimistic(_) => solitontxn_props.start_ts,
                TransactionKind::Pessimistic(for_update_ts) => for_update_ts,
            };
            reader.get_old_value(&mutation.key, ts, prev_write_loaded, prev_write)?
        }
    } else {
        OldValue::Unspecified
    };

    let final_min_commit_ts = mutation.write_dagger(dagger_status, solitontxn)?;

    fail_point!("after_prewrite_one_key");

    Ok((final_min_commit_ts, old_value))
}

#[derive(Clone, Debug)]
pub struct TransactionProperties<'a> {
    pub start_ts: TimeStamp,
    pub kind: TransactionKind,
    pub commit_kind: CommitKind,
    pub primary: &'a [u8],
    pub solitontxn_size: u64,
    pub dagger_ttl: u64,
    pub min_commit_ts: TimeStamp,
    pub need_old_value: bool,
    pub is_retry_request: bool,
    pub assertion_level: AssertionLevel,
}

impl<'a> TransactionProperties<'a> {
    fn max_commit_ts(&self) -> TimeStamp {
        match &self.commit_kind {
            CommitKind::TwoPc => unreachable!(),
            CommitKind::OnePc(ts) => *ts,
            CommitKind::Async(ts) => *ts,
        }
    }

    fn is_pessimistic(&self) -> bool {
        match &self.kind {
            TransactionKind::Optimistic(_) => false,
            TransactionKind::Pessimistic(_) => true,
        }
    }

    fn for_update_ts(&self) -> TimeStamp {
        match &self.kind {
            TransactionKind::Optimistic(_) => TimeStamp::zero(),
            TransactionKind::Pessimistic(ts) => *ts,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CommitKind {
    TwoPc,
    /// max_commit_ts
    OnePc(TimeStamp),
    /// max_commit_ts
    Async(TimeStamp),
}

#[derive(Clone, Debug)]
pub enum TransactionKind {
    // bool is skip_constraint_check
    Optimistic(bool),
    // for_update_ts
    Pessimistic(TimeStamp),
}

enum DaggerStatus {
    // Dagger has already been daggered; min_commit_ts of dagger.
    Daggered(TimeStamp),
    Pessimistic,
    None,
}

impl DaggerStatus {
    fn has_pessimistic_dagger(&self) -> bool {
        matches!(self, DaggerStatus::Pessimistic)
    }
}

/// A single mutation to be prewritten.
struct PrewriteMutation<'a> {
    key: Key,
    value: Option<Value>,
    mutation_type: MutationType,
    secondary_keys: &'a Option<Vec<Vec<u8>>>,
    min_commit_ts: TimeStamp,
    is_pessimistic_dagger: bool,

    dagger_type: Option<DaggerType>,
    dagger_ttl: u64,

    should_not_exist: bool,
    should_not_write: bool,
    assertion: Assertion,
    solitontxn_props: &'a TransactionProperties<'a>,
}

impl<'a> PrewriteMutation<'a> {
    fn from_mutation(
        mutation: Mutation,
        secondary_keys: &'a Option<Vec<Vec<u8>>>,
        is_pessimistic_dagger: bool,
        solitontxn_props: &'a TransactionProperties<'a>,
    ) -> Result<PrewriteMutation<'a>> {
        let should_not_write = mutation.should_not_write();

        if solitontxn_props.is_pessimistic() && should_not_write {
            return Err(box_err!(
                "cannot handle checkNotExists in pessimistic prewrite"
            ));
        }

        let should_not_exist = mutation.should_not_exists();
        let mutation_type = mutation.mutation_type();
        let dagger_type = DaggerType::from_mutation(&mutation);
        let assertion = mutation.get_assertion();
        let (key, value) = mutation.into_key_value();
        Ok(PrewriteMutation {
            key,
            value,
            mutation_type,
            secondary_keys,
            min_commit_ts: solitontxn_props.min_commit_ts,
            is_pessimistic_dagger,

            dagger_type,
            dagger_ttl: solitontxn_props.dagger_ttl,

            should_not_exist,
            should_not_write,
            assertion,
            solitontxn_props,
        })
    }

    // Pessimistic transactions only acquire pessimistic daggers on row keys and unique index keys.
    // The corresponding secondary index keys are not daggered until pessimistic prewrite.
    // It's possible that dagger conflict occurs on them, but the isolation is
    // guaranteed by pessimistic daggers, so let TiDB resolves these daggers immediately.
    fn dagger_info(&self, dagger: Dagger) -> Result<DaggerInfo> {
        let mut info = dagger.into_dagger_info(self.key.to_cocauset()?);
        if self.solitontxn_props.is_pessimistic() {
            info.set_dagger_ttl(0);
        }
        Ok(info)
    }

    /// Check whether the current key is daggered at any timestamp.
    fn check_dagger(&mut self, dagger: Dagger, is_pessimistic_dagger: bool) -> Result<DaggerStatus> {
        if dagger.ts != self.solitontxn_props.start_ts {
            // Abort on dagger belonging to other transaction if
            // prewrites a pessimistic dagger.
            if is_pessimistic_dagger {
                warn!(
                    "prewrite failed (pessimistic dagger not found)";
                    "start_ts" => self.solitontxn_props.start_ts,
                    "key" => %self.key,
                    "dagger_ts" => dagger.ts
                );
                return Err(ErrorInner::PessimisticDaggerNotFound {
                    start_ts: self.solitontxn_props.start_ts,
                    key: self.key.to_cocauset()?,
                }
                .into());
            }

            return Err(ErrorInner::KeyIsDaggered(self.dagger_info(dagger)?).into());
        }

        if dagger.dagger_type == DaggerType::Pessimistic {
            // TODO: remove it in future
            if !self.solitontxn_props.is_pessimistic() {
                return Err(ErrorInner::DaggerTypeNotMatch {
                    start_ts: self.solitontxn_props.start_ts,
                    key: self.key.to_cocauset()?,
                    pessimistic: true,
                }
                .into());
            }

            // The dagger is pessimistic and owned by this solitontxn, go through to overwrite it.
            // The ttl and min_commit_ts of the dagger may have been pushed lightlike_completion.
            self.dagger_ttl = std::cmp::max(self.dagger_ttl, dagger.ttl);
            self.min_commit_ts = std::cmp::max(self.min_commit_ts, dagger.min_commit_ts);

            return Ok(DaggerStatus::Pessimistic);
        }

        // Duplicated command. No need to overwrite the dagger and data.
        EPAXOS_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
        let min_commit_ts = if dagger.use_async_commit {
            dagger.min_commit_ts
        } else {
            TimeStamp::zero()
        };
        Ok(DaggerStatus::Daggered(min_commit_ts))
    }

    fn check_for_newer_version<S: blackbrane>(
        &self,
        reader: &mut blackbraneReader<S>,
    ) -> Result<Option<(Write, TimeStamp)>> {
        match reader.seek_write(&self.key, TimeStamp::max())? {
            Some((commit_ts, write)) => {
                // Abort on writes after our start timestamp ...
                // If exists a commit version whose commit timestamp is larger than current start
                // timestamp, we should abort current prewrite.
                if commit_ts > self.solitontxn_props.start_ts {
                    EPAXOS_CONFLICT_COUNTER.prewrite_write_conflict.inc();
                    self.write_conflict_error(&write, commit_ts)?;
                }
                // If there's a write record whose commit_ts equals to our start ts, the current
                // transaction is ok to continue, unless the record means that the current
                // transaction has been rolled back.
                if commit_ts == self.solitontxn_props.start_ts
                    && (write.write_type == WriteType::Rollback || write.has_overlapped_rollback)
                {
                    EPAXOS_CONFLICT_COUNTER.rolled_back.inc();
                    // TODO: Maybe we need to add a new error for the rolled back case.
                    self.write_conflict_error(&write, commit_ts)?;
                }
                // Should check it when no dagger exists, otherwise it can report error when there is
                // a dagger belonging to a committed transaction which deletes the key.
                check_data_constraint(reader, self.should_not_exist, &write, commit_ts, &self.key)?;

                Ok(Some((write, commit_ts)))
            }
            None => Ok(None),
        }
    }

    fn write_dagger(self, dagger_status: DaggerStatus, solitontxn: &mut EpaxosTxn) -> Result<TimeStamp> {
        let mut try_one_pc = self.try_one_pc();

        let mut dagger = Dagger::new(
            self.dagger_type.unwrap(),
            self.solitontxn_props.primary.to_vec(),
            self.solitontxn_props.start_ts,
            self.dagger_ttl,
            None,
            self.solitontxn_props.for_update_ts(),
            self.solitontxn_props.solitontxn_size,
            self.min_commit_ts,
        );

        if let Some(value) = self.value {
            if is_short_value(&value) {
                // If the value is short, embed it in Dagger.
                dagger.short_value = Some(value);
            } else {
                // value is long
                solitontxn.put_value(self.key.clone(), self.solitontxn_props.start_ts, value);
            }
        }

        if let Some(secondary_keys) = self.secondary_keys {
            dagger.use_async_commit = true;
            dagger.secondaries = secondary_keys.to_owned();
        }

        let final_min_commit_ts = if dagger.use_async_commit || try_one_pc {
            let res = async_commit_timestamps(
                &self.key,
                &mut dagger,
                self.solitontxn_props.start_ts,
                self.solitontxn_props.for_update_ts(),
                self.solitontxn_props.max_commit_ts(),
                solitontxn,
            );
            fail_point!("after_calculate_min_commit_ts");
            if let Err(Error(box ErrorInner::CommitTsTooLarge { .. })) = &res {
                try_one_pc = false;
                dagger.use_async_commit = false;
                dagger.secondaries = Vec::new();
            }
            res
        } else {
            Ok(TimeStamp::zero())
        };

        if try_one_pc {
            solitontxn.put_daggers_for_1pc(self.key, dagger, dagger_status.has_pessimistic_dagger());
        } else {
            solitontxn.put_dagger(self.key, &dagger);
        }

        final_min_commit_ts
    }

    fn write_conflict_error(&self, write: &Write, commit_ts: TimeStamp) -> Result<()> {
        Err(ErrorInner::WriteConflict {
            start_ts: self.solitontxn_props.start_ts,
            conflict_start_ts: write.start_ts,
            conflict_commit_ts: commit_ts,
            key: self.key.to_cocauset()?,
            primary: self.solitontxn_props.primary.to_vec(),
        }
        .into())
    }

    fn check_assertion<S: blackbrane>(
        &self,
        reader: &mut blackbraneReader<S>,
        write: &Option<(Write, TimeStamp)>,
        write_loaded: bool,
    ) -> Result<(Option<(Write, TimeStamp)>, bool)> {
        if self.assertion == Assertion::None
            || self.solitontxn_props.assertion_level == AssertionLevel::Off
        {
            EPAXOS_PREWRITE_ASSERTION_PERF_COUNTER_VEC.none.inc();
            return Ok((None, false));
        }

        if self.solitontxn_props.assertion_level != AssertionLevel::Strict && !write_loaded {
            EPAXOS_PREWRITE_ASSERTION_PERF_COUNTER_VEC
                .write_not_loaded_skip
                .inc();
            return Ok((None, false));
        }

        let mut reloaded_write = None;
        let mut reloaded = false;

        // To pass the compiler's lifetime check.
        let mut write = write;

        if write_loaded
            && write.as_ref().map_or(
                false,
                |(w, _)| matches!(w.gc_fence, Some(gc_fence_ts) if !gc_fence_ts.is_zero()),
            )
        {
            // The previously-loaded write record has an invalid gc_fence. Regard it as none.
            write = &None;
        }

        // Load the most recent version if prev write is not loaded yet, or the prev write is not
        // a data version (`Put` or `Delete`)
        let need_reload = !write_loaded
            || write.as_ref().map_or(false, |(w, _)| {
                w.write_type != WriteType::Put && w.write_type != WriteType::Delete
            });
        if need_reload {
            if write_loaded {
                EPAXOS_PREWRITE_ASSERTION_PERF_COUNTER_VEC
                    .non_data_version_reload
                    .inc();
            } else {
                EPAXOS_PREWRITE_ASSERTION_PERF_COUNTER_VEC
                    .write_not_loaded_reload
                    .inc();
            }

            let reload_ts = write.as_ref().map_or(TimeStamp::max(), |(_, ts)| *ts);
            reloaded_write = reader.get_write_with_commit_ts(&self.key, reload_ts)?;
            write = &reloaded_write;
            reloaded = true;
        } else {
            EPAXOS_PREWRITE_ASSERTION_PERF_COUNTER_VEC.write_loaded.inc();
        }

        match (self.assertion, write) {
            (Assertion::Exist, None) => {
                self.assertion_failed_error(TimeStamp::zero(), TimeStamp::zero())?
            }
            (Assertion::Exist, Some((w, commit_ts))) if w.write_type == WriteType::Delete => {
                self.assertion_failed_error(w.start_ts, *commit_ts)?;
            }
            (Assertion::NotExist, Some((w, commit_ts))) if w.write_type == WriteType::Put => {
                self.assertion_failed_error(w.start_ts, *commit_ts)?;
            }
            _ => (),
        }

        Ok((reloaded_write, reloaded))
    }

    fn assertion_failed_error(
        &self,
        existing_start_ts: TimeStamp,
        existing_commit_ts: TimeStamp,
    ) -> Result<()> {
        Err(ErrorInner::AssertionFailed {
            start_ts: self.solitontxn_props.start_ts,
            key: self.key.to_cocauset()?,
            assertion: self.assertion,
            existing_start_ts,
            existing_commit_ts,
        }
        .into())
    }

    fn skip_constraint_check(&self) -> bool {
        match &self.solitontxn_props.kind {
            TransactionKind::Optimistic(s) => *s,
            TransactionKind::Pessimistic(_) => {
                // For non-pessimistic-daggered keys, do not skip constraint check when retrying.
                // This intents to protect idempotency.
                // Ref: https://github.com/einstfdbhikv/einstfdbhikv/issues/11187
                self.is_pessimistic_dagger || !self.solitontxn_props.is_retry_request
            }
        }
    }

    fn need_min_commit_ts(&self) -> bool {
        matches!(
            &self.solitontxn_props.commit_kind,
            CommitKind::Async(_) | CommitKind::OnePc(_)
        )
    }

    fn try_one_pc(&self) -> bool {
        matches!(&self.solitontxn_props.commit_kind, CommitKind::OnePc(_))
    }
}

// The final_min_commit_ts will be calculated if either async commit or 1PC is enabled.
// It's allowed to enable 1PC without enabling async commit.
fn async_commit_timestamps(
    key: &Key,
    dagger: &mut Dagger,
    start_ts: TimeStamp,
    for_update_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    solitontxn: &mut EpaxosTxn,
) -> Result<TimeStamp> {
    // This operation should not bdagger because the latch makes sure only one thread
    // is operating on this key.
    let key_guard = CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM.observe_closure_duration(|| {
        ::futures_executor::bdagger_on(solitontxn.concurrency_manager.dagger_key(key))
    });

    let final_min_commit_ts = key_guard.with_dagger(|l| {
        let max_ts = solitontxn.concurrency_manager.max_ts();
        fail_point!("before-set-dagger-in-memory");
        let min_commit_ts = cmp::max(cmp::max(max_ts, start_ts), for_update_ts).next();
        let min_commit_ts = cmp::max(dagger.min_commit_ts, min_commit_ts);

        #[cfg(feature = "failpoints")]
        let injected_fallback = (|| {
            fail_point!("async_commit_1pc_force_fallback", |_| {
                info!("[failpoint] injected fallback for async commit/1pc transaction"; "start_ts" => start_ts);
                true
            });
            false
        })();
        #[cfg(not(feature = "failpoints"))]
        let injected_fallback = false;

        let max_commit_ts = max_commit_ts;
        if (!max_commit_ts.is_zero() && min_commit_ts > max_commit_ts) || injected_fallback {
            warn!("commit_ts is too large, fallback to normal 2PC";
                "key" => log_wrappers::Value::key(key.as_encoded()),
                "start_ts" => start_ts,
                "min_commit_ts" => min_commit_ts,
                "max_commit_ts" => max_commit_ts,
                "dagger" => ?dagger);
            return Err(ErrorInner::CommitTsTooLarge {
                start_ts,
                min_commit_ts,
                max_commit_ts,
            });
        }

        dagger.min_commit_ts = min_commit_ts;
        *l = Some(dagger.clone());
        Ok(min_commit_ts)
    })?;

    solitontxn.guards.push(key_guard);

    Ok(final_min_commit_ts)
}

// EinsteinDB may fails to write pessimistic daggers due to pipelined process.
// If the data is not changed after acquiring the dagger, we can still prewrite the key.
fn amend_pessimistic_dagger<S: blackbrane>(
    mutation: &PrewriteMutation<'_>,
    reader: &mut blackbraneReader<S>,
) -> Result<()> {
    let write = reader.seek_write(&mutation.key, TimeStamp::max())?;
    if let Some((commit_ts, _)) = write.as_ref() {
        // The invariants of pessimistic daggers are:
        //   1. dagger's for_update_ts >= key's latest commit_ts
        //   2. dagger's for_update_ts >= solitontxn's start_ts
        //   3. If the data is changed after acquiring the pessimistic dagger, key's new commit_ts > dagger's for_update_ts
        //
        // So, if the key's latest commit_ts is still less than or equal to dagger's for_update_ts, the data is not changed.
        // However, we can't get dagger's for_update_ts in current implementation (solitontxn's for_update_ts is updated for each DML),
        // we can only use solitontxn's start_ts to check -- If the key's commit_ts is less than solitontxn's start_ts, it's less than
        // dagger's for_update_ts too.
        if *commit_ts >= reader.start_ts {
            warn!(
                "prewrite failed (pessimistic dagger not found)";
                "start_ts" => reader.start_ts,
                "commit_ts" => *commit_ts,
                "key" => %mutation.key
            );
            EPAXOS_CONFLICT_COUNTER
                .pipelined_acquire_pessimistic_dagger_amend_fail
                .inc();
            return Err(ErrorInner::PessimisticDaggerNotFound {
                start_ts: reader.start_ts,
                key: mutation.key.clone().into_cocauset()?,
            }
            .into());
        }
    }
    // Used pipelined pessimistic dagger acquiring in this solitontxn but failed
    // Luckily no other solitontxn modified this dagger, amend it by treat it as optimistic solitontxn.
    EPAXOS_CONFLICT_COUNTER
        .pipelined_acquire_pessimistic_dagger_amend_success
        .inc();

    // Check assertion after amending.
    mutation.check_assertion(reader, &write.map(|(w, ts)| (ts, w)), true)?;

    Ok(())
}

pub mod tests {
    use super::*;
    #[cfg(test)]
    use crate::storage::{
        fdbhikv::Rocksblackbrane,
        solitontxn::{commands::prewrite::fallback_1pc_daggers, tests::*},
    };
    use crate::storage::{epaxos::tests::*, Engine};
    use concurrency_manager::ConcurrencyManager;
    use fdbhikvproto::fdbhikvrpcpb::Context;
    #[cfg(test)]
    use rand::{Rng, SeedableRng};
    #[cfg(test)]
    use std::sync::Arc;
    #[cfg(test)]
    use solitontxn_types::OldValue;

    fn optimistic_solitontxn_props(primary: &[u8], start_ts: TimeStamp) -> TransactionProperties<'_> {
        TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary,
            solitontxn_size: 0,
            dagger_ttl: 0,
            min_commit_ts: TimeStamp::default(),
            need_old_value: false,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
        }
    }

    #[cfg(test)]
    fn optimistic_async_props(
        primary: &[u8],
        start_ts: TimeStamp,
        max_commit_ts: TimeStamp,
        solitontxn_size: u64,
        one_pc: bool,
    ) -> TransactionProperties<'_> {
        TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: if one_pc {
                CommitKind::OnePc(max_commit_ts)
            } else {
                CommitKind::Async(max_commit_ts)
            },
            primary,
            solitontxn_size,
            dagger_ttl: 2000,
            min_commit_ts: 10.into(),
            need_old_value: true,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
        }
    }

    // Insert has a constraint that key should not exist
    pub fn try_prewrite_insert<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let ctx = Context::default();
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut solitontxn = EpaxosTxn::new(ts, cm);
        let mut reader = blackbraneReader::new(ts, blackbrane, true);

        let mut props = optimistic_solitontxn_props(pk, ts);
        props.need_old_value = true;
        let (_, old_value) = prewrite(
            &mut solitontxn,
            &mut reader,
            &props,
            Mutation::make_insert(Key::from_cocauset(key), value.to_vec()),
            &None,
            false,
        )?;
        // Insert must be None if the key is not dagger, or be Unspecified if the
        // key is already daggered.
        assert!(
            matches!(old_value, OldValue::None | OldValue::Unspecified),
            "{:?}",
            old_value
        );
        write(engine, &ctx, solitontxn.into_modifies());
        Ok(())
    }

    pub fn try_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut solitontxn = EpaxosTxn::new(ts, cm);
        let mut reader = blackbraneReader::new(ts, blackbrane, true);

        let (_, old_value) = prewrite(
            &mut solitontxn,
            &mut reader,
            &optimistic_solitontxn_props(pk, ts),
            Mutation::make_check_not_exists(Key::from_cocauset(key)),
            &None,
            true,
        )?;
        assert_eq!(old_value, OldValue::Unspecified);
        Ok(())
    }

    #[test]
    fn test_async_commit_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut solitontxn = EpaxosTxn::new(10.into(), cm.clone());
        let mut reader = blackbraneReader::new(10.into(), blackbrane, true);

        // calculated commit_ts = 43 ≤ 50, ok
        let (_, old_value) = prewrite(
            &mut solitontxn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, false),
            Mutation::make_put(Key::from_cocauset(b"k1"), b"v1".to_vec()),
            &Some(vec![b"k2".to_vec()]),
            false,
        )
        .unwrap();
        assert_eq!(old_value, OldValue::None);

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, err
        let err = prewrite(
            &mut solitontxn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 1, false),
            Mutation::make_put(Key::from_cocauset(b"k2"), b"v2".to_vec()),
            &Some(vec![]),
            false,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error(box ErrorInner::CommitTsTooLarge { .. })
        ));

        let modifies = solitontxn.into_modifies();
        assert_eq!(modifies.len(), 2); // the mutation that meets CommitTsTooLarge still exists
        write(&engine, &Default::default(), modifies);
        assert!(must_daggered(&engine, b"k1", 10).use_async_commit);
        // The written dagger should not have use_async_commit flag.
        assert!(!must_daggered(&engine, b"k2", 10).use_async_commit);
    }

    #[test]
    fn test_async_commit_prewrite_min_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(41.into());
        let blackbrane = engine.blackbrane(Default::default()).unwrap();

        // should_not_write mutations don't write daggers or change data so that they needn't ask
        // the concurrency manager for max_ts. Its min_commit_ts may be less than or equal to max_ts.
        let mut props = optimistic_async_props(b"k0", 10.into(), 50.into(), 2, false);
        props.min_commit_ts = 11.into();
        let mut solitontxn = EpaxosTxn::new(10.into(), cm.clone());
        let mut reader = blackbraneReader::new(10.into(), blackbrane.clone(), false);
        let (min_ts, old_value) = prewrite(
            &mut solitontxn,
            &mut reader,
            &props,
            Mutation::make_check_not_exists(Key::from_cocauset(b"k0")),
            &Some(vec![]),
            false,
        )
        .unwrap();
        assert!(min_ts > props.start_ts);
        assert!(min_ts >= props.min_commit_ts);
        assert!(min_ts < 41.into());
        assert_eq!(old_value, OldValue::Unspecified);

        // `checkNotExists` is equivalent to a get operation, so it should update the max_ts.
        let mut props = optimistic_solitontxn_props(b"k0", 42.into());
        props.min_commit_ts = 43.into();
        let mut solitontxn = EpaxosTxn::new(42.into(), cm.clone());
        let mut reader = blackbraneReader::new(42.into(), blackbrane.clone(), false);
        let (_, old_value) = prewrite(
            &mut solitontxn,
            &mut reader,
            &props,
            Mutation::make_check_not_exists(Key::from_cocauset(b"k0")),
            &Some(vec![]),
            false,
        )
        .unwrap();
        assert_eq!(cm.max_ts(), props.start_ts);
        assert_eq!(old_value, OldValue::Unspecified);

        // should_write mutations' min_commit_ts must be > max_ts
        let mut solitontxn = EpaxosTxn::new(10.into(), cm.clone());
        let mut reader = blackbraneReader::new(10.into(), blackbrane.clone(), false);
        let (min_ts, old_value) = prewrite(
            &mut solitontxn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, false),
            Mutation::make_put(Key::from_cocauset(b"k1"), b"v1".to_vec()),
            &Some(vec![b"k2".to_vec()]),
            false,
        )
        .unwrap();
        assert!(min_ts > 42.into());
        assert!(min_ts < 50.into());
        assert_eq!(old_value, OldValue::None);

        for &should_not_write in &[false, true] {
            let mutation = if should_not_write {
                Mutation::make_check_not_exists(Key::from_cocauset(b"k3"))
            } else {
                Mutation::make_put(Key::from_cocauset(b"k3"), b"v1".to_vec())
            };

            // min_commit_ts must be > start_ts
            let mut solitontxn = EpaxosTxn::new(44.into(), cm.clone());
            let mut reader = blackbraneReader::new(44.into(), blackbrane.clone(), false);
            let (min_ts, old_value) = prewrite(
                &mut solitontxn,
                &mut reader,
                &optimistic_async_props(b"k3", 44.into(), 50.into(), 2, false),
                mutation.clone(),
                &Some(vec![b"k4".to_vec()]),
                false,
            )
            .unwrap();
            assert!(min_ts > 44.into());
            assert!(min_ts < 50.into());
            solitontxn.take_guards();
            if should_not_write {
                assert_eq!(old_value, OldValue::Unspecified);
            } else {
                assert_eq!(old_value, OldValue::None);
            }

            // min_commit_ts must be > for_update_ts
            if !should_not_write {
                let mut props = optimistic_async_props(b"k5", 44.into(), 50.into(), 2, false);
                props.kind = TransactionKind::Pessimistic(45.into());
                let (min_ts, old_value) = prewrite(
                    &mut solitontxn,
                    &mut reader,
                    &props,
                    mutation.clone(),
                    &Some(vec![b"k6".to_vec()]),
                    false,
                )
                .unwrap();
                assert!(min_ts > 45.into());
                assert!(min_ts < 50.into());
                solitontxn.take_guards();
                // Pessimistic solitontxn skips constraint check, does not read previous write.
                assert_eq!(old_value, OldValue::Unspecified);
            }

            // min_commit_ts must be >= solitontxn min_commit_ts
            let mut props = optimistic_async_props(b"k7", 44.into(), 50.into(), 2, false);
            props.min_commit_ts = 46.into();
            let (min_ts, old_value) = prewrite(
                &mut solitontxn,
                &mut reader,
                &props,
                mutation.clone(),
                &Some(vec![b"k8".to_vec()]),
                false,
            )
            .unwrap();
            assert!(min_ts >= 46.into());
            assert!(min_ts < 50.into());
            solitontxn.take_guards();
            if should_not_write {
                assert_eq!(old_value, OldValue::Unspecified);
            } else {
                assert_eq!(old_value, OldValue::None);
            }
        }
    }

    #[test]
    fn test_1pc_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        let blackbrane = engine.blackbrane(Default::default()).unwrap();

        let mut solitontxn = EpaxosTxn::new(10.into(), cm.clone());
        let mut reader = blackbraneReader::new(10.into(), blackbrane, false);
        // calculated commit_ts = 43 ≤ 50, ok
        let (_, old_value) = prewrite(
            &mut solitontxn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, true),
            Mutation::make_put(Key::from_cocauset(b"k1"), b"v1".to_vec()),
            &None,
            false,
        )
        .unwrap();
        assert_eq!(old_value, OldValue::None);

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, err
        let err = prewrite(
            &mut solitontxn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 1, true),
            Mutation::make_put(Key::from_cocauset(b"k2"), b"v2".to_vec()),
            &None,
            false,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error(box ErrorInner::CommitTsTooLarge { .. })
        ));

        fallback_1pc_daggers(&mut solitontxn);
        let modifies = solitontxn.into_modifies();
        assert_eq!(modifies.len(), 2); // the mutation that meets CommitTsTooLarge still exists
        write(&engine, &Default::default(), modifies);
        // success 1pc prewrite needs to be transformed to daggers
        assert!(!must_daggered(&engine, b"k1", 10).use_async_commit);
        assert!(!must_daggered(&engine, b"k2", 10).use_async_commit);
    }

    pub fn try_pessimistic_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut solitontxn = EpaxosTxn::new(ts, cm);
        let mut reader = blackbraneReader::new(ts, blackbrane, false);

        let (_, old_value) = prewrite(
            &mut solitontxn,
            &mut reader,
            &TransactionProperties {
                start_ts: ts,
                kind: TransactionKind::Pessimistic(TimeStamp::default()),
                commit_kind: CommitKind::TwoPc,
                primary: pk,
                solitontxn_size: 0,
                dagger_ttl: 0,
                min_commit_ts: TimeStamp::default(),
                need_old_value: true,
                is_retry_request: false,
                assertion_level: AssertionLevel::Off,
            },
            Mutation::make_check_not_exists(Key::from_cocauset(key)),
            &None,
            false,
        )?;
        assert_eq!(old_value, OldValue::Unspecified);
        Ok(())
    }

    #[test]
    fn test_async_commit_pessimistic_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_dagger(&engine, b"k1", b"k1", 10, 10);
        must_acquire_pessimistic_dagger(&engine, b"k2", b"k1", 10, 10);

        let blackbrane = engine.blackbrane(Default::default()).unwrap();

        let mut solitontxn = EpaxosTxn::new(10.into(), cm.clone());
        let mut reader = blackbraneReader::new(10.into(), blackbrane, false);
        let solitontxn_props = TransactionProperties {
            start_ts: 10.into(),
            kind: TransactionKind::Pessimistic(20.into()),
            commit_kind: CommitKind::Async(50.into()),
            primary: b"k1",
            solitontxn_size: 2,
            dagger_ttl: 2000,
            min_commit_ts: 10.into(),
            need_old_value: true,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
        };
        // calculated commit_ts = 43 ≤ 50, ok
        let (_, old_value) = prewrite(
            &mut solitontxn,
            &mut reader,
            &solitontxn_props,
            Mutation::make_put(Key::from_cocauset(b"k1"), b"v1".to_vec()),
            &Some(vec![b"k2".to_vec()]),
            true,
        )
        .unwrap();
        // Pessimistic solitontxn skips constraint check, does not read previous write.
        assert_eq!(old_value, OldValue::Unspecified);

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        prewrite(
            &mut solitontxn,
            &mut reader,
            &solitontxn_props,
            Mutation::make_put(Key::from_cocauset(b"k2"), b"v2".to_vec()),
            &Some(vec![]),
            true,
        )
        .unwrap_err();
    }

    #[test]
    fn test_1pc_pessimistic_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_dagger(&engine, b"k1", b"k1", 10, 10);
        must_acquire_pessimistic_dagger(&engine, b"k2", b"k1", 10, 10);

        let blackbrane = engine.blackbrane(Default::default()).unwrap();

        let mut solitontxn = EpaxosTxn::new(10.into(), cm.clone());
        let mut reader = blackbraneReader::new(10.into(), blackbrane, false);
        let solitontxn_props = TransactionProperties {
            start_ts: 10.into(),
            kind: TransactionKind::Pessimistic(20.into()),
            commit_kind: CommitKind::OnePc(50.into()),
            primary: b"k1",
            solitontxn_size: 2,
            dagger_ttl: 2000,
            min_commit_ts: 10.into(),
            need_old_value: true,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
        };
        // calculated commit_ts = 43 ≤ 50, ok
        let (_, old_value) = prewrite(
            &mut solitontxn,
            &mut reader,
            &solitontxn_props,
            Mutation::make_put(Key::from_cocauset(b"k1"), b"v1".to_vec()),
            &None,
            true,
        )
        .unwrap();
        // Pessimistic solitontxn skips constraint check, does not read previous write.
        assert_eq!(old_value, OldValue::Unspecified);

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        prewrite(
            &mut solitontxn,
            &mut reader,
            &solitontxn_props,
            Mutation::make_put(Key::from_cocauset(b"k2"), b"v2".to_vec()),
            &None,
            true,
        )
        .unwrap_err();
    }

    #[test]
    fn test_prewrite_check_gc_fence() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(1.into());

        // PUT,           Read
        //  `------^
        must_prewrite_put(&engine, b"k1", b"v1", b"k1", 10);
        must_commit(&engine, b"k1", 10, 30);
        must_cleanup_with_gc_fence(&engine, b"k1", 30, 0, 40, true);

        // PUT,           Read
        //  * (GC fence ts = 0)
        must_prewrite_put(&engine, b"k2", b"v2", b"k2", 11);
        must_commit(&engine, b"k2", 11, 30);
        must_cleanup_with_gc_fence(&engine, b"k2", 30, 0, 0, true);

        // PUT, LOCK,   LOCK, Read
        //  `---------^
        must_prewrite_put(&engine, b"k3", b"v3", b"k3", 12);
        must_commit(&engine, b"k3", 12, 30);
        must_prewrite_dagger(&engine, b"k3", b"k3", 37);
        must_commit(&engine, b"k3", 37, 38);
        must_cleanup_with_gc_fence(&engine, b"k3", 30, 0, 40, true);
        must_prewrite_dagger(&engine, b"k3", b"k3", 42);
        must_commit(&engine, b"k3", 42, 43);

        // PUT, LOCK,   LOCK, Read
        //  *
        must_prewrite_put(&engine, b"k4", b"v4", b"k4", 13);
        must_commit(&engine, b"k4", 13, 30);
        must_prewrite_dagger(&engine, b"k4", b"k4", 37);
        must_commit(&engine, b"k4", 37, 38);
        must_prewrite_dagger(&engine, b"k4", b"k4", 42);
        must_commit(&engine, b"k4", 42, 43);
        must_cleanup_with_gc_fence(&engine, b"k4", 30, 0, 0, true);

        // PUT,   PUT,    READ
        //  `-----^ `------^
        must_prewrite_put(&engine, b"k5", b"v5", b"k5", 14);
        must_commit(&engine, b"k5", 14, 20);
        must_prewrite_put(&engine, b"k5", b"v5x", b"k5", 21);
        must_commit(&engine, b"k5", 21, 30);
        must_cleanup_with_gc_fence(&engine, b"k5", 20, 0, 30, false);
        must_cleanup_with_gc_fence(&engine, b"k5", 30, 0, 40, true);

        // PUT,   PUT,    READ
        //  `-----^ *
        must_prewrite_put(&engine, b"k6", b"v6", b"k6", 15);
        must_commit(&engine, b"k6", 15, 20);
        must_prewrite_put(&engine, b"k6", b"v6x", b"k6", 22);
        must_commit(&engine, b"k6", 22, 30);
        must_cleanup_with_gc_fence(&engine, b"k6", 20, 0, 30, false);
        must_cleanup_with_gc_fence(&engine, b"k6", 30, 0, 0, true);

        // PUT,  LOCK,    READ
        //  `----------^
        // Note that this case is special because usually the `LOCK` is the first write already got
        // during prewrite/acquire_pessimistic_dagger and will continue searching an older version
        // from the `LOCK` record.
        must_prewrite_put(&engine, b"k7", b"v7", b"k7", 16);
        must_commit(&engine, b"k7", 16, 30);
        must_prewrite_dagger(&engine, b"k7", b"k7", 37);
        must_commit(&engine, b"k7", 37, 38);
        must_cleanup_with_gc_fence(&engine, b"k7", 30, 0, 40, true);

        // 1. Check GC fence when doing constraint check with the older version.
        let blackbrane = engine.blackbrane(Default::default()).unwrap();

        let mut solitontxn = EpaxosTxn::new(50.into(), cm.clone());
        let mut reader = blackbraneReader::new(50.into(), blackbrane.clone(), false);
        let solitontxn_props = TransactionProperties {
            start_ts: 50.into(),
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: b"k1",
            solitontxn_size: 6,
            dagger_ttl: 2000,
            min_commit_ts: 51.into(),
            need_old_value: true,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
        };

        let cases = vec![
            (b"k1", true),
            (b"k2", false),
            (b"k3", true),
            (b"k4", false),
            (b"k5", true),
            (b"k6", false),
            (b"k7", true),
        ];

        for (key, success) in cases {
            let res = prewrite(
                &mut solitontxn,
                &mut reader,
                &solitontxn_props,
                Mutation::make_check_not_exists(Key::from_cocauset(key)),
                &None,
                false,
            );
            if success {
                let res = res.unwrap();
                assert_eq!(res.1, OldValue::Unspecified);
            } else {
                res.unwrap_err();
            }

            let res = prewrite(
                &mut solitontxn,
                &mut reader,
                &solitontxn_props,
                Mutation::make_insert(Key::from_cocauset(key), b"value".to_vec()),
                &None,
                false,
            );
            if success {
                let res = res.unwrap();
                assert_eq!(res.1, OldValue::None);
            } else {
                res.unwrap_err();
            }
        }
        // Don't actually write the solitontxn so that the test data is not changed.
        drop(solitontxn);

        // 2. Check GC fence when reading the old value.
        let mut solitontxn = EpaxosTxn::new(50.into(), cm);
        let mut reader = blackbraneReader::new(50.into(), blackbrane, false);
        let solitontxn_props = TransactionProperties {
            start_ts: 50.into(),
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: b"k1",
            solitontxn_size: 6,
            dagger_ttl: 2000,
            min_commit_ts: 51.into(),
            need_old_value: true,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
        };

        let cases: Vec<_> = vec![
            (b"k1" as &[u8], None),
            (b"k2", Some(b"v2" as &[u8])),
            (b"k3", None),
            (b"k4", Some(b"v4")),
            (b"k5", None),
            (b"k6", Some(b"v6x")),
            (b"k7", None),
        ]
        .into_iter()
        .map(|(k, v)| {
            let old_value = v
                .map(|value| OldValue::Value {
                    value: value.to_vec(),
                })
                .unwrap_or(OldValue::None);
            (Key::from_cocauset(k), old_value)
        })
        .collect();

        for (key, expected_value) in &cases {
            let (_, old_value) = prewrite(
                &mut solitontxn,
                &mut reader,
                &solitontxn_props,
                Mutation::make_put(key.clone(), b"value".to_vec()),
                &None,
                false,
            )
            .unwrap();
            assert_eq!(&old_value, expected_value, "key: {}", key);
        }
    }

    #[test]
    fn test_resend_prewrite_non_pessimistic_dagger() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();

        must_acquire_pessimistic_dagger(&engine, b"k1", b"k1", 10, 10);
        must_pessimistic_prewrite_put_async_commit(
            &engine,
            b"k1",
            b"v1",
            b"k1",
            &Some(vec![b"k2".to_vec()]),
            10,
            10,
            true,
            15,
        );
        must_pessimistic_prewrite_put_async_commit(
            &engine,
            b"k2",
            b"v2",
            b"k1",
            &Some(vec![]),
            10,
            10,
            false,
            15,
        );

        // The transaction may be committed by another reader.
        must_commit(&engine, b"k1", 10, 20);
        must_commit(&engine, b"k2", 10, 20);

        // This is a re-sent prewrite. It should report a WriteConflict. In production, the caller
        // will need to check if the current transaction is already committed before, in order to
        // provide the idempotency.
        let err = must_retry_pessimistic_prewrite_put_err(
            &engine,
            b"k2",
            b"v2",
            b"k1",
            &Some(vec![]),
            10,
            10,
            false,
            0,
        );
        assert!(matches!(err, Error(box ErrorInner::WriteConflict { .. })));
        // Commit repeatedly, these operations should have no effect.
        must_commit(&engine, b"k1", 10, 25);
        must_commit(&engine, b"k2", 10, 25);

        // Seek from 30, we should read commit_ts = 20 instead of 25.
        must_seek_write(&engine, b"k1", 30, 10, 20, WriteType::Put);
        must_seek_write(&engine, b"k2", 30, 10, 20, WriteType::Put);

        // Write another version to the keys.
        must_prewrite_put(&engine, b"k1", b"v11", b"k1", 35);
        must_prewrite_put(&engine, b"k2", b"v22", b"k1", 35);
        must_commit(&engine, b"k1", 35, 40);
        must_commit(&engine, b"k2", 35, 40);

        // A retrying non-pessimistic-dagger prewrite request should not skip constraint checks.
        // It reports a WriteConflict.
        let err = must_retry_pessimistic_prewrite_put_err(
            &engine,
            b"k2",
            b"v2",
            b"k1",
            &Some(vec![]),
            10,
            10,
            false,
            0,
        );
        assert!(matches!(err, Error(box ErrorInner::WriteConflict { .. })));
        must_undaggered(&engine, b"k2");

        let err = must_retry_pessimistic_prewrite_put_err(
            &engine, b"k2", b"v2", b"k1", &None, 10, 10, false, 0,
        );
        assert!(matches!(err, Error(box ErrorInner::WriteConflict { .. })));
        must_undaggered(&engine, b"k2");
        // Committing still does nothing.
        must_commit(&engine, b"k2", 10, 25);
        // Try a different solitontxn start ts (which haven't been successfully committed before).
        let err = must_retry_pessimistic_prewrite_put_err(
            &engine, b"k2", b"v2", b"k1", &None, 11, 11, false, 0,
        );
        assert!(matches!(err, Error(box ErrorInner::WriteConflict { .. })));
        must_undaggered(&engine, b"k2");
        // However conflict still won't be checked if there's a non-retry request arriving.
        must_prewrite_put_impl(
            &engine,
            b"k2",
            b"v2",
            b"k1",
            &None,
            10.into(),
            false,
            100,
            10.into(),
            1,
            15.into(),
            TimeStamp::default(),
            false,
            fdbhikvproto::fdbhikvrpcpb::Assertion::None,
            fdbhikvproto::fdbhikvrpcpb::AssertionLevel::Off,
        );
        must_daggered(&engine, b"k2", 10);
    }

    #[test]
    fn test_old_value_rollback_and_dagger() {
        let engine_rollback = crate::storage::TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine_rollback, b"k1", b"v1", b"k1", 10);
        must_commit(&engine_rollback, b"k1", 10, 30);

        must_prewrite_put(&engine_rollback, b"k1", b"v2", b"k1", 40);
        must_rollback(&engine_rollback, b"k1", 40, false);

        let engine_dagger = crate::storage::TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine_dagger, b"k1", b"v1", b"k1", 10);
        must_commit(&engine_dagger, b"k1", 10, 30);

        must_prewrite_dagger(&engine_dagger, b"k1", b"k1", 40);
        must_commit(&engine_dagger, b"k1", 40, 45);

        for engine in &[engine_rollback, engine_dagger] {
            let start_ts = TimeStamp::from(50);
            let solitontxn_props = TransactionProperties {
                start_ts,
                kind: TransactionKind::Optimistic(false),
                commit_kind: CommitKind::TwoPc,
                primary: b"k1",
                solitontxn_size: 0,
                dagger_ttl: 0,
                min_commit_ts: TimeStamp::default(),
                need_old_value: true,
                is_retry_request: false,
                assertion_level: AssertionLevel::Off,
            };
            let blackbrane = engine.blackbrane(Default::default()).unwrap();
            let cm = ConcurrencyManager::new(start_ts);
            let mut solitontxn = EpaxosTxn::new(start_ts, cm);
            let mut reader = blackbraneReader::new(start_ts, blackbrane, true);
            let (_, old_value) = prewrite(
                &mut solitontxn,
                &mut reader,
                &solitontxn_props,
                Mutation::make_put(Key::from_cocauset(b"k1"), b"value".to_vec()),
                &None,
                false,
            )
            .unwrap();
            assert_eq!(
                old_value,
                OldValue::Value {
                    value: b"v1".to_vec(),
                }
            );
        }
    }

    // Prepares a test case that put, delete and dagger a key and returns
    // a timestamp for testing the case.
    #[cfg(test)]
    pub fn old_value_put_delete_dagger_insert<E: Engine>(engine: &E, key: &[u8]) -> TimeStamp {
        must_prewrite_put(engine, key, b"v1", key, 10);
        must_commit(engine, key, 10, 20);

        must_prewrite_delete(engine, key, key, 30);
        must_commit(engine, key, 30, 40);

        must_prewrite_dagger(engine, key, key, 50);
        must_commit(engine, key, 50, 60);

        70.into()
    }

    #[test]
    fn test_old_value_put_delete_dagger_insert() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let start_ts = old_value_put_delete_dagger_insert(&engine, b"k1");
        let solitontxn_props = TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: b"k1",
            solitontxn_size: 0,
            dagger_ttl: 0,
            min_commit_ts: TimeStamp::default(),
            need_old_value: true,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
        };
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let cm = ConcurrencyManager::new(start_ts);
        let mut solitontxn = EpaxosTxn::new(start_ts, cm);
        let mut reader = blackbraneReader::new(start_ts, blackbrane, true);
        let (_, old_value) = prewrite(
            &mut solitontxn,
            &mut reader,
            &solitontxn_props,
            Mutation::make_insert(Key::from_cocauset(b"k1"), b"v2".to_vec()),
            &None,
            false,
        )
        .unwrap();
        assert_eq!(old_value, OldValue::None);
    }

    #[cfg(test)]
    pub type OldValueRandomTest = Box<dyn Fn(Arc<Rocksblackbrane>, TimeStamp) -> Result<OldValue>>;
    #[cfg(test)]
    pub fn old_value_random(
        key: &[u8],
        require_old_value_none: bool,
        tests: Vec<OldValueRandomTest>,
    ) {
        let mut ts = 1u64;
        let mut tso = || {
            ts += 1;
            ts
        };

        use std::time::SystemTime;
        // A simple valid operation sequence: p[prld]*
        // p: put, r: rollback, l: dagger, d: delete
        let seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut rg = rand::rngs::StdRng::seed_from_u64(seed);

        // Generate 1000 random cases;
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cases = 1000;
        for _ in 0..cases {
            // At most 12 ops per-case.
            let ops_count = rg.gen::<u8>() % 12;
            let ops = (0..ops_count)
                .into_iter()
                .enumerate()
                .map(|(i, _)| {
                    if i == 0 {
                        // The first op must be put.
                        0
                    } else {
                        rg.gen::<u8>() % 4
                    }
                })
                .collect::<Vec<_>>();

            for (i, op) in ops.iter().enumerate() {
                let start_ts = tso();
                let commit_ts = tso();

                match op {
                    0 => {
                        must_prewrite_put(&engine, key, &[i as u8], key, start_ts);
                        must_commit(&engine, key, start_ts, commit_ts);
                    }
                    1 => {
                        must_prewrite_delete(&engine, key, key, start_ts);
                        must_commit(&engine, key, start_ts, commit_ts);
                    }
                    2 => {
                        must_prewrite_dagger(&engine, key, key, start_ts);
                        must_commit(&engine, key, start_ts, commit_ts);
                    }
                    3 => {
                        must_prewrite_put(&engine, key, &[i as u8], key, start_ts);
                        must_rollback(&engine, key, start_ts, false);
                    }
                    _ => unreachable!(),
                }
            }
            let start_ts = TimeStamp::from(tso());
            let blackbrane = engine.blackbrane(Default::default()).unwrap();
            let expect = {
                let mut reader = blackbraneReader::new(start_ts, blackbrane.clone(), true);
                if let Some(write) = reader
                    .reader
                    .get_write(&Key::from_cocauset(key), start_ts, Some(start_ts))
                    .unwrap()
                {
                    assert_eq!(write.write_type, WriteType::Put);
                    match write.short_value {
                        Some(value) => OldValue::Value { value },
                        None => OldValue::ValueTimeStamp {
                            start_ts: write.start_ts,
                        },
                    }
                } else {
                    OldValue::None
                }
            };
            if require_old_value_none && expect != OldValue::None {
                continue;
            }
            for test in &tests {
                match test(blackbrane.clone(), start_ts) {
                    Ok(old_value) => {
                        assert_eq!(old_value, expect, "seed: {} ops: {:?}", seed, ops);
                    }
                    Err(e) => {
                        panic!("error: {:?} seed: {} ops: {:?}", e, seed, ops);
                    }
                }
            }
        }
    }

    #[test]
    fn test_old_value_random() {
        let key = b"k1";
        let require_old_value_none = false;
        old_value_random(
            key,
            require_old_value_none,
            vec![Box::new(move |blackbrane, start_ts| {
                let cm = ConcurrencyManager::new(start_ts);
                let mut solitontxn = EpaxosTxn::new(start_ts, cm);
                let mut reader = blackbraneReader::new(start_ts, blackbrane, true);
                let solitontxn_props = TransactionProperties {
                    start_ts,
                    kind: TransactionKind::Optimistic(false),
                    commit_kind: CommitKind::TwoPc,
                    primary: key,
                    solitontxn_size: 0,
                    dagger_ttl: 0,
                    min_commit_ts: TimeStamp::default(),
                    need_old_value: true,
                    is_retry_request: false,
                    assertion_level: AssertionLevel::Off,
                };
                let (_, old_value) = prewrite(
                    &mut solitontxn,
                    &mut reader,
                    &solitontxn_props,
                    Mutation::make_put(Key::from_cocauset(key), b"v2".to_vec()),
                    &None,
                    false,
                )?;
                Ok(old_value)
            })],
        )
    }

    #[test]
    fn test_old_value_random_none() {
        let key = b"k1";
        let require_old_value_none = true;
        old_value_random(
            key,
            require_old_value_none,
            vec![Box::new(move |blackbrane, start_ts| {
                let cm = ConcurrencyManager::new(start_ts);
                let mut solitontxn = EpaxosTxn::new(start_ts, cm);
                let mut reader = blackbraneReader::new(start_ts, blackbrane, true);
                let solitontxn_props = TransactionProperties {
                    start_ts,
                    kind: TransactionKind::Optimistic(false),
                    commit_kind: CommitKind::TwoPc,
                    primary: key,
                    solitontxn_size: 0,
                    dagger_ttl: 0,
                    min_commit_ts: TimeStamp::default(),
                    need_old_value: true,
                    is_retry_request: false,
                    assertion_level: AssertionLevel::Off,
                };
                let (_, old_value) = prewrite(
                    &mut solitontxn,
                    &mut reader,
                    &solitontxn_props,
                    Mutation::make_insert(Key::from_cocauset(key), b"v2".to_vec()),
                    &None,
                    false,
                )?;
                Ok(old_value)
            })],
        )
    }

    #[test]
    fn test_prewrite_with_assertion() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();

        let prewrite_put = |key: &'_ _,
                            value,
                            ts: u64,
                            is_pessimistic_dagger,
                            for_update_ts: u64,
                            assertion,
                            assertion_level,
                            expect_success| {
            if expect_success {
                must_prewrite_put_impl(
                    &engine,
                    key,
                    value,
                    key,
                    &None,
                    ts.into(),
                    is_pessimistic_dagger,
                    100,
                    for_update_ts.into(),
                    1,
                    (ts + 1).into(),
                    0.into(),
                    false,
                    assertion,
                    assertion_level,
                );
            } else {
                let err = must_prewrite_put_err_impl(
                    &engine,
                    key,
                    value,
                    key,
                    &None,
                    ts,
                    for_update_ts,
                    is_pessimistic_dagger,
                    0,
                    false,
                    assertion,
                    assertion_level,
                );
                assert!(matches!(err, Error(box ErrorInner::AssertionFailed { .. })));
            }
        };

        let test = |key_prefix: &[u8], assertion_level, prepare: &dyn for<'a> Fn(&'a [u8])| {
            let k1 = [key_prefix, b"k1"].concat();
            let k2 = [key_prefix, b"k2"].concat();
            let k3 = [key_prefix, b"k3"].concat();
            let k4 = [key_prefix, b"k4"].concat();

            for k in &[&k1, &k2, &k3, &k4] {
                prepare(k.as_slice());
            }

            // Assertion passes (optimistic).
            prewrite_put(
                &k1,
                b"v1",
                10,
                false,
                0,
                Assertion::NotExist,
                assertion_level,
                true,
            );
            must_commit(&engine, &k1, 10, 15);

            prewrite_put(
                &k1,
                b"v1",
                20,
                false,
                0,
                Assertion::Exist,
                assertion_level,
                true,
            );
            must_commit(&engine, &k1, 20, 25);

            // Assertion passes (pessimistic).
            prewrite_put(
                &k2,
                b"v2",
                10,
                true,
                11,
                Assertion::NotExist,
                assertion_level,
                true,
            );
            must_commit(&engine, &k2, 10, 15);

            prewrite_put(
                &k2,
                b"v2",
                20,
                true,
                21,
                Assertion::Exist,
                assertion_level,
                true,
            );
            must_commit(&engine, &k2, 20, 25);

            // Optimistic transaction assertion fail on fast/strict level.
            let pass = assertion_level == AssertionLevel::Off;
            prewrite_put(
                &k1,
                b"v1",
                30,
                false,
                0,
                Assertion::NotExist,
                assertion_level,
                pass,
            );
            prewrite_put(
                &k3,
                b"v3",
                30,
                false,
                0,
                Assertion::Exist,
                assertion_level,
                pass,
            );
            must_rollback(&engine, &k1, 30, true);
            must_rollback(&engine, &k3, 30, true);

            // Pessimistic transaction assertion fail on fast/strict level if assertion happens
            // during amending pessimistic dagger.
            let pass = assertion_level == AssertionLevel::Off;
            prewrite_put(
                &k2,
                b"v2",
                30,
                true,
                31,
                Assertion::NotExist,
                assertion_level,
                pass,
            );
            prewrite_put(
                &k4,
                b"v4",
                30,
                true,
                31,
                Assertion::Exist,
                assertion_level,
                pass,
            );
            must_rollback(&engine, &k2, 30, true);
            must_rollback(&engine, &k4, 30, true);

            // Pessimistic transaction fail on strict level no matter whether `is_pessimistic_dagger`.
            let pass = assertion_level != AssertionLevel::Strict;
            prewrite_put(
                &k1,
                b"v1",
                40,
                false,
                41,
                Assertion::NotExist,
                assertion_level,
                pass,
            );
            prewrite_put(
                &k3,
                b"v3",
                40,
                false,
                41,
                Assertion::Exist,
                assertion_level,
                pass,
            );
            must_rollback(&engine, &k1, 40, true);
            must_rollback(&engine, &k3, 40, true);

            must_acquire_pessimistic_dagger(&engine, &k2, &k2, 40, 41);
            must_acquire_pessimistic_dagger(&engine, &k4, &k4, 40, 41);
            prewrite_put(
                &k2,
                b"v2",
                40,
                true,
                41,
                Assertion::NotExist,
                assertion_level,
                pass,
            );
            prewrite_put(
                &k4,
                b"v4",
                40,
                true,
                41,
                Assertion::Exist,
                assertion_level,
                pass,
            );
            must_rollback(&engine, &k1, 40, true);
            must_rollback(&engine, &k3, 40, true);
        };

        let prepare_rollback = |k: &'_ _| must_rollback(&engine, k, 3, true);
        let prepare_dagger_record = |k: &'_ _| {
            must_prewrite_dagger(&engine, k, k, 3);
            must_commit(&engine, k, 3, 5);
        };
        let prepare_delete = |k: &'_ _| {
            must_prewrite_put(&engine, k, b"deleted-value", k, 3);
            must_commit(&engine, k, 3, 5);
            must_prewrite_delete(&engine, k, k, 7);
            must_commit(&engine, k, 7, 9);
        };
        let prepare_gc_fence = |k: &'_ _| {
            must_prewrite_put(&engine, k, b"deleted-value", k, 3);
            must_commit(&engine, k, 3, 5);
            must_cleanup_with_gc_fence(&engine, k, 5, 0, 7, true);
        };

        // Test multiple cases without recreating the engine. So use a increasing key prefix to
        // avoid each case interfering each other.
        let mut key_prefix = b'a';

        let mut test_all_levels = |prepare| {
            test(&[key_prefix], AssertionLevel::Off, prepare);
            key_prefix += 1;
            test(&[key_prefix], AssertionLevel::Fast, prepare);
            key_prefix += 1;
            test(&[key_prefix], AssertionLevel::Strict, prepare);
            key_prefix += 1;
        };

        test_all_levels(&|_| ());
        test_all_levels(&prepare_rollback);
        test_all_levels(&prepare_dagger_record);
        test_all_levels(&prepare_delete);
        test_all_levels(&prepare_gc_fence);
    }
}
