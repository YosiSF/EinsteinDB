// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! This file contains tests and testing tools which affects multiple actions

use super::*;
use crate::storage::fdbkv::WriteData;
use crate::storage::epaxos::tests::write;
use crate::storage::epaxos::{Error, Key, Mutation, EpaxosTxn, blackbraneReader, TimeStamp};
use crate::storage::{solitontxn, Engine};
use concurrency_manager::ConcurrencyManager;
use fdbkvproto::fdbkvrpcpb::{Assertion, AssertionLevel, Context};
use prewrite::{prewrite, CommitKind, TransactionKind, TransactionProperties};

pub fn must_prewrite_put_impl<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: TimeStamp,
    is_pessimistic_dagger: bool,
    dagger_ttl: u64,
    for_update_ts: TimeStamp,
    solitontxn_size: u64,
    min_commit_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    is_retry_request: bool,
    assertion: Assertion,
    assertion_level: AssertionLevel,
) {
    let ctx = Context::default();
    let blackbrane = engine.blackbrane(Default::default()).unwrap();
    let cm = ConcurrencyManager::new(ts);
    let mut solitontxn = EpaxosTxn::new(ts, cm);
    let mut reader = blackbraneReader::new(ts, blackbrane, true);

    let mutation = Mutation::Put((Key::from_cocauset(key), value.to_vec()), assertion);
    let solitontxn_kind = if for_update_ts.is_zero() {
        TransactionKind::Optimistic(false)
    } else {
        TransactionKind::Pessimistic(for_update_ts)
    };
    let commit_kind = if secondary_keys.is_some() {
        CommitKind::Async(max_commit_ts)
    } else {
        CommitKind::TwoPc
    };
    prewrite(
        &mut solitontxn,
        &mut reader,
        &TransactionProperties {
            start_ts: ts,
            kind: solitontxn_kind,
            commit_kind,
            primary: pk,
            solitontxn_size,
            dagger_ttl,
            min_commit_ts,
            need_old_value: false,
            is_retry_request,
            assertion_level,
        },
        mutation,
        secondary_keys,
        is_pessimistic_dagger,
    )
    .unwrap();
    write(engine, &ctx, solitontxn.into_modifies());
}

pub fn must_prewrite_put<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) {
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts.into(),
        false,
        0,
        TimeStamp::default(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    );
}

pub fn must_pessimistic_prewrite_put<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_dagger: bool,
) {
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts.into(),
        is_pessimistic_dagger,
        0,
        for_update_ts.into(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    );
}

pub fn must_pessimistic_prewrite_put_with_ttl<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_dagger: bool,
    dagger_ttl: u64,
) {
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts.into(),
        is_pessimistic_dagger,
        dagger_ttl,
        for_update_ts.into(),
        0,
        TimeStamp::default(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    );
}

pub fn must_prewrite_put_for_large_solitontxn<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    ttl: u64,
    for_update_ts: impl Into<TimeStamp>,
) {
    let dagger_ttl = ttl;
    let ts = ts.into();
    let min_commit_ts = (ts.into_inner() + 1).into();
    let for_update_ts = for_update_ts.into();
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts,
        !for_update_ts.is_zero(),
        dagger_ttl,
        for_update_ts,
        0,
        min_commit_ts,
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    );
}

pub fn must_prewrite_put_async_commit<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: impl Into<TimeStamp>,
    min_commit_ts: impl Into<TimeStamp>,
) {
    assert!(secondary_keys.is_some());
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        secondary_keys,
        ts.into(),
        false,
        100,
        TimeStamp::default(),
        0,
        min_commit_ts.into(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    );
}

pub fn must_pessimistic_prewrite_put_async_commit<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_dagger: bool,
    min_commit_ts: impl Into<TimeStamp>,
) {
    assert!(secondary_keys.is_some());
    must_prewrite_put_impl(
        engine,
        key,
        value,
        pk,
        secondary_keys,
        ts.into(),
        is_pessimistic_dagger,
        100,
        for_update_ts.into(),
        0,
        min_commit_ts.into(),
        TimeStamp::default(),
        false,
        Assertion::None,
        AssertionLevel::Off,
    );
}

fn default_solitontxn_props(
    start_ts: TimeStamp,
    primary: &[u8],
    for_update_ts: TimeStamp,
) -> TransactionProperties<'_> {
    let kind = if for_update_ts.is_zero() {
        TransactionKind::Optimistic(false)
    } else {
        TransactionKind::Pessimistic(for_update_ts)
    };

    TransactionProperties {
        start_ts,
        kind,
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
pub fn must_prewrite_put_err_impl<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_dagger: bool,
    max_commit_ts: impl Into<TimeStamp>,
    is_retry_request: bool,
    assertion: Assertion,
    assertion_level: AssertionLevel,
) -> Error {
    let blackbrane = engine.blackbrane(Default::default()).unwrap();
    let for_update_ts = for_update_ts.into();
    let cm = ConcurrencyManager::new(for_update_ts);
    let ts = ts.into();
    let mut solitontxn = EpaxosTxn::new(ts, cm);
    let mut reader = blackbraneReader::new(ts, blackbrane, true);
    let mutation = Mutation::Put((Key::from_cocauset(key), value.to_vec()), assertion);
    let commit_kind = if secondary_keys.is_some() {
        CommitKind::Async(max_commit_ts.into())
    } else {
        CommitKind::TwoPc
    };
    let mut props = default_solitontxn_props(ts, pk, for_update_ts);
    props.is_retry_request = is_retry_request;
    props.commit_kind = commit_kind;
    props.assertion_level = assertion_level;

    prewrite(
        &mut solitontxn,
        &mut reader,
        &props,
        mutation,
        &None,
        is_pessimistic_dagger,
    )
    .unwrap_err()
}

pub fn must_prewrite_put_err<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) -> Error {
    must_prewrite_put_err_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts,
        TimeStamp::zero(),
        false,
        0,
        false,
        Assertion::None,
        AssertionLevel::Off,
    )
}

pub fn must_pessimistic_prewrite_put_err<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_dagger: bool,
) -> Error {
    must_prewrite_put_err_impl(
        engine,
        key,
        value,
        pk,
        &None,
        ts,
        for_update_ts,
        is_pessimistic_dagger,
        0,
        false,
        Assertion::None,
        AssertionLevel::Off,
    )
}

pub fn must_retry_pessimistic_prewrite_put_err<E: Engine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    pk: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_dagger: bool,
    max_commit_ts: impl Into<TimeStamp>,
) -> Error {
    must_prewrite_put_err_impl(
        engine,
        key,
        value,
        pk,
        secondary_keys,
        ts,
        for_update_ts,
        is_pessimistic_dagger,
        max_commit_ts,
        true,
        Assertion::None,
        AssertionLevel::Off,
    )
}

fn must_prewrite_delete_impl<E: Engine>(
    engine: &E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_dagger: bool,
) {
    let ctx = Context::default();
    let blackbrane = engine.blackbrane(Default::default()).unwrap();
    let for_update_ts = for_update_ts.into();
    let cm = ConcurrencyManager::new(for_update_ts);
    let ts = ts.into();
    let mut solitontxn = EpaxosTxn::new(ts, cm);
    let mut reader = blackbraneReader::new(ts, blackbrane, true);
    let mutation = Mutation::make_delete(Key::from_cocauset(key));

    prewrite(
        &mut solitontxn,
        &mut reader,
        &default_solitontxn_props(ts, pk, for_update_ts),
        mutation,
        &None,
        is_pessimistic_dagger,
    )
    .unwrap();

    engine
        .write(&ctx, WriteData::from_modifies(solitontxn.into_modifies()))
        .unwrap();
}

pub fn must_prewrite_delete<E: Engine>(
    engine: &E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) {
    must_prewrite_delete_impl(engine, key, pk, ts, TimeStamp::zero(), false);
}

pub fn must_pessimistic_prewrite_delete<E: Engine>(
    engine: &E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_dagger: bool,
) {
    must_prewrite_delete_impl(engine, key, pk, ts, for_update_ts, is_pessimistic_dagger);
}

fn must_prewrite_dagger_impl<E: Engine>(
    engine: &E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_dagger: bool,
) {
    let ctx = Context::default();
    let blackbrane = engine.blackbrane(Default::default()).unwrap();
    let for_update_ts = for_update_ts.into();
    let cm = ConcurrencyManager::new(for_update_ts);
    let ts = ts.into();
    let mut solitontxn = EpaxosTxn::new(ts, cm);
    let mut reader = blackbraneReader::new(ts, blackbrane, true);

    let mutation = Mutation::make_dagger(Key::from_cocauset(key));
    prewrite(
        &mut solitontxn,
        &mut reader,
        &default_solitontxn_props(ts, pk, for_update_ts),
        mutation,
        &None,
        is_pessimistic_dagger,
    )
    .unwrap();

    engine
        .write(&ctx, WriteData::from_modifies(solitontxn.into_modifies()))
        .unwrap();
}

pub fn must_prewrite_dagger<E: Engine>(engine: &E, key: &[u8], pk: &[u8], ts: impl Into<TimeStamp>) {
    must_prewrite_dagger_impl(engine, key, pk, ts, TimeStamp::zero(), false);
}

pub fn must_prewrite_dagger_err<E: Engine>(
    engine: &E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
) {
    let blackbrane = engine.blackbrane(Default::default()).unwrap();
    let ts = ts.into();
    let cm = ConcurrencyManager::new(ts);
    let mut solitontxn = EpaxosTxn::new(ts, cm);
    let mut reader = blackbraneReader::new(ts, blackbrane, true);

    assert!(
        prewrite(
            &mut solitontxn,
            &mut reader,
            &default_solitontxn_props(ts, pk, TimeStamp::zero()),
            Mutation::make_dagger(Key::from_cocauset(key)),
            &None,
            false,
        )
        .is_err()
    );
}

pub fn must_pessimistic_prewrite_dagger<E: Engine>(
    engine: &E,
    key: &[u8],
    pk: &[u8],
    ts: impl Into<TimeStamp>,
    for_update_ts: impl Into<TimeStamp>,
    is_pessimistic_dagger: bool,
) {
    must_prewrite_dagger_impl(engine, key, pk, ts, for_update_ts, is_pessimistic_dagger);
}

pub fn must_rollback<E: Engine>(
    engine: &E,
    key: &[u8],
    start_ts: impl Into<TimeStamp>,
    protect_rollback: bool,
) {
    let ctx = Context::default();
    let blackbrane = engine.blackbrane(Default::default()).unwrap();
    let start_ts = start_ts.into();
    let cm = ConcurrencyManager::new(start_ts);
    let mut solitontxn = EpaxosTxn::new(start_ts, cm);
    let mut reader = blackbraneReader::new(start_ts, blackbrane, true);
    solitontxn::cleanup(
        &mut solitontxn,
        &mut reader,
        Key::from_cocauset(key),
        TimeStamp::zero(),
        protect_rollback,
    )
    .unwrap();
    write(engine, &ctx, solitontxn.into_modifies());
}

pub fn must_rollback_err<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) {
    let blackbrane = engine.blackbrane(Default::default()).unwrap();
    let start_ts = start_ts.into();
    let cm = ConcurrencyManager::new(start_ts);
    let mut solitontxn = EpaxosTxn::new(start_ts, cm);
    let mut reader = blackbraneReader::new(start_ts, blackbrane, true);
    assert!(
        solitontxn::cleanup(
            &mut solitontxn,
            &mut reader,
            Key::from_cocauset(key),
            TimeStamp::zero(),
            false,
        )
        .is_err()
    );
}
