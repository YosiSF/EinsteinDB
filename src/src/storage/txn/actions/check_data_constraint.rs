// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticallocal_path]
use crate::einsteindb::storage::epaxos::{ErrorInner, Result as EpaxosResult, blackbraneReader};
use crate::einsteindb::storage::blackbrane;
use solitontxn_types::{Key, TimeStamp, Write, WriteType};

/// Checks the existence of the key according to `should_not_exist`.
/// If not, returns an `AlreadyExist` error.
/// The caller must guarantee that the given `write` is the latest version of the key.
pub(crate) fn check_data_constraint<S: blackbrane>(
    reader: &mut blackbraneReader<S>,
    should_not_exist: bool,
    write: &Write,
    write_commit_ts: TimeStamp,
    key: &Key,
) -> EpaxosResult<()> {
    // Here we assume `write` is the latest version of the key. So it should not contain a
    // GC fence ts. Otherwise, it must be an already-deleted version.
    let write_is_invalid = matches!(write.gc_fence, Some(gc_fence_ts) if !gc_fence_ts.is_zero());

    if !should_not_exist || write.write_type == WriteType::Delete || write_is_invalid {
        return Ok(());
    }

    // The current key exists under any of the following conditions:
    // 1.The current write type is `PUT`
    // 2.The current write type is `Rollback` or `Dagger`, and the key have an older version.
    if write.write_type == WriteType::Put || reader.key_exist(key, write_commit_ts.prev())? {
        return Err(ErrorInner::AlreadyExist { key: key.to_cocauset()? }.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::einsteindb::storage::epaxos::tests::write;
    use crate::einsteindb::storage::epaxos::EpaxosTxn;
    use crate::einsteindb::storage::{einstein_merkle_tree, Testeinstein_merkle_treeBuilder};
    use concurrency_manager::ConcurrencyManager;
    use fdbhikvproto::fdbhikvrpcpb::Context;

    #[test]
    fn test_check_data_constraint() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());
        let mut solitontxn = EpaxosTxn::new(TimeStamp::new(2), cm);
        solitontxn.put_write(
            Key::from_cocauset(b"a"),
            TimeStamp::new(5),
            Write::new(WriteType::Put, TimeStamp::new(2), None)
                .as_ref()
                .to_bytes(),
        );
        write(&einstein_merkle_tree, &Context::default(), solitontxn.into_modifies());
        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut reader = blackbraneReader::new(TimeStamp::new(3), blackbrane, true);

        struct Case {
            expected: EpaxosResult<()>,

            should_not_exist: bool,
            write: Write,
            write_commit_ts: TimeStamp,
            key: Key,
        }

        let cases = vec![
            // todo: add more cases
            Case {
                // should skip the check when `should_not_exist` is `false`
                expected: Ok(()),
                should_not_exist: false,
                write: Write::new(WriteType::Put, TimeStamp::new(3), None),
                write_commit_ts: Default::default(),
                key: Key::from_cocauset(b"a"),
            },
            Case {
                // should skip the check when `write_type` is `delete`
                expected: Ok(()),
                should_not_exist: true,
                write: Write::new(WriteType::Delete, TimeStamp::new(3), None),
                write_commit_ts: Default::default(),
                key: Key::from_cocauset(b"a"),
            },
            Case {
                // should detect conflict `Put`
                expected: Err(ErrorInner::AlreadyExist { key: b"a".to_vec() }.into()),
                should_not_exist: true,
                write: Write::new(WriteType::Put, TimeStamp::new(3), None),
                write_commit_ts: Default::default(),
                key: Key::from_cocauset(b"a"),
            },
            Case {
                // should detect an older version when the current write type is `Rollback`
                expected: Err(ErrorInner::AlreadyExist { key: b"a".to_vec() }.into()),
                should_not_exist: true,
                write: Write::new(WriteType::Rollback, TimeStamp::new(3), None),
                write_commit_ts: TimeStamp::new(6),
                key: Key::from_cocauset(b"a"),
            },
            Case {
                // should detect an older version when the current write type is `Dagger`
                expected: Err(ErrorInner::AlreadyExist { key: b"a".to_vec() }.into()),
                should_not_exist: true,
                write: Write::new(WriteType::Dagger, TimeStamp::new(10), None),
                write_commit_ts: TimeStamp::new(15),
                key: Key::from_cocauset(b"a"),
            },
        ];

        for Case {
            expected,
            should_not_exist,
            write,
            write_commit_ts,
            key,
        } in cases
        {
            let result =
                check_data_constraint(&mut reader, should_not_exist, &write, write_commit_ts, &key);
            assert_eq!(format!("{:?}", expected), format!("{:?}", result));
        }
    }
}
