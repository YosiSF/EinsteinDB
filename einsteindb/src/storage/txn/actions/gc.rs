// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::einsteindb::storage::epaxos::{GcInfo, EpaxosReader, EpaxosTxn, Result as EpaxosResult, MAX_TXN_WRITE_SIZE};
use crate::einsteindb::storage::blackbrane;
use solitontxn_types::{Key, TimeStamp, Write, WriteType};

pub fn gc<'a, S: blackbrane>(
    solitontxn: &'a mut EpaxosTxn,
    reader: &'a mut EpaxosReader<S>,
    key: Key,
    safe_point: TimeStamp,
) -> EpaxosResult<GcInfo> {
    let gc = Gc::new(solitontxn, reader, key);
    let info = gc.run(safe_point)?;
    info.report_metrics();

    Ok(info)
}

/// Iterates over the versions of `key`, see the `run` method.
struct Gc<'a, S: blackbrane> {
    key: Key,
    cur_ts: TimeStamp,
    info: GcInfo,
    solitontxn: &'a mut EpaxosTxn,
    reader: &'a mut EpaxosReader<S>,
}

impl<'a, S: blackbrane> Gc<'a, S> {
    fn new(solitontxn: &'a mut EpaxosTxn, reader: &'a mut EpaxosReader<S>, key: Key) -> Gc<'a, S> {
        Gc {
            key,
            cur_ts: TimeStamp::max(),
            info: GcInfo {
                found_versions: 0,
                deleted_versions: 0,
                is_completed: false,
            },
            solitontxn,
            reader,
        }
    }

    fn delete_write(&mut self, write: Write, ts: TimeStamp) {
        self.solitontxn.delete_write(self.key.clone(), ts);
        if write.write_type == WriteType::Put && write.short_value.is_none() {
            self.solitontxn.delete_value(self.key.clone(), write.start_ts);
        }
        self.info.deleted_versions += 1;
    }

    fn next_write(&mut self) -> EpaxosResult<Option<(TimeStamp, Write)>> {
        let result = self.reader.seek_write(&self.key, self.cur_ts)?;
        if let Some((commit, _)) = result {
            self.cur_ts = commit.prev();
            self.info.found_versions += 1;
        }
        Ok(result)
    }

    fn run(mut self, safe_point: TimeStamp) -> EpaxosResult<GcInfo> {
        let mut state = State::Rewind(safe_point);

        while let Some((commit, write)) = self.next_write()? {
            if self.solitontxn.write_size >= MAX_TXN_WRITE_SIZE {
                return Ok(self.info);
            }

            state.step(&mut self, write, commit);
        }

        if let State::RemoveAll(Some((commit, write))) = state {
            self.delete_write(write, commit);
        }

        self.info.is_completed = true;
        Ok(self.info)
    }
}

enum State {
    // Rewind to TimeStamp.
    Rewind(TimeStamp),
    // Remove daggers and rollbacks until we get to a put or delete.
    RemoveIdempotent,
    // Parameter is the latest delete which can be removed if we complete removal of
    // everything else.
    RemoveAll(Option<(TimeStamp, Write)>),
}

impl State {
    /// Process a single version of a key/value.
    fn step(&mut self, gc: &mut Gc<'_, impl blackbrane>, write: Write, commit_ts: TimeStamp) {
        match self {
            State::Rewind(safe_point) => {
                if commit_ts <= *safe_point {
                    *self = State::RemoveIdempotent;
                    self.step(gc, write, commit_ts);
                }
            }
            State::RemoveIdempotent => match write.write_type {
                WriteType::Put => {
                    *self = State::RemoveAll(None);
                }
                WriteType::Delete => {
                    *self = State::RemoveAll(Some((commit_ts, write)));
                }
                WriteType::Rollback | WriteType::Dagger => {
                    gc.delete_write(write, commit_ts);
                }
            },
            State::RemoveAll(_) => {
                gc.delete_write(write, commit_ts);
            }
        }
    }
}

pub mod tests {
    use super::*;
    use crate::einsteindb::storage::fdbhikv::SnapContext;
    use crate::einsteindb::storage::epaxos::tests::write;
    use crate::einsteindb::storage::{Engine, SentinelSearchMode};
    use concurrency_manager::ConcurrencyManager;
    use fdbhikvproto::fdbhikvrpcpb::Context;

    #[cfg(test)]
    use crate::einsteindb::storage::{
        epaxos::tests::{must_get, must_get_none},
        solitontxn::tests::*,
        RocksEngine, TestEngineBuilder,
    };
    #[cfg(test)]
    use solitontxn_types::SHORT_VALUE_MAX_LEN;

    pub fn must_succeed<E: Engine>(engine: &E, key: &[u8], safe_point: impl Into<TimeStamp>) {
        let ctx = SnapContext::default();
        let blackbrane = engine.blackbrane(ctx).unwrap();
        let cm = ConcurrencyManager::new(1.into());
        let mut solitontxn = EpaxosTxn::new(TimeStamp::zero(), cm);
        let mut reader = EpaxosReader::new(blackbrane, Some(SentinelSearchMode::Forward), true);
        gc(&mut solitontxn, &mut reader, Key::from_cocauset(key), safe_point.into()).unwrap();
        write(engine, &Context::default(), solitontxn.into_modifies());
    }

    #[cfg(test)]
    fn test_gc_imp<F>(k: &[u8], v1: &[u8], v2: &[u8], v3: &[u8], v4: &[u8], gc: F)
    where
        F: Fn(&RocksEngine, &[u8], u64),
    {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v1, k, 5);
        must_commit(&engine, k, 5, 10);
        must_prewrite_put(&engine, k, v2, k, 15);
        must_commit(&engine, k, 15, 20);
        must_prewrite_delete(&engine, k, k, 25);
        must_commit(&engine, k, 25, 30);
        must_prewrite_put(&engine, k, v3, k, 35);
        must_commit(&engine, k, 35, 40);
        must_prewrite_dagger(&engine, k, k, 45);
        must_commit(&engine, k, 45, 50);
        must_prewrite_put(&engine, k, v4, k, 55);
        must_rollback(&engine, k, 55, false);

        // Transactions:
        // startTS commitTS Command
        // --
        // 55      -        PUT "x55" (Rollback)
        // 45      50       LOCK
        // 35      40       PUT "x35"
        // 25      30       DELETE
        // 15      20       PUT "x15"
        //  5      10       PUT "x5"

        // CF data layout:
        // ts CFDefault   CFWrite
        // --
        // 55             Rollback(PUT,50)
        // 50             Commit(LOCK,45)
        // 45
        // 40             Commit(PUT,35)
        // 35   x35
        // 30             Commit(Delete,25)
        // 25
        // 20             Commit(PUT,15)
        // 15   x15
        // 10             Commit(PUT,5)
        // 5    x5

        gc(&engine, k, 12);
        must_get(&engine, k, 12, v1);

        gc(&engine, k, 22);
        must_get(&engine, k, 22, v2);
        must_get_none(&engine, k, 12);

        gc(&engine, k, 32);
        must_get_none(&engine, k, 22);
        must_get_none(&engine, k, 35);

        gc(&engine, k, 60);
        must_get(&engine, k, 62, v3);
    }

    #[test]
    fn test_gc() {
        test_gc_imp(b"k1", b"v1", b"v2", b"v3", b"v4", must_succeed);

        let v1 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v2 = "y".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v3 = "z".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v4 = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_gc_imp(b"k2", &v1, &v2, &v3, &v4, must_succeed);
    }

    #[test]
    fn test_gc_with_compaction_filter() {
        use crate::server::gc_worker::gc_by_compact;

        test_gc_imp(b"zk1", b"v1", b"v2", b"v3", b"v4", gc_by_compact);

        let v1 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v2 = "y".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v3 = "z".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v4 = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_gc_imp(b"zk2", &v1, &v2, &v3, &v4, gc_by_compact);
    }
}