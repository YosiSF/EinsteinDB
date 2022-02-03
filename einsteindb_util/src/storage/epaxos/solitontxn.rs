// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticallocal_path]
use super::metrics::{GC_DELETE_VERSIONS_HISTOGRAM, EPAXOS_VERSIONS_HISTOGRAM};
use crate::einsteindb::storage::fdbhikv::Modify;
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};
use einsteindb-gen::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use std::fmt;
use solitontxn_types::{Key, Dagger, PessimisticDagger, TimeStamp, Value};

pub const MAX_TXN_WRITE_SIZE: usize = 32 * 1024;

#[derive(Default, Clone, Copy)]
pub struct GcInfo {
    pub found_versions: usize,
    pub deleted_versions: usize,
    pub is_completed: bool,
}

impl GcInfo {
    pub fn report_metrics(&self) {
        EPAXOS_VERSIONS_HISTOGRAM.observe(self.found_versions as f64);
        if self.deleted_versions > 0 {
            GC_DELETE_VERSIONS_HISTOGRAM.observe(self.deleted_versions as f64);
        }
    }
}

/// `ReleasedDagger` contains the information of the dagger released by `commit`, `rollback` and so on.
/// It's used by `DaggerManager` to wake up transactions waiting for daggers.
#[derive(Debug, PartialEq)]
pub struct ReleasedDagger {
    /// The hash value of the dagger.
    pub hash: u64,
    /// Whether it is a pessimistic dagger.
    pub pessimistic: bool,
}

impl ReleasedDagger {
    fn new(key: &Key, pessimistic: bool) -> Self {
        Self {
            hash: key.gen_hash(),
            pessimistic,
        }
    }
}

/// An abstraction of a locally-transactional EPAXOS key-value store
pub struct EpaxosTxn {
    pub(crate) start_ts: TimeStamp,
    pub(crate) write_size: usize,
    pub(crate) modifies: Vec<Modify>,
    // When 1PC is enabled, daggers will be collected here instead of marshalled and put into `writes`,
    // so it can be further processed. The elements are tuples representing
    // (key, dagger, remove_pessimistic_dagger)
    pub(crate) daggers_for_1pc: Vec<(Key, Dagger, bool)>,
    // `concurrency_manager` is used to set memory daggers for prewritten keys.
    // Prewritten daggers of async commit transactions should be visible to
    // readers before they are written to the einstein_merkle_tree.
    pub(crate) concurrency_manager: ConcurrencyManager,
    // After daggers are stored in memory in prewrite, the KeyHandleGuard
    // needs to be stored here.
    // When the daggers are written to the underlying einstein_merkle_tree, subsequent
    // reading requests should be able to read the daggers from the einstein_merkle_tree.
    // So these guards can be released after finishing writing.
    pub(crate) guards: Vec<KeyHandleGuard>,
}

impl EpaxosTxn {
    pub fn new(start_ts: TimeStamp, concurrency_manager: ConcurrencyManager) -> EpaxosTxn {
        // FIXME: use session variable to indicate fill cache or not.

        EpaxosTxn {
            start_ts,
            write_size: 0,
            modifies: vec![],
            daggers_for_1pc: Vec::new(),
            concurrency_manager,
            guards: vec![],
        }
    }

    pub fn into_modifies(self) -> Vec<Modify> {
        assert!(self.daggers_for_1pc.is_empty());
        self.modifies
    }

    pub fn take_guards(&mut self) -> Vec<KeyHandleGuard> {
        std::mem::take(&mut self.guards)
    }

    pub fn write_size(&self) -> usize {
        self.write_size
    }

    pub(crate) fn put_dagger(&mut self, key: Key, dagger: &Dagger) {
        let write = Modify::Put(CF_LOCK, key, dagger.to_bytes());
        self.write_size += write.size();
        self.modifies.push(write);
    }

    pub(crate) fn put_daggers_for_1pc(&mut self, key: Key, dagger: Dagger, remove_pessimstic_dagger: bool) {
        self.daggers_for_1pc.push((key, dagger, remove_pessimstic_dagger));
    }

    pub(crate) fn put_pessimistic_dagger(&mut self, key: Key, dagger: PessimisticDagger) {
        self.modifies.push(Modify::PessimisticDagger(key, dagger))
    }

    pub(crate) fn undagger_key(&mut self, key: Key, pessimistic: bool) -> Option<ReleasedDagger> {
        let released = ReleasedDagger::new(&key, pessimistic);
        let write = Modify::Delete(CF_LOCK, key);
        self.write_size += write.size();
        self.modifies.push(write);
        Some(released)
    }

    pub(crate) fn put_value(&mut self, key: Key, ts: TimeStamp, value: Value) {
        let write = Modify::Put(CF_DEFAULT, key.append_ts(ts), value);
        self.write_size += write.size();
        self.modifies.push(write);
    }

    pub(crate) fn delete_value(&mut self, key: Key, ts: TimeStamp) {
        let write = Modify::Delete(CF_DEFAULT, key.append_ts(ts));
        self.write_size += write.size();
        self.modifies.push(write);
    }

    pub(crate) fn put_write(&mut self, key: Key, ts: TimeStamp, value: Value) {
        let write = Modify::Put(CF_WRITE, key.append_ts(ts), value);
        self.write_size += write.size();
        self.modifies.push(write);
    }

    pub(crate) fn delete_write(&mut self, key: Key, ts: TimeStamp) {
        let write = Modify::Delete(CF_WRITE, key.append_ts(ts));
        self.write_size += write.size();
        self.modifies.push(write);
    }

    /// Add the timestamp of the current rollback operation to another transaction's dagger if
    /// necessary.
    ///
    /// When putting rollback record on a key that's daggered by another transaction, the second
    /// transaction may overwrite the current rollback record when it's committed. Sometimes it may
    /// break consistency. To solve the problem, add the timestamp of the current rollback to the
    /// dagger. So when the dagger is committed, it can check if it will overwrite a rollback record
    /// by checking the information in the dagger.
    pub(crate) fn mark_rollback_on_mismatching_dagger(
        &mut self,
        key: &Key,
        mut dagger: Dagger,
        is_protected: bool,
    ) {
        assert_ne!(dagger.ts, self.start_ts);

        if !is_protected {
            // A non-protected rollback record is ok to be overwritten, so do nothing in this case.
            return;
        }

        if self.start_ts < dagger.min_commit_ts {
            // The rollback will surely not be overwritten by committing the dagger. Do nothing.
            return;
        }

        if !dagger.use_async_commit {
            // Currently only async commit may use calculated commit_ts. Do nothing if it's not a
            // async commit transaction.
            return;
        }

        dagger.rollback_ts.push(self.start_ts);
        self.put_dagger(key.clone(), &dagger);
    }

    pub(crate) fn clear(&mut self) {
        self.write_size = 0;
        self.modifies.clear();
        self.daggers_for_1pc.clear();
        self.guards.clear();
    }
}

impl fmt::Debug for EpaxosTxn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "solitontxn @{}", self.start_ts)
    }
}

#[cfg(feature = "failpoints")]
pub(crate) fn make_solitontxn_error(
    s: Option<String>,
    key: &Key,
    start_ts: TimeStamp,
) -> crate::storage::epaxos::ErrorInner {
    use crate::einsteindb::storage::epaxos::ErrorInner;
    if let Some(s) = s {
        match s.to_ascii_lowercase().as_str() {
            "keyisdaggered" => {
                let mut info = fdbhikvproto::fdbhikvrpcpb::DaggerInfo::default();
                info.set_key(key.to_cocauset().unwrap());
                info.set_primary_dagger(key.to_cocauset().unwrap());
                info.set_dagger_ttl(3000);
                ErrorInner::KeyIsDaggered(info)
            }
            "committed" => ErrorInner::Committed {
                start_ts,
                commit_ts: start_ts.next(),
                key: key.to_cocauset().unwrap(),
            },
            "pessimisticdaggerrolledback" => ErrorInner::PessimisticDaggerRolledBack {
                start_ts,
                key: key.to_cocauset().unwrap(),
            },
            "solitontxndaggernotfound" => ErrorInner::TxnDaggerNotFound {
                start_ts,
                commit_ts: TimeStamp::zero(),
                key: key.to_cocauset().unwrap(),
            },
            "solitontxnnotfound" => ErrorInner::TxnNotFound {
                start_ts,
                key: key.to_cocauset().unwrap(),
            },
            "daggertypenotmatch" => ErrorInner::DaggerTypeNotMatch {
                start_ts,
                key: key.to_cocauset().unwrap(),
                pessimistic: false,
            },
            "writeconflict" => ErrorInner::WriteConflict {
                start_ts,
                conflict_start_ts: TimeStamp::zero(),
                conflict_commit_ts: TimeStamp::zero(),
                key: key.to_cocauset().unwrap(),
                primary: vec![],
            },
            "deaddagger" => ErrorInner::Deaddagger {
                start_ts,
                dagger_ts: TimeStamp::zero(),
                dagger_key: key.to_cocauset().unwrap(),
                deaddagger_key_hash: 0,
                wait_chain: vec![],
            },
            "alreadyexist" => ErrorInner::AlreadyExist {
                key: key.to_cocauset().unwrap(),
            },
            "committsexpired" => ErrorInner::CommitTsExpired {
                start_ts,
                commit_ts: TimeStamp::zero(),
                key: key.to_cocauset().unwrap(),
                min_commit_ts: TimeStamp::zero(),
            },
            "pessimisticdaggernotfound" => ErrorInner::PessimisticDaggerNotFound {
                start_ts,
                key: key.to_cocauset().unwrap(),
            },
            _ => ErrorInner::Other(box_err!("unexpected error string")),
        }
    } else {
        ErrorInner::Other(box_err!("empty error string"))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use crate::einsteindb::storage::fdbhikv::{Rockseinstein_merkle_tree, SentinelSearchMode, WriteData};
    use crate::einsteindb::storage::epaxos::tests::*;
    use crate::einsteindb::storage::epaxos::{Error, ErrorInner, Mutation, EpaxosReader, blackbraneReader};
    use crate::einsteindb::storage::solitontxn::commands::*;
    use crate::einsteindb::storage::solitontxn::tests::*;
    use crate::einsteindb::storage::solitontxn::{
        commit, prewrite, CommitKind, TransactionKind, TransactionProperties,
    };
    use crate::einsteindb::storage::SecondaryDaggerCausetatus;
    use crate::einsteindb::storage::{
        fdbhikv::{einstein_merkle_tree, Testeinstein_merkle_treeBuilder},
        TxnStatus,
    };
    use fdbhikvproto::fdbhikvrpcpb::{AssertionLevel, Context};
    use solitontxn_types::{TimeStamp, WriteType, SHORT_VALUE_MAX_LEN};

    fn test_epaxos_solitontxn_read_imp(k1: &[u8], k2: &[u8], v: &[u8]) {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        must_get_none(&einstein_merkle_tree, k1, 1);

        must_prewrite_put(&einstein_merkle_tree, k1, v, k1, 2);
        must_rollback(&einstein_merkle_tree, k1, 2, false);
        // should ignore rollback
        must_get_none(&einstein_merkle_tree, k1, 3);

        must_prewrite_dagger(&einstein_merkle_tree, k1, k1, 3);
        must_commit(&einstein_merkle_tree, k1, 3, 4);
        // should ignore read dagger
        must_get_none(&einstein_merkle_tree, k1, 5);

        must_prewrite_put(&einstein_merkle_tree, k1, v, k1, 5);
        must_prewrite_put(&einstein_merkle_tree, k2, v, k1, 5);
        // should not be affected by later daggers
        must_get_none(&einstein_merkle_tree, k1, 4);
        // should read pending daggers
        must_get_err(&einstein_merkle_tree, k1, 7);
        // should ignore the primary dagger and get none when reading the latest record
        must_get_none(&einstein_merkle_tree, k1, u64::max_value());
        // should read secondary daggers even when reading the latest record
        must_get_err(&einstein_merkle_tree, k2, u64::max_value());

        must_commit(&einstein_merkle_tree, k1, 5, 10);
        must_commit(&einstein_merkle_tree, k2, 5, 10);
        must_get_none(&einstein_merkle_tree, k1, 3);
        // should not read with ts < commit_ts
        must_get_none(&einstein_merkle_tree, k1, 7);
        // should read with ts > commit_ts
        must_get(&einstein_merkle_tree, k1, 13, v);
        // should read the latest record if `ts == u64::max_value()`
        must_get(&einstein_merkle_tree, k1, u64::max_value(), v);

        must_prewrite_delete(&einstein_merkle_tree, k1, k1, 15);
        // should ignore the dagger and get previous record when reading the latest record
        must_get(&einstein_merkle_tree, k1, u64::max_value(), v);
        must_commit(&einstein_merkle_tree, k1, 15, 20);
        must_get_none(&einstein_merkle_tree, k1, 3);
        must_get_none(&einstein_merkle_tree, k1, 7);
        must_get(&einstein_merkle_tree, k1, 13, v);
        must_get(&einstein_merkle_tree, k1, 17, v);
        must_get_none(&einstein_merkle_tree, k1, 23);

        // intersecting timestamps with pessimistic solitontxn
        // T1: start_ts = 25, commit_ts = 27
        // T2: start_ts = 23, commit_ts = 31
        must_prewrite_put(&einstein_merkle_tree, k1, v, k1, 25);
        must_commit(&einstein_merkle_tree, k1, 25, 27);
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k1, k1, 23, 29);
        must_get(&einstein_merkle_tree, k1, 30, v);
        must_pessimistic_prewrite_delete(&einstein_merkle_tree, k1, k1, 23, 29, true);
        must_get_err(&einstein_merkle_tree, k1, 30);
        // should read the latest record when `ts == u64::max_value()`
        // even if dagger.start_ts(23) < latest write.commit_ts(27)
        must_get(&einstein_merkle_tree, k1, u64::max_value(), v);
        must_commit(&einstein_merkle_tree, k1, 23, 31);
        must_get(&einstein_merkle_tree, k1, 30, v);
        must_get_none(&einstein_merkle_tree, k1, 32);
    }

    #[test]
    fn test_epaxos_solitontxn_read() {
        test_epaxos_solitontxn_read_imp(b"k1", b"k2", b"v1");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_epaxos_solitontxn_read_imp(b"k1", b"k2", &long_value);
    }

    fn test_epaxos_solitontxn_prewrite_imp(k: &[u8], v: &[u8]) {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        must_prewrite_put(&einstein_merkle_tree, k, v, k, 5);
        // Key is daggered.
        must_daggered(&einstein_merkle_tree, k, 5);
        // Retry prewrite.
        must_prewrite_put(&einstein_merkle_tree, k, v, k, 5);
        // Conflict.
        must_prewrite_dagger_err(&einstein_merkle_tree, k, k, 6);

        must_commit(&einstein_merkle_tree, k, 5, 10);
        must_written(&einstein_merkle_tree, k, 5, 10, WriteType::Put);
        // Delayed prewrite request after committing should do nothing.
        must_prewrite_put_err(&einstein_merkle_tree, k, v, k, 5);
        must_undaggered(&einstein_merkle_tree, k);
        // Write conflict.
        must_prewrite_dagger_err(&einstein_merkle_tree, k, k, 6);
        must_undaggered(&einstein_merkle_tree, k);
        // Not conflict.
        must_prewrite_dagger(&einstein_merkle_tree, k, k, 12);
        must_daggered(&einstein_merkle_tree, k, 12);
        must_rollback(&einstein_merkle_tree, k, 12, false);
        must_undaggered(&einstein_merkle_tree, k);
        must_written(&einstein_merkle_tree, k, 12, 12, WriteType::Rollback);
        // Cannot retry Prewrite after rollback.
        must_prewrite_dagger_err(&einstein_merkle_tree, k, k, 12);
        // Can prewrite after rollback.
        must_prewrite_delete(&einstein_merkle_tree, k, k, 13);
        must_rollback(&einstein_merkle_tree, k, 13, false);
        must_undaggered(&einstein_merkle_tree, k);
    }

    #[test]
    fn test_epaxos_solitontxn_prewrite_insert() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let (k1, v1, v2, v3) = (b"k1", b"v1", b"v2", b"v3");
        must_prewrite_put(&einstein_merkle_tree, k1, v1, k1, 1);
        must_commit(&einstein_merkle_tree, k1, 1, 2);

        // "k1" already exist, returns AlreadyExist error.
        assert!(matches!(
            try_prewrite_insert(&einstein_merkle_tree, k1, v2, k1, 3),
            Err(Error(box ErrorInner::AlreadyExist { .. }))
        ));

        // Delete "k1"
        must_prewrite_delete(&einstein_merkle_tree, k1, k1, 4);

        // There is a dagger, returns KeyIsDaggered error.
        assert!(matches!(
            try_prewrite_insert(&einstein_merkle_tree, k1, v2, k1, 6),
            Err(Error(box ErrorInner::KeyIsDaggered(_)))
        ));

        must_commit(&einstein_merkle_tree, k1, 4, 5);

        // After delete "k1", insert returns ok.
        assert!(try_prewrite_insert(&einstein_merkle_tree, k1, v2, k1, 6).is_ok());
        must_commit(&einstein_merkle_tree, k1, 6, 7);

        // Rollback
        must_prewrite_put(&einstein_merkle_tree, k1, v3, k1, 8);
        must_rollback(&einstein_merkle_tree, k1, 8, false);

        assert!(matches!(
            try_prewrite_insert(&einstein_merkle_tree, k1, v3, k1, 9),
            Err(Error(box ErrorInner::AlreadyExist { .. }))
        ));

        // Delete "k1" again
        must_prewrite_delete(&einstein_merkle_tree, k1, k1, 10);
        must_commit(&einstein_merkle_tree, k1, 10, 11);

        // Rollback again
        must_prewrite_put(&einstein_merkle_tree, k1, v3, k1, 12);
        must_rollback(&einstein_merkle_tree, k1, 12, false);

        // After delete "k1", insert returns ok.
        assert!(try_prewrite_insert(&einstein_merkle_tree, k1, v2, k1, 13).is_ok());
        must_commit(&einstein_merkle_tree, k1, 13, 14);
    }

    #[test]
    fn test_epaxos_solitontxn_prewrite_check_not_exist() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let (k1, v1, v2, v3) = (b"k1", b"v1", b"v2", b"v3");
        must_prewrite_put(&einstein_merkle_tree, k1, v1, k1, 1);
        must_commit(&einstein_merkle_tree, k1, 1, 2);

        // "k1" already exist, returns AlreadyExist error.
        assert!(try_prewrite_check_not_exists(&einstein_merkle_tree, k1, k1, 3).is_err());

        // Delete "k1"
        must_prewrite_delete(&einstein_merkle_tree, k1, k1, 4);
        must_commit(&einstein_merkle_tree, k1, 4, 5);

        // After delete "k1", check_not_exists returns ok.
        assert!(try_prewrite_check_not_exists(&einstein_merkle_tree, k1, k1, 6).is_ok());

        assert!(try_prewrite_insert(&einstein_merkle_tree, k1, v2, k1, 7).is_ok());
        must_commit(&einstein_merkle_tree, k1, 7, 8);

        // Rollback
        must_prewrite_put(&einstein_merkle_tree, k1, v3, k1, 9);
        must_rollback(&einstein_merkle_tree, k1, 9, false);
        assert!(try_prewrite_check_not_exists(&einstein_merkle_tree, k1, k1, 10).is_err());

        // Delete "k1" again
        must_prewrite_delete(&einstein_merkle_tree, k1, k1, 11);
        must_commit(&einstein_merkle_tree, k1, 11, 12);

        // Rollback again
        must_prewrite_put(&einstein_merkle_tree, k1, v3, k1, 13);
        must_rollback(&einstein_merkle_tree, k1, 13, false);

        // After delete "k1", check_not_exists returns ok.
        assert!(try_prewrite_check_not_exists(&einstein_merkle_tree, k1, k1, 14).is_ok());
    }

    #[test]
    fn test_epaxos_solitontxn_pessmistic_prewrite_check_not_exist() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let k = b"k1";
        assert!(try_pessimistic_prewrite_check_not_exists(&einstein_merkle_tree, k, k, 3).is_err())
    }

    #[test]
    fn test_rollback_dagger_optimistic() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");
        must_prewrite_put(&einstein_merkle_tree, k, v, k, 5);
        must_commit(&einstein_merkle_tree, k, 5, 10);

        // Dagger
        must_prewrite_dagger(&einstein_merkle_tree, k, k, 15);
        must_daggered(&einstein_merkle_tree, k, 15);

        // Rollback dagger
        must_rollback(&einstein_merkle_tree, k, 15, false);
        // Rollbacks of optimistic transactions needn't be protected
        must_get_rollback_protected(&einstein_merkle_tree, k, 15, false);
    }

    #[test]
    fn test_rollback_dagger_pessimistic() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        let (k1, k2, v) = (b"k1", b"k2", b"v1");

        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k1, k1, 5, 5);
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k2, k1, 5, 7);
        must_rollback(&einstein_merkle_tree, k1, 5, false);
        must_rollback(&einstein_merkle_tree, k2, 5, false);
        // The rollback of the primary key should be protected
        must_get_rollback_protected(&einstein_merkle_tree, k1, 5, true);
        // The rollback of the secondary key needn't be protected
        must_get_rollback_protected(&einstein_merkle_tree, k2, 5, false);

        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k1, k1, 15, 15);
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k2, k1, 15, 17);
        must_pessimistic_prewrite_put(&einstein_merkle_tree, k1, v, k1, 15, 17, true);
        must_pessimistic_prewrite_put(&einstein_merkle_tree, k2, v, k1, 15, 17, true);
        must_rollback(&einstein_merkle_tree, k1, 15, false);
        must_rollback(&einstein_merkle_tree, k2, 15, false);
        // The rollback of the primary key should be protected
        must_get_rollback_protected(&einstein_merkle_tree, k1, 15, true);
        // The rollback of the secondary key needn't be protected
        must_get_rollback_protected(&einstein_merkle_tree, k2, 15, false);
    }

    #[test]
    fn test_rollback_del() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");
        must_prewrite_put(&einstein_merkle_tree, k, v, k, 5);
        must_commit(&einstein_merkle_tree, k, 5, 10);

        // Prewrite delete
        must_prewrite_delete(&einstein_merkle_tree, k, k, 15);
        must_daggered(&einstein_merkle_tree, k, 15);

        // Rollback delete
        must_rollback(&einstein_merkle_tree, k, 15, false);
    }

    #[test]
    fn test_rollback_overlapped() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let (k1, v1) = (b"key1", b"v1");
        let (k2, v2) = (b"key2", b"v2");

        must_prewrite_put(&einstein_merkle_tree, k1, v1, k1, 10);
        must_prewrite_put(&einstein_merkle_tree, k2, v2, k2, 11);
        must_commit(&einstein_merkle_tree, k1, 10, 20);
        must_commit(&einstein_merkle_tree, k2, 11, 20);
        let w1 = must_written(&einstein_merkle_tree, k1, 10, 20, WriteType::Put);
        let w2 = must_written(&einstein_merkle_tree, k2, 11, 20, WriteType::Put);
        assert!(!w1.has_overlapped_rollback);
        assert!(!w2.has_overlapped_rollback);

        must_cleanup(&einstein_merkle_tree, k1, 20, 0);
        must_rollback(&einstein_merkle_tree, k2, 20, false);

        let w1r = must_written(&einstein_merkle_tree, k1, 10, 20, WriteType::Put);
        assert!(w1r.has_overlapped_rollback);
        // The only difference between w1r and w1 is the overlapped_rollback flag.
        assert_eq!(w1r.set_overlapped_rollback(false, None), w1);

        let w2r = must_written(&einstein_merkle_tree, k2, 11, 20, WriteType::Put);
        // Rollback is invoked on secondaries, so the rollback is not protected and overlapped_rollback
        // won't be set.
        assert_eq!(w2r, w2);
    }

    #[test]
    fn test_epaxos_solitontxn_prewrite() {
        test_epaxos_solitontxn_prewrite_imp(b"k1", b"v1");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_epaxos_solitontxn_prewrite_imp(b"k2", &long_value);
    }

    #[test]
    fn test_epaxos_solitontxn_rollback_after_commit() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        let k = b"k";
        let v = b"v";
        let t1 = 1;
        let t2 = 10;
        let t3 = 20;
        let t4 = 30;

        must_prewrite_put(&einstein_merkle_tree, k, v, k, t1);

        must_rollback(&einstein_merkle_tree, k, t2, false);
        must_rollback(&einstein_merkle_tree, k, t2, false);
        must_rollback(&einstein_merkle_tree, k, t4, false);

        must_commit(&einstein_merkle_tree, k, t1, t3);
        // The rollback should be failed since the transaction
        // was committed before.
        must_rollback_err(&einstein_merkle_tree, k, t1);
        must_get(&einstein_merkle_tree, k, t4, v);
    }

    fn test_epaxos_solitontxn_rollback_imp(k: &[u8], v: &[u8]) {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        must_prewrite_put(&einstein_merkle_tree, k, v, k, 5);
        must_rollback(&einstein_merkle_tree, k, 5, false);
        // Rollback should be idempotent
        must_rollback(&einstein_merkle_tree, k, 5, false);
        // Dagger should be released after rollback
        must_undaggered(&einstein_merkle_tree, k);
        must_prewrite_dagger(&einstein_merkle_tree, k, k, 10);
        must_rollback(&einstein_merkle_tree, k, 10, false);
        // data should be dropped after rollback
        must_get_none(&einstein_merkle_tree, k, 20);

        // Can't rollback committed transaction.
        must_prewrite_put(&einstein_merkle_tree, k, v, k, 25);
        must_commit(&einstein_merkle_tree, k, 25, 30);
        must_rollback_err(&einstein_merkle_tree, k, 25);
        must_rollback_err(&einstein_merkle_tree, k, 25);

        // Can't rollback other transaction's dagger
        must_prewrite_delete(&einstein_merkle_tree, k, k, 35);
        must_rollback(&einstein_merkle_tree, k, 34, true);
        must_rollback(&einstein_merkle_tree, k, 36, true);
        must_written(&einstein_merkle_tree, k, 34, 34, WriteType::Rollback);
        must_written(&einstein_merkle_tree, k, 36, 36, WriteType::Rollback);
        must_daggered(&einstein_merkle_tree, k, 35);
        must_commit(&einstein_merkle_tree, k, 35, 40);
        must_get(&einstein_merkle_tree, k, 39, v);
        must_get_none(&einstein_merkle_tree, k, 41);
    }

    #[test]
    fn test_epaxos_solitontxn_rollback() {
        test_epaxos_solitontxn_rollback_imp(b"k", b"v");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_epaxos_solitontxn_rollback_imp(b"k2", &long_value);
    }

    #[test]
    fn test_epaxos_solitontxn_rollback_before_prewrite() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let key = b"key";
        must_rollback(&einstein_merkle_tree, key, 5, false);
        must_prewrite_dagger_err(&einstein_merkle_tree, key, key, 5);
    }

    fn test_write_imp(k: &[u8], v: &[u8], k2: &[u8]) {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        must_prewrite_put(&einstein_merkle_tree, k, v, k, 5);
        must_seek_write_none(&einstein_merkle_tree, k, 5);

        must_commit(&einstein_merkle_tree, k, 5, 10);
        must_seek_write(&einstein_merkle_tree, k, TimeStamp::max(), 5, 10, WriteType::Put);
        must_seek_write_none(&einstein_merkle_tree, k2, TimeStamp::max());
        must_get_commit_ts(&einstein_merkle_tree, k, 5, 10);

        must_prewrite_delete(&einstein_merkle_tree, k, k, 15);
        must_rollback(&einstein_merkle_tree, k, 15, false);
        must_seek_write(&einstein_merkle_tree, k, TimeStamp::max(), 15, 15, WriteType::Rollback);
        must_get_commit_ts(&einstein_merkle_tree, k, 5, 10);
        must_get_commit_ts_none(&einstein_merkle_tree, k, 15);

        must_prewrite_dagger(&einstein_merkle_tree, k, k, 25);
        must_commit(&einstein_merkle_tree, k, 25, 30);
        must_seek_write(&einstein_merkle_tree, k, TimeStamp::max(), 25, 30, WriteType::Dagger);
        must_get_commit_ts(&einstein_merkle_tree, k, 25, 30);
    }

    #[test]
    fn test_write() {
        test_write_imp(b"kk", b"v1", b"k");

        let v2 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_write_imp(b"kk", &v2, b"k");
    }

    fn test_mutant_search_keys_imp(keys: Vec<&[u8]>, values: Vec<&[u8]>) {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        must_prewrite_put(&einstein_merkle_tree, keys[0], values[0], keys[0], 1);
        must_commit(&einstein_merkle_tree, keys[0], 1, 10);
        must_prewrite_dagger(&einstein_merkle_tree, keys[1], keys[1], 1);
        must_commit(&einstein_merkle_tree, keys[1], 1, 5);
        must_prewrite_delete(&einstein_merkle_tree, keys[2], keys[2], 1);
        must_commit(&einstein_merkle_tree, keys[2], 1, 20);
        must_prewrite_put(&einstein_merkle_tree, keys[3], values[1], keys[3], 1);
        must_prewrite_dagger(&einstein_merkle_tree, keys[4], keys[4], 10);
        must_prewrite_delete(&einstein_merkle_tree, keys[5], keys[5], 5);

        must_mutant_search_keys(&einstein_merkle_tree, None, 100, vec![keys[0], keys[1], keys[2]], None);
        must_mutant_search_keys(&einstein_merkle_tree, None, 3, vec![keys[0], keys[1], keys[2]], None);
        must_mutant_search_keys(&einstein_merkle_tree, None, 2, vec![keys[0], keys[1]], Some(keys[1]));
        must_mutant_search_keys(&einstein_merkle_tree, Some(keys[1]), 1, vec![keys[1]], Some(keys[1]));
    }

    #[test]
    fn test_mutant_search_keys() {
        test_mutant_search_keys_imp(vec![b"a", b"c", b"e", b"b", b"d", b"f"], vec![b"a", b"b"]);

        let v1 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v4 = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mutant_search_keys_imp(vec![b"a", b"c", b"e", b"b", b"d", b"f"], vec![&v1, &v4]);
    }

    pub fn solitontxn_props(
        start_ts: TimeStamp,
        primary: &[u8],
        commit_kind: CommitKind,
        for_update_ts: Option<TimeStamp>,
        solitontxn_size: u64,
        skip_constraint_check: bool,
    ) -> TransactionProperties<'_> {
        let kind = if let Some(ts) = for_update_ts {
            TransactionKind::Pessimistic(ts)
        } else {
            TransactionKind::Optimistic(skip_constraint_check)
        };

        TransactionProperties {
            start_ts,
            kind,
            commit_kind,
            primary,
            solitontxn_size,
            dagger_ttl: 0,
            min_commit_ts: TimeStamp::default(),
            need_old_value: false,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
        }
    }

    fn test_write_size_imp(k: &[u8], v: &[u8], pk: &[u8]) {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let ctx = Context::default();
        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let cm = ConcurrencyManager::new(10.into());
        let mut solitontxn = EpaxosTxn::new(10.into(), cm.clone());
        let mut reader = blackbraneReader::new(10.into(), blackbrane, true);
        let key = Key::from_cocauset(k);
        assert_eq!(solitontxn.write_size(), 0);

        prewrite(
            &mut solitontxn,
            &mut reader,
            &solitontxn_props(10.into(), pk, CommitKind::TwoPc, None, 0, false),
            Mutation::make_put(key.clone(), v.to_vec()),
            &None,
            false,
        )
        .unwrap();
        assert!(solitontxn.write_size() > 0);
        einstein_merkle_tree
            .write(&ctx, WriteData::from_modifies(solitontxn.into_modifies()))
            .unwrap();

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut solitontxn = EpaxosTxn::new(10.into(), cm);
        let mut reader = blackbraneReader::new(10.into(), blackbrane, true);
        commit(&mut solitontxn, &mut reader, key, 15.into()).unwrap();
        assert!(solitontxn.write_size() > 0);
        einstein_merkle_tree
            .write(&ctx, WriteData::from_modifies(solitontxn.into_modifies()))
            .unwrap();
    }

    #[test]
    fn test_write_size() {
        test_write_size_imp(b"key", b"value", b"pk");

        let v = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_write_size_imp(b"key", &v, b"pk");
    }

    #[test]
    fn test_skip_constraint_check() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let (key, value) = (b"key", b"value");

        must_prewrite_put(&einstein_merkle_tree, key, value, key, 5);
        must_commit(&einstein_merkle_tree, key, 5, 10);

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let cm = ConcurrencyManager::new(10.into());
        let mut solitontxn = EpaxosTxn::new(5.into(), cm.clone());
        let mut reader = blackbraneReader::new(5.into(), blackbrane, true);
        assert!(
            prewrite(
                &mut solitontxn,
                &mut reader,
                &solitontxn_props(5.into(), key, CommitKind::TwoPc, None, 0, false),
                Mutation::make_put(Key::from_cocauset(key), value.to_vec()),
                &None,
                false,
            )
            .is_err()
        );

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut solitontxn = EpaxosTxn::new(5.into(), cm);
        let mut reader = blackbraneReader::new(5.into(), blackbrane, true);
        prewrite(
            &mut solitontxn,
            &mut reader,
            &solitontxn_props(5.into(), key, CommitKind::TwoPc, None, 0, true),
            Mutation::make_put(Key::from_cocauset(key), value.to_vec()),
            &None,
            false,
        )
        .unwrap();
    }

    #[test]
    fn test_read_commit() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let (key, v1, v2) = (b"key", b"v1", b"v2");

        must_prewrite_put(&einstein_merkle_tree, key, v1, key, 5);
        must_commit(&einstein_merkle_tree, key, 5, 10);
        must_prewrite_put(&einstein_merkle_tree, key, v2, key, 15);
        must_get_err(&einstein_merkle_tree, key, 20);
        must_get_no_dagger_check(&einstein_merkle_tree, key, 12, v1);
        must_get_no_dagger_check(&einstein_merkle_tree, key, 20, v1);
    }

    #[test]
    fn test_collapse_prev_rollback() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let (key, value) = (b"key", b"value");

        // Add a Rollback whose start ts is 1.
        must_prewrite_put(&einstein_merkle_tree, key, value, key, 1);
        must_rollback(&einstein_merkle_tree, key, 1, false);
        must_get_rollback_ts(&einstein_merkle_tree, key, 1);

        // Add a Rollback whose start ts is 2, the previous Rollback whose
        // start ts is 1 will be collapsed.
        must_prewrite_put(&einstein_merkle_tree, key, value, key, 2);
        must_rollback(&einstein_merkle_tree, key, 2, false);
        must_get_none(&einstein_merkle_tree, key, 2);
        must_get_rollback_ts(&einstein_merkle_tree, key, 2);
        must_get_rollback_ts_none(&einstein_merkle_tree, key, 1);

        // Rollback arrive before Prewrite, it will collapse the
        // previous rollback whose start ts is 2.
        must_rollback(&einstein_merkle_tree, key, 3, false);
        must_get_none(&einstein_merkle_tree, key, 3);
        must_get_rollback_ts(&einstein_merkle_tree, key, 3);
        must_get_rollback_ts_none(&einstein_merkle_tree, key, 2);
    }

    #[test]
    fn test_mutant_search_values_in_default() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        must_prewrite_put(
            &einstein_merkle_tree,
            &[2],
            "v".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
            &[2],
            3,
        );
        must_commit(&einstein_merkle_tree, &[2], 3, 3);

        must_prewrite_put(
            &einstein_merkle_tree,
            &[3],
            "a".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
            &[3],
            3,
        );
        must_commit(&einstein_merkle_tree, &[3], 3, 4);

        must_prewrite_put(
            &einstein_merkle_tree,
            &[3],
            "b".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
            &[3],
            5,
        );
        must_commit(&einstein_merkle_tree, &[3], 5, 5);

        must_prewrite_put(
            &einstein_merkle_tree,
            &[6],
            "x".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
            &[6],
            3,
        );
        must_commit(&einstein_merkle_tree, &[6], 3, 6);

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, Some(SentinelSearchMode::Lightlike), true);

        let v = reader.mutant_search_values_in_default(&Key::from_cocauset(&[3])).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(
            v[1],
            (3.into(), "a".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes())
        );
        assert_eq!(
            v[0],
            (5.into(), "b".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes())
        );
    }

    #[test]
    fn test_seek_ts() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        must_prewrite_put(&einstein_merkle_tree, &[2], b"vv", &[2], 3);
        must_commit(&einstein_merkle_tree, &[2], 3, 3);

        must_prewrite_put(
            &einstein_merkle_tree,
            &[3],
            "a".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
            &[3],
            4,
        );
        must_commit(&einstein_merkle_tree, &[3], 4, 4);

        must_prewrite_put(
            &einstein_merkle_tree,
            &[5],
            "b".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
            &[5],
            2,
        );
        must_commit(&einstein_merkle_tree, &[5], 2, 5);

        must_prewrite_put(&einstein_merkle_tree, &[6], b"xxx", &[6], 3);
        must_commit(&einstein_merkle_tree, &[6], 3, 6);

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, Some(SentinelSearchMode::Lightlike), true);

        assert_eq!(
            reader.seek_ts(3.into()).unwrap().unwrap(),
            Key::from_cocauset(&[2])
        );
    }

    #[test]
    fn test_pessimistic_solitontxn_ttl() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        let (k, v) = (b"k", b"v");

        // Pessimistic prewrite keeps the larger TTL of the prewrite request and the original
        // pessimisitic dagger.
        must_acquire_pessimistic_dagger_with_ttl(&einstein_merkle_tree, k, k, 10, 10, 100);
        must_pessimistic_daggered(&einstein_merkle_tree, k, 10, 10);
        must_pessimistic_prewrite_put_with_ttl(&einstein_merkle_tree, k, v, k, 10, 10, true, 110);
        must_daggered_with_ttl(&einstein_merkle_tree, k, 10, 110);

        must_rollback(&einstein_merkle_tree, k, 10, false);

        // TTL not changed if the pessimistic dagger's TTL is larger than that provided in the
        // prewrite request.
        must_acquire_pessimistic_dagger_with_ttl(&einstein_merkle_tree, k, k, 20, 20, 100);
        must_pessimistic_daggered(&einstein_merkle_tree, k, 20, 20);
        must_pessimistic_prewrite_put_with_ttl(&einstein_merkle_tree, k, v, k, 20, 20, true, 90);
        must_daggered_with_ttl(&einstein_merkle_tree, k, 20, 100);
    }

    #[test]
    fn test_constraint_check_with_overlapping_solitontxn() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        must_prewrite_put(&einstein_merkle_tree, k, v, k, 10);
        must_commit(&einstein_merkle_tree, k, 10, 11);
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k, k, 5, 12);
        must_pessimistic_prewrite_dagger(&einstein_merkle_tree, k, k, 5, 12, true);
        must_commit(&einstein_merkle_tree, k, 5, 15);

        // Now in write cf:
        // start_ts = 10, commit_ts = 11, Put("v1")
        // start_ts = 5,  commit_ts = 15, Dagger

        must_get(&einstein_merkle_tree, k, 19, v);
        assert!(try_prewrite_insert(&einstein_merkle_tree, k, v, k, 20).is_err());
    }

    #[test]
    fn test_dagger_info_validation() {
        use fdbhikvproto::fdbhikvrpcpb::{DaggerInfo, Op};

        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let k = b"k";
        let v = b"v";

        let assert_dagger_info_eq = |e, expected_dagger_info: &DaggerInfo| match e {
            Error(box ErrorInner::KeyIsDaggered(info)) => assert_eq!(info, *expected_dagger_info),
            _ => panic!("unexpected error"),
        };

        for is_optimistic in &[false, true] {
            let mut expected_dagger_info = DaggerInfo::default();
            expected_dagger_info.set_primary_dagger(k.to_vec());
            expected_dagger_info.set_dagger_version(10);
            expected_dagger_info.set_key(k.to_vec());
            expected_dagger_info.set_dagger_ttl(3);
            if *is_optimistic {
                expected_dagger_info.set_solitontxn_size(10);
                expected_dagger_info.set_dagger_type(Op::Put);
                // Write an optimistic dagger.
                must_prewrite_put_impl(
                    &einstein_merkle_tree,
                    expected_dagger_info.get_key(),
                    v,
                    expected_dagger_info.get_primary_dagger(),
                    &None,
                    expected_dagger_info.get_dagger_version().into(),
                    false,
                    expected_dagger_info.get_dagger_ttl(),
                    TimeStamp::zero(),
                    expected_dagger_info.get_solitontxn_size(),
                    TimeStamp::zero(),
                    TimeStamp::zero(),
                    false,
                    fdbhikvproto::fdbhikvrpcpb::Assertion::None,
                    fdbhikvproto::fdbhikvrpcpb::AssertionLevel::Off,
                );
            } else {
                expected_dagger_info.set_dagger_type(Op::PessimisticDagger);
                expected_dagger_info.set_dagger_for_update_ts(10);
                // Write a pessimistic dagger.
                must_acquire_pessimistic_dagger_impl(
                    &einstein_merkle_tree,
                    expected_dagger_info.get_key(),
                    expected_dagger_info.get_primary_dagger(),
                    expected_dagger_info.get_dagger_version(),
                    false,
                    expected_dagger_info.get_dagger_ttl(),
                    expected_dagger_info.get_dagger_for_update_ts(),
                    false,
                    false,
                    TimeStamp::zero(),
                );
            }

            assert_dagger_info_eq(
                must_prewrite_put_err(&einstein_merkle_tree, k, v, k, 20),
                &expected_dagger_info,
            );

            assert_dagger_info_eq(
                must_acquire_pessimistic_dagger_err(&einstein_merkle_tree, k, k, 30, 30),
                &expected_dagger_info,
            );

            // If the dagger is not expired, cleanup will return the dagger info.
            assert_dagger_info_eq(must_cleanup_err(&einstein_merkle_tree, k, 10, 1), &expected_dagger_info);

            expected_dagger_info.set_dagger_ttl(0);
            assert_dagger_info_eq(
                must_pessimistic_prewrite_put_err(&einstein_merkle_tree, k, v, k, 40, 40, false),
                &expected_dagger_info,
            );

            // Delete the dagger
            if *is_optimistic {
                must_rollback(&einstein_merkle_tree, k, expected_dagger_info.get_dagger_version(), false);
            } else {
                pessimistic_rollback::tests::must_success(
                    &einstein_merkle_tree,
                    k,
                    expected_dagger_info.get_dagger_version(),
                    expected_dagger_info.get_dagger_for_update_ts(),
                );
            }
        }
    }

    #[test]
    fn test_non_pessimistic_dagger_conflict_with_optimistic_solitontxn() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        must_prewrite_put(&einstein_merkle_tree, k, v, k, 2);
        must_daggered(&einstein_merkle_tree, k, 2);
        must_pessimistic_prewrite_put_err(&einstein_merkle_tree, k, v, k, 1, 1, false);
        must_pessimistic_prewrite_put_err(&einstein_merkle_tree, k, v, k, 3, 3, false);
    }

    #[test]
    fn test_non_pessimistic_dagger_conflict_with_pessismitic_solitontxn() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        // k1 is a row key, k2 is the corresponding index key.
        let (k1, v1) = (b"k1", b"v1");
        let (k2, v2) = (b"k2", b"v2");
        let (k3, v3) = (b"k3", b"v3");

        // Commit k3 at 20.
        must_prewrite_put(&einstein_merkle_tree, k3, v3, k3, 1);
        must_commit(&einstein_merkle_tree, k3, 1, 20);

        // Txn-10 acquires pessimistic daggers on k1 and k3.
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k1, k1, 10, 10);
        must_acquire_pessimistic_dagger_err(&einstein_merkle_tree, k3, k1, 10, 10);
        // Update for_update_ts to 20 due to write conflict
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k3, k1, 10, 20);
        must_pessimistic_prewrite_put(&einstein_merkle_tree, k1, v1, k1, 10, 20, true);
        must_pessimistic_prewrite_put(&einstein_merkle_tree, k3, v3, k1, 10, 20, true);
        // Write a non-pessimistic dagger with for_update_ts 20.
        must_pessimistic_prewrite_put(&einstein_merkle_tree, k2, v2, k1, 10, 20, false);
        // Roll back the primary key due to timeout, but the non-pessimistic dagger is not rolled
        // back.
        must_rollback(&einstein_merkle_tree, k1, 10, false);

        // Txn-15 acquires pessimistic daggers on k1.
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k1, k1, 15, 15);
        must_pessimistic_prewrite_put(&einstein_merkle_tree, k1, v1, k1, 15, 15, true);
        // There is a non-pessimistic dagger conflict here.
        match must_pessimistic_prewrite_put_err(&einstein_merkle_tree, k2, v2, k1, 15, 15, false) {
            Error(box ErrorInner::KeyIsDaggered(info)) => assert_eq!(info.get_dagger_ttl(), 0),
            e => panic!("unexpected error: {}", e),
        };
    }

    #[test]
    fn test_commit_pessimistic_dagger() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        let k = b"k";
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k, k, 10, 10);
        must_commit_err(&einstein_merkle_tree, k, 20, 30);
        must_commit(&einstein_merkle_tree, k, 10, 20);
        must_seek_write(&einstein_merkle_tree, k, 30, 10, 20, WriteType::Dagger);
    }

    #[test]
    fn test_amend_pessimistic_dagger() {
        fn fail_to_write_pessimistic_dagger<E: einstein_merkle_tree>(
            einstein_merkle_tree: &E,
            key: &[u8],
            start_ts: impl Into<TimeStamp>,
            for_update_ts: impl Into<TimeStamp>,
        ) {
            let start_ts = start_ts.into();
            let for_update_ts = for_update_ts.into();
            must_acquire_pessimistic_dagger(einstein_merkle_tree, key, key, start_ts, for_update_ts);
            // Delete the pessimistic dagger to pretend write failure.
            pessimistic_rollback::tests::must_success(einstein_merkle_tree, key, start_ts, for_update_ts);
        }

        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let (k, mut v) = (b"k", b"v".to_vec());

        // Key not exist; should succeed.
        fail_to_write_pessimistic_dagger(&einstein_merkle_tree, k, 10, 10);
        must_pessimistic_prewrite_put(&einstein_merkle_tree, k, &v, k, 10, 10, true);
        must_commit(&einstein_merkle_tree, k, 10, 20);
        must_get(&einstein_merkle_tree, k, 20, &v);

        // for_update_ts(30) >= start_ts(30) > commit_ts(20); should succeed.
        v.push(0);
        fail_to_write_pessimistic_dagger(&einstein_merkle_tree, k, 30, 30);
        must_pessimistic_prewrite_put(&einstein_merkle_tree, k, &v, k, 30, 30, true);
        must_commit(&einstein_merkle_tree, k, 30, 40);
        must_get(&einstein_merkle_tree, k, 40, &v);

        // for_update_ts(40) >= commit_ts(40) > start_ts(35); should fail.
        fail_to_write_pessimistic_dagger(&einstein_merkle_tree, k, 35, 40);
        must_pessimistic_prewrite_put_err(&einstein_merkle_tree, k, &v, k, 35, 40, true);

        // KeyIsDaggered; should fail.
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k, k, 50, 50);
        must_pessimistic_prewrite_put_err(&einstein_merkle_tree, k, &v, k, 60, 60, true);
        pessimistic_rollback::tests::must_success(&einstein_merkle_tree, k, 50, 50);

        // The solitontxn has been rolled back; should fail.
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k, k, 80, 80);
        must_cleanup(&einstein_merkle_tree, k, 80, TimeStamp::max());
        must_pessimistic_prewrite_put_err(&einstein_merkle_tree, k, &v, k, 80, 80, true);
    }

    #[test]
    fn test_async_prewrite_primary() {
        // copy must_prewrite_put_impl, check that the key is written with the correct secondaries and the right timestamp

        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let ctx = Context::default();
        let cm = ConcurrencyManager::new(42.into());

        let do_prewrite = || {
            let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
            let mut solitontxn = EpaxosTxn::new(TimeStamp::new(2), cm.clone());
            let mut reader = blackbraneReader::new(TimeStamp::new(2), blackbrane, true);
            let mutation = Mutation::make_put(Key::from_cocauset(b"key"), b"value".to_vec());
            let (min_commit_ts, _) = prewrite(
                &mut solitontxn,
                &mut reader,
                &solitontxn_props(
                    TimeStamp::new(2),
                    b"key",
                    CommitKind::Async(TimeStamp::zero()),
                    None,
                    0,
                    false,
                ),
                mutation,
                &Some(vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]),
                false,
            )
            .unwrap();
            let modifies = solitontxn.into_modifies();
            if !modifies.is_empty() {
                einstein_merkle_tree
                    .write(&ctx, WriteData::from_modifies(modifies))
                    .unwrap();
            }
            min_commit_ts
        };

        assert_eq!(do_prewrite(), 43.into());

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, None, true);
        let dagger = reader.load_dagger(&Key::from_cocauset(b"key")).unwrap().unwrap();
        assert_eq!(dagger.ts, TimeStamp::new(2));
        assert_eq!(dagger.use_async_commit, true);
        assert_eq!(
            dagger.secondaries,
            vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]
        );

        // max_ts in the concurrency manager is 42, so the min_commit_ts is 43.
        assert_eq!(dagger.min_commit_ts, TimeStamp::new(43));

        // A duplicate prewrite request should return the min_commit_ts in the primary key
        assert_eq!(do_prewrite(), 43.into());
    }

    #[test]
    fn test_async_pessimistic_prewrite_primary() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let ctx = Context::default();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_dagger(&einstein_merkle_tree, b"key", b"key", 2, 2);

        let do_pessimistic_prewrite = || {
            let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
            let mut solitontxn = EpaxosTxn::new(TimeStamp::new(2), cm.clone());
            let mut reader = blackbraneReader::new(TimeStamp::new(2), blackbrane, true);
            let mutation = Mutation::make_put(Key::from_cocauset(b"key"), b"value".to_vec());
            let (min_commit_ts, _) = prewrite(
                &mut solitontxn,
                &mut reader,
                &solitontxn_props(
                    TimeStamp::new(2),
                    b"key",
                    CommitKind::Async(TimeStamp::zero()),
                    Some(4.into()),
                    4,
                    false,
                ),
                mutation,
                &Some(vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]),
                true,
            )
            .unwrap();
            let modifies = solitontxn.into_modifies();
            if !modifies.is_empty() {
                einstein_merkle_tree
                    .write(&ctx, WriteData::from_modifies(modifies))
                    .unwrap();
            }
            min_commit_ts
        };

        assert_eq!(do_pessimistic_prewrite(), 43.into());

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, None, true);
        let dagger = reader.load_dagger(&Key::from_cocauset(b"key")).unwrap().unwrap();
        assert_eq!(dagger.ts, TimeStamp::new(2));
        assert_eq!(dagger.use_async_commit, true);
        assert_eq!(
            dagger.secondaries,
            vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]
        );

        // max_ts in the concurrency manager is 42, so the min_commit_ts is 43.
        assert_eq!(dagger.min_commit_ts, TimeStamp::new(43));

        // A duplicate prewrite request should return the min_commit_ts in the primary key
        assert_eq!(do_pessimistic_prewrite(), 43.into());
    }

    #[test]
    fn test_async_commit_pushed_min_commit_ts() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        // Simulate that min_commit_ts is pushed lightlike_completion larger than latest_ts
        must_acquire_pessimistic_dagger_impl(
            &einstein_merkle_tree, b"key", b"key", 2, false, 20000, 2, false, false, 100,
        );

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut solitontxn = EpaxosTxn::new(TimeStamp::new(2), cm);
        let mut reader = blackbraneReader::new(TimeStamp::new(2), blackbrane, true);
        let mutation = Mutation::make_put(Key::from_cocauset(b"key"), b"value".to_vec());
        let (min_commit_ts, _) = prewrite(
            &mut solitontxn,
            &mut reader,
            &solitontxn_props(
                TimeStamp::new(2),
                b"key",
                CommitKind::Async(TimeStamp::zero()),
                Some(4.into()),
                4,
                false,
            ),
            mutation,
            &Some(vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]),
            true,
        )
        .unwrap();
        assert_eq!(min_commit_ts.into_inner(), 100);
    }

    #[test]
    fn test_solitontxn_timestamp_overlapping() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let (k, v) = (b"k1", b"v1");

        // Prepare a committed transaction.
        must_prewrite_put(&einstein_merkle_tree, k, v, k, 10);
        must_daggered(&einstein_merkle_tree, k, 10);
        must_commit(&einstein_merkle_tree, k, 10, 20);
        must_undaggered(&einstein_merkle_tree, k);
        must_written(&einstein_merkle_tree, k, 10, 20, WriteType::Put);

        // Optimistic transaction allows the start_ts equals to another transaction's commit_ts
        // on the same key.
        must_prewrite_put(&einstein_merkle_tree, k, v, k, 20);
        must_daggered(&einstein_merkle_tree, k, 20);
        must_commit(&einstein_merkle_tree, k, 20, 30);
        must_undaggered(&einstein_merkle_tree, k);

        // ...but it can be rejected by overlapped rollback flag.
        must_cleanup(&einstein_merkle_tree, k, 30, 0);
        let w = must_written(&einstein_merkle_tree, k, 20, 30, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        must_undaggered(&einstein_merkle_tree, k);
        must_prewrite_put_err(&einstein_merkle_tree, k, v, k, 30);
        must_undaggered(&einstein_merkle_tree, k);

        // Prepare a committed transaction.
        must_prewrite_put(&einstein_merkle_tree, k, v, k, 40);
        must_daggered(&einstein_merkle_tree, k, 40);
        must_commit(&einstein_merkle_tree, k, 40, 50);
        must_undaggered(&einstein_merkle_tree, k);
        must_written(&einstein_merkle_tree, k, 40, 50, WriteType::Put);

        // Pessimistic transaction also works in the same case.
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k, k, 50, 50);
        must_pessimistic_daggered(&einstein_merkle_tree, k, 50, 50);
        must_pessimistic_prewrite_put(&einstein_merkle_tree, k, v, k, 50, 50, true);
        must_commit(&einstein_merkle_tree, k, 50, 60);
        must_undaggered(&einstein_merkle_tree, k);
        must_written(&einstein_merkle_tree, k, 50, 60, WriteType::Put);

        // .. and it can also be rejected by overlapped rollback flag.
        must_cleanup(&einstein_merkle_tree, k, 60, 0);
        let w = must_written(&einstein_merkle_tree, k, 50, 60, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        must_undaggered(&einstein_merkle_tree, k);
        must_acquire_pessimistic_dagger_err(&einstein_merkle_tree, k, k, 60, 60);
        must_undaggered(&einstein_merkle_tree, k);
    }

    #[test]
    fn test_rollback_while_other_transaction_running() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let (k, v) = (b"k1", b"v1");

        must_prewrite_put_async_commit(&einstein_merkle_tree, k, v, k, &Some(vec![]), 10, 0);
        must_cleanup(&einstein_merkle_tree, k, 15, 0);
        must_commit(&einstein_merkle_tree, k, 10, 15);
        let w = must_written(&einstein_merkle_tree, k, 10, 15, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        // GC fence shouldn't be set in this case.
        assert!(w.gc_fence.is_none());

        must_prewrite_put_async_commit(&einstein_merkle_tree, k, v, k, &Some(vec![]), 20, 0);
        check_solitontxn_status::tests::must_success(&einstein_merkle_tree, k, 25, 0, 0, true, false, false, |s| {
            s == TxnStatus::DaggerNotExist
        });
        must_commit(&einstein_merkle_tree, k, 20, 25);
        let w = must_written(&einstein_merkle_tree, k, 20, 25, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        assert!(w.gc_fence.is_none());

        must_prewrite_put_async_commit(&einstein_merkle_tree, k, v, k, &Some(vec![]), 30, 0);
        check_secondary_daggers::tests::must_success(
            &einstein_merkle_tree,
            k,
            35,
            SecondaryDaggerCausetatus::RolledBack,
        );
        must_commit(&einstein_merkle_tree, k, 30, 35);
        let w = must_written(&einstein_merkle_tree, k, 30, 35, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        assert!(w.gc_fence.is_none());

        // Do not commit with overlapped_rollback if the rollback ts doesn't equal to commit_ts.
        must_prewrite_put_async_commit(&einstein_merkle_tree, k, v, k, &Some(vec![]), 40, 0);
        must_cleanup(&einstein_merkle_tree, k, 44, 0);
        must_commit(&einstein_merkle_tree, k, 40, 45);
        let w = must_written(&einstein_merkle_tree, k, 40, 45, WriteType::Put);
        assert!(!w.has_overlapped_rollback);

        // Do not put rollback mark to the dagger if the dagger is not async commit or if dagger.ts is
        // before start_ts or min_commit_ts.
        must_prewrite_put(&einstein_merkle_tree, k, v, k, 50);
        must_cleanup(&einstein_merkle_tree, k, 55, 0);
        let l = must_daggered(&einstein_merkle_tree, k, 50);
        assert!(l.rollback_ts.is_empty());
        must_commit(&einstein_merkle_tree, k, 50, 56);

        must_prewrite_put_async_commit(&einstein_merkle_tree, k, v, k, &Some(vec![]), 60, 0);
        must_cleanup(&einstein_merkle_tree, k, 59, 0);
        let l = must_daggered(&einstein_merkle_tree, k, 60);
        assert!(l.rollback_ts.is_empty());
        must_commit(&einstein_merkle_tree, k, 60, 65);

        must_prewrite_put_async_commit(&einstein_merkle_tree, k, v, k, &Some(vec![]), 70, 75);
        must_cleanup(&einstein_merkle_tree, k, 74, 0);
        must_cleanup(&einstein_merkle_tree, k, 75, 0);
        let l = must_daggered(&einstein_merkle_tree, k, 70);
        assert_eq!(l.min_commit_ts, 75.into());
        assert_eq!(l.rollback_ts, vec![75.into()]);
    }

    #[test]
    fn test_gc_fence() {
        let rollback = |einstein_merkle_tree: &Rockseinstein_merkle_tree, k: &[u8], start_ts: u64| {
            must_cleanup(einstein_merkle_tree, k, start_ts, 0);
        };
        let check_status = |einstein_merkle_tree: &Rockseinstein_merkle_tree, k: &[u8], start_ts: u64| {
            check_solitontxn_status::tests::must_success(
                einstein_merkle_tree,
                k,
                start_ts,
                0,
                0,
                true,
                false,
                false,
                |_| true,
            );
        };
        let check_secondary = |einstein_merkle_tree: &Rockseinstein_merkle_tree, k: &[u8], start_ts: u64| {
            check_secondary_daggers::tests::must_success(
                einstein_merkle_tree,
                k,
                start_ts,
                SecondaryDaggerCausetatus::RolledBack,
            );
        };

        for &rollback in &[rollback, check_status, check_secondary] {
            let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

            // Get gc fence without any newer versions.
            must_prewrite_put(&einstein_merkle_tree, b"k1", b"v1", b"k1", 101);
            must_commit(&einstein_merkle_tree, b"k1", 101, 102);
            rollback(&einstein_merkle_tree, b"k1", 102);
            must_get_overlapped_rollback(&einstein_merkle_tree, b"k1", 102, 101, WriteType::Put, Some(0));

            // Get gc fence with a newer put.
            must_prewrite_put(&einstein_merkle_tree, b"k1", b"v1", b"k1", 103);
            must_commit(&einstein_merkle_tree, b"k1", 103, 104);
            must_prewrite_put(&einstein_merkle_tree, b"k1", b"v1", b"k1", 105);
            must_commit(&einstein_merkle_tree, b"k1", 105, 106);
            rollback(&einstein_merkle_tree, b"k1", 104);
            must_get_overlapped_rollback(&einstein_merkle_tree, b"k1", 104, 103, WriteType::Put, Some(106));

            // Get gc fence with a newer delete.
            must_prewrite_put(&einstein_merkle_tree, b"k1", b"v1", b"k1", 107);
            must_commit(&einstein_merkle_tree, b"k1", 107, 108);
            must_prewrite_delete(&einstein_merkle_tree, b"k1", b"k1", 109);
            must_commit(&einstein_merkle_tree, b"k1", 109, 110);
            rollback(&einstein_merkle_tree, b"k1", 108);
            must_get_overlapped_rollback(&einstein_merkle_tree, b"k1", 108, 107, WriteType::Put, Some(110));

            // Get gc fence with a newer rollback and dagger.
            must_prewrite_put(&einstein_merkle_tree, b"k1", b"v1", b"k1", 111);
            must_commit(&einstein_merkle_tree, b"k1", 111, 112);
            must_prewrite_put(&einstein_merkle_tree, b"k1", b"v1", b"k1", 113);
            must_rollback(&einstein_merkle_tree, b"k1", 113, false);
            must_prewrite_dagger(&einstein_merkle_tree, b"k1", b"k1", 115);
            must_commit(&einstein_merkle_tree, b"k1", 115, 116);
            rollback(&einstein_merkle_tree, b"k1", 112);
            must_get_overlapped_rollback(&einstein_merkle_tree, b"k1", 112, 111, WriteType::Put, Some(0));

            // Get gc fence with a newer put after some rollbacks and daggers.
            must_prewrite_put(&einstein_merkle_tree, b"k1", b"v1", b"k1", 121);
            must_commit(&einstein_merkle_tree, b"k1", 121, 122);
            must_prewrite_put(&einstein_merkle_tree, b"k1", b"v1", b"k1", 123);
            must_rollback(&einstein_merkle_tree, b"k1", 123, false);
            must_prewrite_dagger(&einstein_merkle_tree, b"k1", b"k1", 125);
            must_commit(&einstein_merkle_tree, b"k1", 125, 126);
            must_prewrite_put(&einstein_merkle_tree, b"k1", b"v1", b"k1", 127);
            must_commit(&einstein_merkle_tree, b"k1", 127, 128);
            rollback(&einstein_merkle_tree, b"k1", 122);
            must_get_overlapped_rollback(&einstein_merkle_tree, b"k1", 122, 121, WriteType::Put, Some(128));

            // A key's gc fence won't be another EPAXOS key.
            must_prewrite_put(&einstein_merkle_tree, b"k1", b"v1", b"k1", 131);
            must_commit(&einstein_merkle_tree, b"k1", 131, 132);
            must_prewrite_put(&einstein_merkle_tree, b"k0", b"v1", b"k0", 133);
            must_commit(&einstein_merkle_tree, b"k0", 133, 134);
            must_prewrite_put(&einstein_merkle_tree, b"k2", b"v1", b"k2", 133);
            must_commit(&einstein_merkle_tree, b"k2", 133, 134);
            rollback(&einstein_merkle_tree, b"k1", 132);
            must_get_overlapped_rollback(&einstein_merkle_tree, b"k1", 132, 131, WriteType::Put, Some(0));
        }
    }

    #[test]
    fn test_overlapped_ts_commit_before_rollback() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let (k1, v1) = (b"key1", b"v1");
        let (k2, v2) = (b"key2", b"v2");
        let key2 = k2.to_vec();
        let secondaries = Some(vec![key2]);

        // T1, start_ts = 10, commit_ts = 20; write k1, k2
        must_prewrite_put_async_commit(&einstein_merkle_tree, k1, v1, k1, &secondaries, 10, 0);
        must_prewrite_put_async_commit(&einstein_merkle_tree, k2, v2, k1, &secondaries, 10, 0);
        must_commit(&einstein_merkle_tree, k1, 10, 20);
        must_commit(&einstein_merkle_tree, k2, 10, 20);

        let w = must_written(&einstein_merkle_tree, k1, 10, 20, WriteType::Put);
        assert!(!w.has_overlapped_rollback);

        // T2, start_ts = 20
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, k2, k2, 20, 25);
        must_pessimistic_prewrite_put(&einstein_merkle_tree, k2, v2, k2, 20, 25, true);

        must_cleanup(&einstein_merkle_tree, k2, 20, 0);

        let w = must_written(&einstein_merkle_tree, k2, 10, 20, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        must_get(&einstein_merkle_tree, k2, 30, v2);
        must_acquire_pessimistic_dagger_err(&einstein_merkle_tree, k2, k2, 20, 25);
    }

    #[test]
    fn test_overlapped_ts_prewrite_before_rollback() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let (k1, v1) = (b"key1", b"v1");
        let (k2, v2) = (b"key2", b"v2");
        let key2 = k2.to_vec();
        let secondaries = Some(vec![key2]);

        // T1, start_ts = 10
        must_prewrite_put_async_commit(&einstein_merkle_tree, k1, v1, k1, &secondaries, 10, 0);
        must_prewrite_put_async_commit(&einstein_merkle_tree, k2, v2, k1, &secondaries, 10, 0);

        // T2, start_ts = 20
        must_prewrite_put_err(&einstein_merkle_tree, k2, v2, k2, 20);
        must_cleanup(&einstein_merkle_tree, k2, 20, 0);

        // commit T1
        must_commit(&einstein_merkle_tree, k1, 10, 20);
        must_commit(&einstein_merkle_tree, k2, 10, 20);

        let w = must_written(&einstein_merkle_tree, k2, 10, 20, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        must_prewrite_put_err(&einstein_merkle_tree, k2, v2, k2, 20);
    }
}
