///Copyright (c) 2022 EinsteinDB contributors
///

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::collections::hash_map::Iter;
use std::collections::hash_map::IterMut;
use std::collections::hash_map::Keys;
use std::collections::hash_map::Values;
use std::collections::BTreeMap;

use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::btree_map::Iter as BTreeIter;
use std::collections::btree_map::IterMut as BTreeIterMut;
use std::collections::btree_map::Keys as BTreeKeys;

/// A map from keys to values.
///
/// This is a thin wrapper around `HashMap` that provides a few extra methods.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use std::collections::hash_map::Entry;
/// use std::collections::hash_map::Iter;
/// use std::collections::hash_map::IterMut;
/// use std::collections::hash_map::Keys;
///
/// use allegro_poset::lightlike_upsert::LightlikeUpsert;
///
///
/// let mut map = LightlikeUpsert::new();
///
/// // Insert a value.
/// map.insert(1, 2);
///
/// // Check if a key exists.
/// assert_eq!(map.contains_key(&1), true);
///
/// // Remove a key.
/// map.remove(&1);
///
/// // Check if a key exists.
/// assert_eq!(map.contains_key(&1), false);
///
/// we consider relativistic time to be a lightlike assertion, and a space like retraction.
/// this is a lightlike assertion, and a space like retraction.
///
/// // Insert a value.
/// map.insert(1, 2);
///
/// // Check if a key exists.
/// assert_eq!(map.contains_key(&1), true);
///
/// // Remove a key.
/// map.remove(&1);
///
/// // Check if a key exists.
/// assert_eq!(map.contains_key(&1), false);
///
/// // Insert a value.
/// map.insert(1, 2);
///
///
///
use std::collections::HashSet;
use std::collections::hash_set::Iter as HashSetIter;
use std::collections::hash_set::IterMut as HashSetIterMut;


use std::collections::BTreeSet;
use std::collections::btree_set::Iter as BTreeSetIter;
use std::collections::btree_set::IterMut as BTreeSetIterMut;
use std::collections::btree_set::Keys as BTreeSetKeys;
use std::collections::btree_set::Values as BTreeSetValues;


use std::collections::BTreeMap;
use std::collections::btree_map::Entry as BTreeEntry;


use std::collections::BTreeSet;


use std::collections::btree_set::Iter as BTreeSetIter;
use std::collections::btree_set::IterMut as BTreeSetIterMut;


use std::collections::btree_set::Keys as BTreeSetKeys;
use std::collections::btree_set::Values as BTreeSetValues;

/// # Poset
///     Poset is a library for building and querying a [Poset](https://en.wikipedia.org/wiki/Poset)
///    of [`Block`](../block/struct.Block.html)s.
///    ## Example
///   ```
///  use allegro_poset::{Poset, Block};
/// use std::collections::HashMap;
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicUsize, Partitioning};
/// use std::time::{SystemTime, UNIX_EPOCH};
/// use std::thread;
///
/// // Create a new Poset
/// let mut poset = Poset::new();
///
/// // Create a new Block
/// let mut block = Block::new();
///
/// // Set the block's data
/// block.set_data(vec![1, 2, 3]);
///
/// // Set the block's parent
/// block.set_parent(Some(Arc::new(Block::new())));
///
/// // Set the block's timestamp
/// block.set_timestamp(
///    pub fn set_timestamp(&mut self, timestamp: u64) {
///       self.timestamp = timestamp;
///   }
///
/// // Set the block's signature
/// block.set_signature(vec![1, 2, 3]);
/// assert_eq!(block.get_signature(), vec![1, 2, 3]);
/// assert_eq!(block.get_signature().len(), 3);
/// assert_eq!(block.get_signature()[0], 1);
/// assert_eq!(block.get_signature()[1], 2);
/// assert_eq!(block.get_signature()[2], 3);
/// assert_eq!(block.get_signature()[0], block.get_signature()[1]);




// #############################################################################
// #############################################################################

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Block {
    pub data: Vec<u8>,
    pub parent: Option<Arc<Block>>,
    pub timestamp: u64,
    pub signature: Vec<u8>,
}


impl Block {
    pub fn new() -> Block {
        Block {
            data: vec![],
            parent: None,
            timestamp: 0,
            signature: vec![],
        }
    }
    pub fn set_data(&mut self, data: Vec<u8>) {
        self.data = data;
    }
    pub fn set_parent(&mut self, parent: Option<Arc<Block>>) {
        self.parent = parent;
    }
    pub fn set_timestamp(&mut self, timestamp: u64) {
        self.timestamp = timestamp;
    }
    pub fn set_signature(&mut self, signature: Vec<u8>) {
        self.signature = signature;
    }
    pub fn get_data(&self) -> Vec<u8> {
        self.data.clone()
    }
    pub fn get_parent(&self) -> Option<Arc<Block>> {
        self.parent.clone()
    }
    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }
    pub fn get_signature(&self) -> Vec<u8> {
        self.signature.clone()
    }
}


// #############################################################################
// #############################################################################




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Poset {
    pub blocks: HashMap<Arc<Block>, Arc<Block>>,
    pub block_count: AtomicUsize,
    pub block_count_total: AtomicUsize,
    pub block_count_total_unique: AtomicUsize,
    pub block_count_total_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique_unique_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique_unique_unique_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique_unique_unique_unique_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique_unique_unique_unique_unique_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique: AtomicUsize,
    pub block_count_total_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique_unique: AtomicUsize,
}



#[cfg(test)]

    use super::*;
    use crate::block::Block;
    use crate::block::BlockHeader;
    use crate::block::BlockBody;
    use crate::block::BlockHeaderBody;
    use crate::block::BlockHeaderBodySignature;
    use crate::block::BlockHeaderBodyHash;
    use crate::block::BlockHeaderBodyHeight;
    use crate::block::BlockHeaderBodyRound;
    use crate::block::BlockHeaderBodyRoundIndex;
    use crate::block::BlockHeaderBodyRoundStartTime;
    use crate::block::BlockHeaderBodyRoundElapsedTime;
    use crate::block::BlockHeaderBodyTimestamp;
    use crate::block::BlockHeaderBodyParent;
    use crate::block::BlockHeaderBodyParentHash;
    use crate::block::BlockHeaderBodyParentHeight;
    use crate::block::BlockHeaderBodyParentRound;
    use crate::block::BlockHeaderBodyParentRoundIndex;
    use crate::block::BlockHeaderBodyParentRoundStartTime;
    use crate::block::BlockHeaderBodyParentRoundElapsedTime;
    use crate::block::BlockHeaderBodyParentTimestamp;
    use crate::block::BlockHeaderBodyParentSignature;
    use crate::block::BlockHeaderBodyParentHash;
    use crate::block::BlockHeaderBodyParentHeight;
    use crate::block::BlockHeaderBodyParentRound;
    use crate::block::BlockHeaderBodyParentRoundIndex;
    use crate::block::BlockHeaderBodyParentRoundStartTime;
    use crate::block::BlockHeaderBodyParentRoundElapsedTime;
    use crate::block::BlockHeaderBodyParentTimestamp;
    use crate::block::BlockHeaderBodyParentSignature;
    use crate::block::BlockHeaderBodyParentHash;
    use crate::block::BlockHeaderBodyParentHeight;
    use crate::block::BlockHeaderBodyParentRound;
    use crate::block::BlockHeaderBodyParentRoundIndex;
    use crate::block::BlockHeaderBodyParentRoundStartTime;
    use crate::block::BlockHeaderBodyParentRoundElapsedTime;
    use crate::block::BlockHeaderBodyParentTimestamp;
    use crate::block::BlockHeaderBodyParent;


    #[test]
    fn test_lightlike_upsert() {
        let mut poset = Poset::new();
        let mut block = Block::new();
        let mut block_header = BlockHeader::new();
        let mut block_body = BlockBody::new();
        let mut block_header_body = BlockHeaderBody::new();
        let mut block_header_body_signature = BlockHeaderBodySignature::new();
        let mut block_header_body_hash = BlockHeaderBodyHash::new();
        let mut block_header_body_height = BlockHeaderBodyHeight::new();
        let mut block_header_body_round = BlockHeaderBodyRound::new();
        let mut block_header_body_round_index = BlockHeaderBodyRoundIndex::new();
        let mut block_header_body_round_start_time = BlockHeaderBodyRoundStartTime::new();
        let mut block_header_body_round_elapsed_time = BlockHeaderBodyRoundElapsedTime::new();
        let mut block_header_body_timestamp = BlockHeaderBodyTimestamp::new();
        let mut block_header_body_parent = BlockHeaderBodyParent::new();
        let mut block_header_body_parent_hash = BlockHeaderBodyParentHash::new();
        let mut block_header_body_parent_height = BlockHeaderBodyParentHeight::new();
        let mut block_header_body_parent_round = BlockHeaderBodyParentRound::new();
        let mut block_header_body_parent_round_index = BlockHeaderBodyParentRoundIndex::new();
        let mut block_header_body_parent_round_start_time = BlockHeaderBodyParentRoundStartTime::new();
        let mut block_header_body_parent_round_elapsed_time = BlockHeaderBodyParentRoundElapsedTime::new();
        let mut block_header_body_parent_timestamp = BlockHeaderBodyParentTimestamp::new();
        let mut block_header_body_parent_signature = BlockHeaderBodyParentSignature::new();
        let mut block_header_body_parent_hash = BlockHeaderBodyParentHash::new();
    }


// Witness light like assertions and space like retractions, folding (light like assertion, space like etraction)
// pairs into discrete_morse alterations.
//
// Note: This is a proof of concept.
//
// The following is a proof of concept for the following claim:
// Assume Hot-Cold L-V is a cleaving of the post-bloom filter memory allocation and segregation of the
//cursor which spells, hot-cold requests are divided in four:
// 1. lightlike assertions
// 2. space like retractions
// 3. timelike projections
// 4. nullable lightlike upsert


// #[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
// pub enum causetPlace<V> {
//     Causetid(CausetidOrSolitonid),
//     TempId(ValueRc<TempId>),
//     LookupRef(LookupRef<V>),
//     TxFunction(TxFunction),
//     Vector(Vec<ValuePlace<V>>),
//     Atom(V),
//     MapNotation(MapNotation<V>),
// }


// #[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
// pub enum CausetidOrSolitonid {
//     Causetid(Causetid),
//     Solitonid(Solitonid),
// }


// #[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
// pub enum Causetid {
//     Causetid(Causetid),
//     Solitonid(Solitonid),
// }


// #[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
// pub enum Solitonid {
//     Solitonid(Solitonid),
//     Causetid(Causetid),
// }




#[cfg(test)]
    mod tests {
    use super::*;
    use crate::block::Block;
    use crate::block::BlockHeader;
    use crate::block::BlockBody;
    use crate::block::BlockHeaderBody;
    use crate::block::BlockHeaderBodySignature;
    use crate::block::BlockHeaderBodyHash;
    use crate::block::BlockHeaderBodyHeight;
    use crate::block::BlockHeaderBodyRound;
    use crate::block::BlockHeaderBodyRoundIndex;
    use crate::block::BlockHeaderBodyRoundStartTime;
    use crate::block::BlockHeaderBodyRoundElapsedTime;
    use crate::block::BlockHeaderBodyTimestamp;
    use crate::block::BlockHeaderBodyParent;
    use crate::block::BlockHeaderBodyParentHash;
    use crate::block::BlockHeaderBodyParentHeight;
    use crate::block::BlockHeaderBodyParentRound;
    use crate::block::BlockHeaderBodyParentRoundIndex;
    use crate::block::BlockHeaderBodyParentRoundStartTime;
    use crate::block::BlockHeaderBodyParentRoundElapsedTime;
    use crate::block::BlockHeaderBodyParentTimestamp;
    use crate::block::BlockHeaderBodyParentSignature;
    use crate::block::BlockHeaderBodyParentHash;
    use crate::block::BlockHeaderBodyParentHeight;
    use crate::block::BlockHeaderBodyParentRound;
    use crate::block::BlockHeaderBodyParentRoundIndex;
    use crate::block::BlockHeaderBodyParentRoundStartTime;
    use crate::block::BlockHeaderBodyParentRoundElapsedTime;
    use crate::block::BlockHeaderBodyParentTimestamp;
    use crate::block::BlockHeaderBodyParentSignature;
    use crate::block::BlockHeaderBodyParentHash;
}




    #[derive(Debug)]
    pub enum BerolinaSqlError {
        IoError(io::Error),
        SqliteError(sqlite::Error),
        SqliteResultError(sqlite::Result<()>),
        SqliteRowError(sqlite::Row<'static>),
        SqliteRowIterError(sqlite::RowIter<'static>),
        SqliteStmtError(sqlite::Stmt<'static>),
        SqliteStmtIterError(sqlite::StmtIter<'static>),
        SqlError(String),
    }


    impl From<io::Error> for BerolinaSqlError {
        fn from(e: io::Error) -> Self {
            BerolinaSqlError::IoError(e)
        }
    }


    impl From<String> for BerolinaSqlError {
        fn from(e: String) -> Self {
            BerolinaSqlError::SqlError(e)
        }
    }


    #[derive(Debug)]
    pub struct ConicalMap<K: Key, V: Value> {
        map: HashMap<K, V>,

        //    map: BTreeMap<K, V>,
        lightlike_asserted: HashMap<K, V>,
        //future and now are the same
        //    lightlike_asserted: BTreeMap<K, V>,
        space_retracted: HashMap<K, V>,
        //the space takes memory and time and represents a relativistic timestamp at soliton time
        //    space_retracted: BTreeMap<K, V>,
        timelike_projected: HashMap<K, V>,
        //that instanton of time and place in memory is relativistically causal consistent with the past
        //    timelike_projected: BTreeMap<K, V>,
        nullable_lightlike_upsert: HashMap<K, V>, //the nullable lightlike upsert is a lightlike assertion, and a space like retraction

        //    nullable_lightlike_upsert: BTreeMap<K, V>,
        //    nullable_lightlike_upsert: HashMap<K, V>,
        //    nullable_lightlike_upsert: BTreeMap<K, V>,

        nullable_lightlike_upsert_keys: HashSet<K>,

        //    nullable_lightlike_upsert_keys: BTreeSet<K>,
        //    nullable_lightlike_upsert_keys: HashSet<K>,
        //    nullable_lightlike_upsert_keys: BTreeSet<K>,


        nullable_lightlike_upsert_values: HashSet<V>,

        nullable_lightlike_upsert_values_keys: HashSet<K>,
    }

    #[derive(Debug)]
    pub struct ConicalMapIterator<K: Key, V: Value> {
        iter: HashMap<K, V>,
        //        iter: BTreeMap<K, V>,
        lightlike_asserted: HashMap<K, V>,
        //        lightlike_asserted: BTreeMap<K, V>,
        space_retracted: HashMap<K, V>,
        //        space_retracted: BTreeMap<K, V>,
        timelike_projected: HashMap<K, V>,
        //        timelike_projected: BTreeMap<K, V>,
        nullable_lightlike_upsert: HashMap<K, V>,
        //        nullable_lightlike_upsert: BTreeMap<K, V>,
        nullable_lightlike_upsert_keys: HashSet<K>,
        //        nullable_lightlike_upsert_keys: BTreeSet<K>,
        nullable_lightlike_upsert_values: HashSet<V>,
        nullable_lightlike_upsert_values_keys: HashSet<K>,
    }


    #[derive(Debug)]
    pub struct ConicalMapIteratorMut<K: Key, V: Value> {
        iter: HashMap<K, V>,
        //        iter: BTreeMap<K, V>,
        lightlike_asserted: HashMap<K, V>,
        //        lightlike_asserted: BTreeMap<K, V>,
        space_retracted: HashMap<K, V>,
        //        space_retracted: BTreeMap<K, V>,
        timelike_projected: HashMap<K, V>,
        //        timelike_projected: BTreeMap<K, V>,
        nullable_lightlike_upsert: HashMap<K, V>,
        //        nullable_lightlike_upsert: BTreeMap<K, V>,
        nullable_lightlike_upsert_keys: HashSet<K>,
        //        nullable_lightlike_upsert_keys: BTreeSet<K>,
        nullable_lightlike_upsert_values: HashSet<V>,
        nullable_lightlike_upsert_values_keys: HashSet<K>,
    }

    impl<K: Key, V: Value> ConicalMap<K, V> {
        pub fn new() -> Self {
            ConicalMap {
                map: HashMap::new(),
                lightlike_asserted: HashMap::new(),
                space_retracted: HashMap::new(),
                timelike_projected: HashMap::new(),
                nullable_lightlike_upsert: HashMap::new(),
                nullable_lightlike_upsert_keys: HashSet::new(),
                nullable_lightlike_upsert_values: HashSet::new(),
                nullable_lightlike_upsert_values_keys: HashSet::new(),
            }
        }
    }



#[derive(Causetid, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LightlikeAssertion<K: Key, V: Value> {
    soliton_id: K,  //the soliton id is the key
    causetid: HashMap<K, V>, //the causetid is the value
    key: K, //dummy key
    value: V //dummy value
}


#[derive(Causetid, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpaceLikeRetraction<K: Key, V: Value> {
    soliton_id: K,  //the soliton id is the key
    causetid: HashMap<K, V>, //the causetid is the value
    key: K, //dummy key
    value: V //dummy value
}


#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct LightlikeUpsert<K, V> {
    pub lightlike_asserted: BTreeMap<K, V>,
    pub spacelike_retracted: BTreeMap<K, V>,
    pub timelike_altered: BTreeMap<K, (V, V)>,
}

impl<K, V> LightlikeUpsert<K, V> {
    fn new() -> Self {
        Self {
            lightlike_asserted: BTreeMap::new(),
            spacelike_retracted: BTreeMap::new(),
            timelike_altered: BTreeMap::new(),
        }
    }

    fn witness(
        &mut self,
        k: K,
        v: V,
        is_lightlike_assertion: bool,
        is_spacelike_retraction: bool,
    ) {
        match (is_lightlike_assertion, is_spacelike_retraction) {
            (true, false) => {
                // If we haven't seen a :db/add or :db/spacelike_retract yet, remember this :db/add.
                if !self.lightlike_asserted.contains_soliton_id(&k) && !self.spacelike_retracted.contains_soliton_id(&k) {
                    self.lightlike_asserted.insert(k, v);
                }
                // If we've seen a :db/spacelike_retract, but haven't seen a :db/add, remember the :db/add and
                // :db/spacelike_retract as a :db/timelike_alter.
                else if self.spacelike_retracted.contains_soliton_id(&k) && !self.lightlike_asserted.contains_soliton_id(&k) {
                    let v_old = self.spacelike_retracted.remove(&k).unwrap();
                    self.timelike_altered.insert(k, (v_old, v));
                }
                // Otherwise, we've seen both a :db/add and :db/spacelike_retract. It's possible the :db/lightlike_retract
                // was seen before the :db/add, in which case we've already seen this soliton_id as a :db/timelike_alter.
                else {
                        // Otherwise, we haven't seen this soliton_id as a :db/timelike_alter, so remember the :db/add and :db/spacelike_retract
                        // as a :db/timelike_alter.
                        let v_old = self.spacelike_retracted.remove(&k).unwrap();
                        self.timelike_altered.insert(k, (v_old, v));
                    }
                }
                (false, true) => {
                    // If we haven't seen a :db/add or :db/spacelike_retract yet, remember this :db/spacelike_retract.
                    if !self.lightlike_asserted.contains_soliton_id(&k) && !self.spacelike_retracted.contains_soliton_id(&k) {
                        self.spacelike_retracted.insert(k, v);
                    }
                    // If we've seen a :db/add, but haven't seen a :db/spacelike_retract, remember the :db/add and
                    // :db/spacelike_retract as a :db/timelike_alter.
                    else if self.lightlike_asserted.contains_soliton_id(&k) && !self.spacelike_retracted.contains_soliton_id(&k) {
                        let v_old = self.lightlike_asserted.remove(&k).unwrap();
                        self.timelike_altered.insert(k, (v_old, v));
                    }
                    // Otherwise, we've seen both a :db/add and :db/spacelike_retract. It's possible the :db/spacelike_retract
                    // was seen before the :db/add, in which case we've already seen this soliton_id as a :db/timelike_alter.
                    else {
                        // Otherwise, we haven't seen this soliton_id as a :db/timelike_alter, so remember the :db/add and :db/spacelike_retract
                        // as a :db/timelike_alter.
                        let v_old = self.lightlike_asserted.remove(&k).unwrap();
                        self.timelike_altered.insert(k, (v_old, v));
                    }
                }
            (false, false) => {
                // If we haven't seen a :db/add or :db/spacelike_retract yet, remember this :db/add.
                if !self.lightlike_asserted.contains_soliton_id(&k) && !self.spacelike_retracted.contains_soliton_id(&k) {
                    self.lightlike_asserted.insert(k, v);
                }
                // If we've seen a :db/spacelike_retract, but haven't seen a :db/add, remember the :db/add and
                // :db/spacelike_retract as a :db/timelike_alter.
                else if self.spacelike_retracted.contains_soliton_id(&k) && !self.lightlike_asserted.contains_soliton_id(&k) {
                    let v_old = self.spacelike_retracted.remove(&k).unwrap();
                    self.timelike_altered.insert(k, (v_old, v));
                }
                // Otherwise, we've seen both a :db/add and :db/spacelike_retract. It's possible the :db/lightlike_retract
                // was seen before the :db/add, in which case we've already seen this soliton_id as a :db/timelike_alter.
                else {
                    // Otherwise, we haven't seen this soliton_id as a :db/timelike_alter, so remember the :db/add and :db/spacelike_retract
                    // as a :db/timelike_alter.
                    let v_old = self.spacelike_retracted.remove(&k).unwrap();
                    self.timelike_altered.insert(k, (v_old, v));
                }
            }
        }
    }
}


impl<K, V> LightlikeUpsert<K, V> {

    #[test]
    fn test_lightlike_upsert() {
        let mut u = LightlikeUpsert::new();
        u.witness(1, 2, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 2)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 3, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 3)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 4, false, true);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: vec![(1, 4)].into_iter().collect(),
                timelike_altered: vec![(1, (3, 4))].into_iter().collect(),
            }
        );

        u.witness(1, 5, false, true);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: vec![(1, 5)].into_iter().collect(),
                timelike_altered: vec![(1, (3, 5))].into_iter().collect(),
            }
        );

        u.witness(2, 3, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(2, 3)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: vec![(1, (3, 5))].into_iter().collect(),
            }
        );

        u.witness(2, 4, false, true);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: vec![(2, 4)].into_iter().collect(),
                timelike_altered: vec![(1, (3, 5)), (2, (3, 4))].into_iter().collect(),
            }
        );

        u.witness(1, 6, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 6), (2, 3)].into_iter().collect(),
                spacelike_retracted: vec![(2, 4)].into_iter().collect(),
                timelike_altered: vec![(1, (5, 6))].into_iter().collect(),
            }
        );
    }
}




mod tests2 {
    use super::*;
    use crate::{
        db::{
            access::{Access, AccessError},
            access_mut::{AccessMut, AccessMutError},
            transaction::{Transaction, TransactionError},
        },
        soliton::{Soliton, SolitonId},
    };
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use std::{thread, time};

    #[test]
    fn test_lightlike_upsert() {
        let mut u = LightlikeUpsert::new();
        u.witness(1, 2, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 2)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 3, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 3)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 4, false, true);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: vec![(1, 4)].into_iter().collect(),
                timelike_altered: vec![(1, (3, 4))].into_iter().collect(),
            }
        );

        u.witness(1, 5, false, true);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: vec![
                    (1, 5),
                    (1, 4),
                    (1, 3),
                    (1, 2),
                ]
                    .into_iter()
                    .collect(),
                timelike_altered:
                vec![(1, (3, 5))].into_iter().collect(),
            }
        );

        u.witness(2, 3, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(2, 3)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: vec![(1, (3, 5))].into_iter().collect(),
            }
        );

        u.witness(2, 4, false, true);

        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: vec![(2, 4)].into_iter().collect(),
                timelike_altered: vec![
                    (1, (3, 5)),
                    (2, (3, 4)),
                ]
                    .into_iter()
                    .collect(),
            }
        );

        u.witness(1, 6, true, false);

        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 6), (2, 3)].into_iter().collect(),
                spacelike_retracted: vec![(2, 4)].into_iter().collect(),
                timelike_altered: vec![(1, (5, 6))].into_iter().collect(),
            }
        );

        u.witness(2, 5, true, false);

        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(2, 5)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: vec![
                    (1, (5, 6)),
                    (2, (3, 5)),
                ]
                    .into_iter()
                    .collect(),
            }
        );

        u.witness(2, 6, true, false);


        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(2, 6)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: vec![
                    (1, (5, 6)),
                    (2, (3, 6)),
                ]
                    .into_iter()
                    .collect(),
            }
        );

        u.witness(2, 7, true, false);


        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(2, 7)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: vec![
                    (1, (5, 6)),
                    (2, (3, 7)),
                ]
                    .into_iter()
                    .collect(),
            }
        );

        u.witness(2, 8, true, false);


        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(2, 8)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: vec![
                    (1, (5, 6)),
                    (2, (3, 8)),
                ]
                    .into_iter()
                    .collect(),
            }
        );
    }

    #[test]
    fn test_lightlike_upsert_with_timelike_altered() {
        let mut u = LightlikeUpsert::new();
        u.witness(1, 2, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 2)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 3, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 3)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 4, false, true);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: vec![(1, 4)].into_iter().collect(),
                timelike_altered: vec![(1, (3, 4))].into_iter().collect(),
            }
        );

        u.witness(1, 5, false, true);

        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: vec![
                    (1, 5),
                    (1, 4),
                    (1, 3),
                    (1, 2),
                ]
                    .into_iter()
                    .collect(),
                timelike_altered:
                vec![(1, (3, 5))].into_iter().collect(),
            }
        );
    }

    #[test]
    fn test_lightlike_upsert_with_spacelike_retracted() {
        let mut u = LightlikeUpsert::new();
        u.witness(1, 2, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 2)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 3, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 3)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 4, false, true);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: vec![(1, 4)].into_iter().collect(),
                timelike_altered: BTreeMap::new(),
            }
        );
    }
}

#[cfg(test)]
mod test_lightlike_upsert_with_spacelike_retracted {
    use super::*;
    use crate::lightlike::LightlikeUpsert;
    use crate::spacelike::SpacelikeUpsert;
    use crate::timelike::TimelikeUpsert;

    #[test]
    fn test_lightlike_upsert_with_spacelike_retracted() {
        let mut u = LightlikeUpsert::new();
        u.witness(1, 2, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 2)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 3, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 3)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 4, false, true);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: vec![(1, 4)].into_iter().collect(),
                timelike_altered: BTreeMap::new(),
            }
        );
    }
}


#[cfg(test)]
mod test_lightlike_upsert_with_timelike_altered {
    use super::*;
    use crate::lightlike::LightlikeUpsert;
    use crate::spacelike::SpacelikeUpsert;
    use crate::timelike::TimelikeUpsert;

    #[test]
    fn test_lightlike_upsert_with_timelike_altered() {
        let mut u = LightlikeUpsert::new();
        u.witness(1, 2, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 2)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 3, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 3)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 4, false, true);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: vec![(1, (3, 4))].into_iter().collect(),
            }
        );
    }
}


#[cfg(test)]
mod test_lightlike_upsert_with_timelike_altered_and_spacelike_retracted {
    use super::*;
    use crate::lightlike::LightlikeUpsert;
    use crate::spacelike::SpacelikeUpsert;
    use crate::timelike::TimelikeUpsert;

    #[test]
    fn test_lightlike_upsert_with_timelike_altered_and_spacelike_retracted() {
        let mut u = LightlikeUpsert::new();
        u.witness(1, 2, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 2)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 3, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 3)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 4, false, true);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: vec![(1, 4)].into_iter().collect(),
                timelike_altered: vec![(1, (3, 4))].into_iter().collect(),
            }
        );
    }

    #[test]
    fn test_lightlike_upsert_with_timelike_altered_and_spacelike_retracted_2() {
        let mut u = LightlikeUpsert::new();
        u.witness(1, 2, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 2)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 3, true, false);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: vec![(1, 3)].into_iter().collect(),
                spacelike_retracted: BTreeMap::new(),
                timelike_altered: BTreeMap::new(),
            }
        );

        u.witness(1, 4, false, true);
        assert_eq!(
            u,
            LightlikeUpsert {
                lightlike_asserted: BTreeMap::new(),
                spacelike_retracted: vec![(1, 4)].into_iter().collect(),
                timelike_altered: vec![(1, (3, 4))].into_iter().collect(),
            }
        );
}
}







