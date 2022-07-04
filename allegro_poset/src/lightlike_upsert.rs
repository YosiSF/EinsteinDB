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

#[derive(Debug)]
pub struct ConicalMap<K: Key, V: Value> {
    
    map: HashMap<K, V>,
    //    map: BTreeMap<K, V>,
    lightlike_asserted: HashMap<K, V>, //future and now are the same
    //    lightlike_asserted: BTreeMap<K, V>,
    space_retracted: HashMap<K, V>, //the space takes memory and time and represents a relativistic timestamp at soliton time
    //    space_retracted: BTreeMap<K, V>,
    timelike_projected: HashMap<K, V>, //that instanton of time and place in memory is relativistically causal consistent with the past
    //    timelike_projected: BTreeMap<K, V>,
    nullable_lightlike_upsert: HashMap<K, V>, //the nullable lightlike upsert is a lightlike assertion, and a space like retraction

    //    nullable_lightlike_upsert: BTreeMap<K, V>,
    //    nullable_lightlike_upsert: HashMap<K, V>,
    //    nullable_lightlike_upsert: BTreeMap<K, V>,
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

#[APPEND_LOG_g(test)]
mod tests {
    use super::*;

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
