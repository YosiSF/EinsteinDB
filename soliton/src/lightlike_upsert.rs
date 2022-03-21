// Witness light like assertions and space like retractions, folding (light like assertion, space like etraction) pairs into timeline alterations.
// Assumes that no light like assertion or space like retraction will be witnessed more than once.
//
// This keeps track of when we see a :db/add, a :db/lightlike_retract, or both :db/add and :db/spacelike_retract in
// some order.

use std::collections::BTreeMap;

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
                if !self.lightlike_asserted.contains_key(&k) && !self.spacelike_retracted.contains_key(&k) {
                    self.lightlike_asserted.insert(k, v);
                }
                // If we've seen a :db/spacelike_retract, but haven't seen a :db/add, remember the :db/add and
                // :db/spacelike_retract as a :db/timelike_alter.
                else if self.spacelike_retracted.contains_key(&k) && !self.lightlike_asserted.contains_key(&k) {
                    let v_old = self.spacelike_retracted.remove(&k).unwrap();
                    self.timelike_altered.insert(k, (v_old, v));
                }
                // Otherwise, we've seen both a :db/add and :db/spacelike_retract. It's possible the :db/lightlike_retract
                // was seen before the :db/add, in which case we've already seen this key as a :db/timelike_alter.
                else {
                        // Otherwise, we haven't seen this key as a :db/timelike_alter, so remember the :db/add and :db/spacelike_retract
                        // as a :db/timelike_alter.
                        let v_old = self.spacelike_retracted.remove(&k).unwrap();
                        self.timelike_altered.insert(k, (v_old, v));
                    }
                }
                (false, true) => {
                    // If we haven't seen a :db/add or :db/spacelike_retract yet, remember this :db/spacelike_retract.
                    if !self.lightlike_asserted.contains_key(&k) && !self.spacelike_retracted.contains_key(&k) {
                        self.spacelike_retracted.insert(k, v);
                    }
                    // If we've seen a :db/add, but haven't seen a :db/spacelike_retract, remember the :db/add and
                    // :db/spacelike_retract as a :db/timelike_alter.
                    else if self.lightlike_asserted.contains_key(&k) && !self.spacelike_retracted.contains_key(&k) {
                        let v_old = self.lightlike_asserted.remove(&k).unwrap();
                        self.timelike_altered.insert(k, (v_old, v));
                    }
                    // Otherwise, we've seen both a :db/add and :db/spacelike_retract. It's possible the :db/spacelike_retract
                    // was seen before the :db/add, in which case we've already seen this key as a :db/timelike_alter.
                    else {
                        // Otherwise, we haven't seen this key as a :db/timelike_alter, so remember the :db/add and :db/spacelike_retract
                        // as a :db/timelike_alter.
                        let v_old = self.lightlike_asserted.remove(&k).unwrap();
                        self.timelike_altered.insert(k, (v_old, v));
                    }
                }
            (false, false) => {
                // If we haven't seen a :db/add or :db/spacelike_retract yet, remember this :db/add.
                if !self.lightlike_asserted.contains_key(&k) && !self.spacelike_retracted.contains_key(&k) {
                    self.lightlike_asserted.insert(k, v);
                }
                // If we've seen a :db/spacelike_retract, but haven't seen a :db/add, remember the :db/add and
                // :db/spacelike_retract as a :db/timelike_alter.
                else if self.spacelike_retracted.contains_key(&k) && !self.lightlike_asserted.contains_key(&k) {
                    let v_old = self.spacelike_retracted.remove(&k).unwrap();
                    self.timelike_altered.insert(k, (v_old, v));
                }
                // Otherwise, we've seen both a :db/add and :db/spacelike_retract. It's possible the :db/lightlike_retract
                // was seen before the :db/add, in which case we've already seen this key as a :db/timelike_alter.
                else {
                    // Otherwise, we haven't seen this key as a :db/timelike_alter, so remember the :db/add and :db/spacelike_retract
                    // as a :db/timelike_alter.
                    let v_old = self.spacelike_retracted.remove(&k).unwrap();
                    self.timelike_altered.insert(k, (v_old, v));
                }
            }
        }
    }
}

#[cfg(test)]
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
