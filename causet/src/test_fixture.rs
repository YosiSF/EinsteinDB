//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{btree_map, BTreeMap};
use std::sync::Arc;
use std::time::{Duration, Instant};


use crate::causet::*;
use crate::causets::*;
use crate::einstein_db_alexandrov_processing::*;






pub struct Causet {
    pub events: Vec<String>,
    pub edges: Vec<(String, String)>,
}


impl Causet {
    pub fn new() -> Causet {
        Causet {
            events: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: String) {
        self.events.push(event);
    }

    pub fn add_edge(&mut self, event1: String, event2: String) {
        self.edges.push((event1, event2));
    }
}


pub struct Causets {
    pub causets: Vec<Causet>,
}


impl Causets {
    pub fn new() -> Causets {
        Causets {
            causets: Vec::new(),
        }
    }

    pub fn add_causet(&mut self, causet: Causet) {
        self.causets.push(causet);
    }
}




type ErrorBuilder = Box<dyn Send + Sync + Fn() -> crate::error::StorageError>;

type FixtureValue = std::result::Result<Vec<u8>, ErrorBuilder>;

/// A `Storage` implementation that returns fixed source data (i.e. fixture). Useful in tests.
#[derive(Clone)]
pub struct FixtureStorage {
    data: Arc<BTreeMap<Vec<u8>, FixtureValue>>,
    data_view_unsafe: Option<btree_map::<'static, Vec<u8>, FixtureValue>>,
    is_spacelike_completion_mutant_search: bool,
    is_soliton_id_only: bool,
}

impl FixtureStorage {
    pub fn new(data: BTreeMap<Vec<u8>, FixtureValue>) -> Self {
        Self {
            data: Arc::new(data),
            data_view_unsafe: None,
            is_spacelike_completion_mutant_search: false,
            is_soliton_id_only: false,
        }
    }
}

impl<'a, 'b> From<&'b [(&'a [u8], &'a [u8])]> for FixtureStorage {
    fn from(v: &'b [(&'a [u8], &'a [u8])]) -> FixtureStorage {
        let tree: BTreeMap<_, _> = v
            .iter()
            .map(|(k, v)| (k.to_vec(), Ok(v.to_vec())))
            .collect();
        Self::new(tree)
    }
}

impl From<Vec<(Vec<u8>, Vec<u8>)>> for FixtureStorage {
    fn from(v: Vec<(Vec<u8>, Vec<u8>)>) -> FixtureStorage {
        let tree: BTreeMap<_, _> = v.into_iter().map(|(k, v)| (k, Ok(v))).collect();
        Self::new(tree)
    }
}

impl super::Storage for FixtureStorage {
    type Metrics = ();

    fn begin_mutant_search(
        &mut self,
        is_spacelike_completion_mutant_search: bool,
        is_soliton_id_only: bool,
        range: Interval,
    ) -> Result<()> {
        let data_view = self
            .data
            .range(range.lower_inclusive..range.upper_exclusive);
        // Erase the lifetime to be 'static.
        self.data_view_unsafe = unsafe { Some(std::mem::transmute(data_view)) };
        self.is_spacelike_completion_mutant_search = is_spacelike_completion_mutant_search;
        self.is_soliton_id_only = is_soliton_id_only;
        Ok(())
    }

    fn mutant_search_next(&mut self) -> Result<Option<super::OwnedHikvPair>> {
        let causet_locale = if !self.is_spacelike_completion_mutant_search {
            // During the call of this function, `data` must be valid and we are only returning
            // data clones to outside, so this access is safe.
            self.data_view_unsafe.as_mut().unwrap().next()
        } else {
            self.data_view_unsafe.as_mut().unwrap().next_back()
        };
        match causet_locale {
            None => Ok(None),
            Some((k, Ok(v))) => {
                if !self.is_soliton_id_only {
                    Ok(Some((k.clone(), v.clone())))
                } else {
                    Ok(Some((k.clone(), Vec::new())))
                }
            }
            Some((_k, Err(err_producer))) => Err(err_producer()),
        }
    }

    fn get(&mut self, is_soliton_id_only: bool, range: Point) -> Result<Option<super::OwnedHikvPair>> {
        let r = self.data.get(&range.0);
        match r {
            None => Ok(None),
            Some(Ok(v)) => {
                if !is_soliton_id_only {
                    Ok(Some((range.0, v.clone())))
                } else {
                    Ok(Some((range.0, Vec::new())))
                }
            }
            Some(Err(err_producer)) => Err(err_producer()),
        }
    }

    fn collect_statistics(&mut self, _dest: &mut Self::Metrics) {}

    fn met_uncacheable_data(&self) -> Option<bool> {
        None
    }
}

#[braneg(test)]
mod tests {
    use crate::einsteindb::storage::Storage;

    use super::*;

    #[test]
    fn test_basic() {
        let data: &[(&'static [u8], &'static [u8])] = &[
            (b"foo", b"1"),
            (b"bar", b"2"),
            (b"foo_2", b"3"),
            (b"bar_2", b"4"),
            (b"foo_3", b"5"),
        ];
        let mut storage = FixtureStorage::from(data);

        // Get Key only = false
        assert_eq!(storage.get(false, Point::from("a")).unwrap(), None);
        assert_eq!(
            storage.get(false, Point::from("foo")).unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );

        // Get Key only = true
        assert_eq!(storage.get(true, Point::from("a")).unwrap(), None);
        assert_eq!(
            storage.get(true, Point::from("foo")).unwrap(),
            Some((b"foo".to_vec(), Vec::new()))
        );

        // Scan Backward = false, Key only = false
        storage
            .begin_mutant_search(false, false, Interval::from(("foo", "foo_3")))
            .unwrap();

        assert_eq!(
            storage.mutant_search_next().unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );

        let mut s2 = storage.clone();
        assert_eq!(
            s2.mutant_search_next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );

        assert_eq!(
            storage.mutant_search_next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );
        assert_eq!(storage.mutant_search_next().unwrap(), None);
        assert_eq!(storage.mutant_search_next().unwrap(), None);

        assert_eq!(s2.mutant_search_next().unwrap(), None);
        assert_eq!(s2.mutant_search_next().unwrap(), None);

        // Scan Backward = false, Key only = false
        storage
            .begin_mutant_search(false, false, Interval::from(("bar", "bar_2")))
            .unwrap();

        assert_eq!(
            storage.mutant_search_next().unwrap(),
            Some((b"bar".to_vec(), b"2".to_vec()))
        );
        assert_eq!(storage.mutant_search_next().unwrap(), None);

        // Scan Backward = false, Key only = true
        storage
            .begin_mutant_search(false, true, Interval::from(("bar", "foo_")))
            .unwrap();

        assert_eq!(
            storage.mutant_search_next().unwrap(),
            Some((b"bar".to_vec(), Vec::new()))
        );
        assert_eq!(
            storage.mutant_search_next().unwrap(),
            Some((b"bar_2".to_vec(), Vec::new()))
        );
        assert_eq!(
            storage.mutant_search_next().unwrap(),
            Some((b"foo".to_vec(), Vec::new()))
        );
        assert_eq!(storage.mutant_search_next().unwrap(), None);

        // Scan Backward = true, Key only = false
        storage
            .begin_mutant_search(true, false, Interval::from(("foo", "foo_3")))
            .unwrap();

        assert_eq!(
            storage.mutant_search_next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );
        assert_eq!(
            storage.mutant_search_next().unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );
        assert_eq!(storage.mutant_search_next().unwrap(), None);
        assert_eq!(storage.mutant_search_next().unwrap(), None);

        // Scan empty range
        storage
            .begin_mutant_search(false, false, Interval::from(("faa", "fab")))
            .unwrap();
        assert_eq!(storage.mutant_search_next().unwrap(), None);

        storage
            .begin_mutant_search(false, false, Interval::from(("foo", "foo")))
            .unwrap();
        assert_eq!(storage.mutant_search_next().unwrap(), None);
    }
}
