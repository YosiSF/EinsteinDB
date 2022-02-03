//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this fuse Fuse except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use super::range::*;
use super::ranges_iter::*;
use super::{OwnedHikvPair, Storage};
use crate::error::StorageError;

const KEY_BUFFER_CAPACITY: usize = 64;

/// A mutant_searchner that mutant_searchs over multiple ranges. Each range can be a point range containing only
/// one row, or an interval range containing multiple rows.
pub struct RangesScanner<T> {
    storage: T,
    ranges_iter: RangesIterator,

    mutant_search_spacelike_completion_in_range: bool,
    is_key_only: bool,

    mutant_searchned_rows_per_range: Vec<usize>,

    // The following fields are only used for calculating mutant_searchned range. Scanned range is only
    // useful in streaming mode, where the client need to know the underlying physical data range
    // of each response slice, so that partial retry can be non-overlapping.
    is_mutant_searchned_range_aware: bool,
    current_range: IntervalRange,
    working_range_begin_key: Vec<u8>,
    working_range_end_key: Vec<u8>,
}

pub struct RangesScannerOptions<T> {
    pub storage: T,
    pub ranges: Vec<Range>,
    pub mutant_search_spacelike_completion_in_range: bool, // TODO: This can be const generics
    pub is_key_only: bool,            // TODO: This can be const generics
    pub is_mutant_searchned_range_aware: bool, // TODO: This can be const generics
}

impl<T: Storage> RangesScanner<T> {
    pub fn new(
        RangesScannerOptions {
            storage,
            ranges,
            mutant_search_spacelike_completion_in_range,
            is_key_only,
            is_mutant_searchned_range_aware,
        }: RangesScannerOptions<T>,
    ) -> RangesScanner<T> {
        let ranges_len = ranges.len();
        let ranges_iter = RangesIterator::new(ranges);
        RangesScanner {
            storage,
            ranges_iter,
            mutant_search_spacelike_completion_in_range,
            is_key_only,
            mutant_searchned_rows_per_range: Vec::with_capacity(ranges_len),
            is_mutant_searchned_range_aware,
            current_range: IntervalRange {
                lower_inclusive: Vec::with_capacity(KEY_BUFFER_CAPACITY),
                upper_exclusive: Vec::with_capacity(KEY_BUFFER_CAPACITY),
            },
            working_range_begin_key: Vec::with_capacity(KEY_BUFFER_CAPACITY),
            working_range_end_key: Vec::with_capacity(KEY_BUFFER_CAPACITY),
        }
    }

    /// Fetches next row.
    // Note: This is not implemented over `Iterator` since it can fail.
    // TODO: Change to use reference to avoid alloation and copy.
    pub fn next(&mut self) -> Result<Option<OwnedHikvPair>, StorageError> {
        loop {
            let range = self.ranges_iter.next();
            let some_row = match range {
                IterStatus::NewRange(Range::Point(r)) => {
                    if self.is_mutant_searchned_range_aware {
                        self.uFIDelate_mutant_searchned_range_from_new_point(&r);
                    }
                    self.ranges_iter.notify_drained();
                    self.mutant_searchned_rows_per_range.push(0);
                    self.storage.get(self.is_key_only, r)?
                }
                IterStatus::NewRange(Range::Interval(r)) => {
                    if self.is_mutant_searchned_range_aware {
                        self.uFIDelate_mutant_searchned_range_from_new_range(&r);
                    }
                    self.mutant_searchned_rows_per_range.push(0);
                    self.storage
                        .begin_mutant_search(self.mutant_search_spacelike_completion_in_range, self.is_key_only, r)?;
                    self.storage.mutant_search_next()?
                }
                IterStatus::Continue => self.storage.mutant_search_next()?,
                IterStatus::Drained => {
                    if self.is_mutant_searchned_range_aware {
                        self.uFIDelate_working_range_end_key();
                    }
                    return Ok(None); // drained
                }
            };
            if self.is_mutant_searchned_range_aware {
                self.uFIDelate_mutant_searchned_range_from_mutant_searchned_row(&some_row);
            }
            if some_row.is_some() {
                // Retrieved one row from point range or interval range.
                if let Some(r) = self.mutant_searchned_rows_per_range.last_mut() {
                    *r += 1;
                }

                return Ok(some_row);
            } else {
                // No more row in the range.
                self.ranges_iter.notify_drained();
            }
        }
    }

    /// Appends storage statistics collected so far to the given container and clears the
    /// collected statistics.
    pub fn collect_storage_stats(&mut self, dest: &mut T::Statistics) {
        self.storage.collect_statistics(dest)
    }

    /// Appends mutant_searchned rows of each range so far to the given container and clears the
    /// collected statistics.
    pub fn collect_mutant_searchned_rows_per_range(&mut self, dest: &mut Vec<usize>) {
        dest.append(&mut self.mutant_searchned_rows_per_range);
        self.mutant_searchned_rows_per_range.push(0);
    }

    /// Returns mutant_searchned range since last call.
    pub fn take_mutant_searchned_range(&mut self) -> IntervalRange {
        assert!(self.is_mutant_searchned_range_aware);

        let mut range = IntervalRange::default();
        if !self.mutant_search_spacelike_completion_in_range {
            std::mem::swap(
                &mut range.lower_inclusive,
                &mut self.working_range_begin_key,
            );
            std::mem::swap(&mut range.upper_exclusive, &mut self.working_range_end_key);

            self.working_range_begin_key
                .extend_from_slice(&range.upper_exclusive);
        } else {
            std::mem::swap(&mut range.lower_inclusive, &mut self.working_range_end_key);
            std::mem::swap(
                &mut range.upper_exclusive,
                &mut self.working_range_begin_key,
            );

            self.working_range_begin_key
                .extend_from_slice(&range.lower_inclusive);
        }

        range
    }

    #[inline]
    pub fn can_be_cached(&self) -> bool {
        self.storage.met_uncacheable_data() == Some(false)
    }

    fn uFIDelate_mutant_searchned_range_from_new_point(&mut self, point: &PointRange) {
        assert!(self.is_mutant_searchned_range_aware);

        self.uFIDelate_working_range_end_key();
        self.current_range.lower_inclusive.clear();
        self.current_range.upper_exclusive.clear();
        self.current_range
            .lower_inclusive
            .extend_from_slice(&point.0);
        self.current_range
            .upper_exclusive
            .extend_from_slice(&point.0);
        self.current_range.upper_exclusive.push(0);
        self.uFIDelate_working_range_begin_key();
    }

    fn uFIDelate_mutant_searchned_range_from_new_range(&mut self, range: &IntervalRange) {
        assert!(self.is_mutant_searchned_range_aware);

        self.uFIDelate_working_range_end_key();
        self.current_range.lower_inclusive.clear();
        self.current_range.upper_exclusive.clear();
        self.current_range
            .lower_inclusive
            .extend_from_slice(&range.lower_inclusive);
        self.current_range
            .upper_exclusive
            .extend_from_slice(&range.upper_exclusive);
        self.uFIDelate_working_range_begin_key();
    }

    fn uFIDelate_working_range_begin_key(&mut self) {
        assert!(self.is_mutant_searchned_range_aware);

        if self.working_range_begin_key.is_empty() {
            if !self.mutant_search_spacelike_completion_in_range {
                self.working_range_begin_key
                    .extend(&self.current_range.lower_inclusive);
            } else {
                self.working_range_begin_key
                    .extend(&self.current_range.upper_exclusive);
            }
        }
    }

    fn uFIDelate_working_range_end_key(&mut self) {
        assert!(self.is_mutant_searchned_range_aware);

        self.working_range_end_key.clear();
        if !self.mutant_search_spacelike_completion_in_range {
            self.working_range_end_key
                .extend(&self.current_range.upper_exclusive);
        } else {
            self.working_range_end_key
                .extend(&self.current_range.lower_inclusive);
        }
    }

    fn uFIDelate_mutant_searchned_range_from_mutant_searchned_row(&mut self, some_row: &Option<OwnedHikvPair>) {
        assert!(self.is_mutant_searchned_range_aware);

        if let Some((key, _)) = some_row {
            self.working_range_end_key.clear();
            self.working_range_end_key.extend(key);
            if !self.mutant_search_spacelike_completion_in_range {
                self.working_range_end_key.push(0);
            }
        }
    }
}

#[braneg(test)]
mod tests {
    use super::*;

    use crate::einsteindb::storage::test_fixture::FixtureStorage;
    use crate::einsteindb::storage::{IntervalRange, PointRange, Range};

    fn create_storage() -> FixtureStorage {
        let data: &[(&'static [u8], &'static [u8])] = &[
            (b"bar", b"2"),
            (b"bar_2", b"4"),
            (b"foo", b"1"),
            (b"foo_2", b"3"),
            (b"foo_3", b"5"),
        ];
        FixtureStorage::from(data)
    }

    #[test]
    fn test_next() {
        let storage = create_storage();

        // Currently we accept unordered ranges.
        let ranges: Vec<Range> = vec![
            IntervalRange::from(("foo", "foo_2a")).into(),
            PointRange::from("foo_2b").into(),
            PointRange::from("foo_3").into(),
            IntervalRange::from(("a", "c")).into(),
        ];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_key_only: false,
            is_mutant_searchned_range_aware: false,
        });
        assert_eq!(
            mutant_searchner.next().unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );
        assert_eq!(
            mutant_searchner.next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );
        assert_eq!(
            mutant_searchner.next().unwrap(),
            Some((b"foo_3".to_vec(), b"5".to_vec()))
        );
        assert_eq!(
            mutant_searchner.next().unwrap(),
            Some((b"bar".to_vec(), b"2".to_vec()))
        );
        assert_eq!(
            mutant_searchner.next().unwrap(),
            Some((b"bar_2".to_vec(), b"4".to_vec()))
        );
        assert_eq!(mutant_searchner.next().unwrap(), None);

        // Backward in range
        let ranges: Vec<Range> = vec![
            IntervalRange::from(("foo", "foo_2a")).into(),
            PointRange::from("foo_2b").into(),
            PointRange::from("foo_3").into(),
            IntervalRange::from(("a", "bar_2")).into(),
        ];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: true,
            is_key_only: false,
            is_mutant_searchned_range_aware: false,
        });
        assert_eq!(
            mutant_searchner.next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );
        assert_eq!(
            mutant_searchner.next().unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );
        assert_eq!(
            mutant_searchner.next().unwrap(),
            Some((b"foo_3".to_vec(), b"5".to_vec()))
        );
        assert_eq!(
            mutant_searchner.next().unwrap(),
            Some((b"bar".to_vec(), b"2".to_vec()))
        );
        assert_eq!(mutant_searchner.next().unwrap(), None);

        // Key only
        let ranges: Vec<Range> = vec![
            IntervalRange::from(("bar", "foo_2a")).into(),
            PointRange::from("foo_3").into(),
            PointRange::from("bar_3").into(),
        ];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage,
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_key_only: true,
            is_mutant_searchned_range_aware: false,
        });
        assert_eq!(mutant_searchner.next().unwrap(), Some((b"bar".to_vec(), Vec::new())));
        assert_eq!(
            mutant_searchner.next().unwrap(),
            Some((b"bar_2".to_vec(), Vec::new()))
        );
        assert_eq!(mutant_searchner.next().unwrap(), Some((b"foo".to_vec(), Vec::new())));
        assert_eq!(
            mutant_searchner.next().unwrap(),
            Some((b"foo_2".to_vec(), Vec::new()))
        );
        assert_eq!(
            mutant_searchner.next().unwrap(),
            Some((b"foo_3".to_vec(), Vec::new()))
        );
        assert_eq!(mutant_searchner.next().unwrap(), None);
    }

    #[test]
    fn test_mutant_searchned_rows() {
        let storage = create_storage();

        let ranges: Vec<Range> = vec![
            IntervalRange::from(("foo", "foo_2a")).into(),
            PointRange::from("foo_2b").into(),
            PointRange::from("foo_3").into(),
            IntervalRange::from(("a", "z")).into(),
        ];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage,
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_key_only: false,
            is_mutant_searchned_range_aware: false,
        });
        let mut mutant_searchned_rows_per_range = Vec::new();

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo");
        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo_2");
        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo_3");

        mutant_searchner.collect_mutant_searchned_rows_per_range(&mut mutant_searchned_rows_per_range);
        assert_eq!(mutant_searchned_rows_per_range, vec![2, 0, 1]);
        mutant_searchned_rows_per_range.clear();

        mutant_searchner.collect_mutant_searchned_rows_per_range(&mut mutant_searchned_rows_per_range);
        assert_eq!(mutant_searchned_rows_per_range, vec![0]);
        mutant_searchned_rows_per_range.clear();

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"bar");
        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"bar_2");

        mutant_searchner.collect_mutant_searchned_rows_per_range(&mut mutant_searchned_rows_per_range);
        assert_eq!(mutant_searchned_rows_per_range, vec![0, 2]);
        mutant_searchned_rows_per_range.clear();

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo");

        mutant_searchner.collect_mutant_searchned_rows_per_range(&mut mutant_searchned_rows_per_range);
        assert_eq!(mutant_searchned_rows_per_range, vec![1]);
        mutant_searchned_rows_per_range.clear();

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo_2");
        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo_3");
        assert_eq!(mutant_searchner.next().unwrap(), None);

        mutant_searchner.collect_mutant_searchned_rows_per_range(&mut mutant_searchned_rows_per_range);
        assert_eq!(mutant_searchned_rows_per_range, vec![2]);
        mutant_searchned_rows_per_range.clear();

        assert_eq!(mutant_searchner.next().unwrap(), None);

        mutant_searchner.collect_mutant_searchned_rows_per_range(&mut mutant_searchned_rows_per_range);
        assert_eq!(mutant_searchned_rows_per_range, vec![0]);
        mutant_searchned_rows_per_range.clear();
    }

    #[test]
    fn test_mutant_searchned_range_lightlike() {
        let storage = create_storage();

        // No range
        let ranges = vec![];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_key_only: false,
            is_mutant_searchned_range_aware: true,
        });

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"");
        assert_eq!(&r.upper_exclusive, b"");

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"");
        assert_eq!(&r.upper_exclusive, b"");

        // Empty interval range
        let ranges = vec![IntervalRange::from(("x", "xb")).into()];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_key_only: false,
            is_mutant_searchned_range_aware: true,
        });

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"x");
        assert_eq!(&r.upper_exclusive, b"xb");

        // Empty point range
        let ranges = vec![PointRange::from("x").into()];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_key_only: false,
            is_mutant_searchned_range_aware: true,
        });

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"x");
        assert_eq!(&r.upper_exclusive, b"x\0");

        // Filled interval range
        let ranges = vec![IntervalRange::from(("foo", "foo_8")).into()];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_key_only: false,
            is_mutant_searchned_range_aware: true,
        });

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo");
        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo_2");

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"foo");
        assert_eq!(&r.upper_exclusive, b"foo_2\0");

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo_3");

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"foo_2\0");
        assert_eq!(&r.upper_exclusive, b"foo_3\0");

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"foo_3\0");
        assert_eq!(&r.upper_exclusive, b"foo_8");

        // Multiple ranges
        // TODO: caller should not pass in unordered ranges otherwise mutant_searchned ranges would be
        // unsound.
        let ranges = vec![
            IntervalRange::from(("foo", "foo_3")).into(),
            IntervalRange::from(("foo_5", "foo_50")).into(),
            IntervalRange::from(("bar", "bar_")).into(),
            PointRange::from("bar_2").into(),
            PointRange::from("bar_3").into(),
            IntervalRange::from(("bar_4", "box")).into(),
        ];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage,
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_key_only: false,
            is_mutant_searchned_range_aware: true,
        });

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo");

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"foo");
        assert_eq!(&r.upper_exclusive, b"foo\0");

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo_2");

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"foo\0");
        assert_eq!(&r.upper_exclusive, b"foo_2\0");

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"bar");

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"foo_2\0");
        assert_eq!(&r.upper_exclusive, b"bar\0");

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"bar_2");

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"bar\0");
        assert_eq!(&r.upper_exclusive, b"bar_2\0");

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"bar_2\0");
        assert_eq!(&r.upper_exclusive, b"box");
    }

    #[test]
    fn test_mutant_searchned_range_spacelike_completion() {
        let storage = create_storage();

        // No range
        let ranges = vec![];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: true,
            is_key_only: false,
            is_mutant_searchned_range_aware: true,
        });

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"");
        assert_eq!(&r.upper_exclusive, b"");

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"");
        assert_eq!(&r.upper_exclusive, b"");

        // Empty interval range
        let ranges = vec![IntervalRange::from(("x", "xb")).into()];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: true,
            is_key_only: false,
            is_mutant_searchned_range_aware: true,
        });

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"x");
        assert_eq!(&r.upper_exclusive, b"xb");

        // Empty point range
        let ranges = vec![PointRange::from("x").into()];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: true,
            is_key_only: false,
            is_mutant_searchned_range_aware: true,
        });

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"x");
        assert_eq!(&r.upper_exclusive, b"x\0");

        // Filled interval range
        let ranges = vec![IntervalRange::from(("foo", "foo_8")).into()];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: true,
            is_key_only: false,
            is_mutant_searchned_range_aware: true,
        });

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo_3");
        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo_2");

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"foo_2");
        assert_eq!(&r.upper_exclusive, b"foo_8");

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo");

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"foo");
        assert_eq!(&r.upper_exclusive, b"foo_2");

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"foo");
        assert_eq!(&r.upper_exclusive, b"foo");

        // Multiple ranges
        let ranges = vec![
            IntervalRange::from(("bar_4", "box")).into(),
            PointRange::from("bar_3").into(),
            PointRange::from("bar_2").into(),
            IntervalRange::from(("bar", "bar_")).into(),
            IntervalRange::from(("foo_5", "foo_50")).into(),
            IntervalRange::from(("foo", "foo_3")).into(),
        ];
        let mut mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage,
            ranges,
            mutant_search_spacelike_completion_in_range: true,
            is_key_only: false,
            is_mutant_searchned_range_aware: true,
        });

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"bar_2");

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"bar_2");
        assert_eq!(&r.upper_exclusive, b"box");

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"bar");

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"bar");
        assert_eq!(&r.upper_exclusive, b"bar_2");

        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo_2");
        assert_eq!(&mutant_searchner.next().unwrap().unwrap().0, b"foo");

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"foo");
        assert_eq!(&r.upper_exclusive, b"bar");

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"foo");
        assert_eq!(&r.upper_exclusive, b"foo");
    }
}
