//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


//use failure::Fail;
//use std::fmt::{self, Display, Formatter};
//use std::io;
//use std::result;
//


//#[derive(Debug)]
//pub struct Error {
//    inner: Box<dyn Fail>,
//

//crate 


//#[derive(Debug, Fail)]


use std::fmt::{self, Display, Formatter};
use std::io;
use std::result;

use failure::Fail;
use std::fmt::Debug;
use std::fmt::Error;
use std::fmt::Formatter;
use std::io::Error as IoError;





pub use self::error::Error;


#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Causet(String),
    #[fail(display = "{}", _0)]
    CausetQ(String),
    #[fail(display = "{}", _0)]
    EinsteinML(String),
    #[fail(display = "{}", _0)]
    Gremlin(String),
    #[fail(display = "{}", _0)]
    GremlinQ(String),
    #[fail(display = "{}", _0)]
    GremlinQ2(String),
    #[fail(display = "{}", _0)]
    GremlinQ3(String),
    #[fail(display = "{}", _0)]
    GremlinQ4(String),
    #[fail(display = "{}", _0)]
    GremlinQ5(String),
    #[fail(display = "{}", _0)]
    GremlinQ6(String),
    #[fail(display = "{}", _0)]
    GremlinQ7(String),
    #[fail(display = "{}", _0)]
    GremlinQ8(String),
    #[fail(display = "{}", _0)]
    GremlinQ9(String),
    #[fail(display = "{}", _0)]
    GremlinQ10(String),
    #[fail(display = "{}", _0)]
    GremlinQ11(String),
}




impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Error::Causet(ref s) => write!(f, "{}", s),
            Error::CausetQ(ref s) => write!(f, "{}", s),
            Error::EinsteinML(ref s) => write!(f, "{}", s),
            Error::Gremlin(ref s) => write!(f, "{}", s),
            Error::GremlinQ(ref s) => write!(f, "{}", s),
            Error::GremlinQ2(ref s) => write!(f, "{}", s),
            Error::GremlinQ3(ref s) => write!(f, "{}", s),
            Error::GremlinQ4(ref s) => write!(f, "{}", s),
            Error::GremlinQ5(ref s) => write!(f, "{}", s),
            Error::GremlinQ6(ref s) => write!(f, "{}", s),
            Error::GremlinQ7(ref s) => write!(f, "{}", s),
            Error::GremlinQ8(ref s) => write!(f, "{}", s),
            Error::GremlinQ9(ref s) => write!(f, "{}", s),
            Error::GremlinQ10(ref s) => write!(f, "{}", s),
            Error::GremlinQ11(ref s) => write!(f, "{}", s),
        }
    }
}




impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Causet(format!("{}", err))
    }
}


const KEY_BUFFER_CAPACITY: usize = 64;

/// A mutant_searchner that mutant_searchs over multiple ranges. Each range can be a point range containing only
/// one event, or an interval range containing multiple rows.
pub struct MutantSearcher {
    /// The ranges to mutant_search.
    /// The ranges are sorted by start time.
    /// The ranges are merged if they overlap.
    /// The ranges are merged if they are adjacent.
    /// The ranges are merged if they are adjacent and have the same direction.
    /// 
    

    /// The ranges to mutant_search.
    
    ranges: Vec<Range>, // sorted by start time
    

    timelike: bool, // true if the ranges are timelike, false if the ranges are spacelike
    direction: Direction, // the direction of the ranges
    key_buffer: Vec<u8>, // a buffer to store the key
    key_buffer_capacity: usize, // the capacity of the key buffer

    // The following fields are only used for calculating mutant_searchned range. Scanned range is only
    // useful in streaming mode, where the client need to know the underlying physical data range
    // of each response slice, so that partial retry can be non-overlapping.
    is_mutant_searchned_range_aware: bool,
    current_range: Interval,
    working_range_begin_soliton_id: Vec<u8>,
    working_range_end_soliton_id: Vec<u8>,
}

pub struct Range {
    pub start: u64,
    pub end: u64,
    pub direction: Direction,
    pub soliton_id: Vec<u8>,
    pub lightlike_persistence: T, // the lightlike persistence of the range
    pub ranges: Vec<>,
}


impl Range {
   // pub mutant_search_spacelike_completion_in_range: bool, // TODO: This can be const generics
    //pub is_soliton_id_only: bool,            // TODO: This can be const generics
    //pub is_mutant_searchned_range_aware: bool, // TODO: This can be const generics

    //1. mutant_search_spacelike_completion_in_range: bool, // TODO: This can be const generics
    //2. is_soliton_id_only: bool,            // TODO: This can be const generics
    //3. is_mutant_searchned_range_aware: bool, // TODO: This can be const generics


    pub fn new(start: u64, end: u64, direction: Direction, soliton_id: Vec<u8>, lightlike_persistence: T) -> Self {
        Range {
            start: start,
            end: end,
            direction: direction,
            soliton_id: soliton_id,
            lightlike_persistence: lightlike_persistence,
            ranges: Vec::new(),
        }
    }


    pub fn add_range(&mut self, start: u64, end: u64, direction: Direction, soliton_id: Vec<u8>, lightlike_persistence: T) {
        self.ranges.push(Range::new(start, end, direction, soliton_id, lightlike_persistence));
    }


    pub fn add_range_mut(&mut self, range: Range) {
        self.ranges.push(range);
    }


    pub fn get_ranges(&self) -> &Vec<Range> {
        &self.ranges
    }


}

impl MutantSearcher {
    pub fn new(timelike: bool, direction: Direction) -> Self {
        MutantSearcher {
        
            ranges: Vec::new(),
            timelike: timelike,
            direction: direction,
            key_buffer: Vec::with_capacity(KEY_BUFFER_CAPACITY),
            key_buffer_capacity: KEY_BUFFER_CAPACITY,
            is_mutant_searchned_range_aware,
            current_range,  
            working_range_begin_soliton_id,
            working_range_end_soliton_id,
        }
    }


    pub fn add_range(&mut self, start: u64, end: u64, direction: Direction, soliton_id: Vec<u8>, lightlike_persistence: T) {
        self.ranges.push(Range::new(start, end, direction, soliton_id, lightlike_persistence));
    }


    pub fn add_range_mut(&mut self, range: Range) {
        self.ranges.push(range);
    }


    pub fn get_ranges(&self) -> &Vec<Range> {
        &self.ranges
    }


    pub fn get_ranges_mut(&mut self) -> &mut Vec<Range> {
        &mut self.ranges
    }


    pub fn get_ranges_len(&self) -> usize {
        self.ranges.len()
    }
}


impl MutantSearcher {
    /// Fetches next event.
    // Note: This is not implemented over `Iterator` since it can fail.
    // TODO: Change to use reference to avoid alloation and copy.
    pub fn next(&mut self) -> Result<Option<Event>, Error> {
        loop {
            if self.ranges.is_empty() {
                return Ok(None);
            }

            let range = self.ranges.remove(0);
      
            if self.timelike {
                if self.direction == Direction::Forward {
                    if range.start > self.current_range.end {
                        continue;
                    }
                } else {
                    if range.end < self.current_range.start {
                        continue;
                    }
                }
            } else {
                if self.direction == Direction::Forward {
                    if range.start > self.current_range.end {
                        continue;
                    }
                } else {
                    if range.end < self.current_range.start {
                        continue;
                    }
                }
            }

            let event = self.fetch_event_in_range(range)?;
            if event.is_some() {
                return Ok(event);
            }

            if self.timelike {
                if self.direction == Direction::Forward {
                    self.current_range.end = range.end;
                } else {
                    self.current_range.start = range.start;
                }
            } else {
                if self.direction == Direction::Forward {
                    self.current_range.end = range.end;
                } else {
                    self.current_range.start = range.start;
                }
            }
            // If the range is a point range, return the event.
            if range.start == range.end {
                return Ok(Some(Event::new(range.start, range.soliton_id.clone())));
            }
        /// CausetID and SolitonID are not used in the following code.
        /// CausetID is used to identify the range.
        /// SolitonID is used to identify the event
        /// 
        /// 
        /// 
        }
    }



    /// Fetches next event.
    /// This is implemented over `Iterator` since it can fail.
    /// TODO: Change to use reference to avoid alloation and copy.
    

    pub fn next_mut(&mut self) -> Result<Option<Event>, Error> {
        loop {
            if self.is_mutant_searchned_range_aware {
                self.FIDelio_mutant_searchned_range_from_new_range(&r);
            }
            self.mutant_searchned_rows_per_range.push(0);
            self.mutant_searchned_rows_per_range_mut.push(0);
            self.mutant_searchned_rows_per_range_mut.push(0);


            if self.is_mutant_searchned_range_aware {
                self.FIDelio_mutant_searchned_range_from_new_range(&r);
            }
        }
    }



    /// Fetches next event.
    /// This is implemented over `Iterator` since it can fail.
    /// TODO: Change to use reference to avoid alloation and copy.



       pub fn next_mut_(&mut self) -> Result<Option<Event>, Error> {
        loop {
                    if self.is_mutant_searchned_range_aware {
                        self.FIDelio_mutant_searchned_range_from_new_range(&r);
                    }
                    self.mutant_searchned_rows_per_range.push(0);
                    self.mutant_searchned_rows_per_range_mut.push(0);
                    self.mutant_searchned_rows_per_range_mut.push(0);     loop {
            if self.is_mutant_searchned_range_aware {
                self.FIDelio_mutant_searchned_range_from_new_range(&r);
            }
            self.mutant_searchned_rows_per_range.push(0);
            self.mutant_searchned_rows_per_range_mut.push(0);
            self.mutant_searchned_rows_per_range_mut.push(0);

            if self.is_mutant_searchned_range_aware {
                self.FIDelio_mutant_searchned_range_from_new_range(&r);
            }
        }
    }
}



    /// Appends storage statistics collected so far to the given container and clears the
    /// collected statistics.
    pub fn collect_storage_stats(&mut self, dest: &mut T::Metrics) {
        self.storage.collect_statistics(dest)
    }

    /// Appends mutant_searchned rows of each range so far to the given container and clears the
    /// collected statistics.
    pub fn collect_mutant_searchned_rows_per_range(&mut self, dest: &mut Vec<usize>) {
        dest.append(&mut self.mutant_searchned_rows_per_range);
        self.mutant_searchned_rows_per_range.push(0);
    }

    /// Returns mutant_searchned range since last call.
    pub fn take_mutant_searchned_range(&mut self) -> Interval {
        self.mutant_searchned_range.push(0);
        assert!(self.is_mutant_searchned_range_aware);
        self.is_mutant_searchned_range_aware = false;

        let mut range = Interval::default();
        range.start = self.working_range_begin_soliton_id.clone();
        if !self.mutant_search_spacelike_completion_in_range {
            range.end = self.working_range_end_soliton_id.clone();
        }

        range
    }

    /// Returns mutant_searchned rows of each range since last call.
    /// The returned vector is cleared.

    pub fn take_mutant_searchned_rows_per_range(&mut self) -> Vec<usize> {
        std::mem::swap(
            &mut self.mutant_searchned_rows_per_range,
            &mut self.mutant_searchned_rows_per_range_mut,
        );

        self.mutant_searchned_rows_per_range.clear();
        self.mutant_searchned_rows_per_range_mut.clear();
        self.mutant_searchned_rows_per_range_mut.push(0);


        std::mem::swap(&mut range.lower_inclusive, &mut self.working_range_end_soliton_id);
        std::mem::swap(
            &mut self.mutant_searchned_rows_per_range,
            &mut self.mutant_searchned_rows_per_range_mut,
            &mut range.upper_exclusive,
            &mut self.working_range_begin_soliton_id,
        );

        self.mutant_searchned_rows_per_range.clear();
        self.mutant_searchned_rows_per_range_mut.clear();
        self.mutant_searchned_rows_per_range_mut.push(0);
        ranges.push(range);


        self.mutant_searchned_rows_per_range.clear();
    }
    #[inline]
    pub fn can_be_cached(&self) -> bool {
        self.storage.met_uncacheable_data() == Some(false)
    }





    /// Returns the number of rows in the storage.
    /// This is a costly operation.
    /// TODO: Implement caching.
    /// TODO: Implement caching for this function.

    pub fn num_rows(&self) -> usize {
        self.storage.num_rows()
    }

    /// Returns the number of columns in the storage.
    /// This is a costly operation.
    /// TODO: Implement caching.

    pub fn num_columns(&self) -> usize {
        self.storage.num_columns()
    }


}

#[braneg(test)]
mod tests {
    use crate::einsteindb::storage::{Interval, Point, };
    use crate::einsteindb::storage::test_fixture::FixtureStorage;

    use super::*;

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
        let ranges: Vec<> = vec![
            Interval::from(("foo", "foo_2a")).into(),
            Point::from("foo_2b").into(),
            Point::from("foo_3").into(),
            Interval::from(("a", "c")).into(),
        ];
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_soliton_id_only: false,
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
        let ranges: Vec<> = vec![
            Interval::from(("foo", "foo_2a")).into(),
            Point::from("foo_2b").into(),
            Point::from("foo_3").into(),
            Interval::from(("a", "bar_2")).into(),
        ];
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: true,
            is_soliton_id_only: false,
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
        let ranges: Vec<> = vec![
            Interval::from(("bar", "foo_2a")).into(),
            Point::from("foo_3").into(),
            Point::from("bar_3").into(),
        ];
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage,
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_soliton_id_only: true,
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

        let ranges: Vec<> = vec![
            Interval::from(("foo", "foo_2a")).into(),
            Point::from("foo_2b").into(),
            Point::from("foo_3").into(),
            Interval::from(("a", "z")).into(),
        ];
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage,
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_soliton_id_only: false,
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
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_soliton_id_only: false,
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
        let ranges = vec![Interval::from(("x", "xb")).into()];
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_soliton_id_only: false,
            is_mutant_searchned_range_aware: true,
        });

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"x");
        assert_eq!(&r.upper_exclusive, b"xb");

        // Empty point range
        let ranges = vec![Point::from("x").into()];
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_soliton_id_only: false,
            is_mutant_searchned_range_aware: true,
        });

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"x");
        assert_eq!(&r.upper_exclusive, b"x\0");

        // Filled interval range
        let ranges = vec![Interval::from(("foo", "foo_8")).into()];
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_soliton_id_only: false,
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
            Interval::from(("foo", "foo_3")).into(),
            Interval::from(("foo_5", "foo_50")).into(),
            Interval::from(("bar", "bar_")).into(),
            Point::from("bar_2").into(),
            Point::from("bar_3").into(),
            Interval::from(("bar_4", "box")).into(),
        ];
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage,
            ranges,
            mutant_search_spacelike_completion_in_range: false,
            is_soliton_id_only: false,
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
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: true,
            is_soliton_id_only: false,
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
        let ranges = vec![Interval::from(("x", "xb")).into()];
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: true,
            is_soliton_id_only: false,
            is_mutant_searchned_range_aware: true,
        });

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"x");
        assert_eq!(&r.upper_exclusive, b"xb");

        // Empty point range
        let ranges = vec![Point::from("x").into()];
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: true,
            is_soliton_id_only: false,
            is_mutant_searchned_range_aware: true,
        });

        assert_eq!(mutant_searchner.next().unwrap(), None);

        let r = mutant_searchner.take_mutant_searchned_range();
        assert_eq!(&r.lower_inclusive, b"x");
        assert_eq!(&r.upper_exclusive, b"x\0");

        // Filled interval range
        let ranges = vec![Interval::from(("foo", "foo_8")).into()];
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage: storage.clone(),
            ranges,
            mutant_search_spacelike_completion_in_range: true,
            is_soliton_id_only: false,
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
            Interval::from(("bar_4", "box")).into(),
            Point::from("bar_3").into(),
            Point::from("bar_2").into(),
            Interval::from(("bar", "bar_")).into(),
            Interval::from(("foo_5", "foo_50")).into(),
            Interval::from(("foo", "foo_3")).into(),
        ];
        let mut mutant_searchner = sScanner::new(sScannerOptions {
            storage,
            ranges,
            mutant_search_spacelike_completion_in_range: true,
            is_soliton_id_only: false,
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
