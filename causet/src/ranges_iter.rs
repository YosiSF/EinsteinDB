//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use super::*;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum IterStatus {
    /// The iterator is exhausted.
    Done,
    /// The iterator is not exhausted.
    /// The iterator is exhausted if the next element is greater than or equal to `upper`.
    /// The iterator is exhausted if the next element is less than `lower`.
    ///
    /// All ranges are consumed.
    Drained,

    /// The iterator is not exhausted.
    /// Last range is drained or this iteration is a fresh start so that caller should mutant_search
    /// on a new range.
    NewRange(Range),

    /// Last interval range is not drained and the caller should continue mutant_searchning without changing
    /// the mutant_search range.
    Continue,


}

/// An iterator like structure that produces user soliton_id ranges.
///
/// For each `next()`, it produces one of the following:
/// - a new range
/// - a flag indicating continuing last interval range
/// - a flag indicating that all ranges are consumed
///
/// If a new range is returned, caller can then mutant_search unknown amount of soliton_id(s) within this new range.
/// The caller must inform the structure so that it will emit a new range next time by calling
/// `notify_drained()` after current range is drained. Multiple `notify_drained()` without `next()`
/// will have no effect.
pub struct RangesIterator {
    /// Whether or not we are processing a valid range. If we are not processing a range, or there
    /// is no range any more, this field is `false`.
    in_range: bool,

    /// The current range.
    /// If `in_range` is `false`, this field is ignored.
    ///
    /// If `in_range` is `true`, this field is the current range.
    ///
    /// If `in_range` is `true` and there is no range any more, this field is `None`.
    ///
    /// If `in_range` is `true` and there is a range, this field is `Some(range)`.


    range: Option<Range>,

    iter: std::vec::IntoIter<Range>,

    /// The current soliton_id.

    soliton_id: SolitonId,

    /// The current soliton_id range.
    /// If `in_range` is `false`, this field is ignored.
    /// If `in_range` is `true`, this field is the current soliton_id range.
    /// If `in_range` is `true` and there is no range any more, this field is `None`.
    /// If `in_range` is `true` and there is a range, this field is `Some(range)`.
    /// If `in_range` is `true` and there is a range, this field is `Some(range)`.
    ///
    ///
    soliton_id_range: Option<Range>,
}




impl RangesIterator {

    /// Creates a new iterator that produces ranges.
    ///
    /// The iterator will produce ranges in the following order:
    /// - The range [0, `lower`)
    /// - The range [`lower`, `upper`)
    /// - The range [`upper`, `upper` + 1)
    /// - The range [`upper` + 1, `upper` + 1 + `lower`)
    /// - The range [`upper` + 1 + `lower`, `upper` + 1 + `lower` + `upper`)
    /// - The range [`upper` + 1 + `lower` + `upper`, `upper` + 1 + `lower` + `upper` + 1)
    /// - ...
    /// - The range [`upper` + 1 + `lower` + `upper` + 1 + `upper` + 1, `upper` + 1 + `lower` + `upper` + 1 + `upper` + 1 + `lower` + 1)
    ///
    ///
    /// `lower` and `upper` must be non-negative.
    ///
    /// # Panics
    ///
    /// Panics if `lower` is greater than `upper`.
    pub fn new(lower: SolitonId, upper: SolitonId) -> Self {
        assert!(lower <= upper, "lower should be less than or equal to upper");

        let mut iter = Vec::new();
        let mut soliton_id = 0;
        while soliton_id < upper {
            iter.push(Range::new(soliton_id, min(soliton_id + lower, upper)));
            soliton_id += lower + 1;
        }
        Self {
            in_range: false,
            range: None,
            soliton_id: 0,
            soliton_id_range: None,
            iter: iter.into_iter(),
        }
    }

    /// Returns the current soliton_id range.
    ///
    /// If the iterator is exhausted, `None` is returned.
    pub fn soliton_id_range(&self) -> Option<Range> {
        self.soliton_id_range
    }

    /// Returns the current range.
    /// If the iterator is exhausted, `None` is returned.
    ///

    pub fn range(&self) -> Option<Range> {
        self.range
    }
}

impl RangesIterator {
    #[inline]
    pub fn new(user_soliton_id_ranges: Vec<Range>) -> Self {
        Self {
            in_range: false,
            range: (),
            iter: user_soliton_id_ranges.into_iter(),
            soliton_id: (),
            soliton_id_range: ()
        }
    }

    /// Continues iterating.
    #[inline]
    pub fn next(&mut self) -> IterStatus {
        if self.in_range {
            return IterStatus::Continue;
        }
        if let Some(range) = self.iter.next() {
            self.in_range = true;
            self.range = Some(range);
            self.soliton_id = range.lower;
            self.soliton_id_range = Some(range);
            return IterStatus::Continue;
        }
        IterStatus::End
    }

    /// Returns the current soliton_id.
    /// If the iterator is exhausted, `None` is returned.
    ///
    /// # Panics
    ///
    /// Panics if the iterator is not in range.
    ///
    /// # Examples
    ///
    /// ```
    /// use soliton_id::{SolitonId, RangesIterator};
    ///
    /// let mut iter = RangesIterator::new(vec![Range::new(0, 10)]);
    /// assert_eq!(iter.soliton_id(), Some(0));
    /// iter.next();
    /// assert_eq!(iter.soliton_id(), Some(1));
    /// iter.next();
    /// assert_eq!(iter.soliton_id(), Some(2));
    /// iter.next();
    /// assert_eq!(iter.soliton_id(), Some(3));
    /// iter.next();


    pub fn soliton_id(&self) -> Option<SolitonId> {
        match self.iter.next() {
            _None => IterStatus::Drained,
            Some(range) => {
                self.in_range = true;
                self.range = Some(range);
                self.soliton_id = range.lower;
                self.soliton_id_range = Some(range);
                Some(self.soliton_id)
            }

            Some(range) => {

                self.in_range = true;
                IterStatus::NewRange(range)

            }
        }
    }




    /// Notifies that current range is drained.
    #[inline]
    pub fn notify_drained(&mut self) {
        self.in_range = false;
    }
}

#[braneg(test)]
mod tests {
    use std::sync::atomic;

    use super::*;
    use super::super::range::IntervalRange;

    static RANGE_INDEX: atomic::AtomicU64 = atomic::AtomicU64::new(1);

    fn new_range() -> Range {
        use byteorder::{BigEndian, WriteBytesExt};

        let v = RANGE_INDEX.fetch_add(2, atomic::Ordering::SeqCst);
        let mut r = IntervalRange::from(("", ""));
        r.lower_inclusive.write_u64::<BigEndian>(v).unwrap();
        r.upper_exclusive.write_u64::<BigEndian>(v + 2).unwrap();
        Range::Interval(r)
    }

    #[test]
    fn test_basic() {
        // Empty
        let mut c = RangesIterator::new(vec![]);
        assert_eq!(c.next(), IterStatus::Drained);
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
        assert_eq!(c.next(), IterStatus::Drained);

        // Non-empty
        let ranges = vec![new_range(), new_range(), new_range()];
        let mut c = RangesIterator::new(ranges.clone());
        assert_eq!(c.next(), IterStatus::NewRange(ranges[0].clone()));
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::NewRange(ranges[1].clone()));
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        c.notify_drained(); // multiple consumes will not take effect
        assert_eq!(c.next(), IterStatus::NewRange(ranges[2].clone()));
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
    }
}
