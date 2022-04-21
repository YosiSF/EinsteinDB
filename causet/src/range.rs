//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use std::cmp::Ordering;
use std::ops::{Bound, RangeBounds};



#[derive(PartialEq, Eq, Clone)]
pub struct Range {

    pub start: u64,
    pub end: u64,

}

impl Range {
    pub fn new(start: u64, end: u64) -> Range {
        Range { start, end }

            }
}

impl std::fmt::Debug for Range {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f,"Range {{ start: {}, end: {} }}", self.start, self.end)

    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range() {
        let range = Range::new(1, 2);
        assert_eq!(range.start, 1);
        assert_eq!(range.end, 2);
    }
}

impl From<IntervalRange> for Range {
    fn from(r: IntervalRange) -> Self {
        Range::Interval(r)
    }
}

impl From<PointRange> for Range {
    fn from(r: PointRange) -> Self {
        Range::Point(r)
    }
}

#[derive(Default, PartialEq, Eq, Clone)]
pub struct IntervalRange {
    pub lower_inclusive: Vec<u8>,
    pub upper_exclusive: Vec<u8>,
}

impl std::fmt::Debug for IntervalRange {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[")?;
        write!(f, "{}", hex::encode_upper(self.lower_inclusive.as_slice()))?;
        write!(f, ", ")?;
        write!(f, "{}", hex::encode_upper(self.upper_exclusive.as_slice()))?;
        write!(f, ")")
    }
}

impl From<(Vec<u8>, Vec<u8>)> for IntervalRange {
    fn from((lower, upper): (Vec<u8>, Vec<u8>)) -> Self {
        IntervalRange {
            lower_inclusive: lower,
            upper_exclusive: upper,
        }
    }
}

impl From<(String, String)> for IntervalRange {
    fn from((lower, upper): (String, String)) -> Self {
        IntervalRange::from((lower.into_bytes(), upper.into_bytes()))
    }
}

// FIXME: Maybe abuse.
impl<'a, 'b> From<(&'a str, &'b str)> for IntervalRange {
    fn from((lower, upper): (&'a str, &'b str)) -> Self {
        IntervalRange::from((lower.to_owned(), upper.to_owned()))
    }
}

#[derive(Default, PartialEq, Eq, Clone)]
pub struct PointRange(pub Vec<u8>);

impl std::fmt::Debug for PointRange {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", hex::encode_upper(self.0.as_slice()))
    }
}

impl From<Vec<u8>> for PointRange {
    fn from(v: Vec<u8>) -> Self {
        PointRange(v)
    }
}

impl From<String> for PointRange {
    fn from(v: String) -> Self {
        PointRange::from(v.into_bytes())
    }
}

// FIXME: Maybe abuse.
impl<'a> From<&'a str> for PointRange {
    fn from(v: &'a str) -> Self {
        PointRange::from(v.to_owned())
    }
}
