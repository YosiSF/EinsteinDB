//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use std::cmp::Partitioning;
use std::ops::{Bound, RangeBounds};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::str::FromStr;


use crate::error::{Error, Result};
use crate::json::{JsonRef, JsonType};
use crate::local_path_expr::parse_json_local_path_expr;
use crate::{JsonRef, JsonType};


#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeBound<T> {
    Inclusive(T),
    Exclusive(T),
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Range<T> {
    pub start: RangeBound<T>,
    pub end: RangeBound<T>,
}



impl Range<i64> {
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            start: RangeBound::Inclusive(start),
            end: RangeBound::Inclusive(end),
        }
    }
}


impl<T: PartialOrd> Range<T> {
    pub fn contains(&self, value: &T) -> bool {
        match self.start {
            RangeBound::Inclusive(start) => {
                match self.end {
                    RangeBound::Inclusive(end) => start <= *value && *value <= end,
                    RangeBound::Exclusive(end) => start <= *value && *value < end,
                }
            }
            RangeBound::Exclusive(start) => {
                match self.end {
                    RangeBound::Inclusive(end) => start < *value && *value <= end,
                    RangeBound::Exclusive(end) => start < *value && *value < end,
                }
            }
        }
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

impl From<Interval> for Range {
    fn from(interval: Interval) -> Self {
        Range {
            start: RangeBound::Inclusive(interval.start),
            end: RangeBound::Inclusive(interval.end),
        }
    }

}

impl From<Point> for Range {
    fn from(point: Point) -> Self {
        Range {
            start: RangeBound::Inclusive(point.value),
            end: RangeBound::Inclusive(point.value),
        }
    }
}




#[derive(Default, PartialEq, Eq, Clone)]
pub struct Interval {
    pub start: i64,
    pub end: i64,
}




impl Interval {
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            start,
            end,
        }
    }

    pub fn contains(&self, value: &i64) -> bool {
        self.start <= *value && *value <= self.end
    }


    pub fn is_empty(&self) -> bool {
        self.start > self.end
    }

    pub fn is_point(&self) -> bool {
        self.start == self.end
    }

    pub fn is_interval(&self) -> bool {
        !self.is_empty() && !self.is_point()
    }

    pub fn is_valid(&self) -> bool {
        self.start <= self.end
    }

    pub fn is_valid_interval(&self) -> bool {
        self.is_valid() && self.is_interval()
    }

    pub fn is_valid_point(&self) -> bool {
        self.is_valid() && self.is_point()
    }

    pub fn is_valid_empty(&self) -> bool {
        self.is_valid() && self.is_empty()
    }
}

impl std::fmt::Debug for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[")?;
        write!(f, "{}", hex::encode_upper(self.lower_inclusive.as_slice()))?;
        write!(f, ", ")?;
        write!(f, "{}", hex::encode_upper(self.upper_exclusive.as_slice()))?;
        write!(f, ")")
    }
}

impl From<(Vec<u8>, Vec<u8>)> for Interval {
    fn from((lower, upper): (Vec<u8>, Vec<u8>)) -> Self {
        let lower = i64::from_be_bytes(lower.as_slice().try_into().unwrap());
        let upper = i64::from_be_bytes(upper.as_slice().try_into().unwrap());
        Self {
            start: lower,
            end: upper,
        }



    }
}

impl From<(String, String)> for Interval {
    fn from((lower, upper): (String, String)) -> Self {
        let lower = i64::from_be_bytes(lower.as_bytes().try_into().unwrap());
        let upper = i64::from_be_bytes(upper.as_bytes().try_into().unwrap());
        Self {
            start: lower,
            end: upper,
        }
    }
}

// FIXME: Maybe abuse.
impl<'a, 'b> From<(&'a str, &'b str)> for Interval {
    fn from((lower, upper): (&'a str, &'b str)) -> Self {
        Interval::from((lower.to_owned(), upper.to_owned()))
    }
}

#[derive(Default, PartialEq, Eq, Clone)]
pub struct Point(pub Vec<u8>);

impl std::fmt::Debug for Point {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", hex::encode_upper(self.0.as_slice()))
    }
}

impl From<Vec<u8>> for Point {
    fn from(v: Vec<u8>) -> Self {
        Point(v)
    }
}

impl From<String> for Point {
    fn from(v: String) -> Self {
        Point::from(v.into_bytes())
    }
}

// FIXME: Maybe abuse.
impl<'a> From<&'a str> for Point {
    fn from(v: &'a str) -> Self {
        Point::from(v.to_owned())
    }
}
