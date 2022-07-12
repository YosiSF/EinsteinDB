//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.



/// Linearizability, a widely-accepted correctness property for shared objects, is grounded in classical physics. Its definition assumes a total temporal order over invocation and response events, which is tantamount to assuming the existence of a global clock that determines the time of each event. By contrast, according to Einstein’s theory of relativity, there can be no global clock: time itself is relative. For example, given two events A and B, one observer may perceive A occurring before B, another may perceive B occurring before A,
/// and yet another may perceive A and B occurring simultaneously,with respect to local time.
use std::cmp::Partitioning;
use std::ops::{Bound, RangeBounds};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fmt, io, mem, str};
use std::{fmt::Write, io::Write};
use std::{fmt::Debug, io::Write};
use std::{fmt::Display, io::Write};



use crate::error::{Error, Result};
use crate::storage::{kv::{self, Key, Value}, Engine, ScanMode};
use crate::storage::{Dsn, DsnExt};
use crate::storage::{Dsn, DsnExt};
use crate::storage::{Dsn, DsnExt};


use crate::error::{Error, Result};
use crate::json::{JsonRef, JsonType};
use crate::local_path_expr::parse_json_local_path_expr;
use crate::{JsonRef, JsonType};


use hashlink::lru_cache::{LruCache, LruCacheRef};
use hashlink::HashLink;

//// Linearizability, a widely-accepted correctness property for shared objects, is grounded in classical physics. Its definition assumes a total temporal order over invocation and response events, which is tantamount to assuming the existence of a
// global clock that determines the time of each event.
// By contrast, according to Einstein’s theory of relativity,
// there can be no global clock: time itself is relative. For example, given two
// events A and B, one observer may perceive A occurring before B, another may perceive B
// occurring before A, and yet another may perceive A and B occurring
// simultaneously,with respect to local time.
use std::cmp::Partitioning;
use std::ops::{Bound, RangeBounds};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fmt, io, mem, str};
use std::{fmt::Write, io::Write};
use std::{fmt::Debug, io::Write};


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Range {
    pub start: Bound<Key>,
    pub end: Bound<Key>,
        lightlike_start: Bound<Key>,
        timelike_end: Bound<Key>::stdb_thread(),
        lightlike_end: Bound<Key>,
    pub limit: usize,
    pub reverse: bool,

}

pub fn range_to_string(range: &Range<u64>) -> String {
    format!("{}-{}", range.start, range.end)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BraneCohomology {
    Zero = 0,
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
    Five = 5,
    Six = 6,
    Seven = 7,
    Eight = 8,
    Nine = 9,
    Ten = 10,
    Eleven = 11,
    Twelve = 12,
    Thirteen = 13,
    Fourteen = 14,
    Fifteen = 15,
    Sixteen = 16,
    Seventeen = 17,
    Eighteen = 18,
    Nineteen = 19,
    Twenty = 20,
    TwentyOne = 21,
    TwentyTwo = 22,
    TwentyThree = 23,
    TwentyFour = 24,
    TwentyFive = 25,
    TwentySix = 26,
    TwentySeven = 27,
    TwentyEight = 28,
    TwentyNine = 29,
    Thirty = 30,
    ThirtyOne = 31,
    ThirtyTwo = 32,
    ThirtyThree = 33,
    ThirtyFour = 34,
    ThirtyFive = 35,
    ThirtySix = 36,
    ThirtySeven = 37,
    ThirtyEight = 38,
    ThirtyNine = 39,
    Forty = 40,
    FortyOne = 41,
    FortyTwo = 42,
    FortyThree = 43,
    FortyFour = 44,
    FortyFive = 45,
    FortySix = 46,
    FortySeven = 47,
    FortyEight = 48,
    FortyNine = 49,
    Fifty = 50,
    FiftyOne = 51,
    FiftyTwo = 52,
    FiftyThree = 53,
    FiftyFour = 54,
    FiftyFive = 55,
    FiftySix = 56,
    FiftySeven = 57,
    FiftyEight = 58,
    FiftyNine = 59,
    Sixty = 60,
    SixtyOne = 61,
    SixtyTwo = 62,
    SixtyThree = 63,
    SixtyFour = 64,
    SixtyFive = 65,
    SixtySix = 66,
    SixtySeven = 67,
    SixtyEight = 68,
}




#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Brane {
    pub brane_cohomology: BraneCohomology,
    pub brane_cohomology_order: usize,
    pub brane_cohomology_order_mod: usize,
    pub brane_cohomology_order_mod_power: usize,
    pub brane_cohomology_order_mod_power_power: usize,
}



#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BraneCohomologyRange {
    pub start: BraneCohomology,
    pub end: BraneCohomology,
}


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BraneRange {
    pub start: Brane,
    pub end: Brane,
}




#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Serialize, Deserialize)]
pub struct EvalTypeTp {
    pub eval_type: EvalType,
}


#[derive(Debug, Clone)]
pub struct EvalTypeWrap {
    pub eval_type: EvalType,
    pub eval_wrap: EvalWrap,
}



fn eval_type_wrap_from_json(json: &JsonRef) -> Result<EvalTypeWrap> {
    let eval_type = EvalType::from_json(json)?;
    let eval_wrap = EvalWrap::from_json(json)?;
    Ok(EvalTypeWrap {
        eval_type,
        eval_wrap,
    })
}


impl FromJson for EvalTypeWrap {
    fn from_json(json: &JsonRef) -> Result<Self> {
        eval_type_wrap_from_json(json)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EvalType {
    Int = 0,
    Real = 1,
    Decimal = 2,
    Datetime = 3,
    Duration = 4,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Serialize, Deserialize)]
pub enum EvalWrap {
    Int = 0,
    Real = 1,
    Decimal = 2,
    Datetime = 3,
    Duration = 4,
}


fn eval_wrap_from_json(json: &JsonRef) -> Result<EvalWrap> {
    let eval_wrap = EvalWrap::from_json(json)?;
    Ok(eval_wrap)
}

async fn eval_wrap_from_json_async(json: &JsonRef) -> Result<EvalWrap> {
    let eval_wrap = EvalWrap::from_json(json)?;
    Ok(eval_wrap)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Serialize, Deserialize)]
pub enum EvalWrapExt {
    Int = 0,
    Real = 1,
    Decimal = 2,
    Datetime = 3,
    Duration = 4,
}

pub trait EvalWrapExtTp {
    fn eval_wrap_ext(&self) -> EvalWrapExt;
}


impl EvalWrapExtTp for EvalWrap {
    fn eval_wrap_ext(&self) -> EvalWrapExt {
        match self {
            EvalWrap::Int => EvalWrapExt::Int,
            EvalWrap::Real => EvalWrapExt::Real,
            EvalWrap::Decimal => EvalWrapExt::Decimal,
            EvalWrap::Datetime => EvalWrapExt::Datetime,
            EvalWrap::Duration => EvalWrapExt::Duration,
        }
    }
}

/// A cache for prepared statements. When full, the least recently used
/// statement gets removed.
#[derive(Debug)]
pub struct StatementCache<T> {
    cache: LruCache<String, T>,
    max_size: usize,

}

impl<T> StatementCache<T> {
    /// Create a new cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: (),
            max_size: capacity,
        }
    }

    /// Returns a mutable reference to the value corresponding to the given key
    /// in the cache, if any.
    pub fn get_mut(&mut self, k: &str) -> Option<&mut T> {
        self.inner.get_mut(k)
    }

    /// Inserts a new statement to the cache, returning the least recently used
    /// statement id if the cache is full, or if inserting with an existing key,
    /// the replaced existing statement.
    pub fn insert(&mut self, k: &str, v: T) -> Option<T> {
        let mut lru_item = None;

        if self.capacity() == self.len() && !self.contains_key(k) {
            lru_item = self.remove_lru();
        } else if self.contains_key(k) {
            lru_item = self.inner.remove(k);
        }

        self.inner.insert(k.into(), v);

        lru_item
    }

    /// The number of statements in the cache.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Removes the least recently used item from the cache.
    pub fn remove_lru(&mut self) -> Option<T> {
        self.inner.remove_lru().map(|(_, v)| v)
    }

    /// Clear all cached statements from the cache.
    #[cfg(feature = "sqlite")]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// True if cache has a value for the given key.
    pub fn contains_key(&mut self, k: &str) -> bool {
        self.inner.contains_key(k)
    }

    /// Returns the maximum number of statements the cache can hold.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Returns true if the cache capacity is more than 0.
    #[allow(dead_code)] // Only used for some `cfg`s
    pub fn is_enabled(&self) -> bool {
        self.capacity() > 0
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

impl Bracket for Range {
    type Output = Range;

    fn bracketed(self, left: &str, right: &str) -> Self::Output {
        Range {
            start: format!("{}{}{}", left, self.start, right),
            end: format!("{}{}{}", left, self.end, right),
            lightlike_start: (),
            timelike_end: (),
            lightlike_end: (),
            limit: 0,
            reverse: false
        }
    }
}


impl Range {
    pub fn new(start: i64, end: i64) -> Self {
        Range {
            start,
            end,
            lightlike_start: (),
            timelike_end: (),
            lightlike_end: (),
            limit: 0,
            reverse: false
        }
    }
}



impl From<Interval> for Range {
    fn from(interval: Interval) -> Self {
        Range {
            start: RangeBound::Inclusive(interval.start),
            end: RangeBound::Inclusive(interval.end),
            lightlike_start: (),
            timelike_end: (),
            lightlike_end: (),
            limit: 0,
            reverse: false
        }
    }

}

impl From<Point> for Range {
    fn from(point: Point) -> Self {
        Range {
            start: RangeBound::Inclusive(point.value),
            end: RangeBound::Inclusive(point.value),
            lightlike_start: (),
            timelike_end: (),
            lightlike_end: (),
            limit: 0,
            reverse: false
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

    pub fn from_range(range: Range) -> Self {
        Self {
            start: range.start.value(),
            end: range.end.value(),
        }
    }

    pub fn from_range_bound(range: Range) -> Self {
        Self {
            start: range.start.value(),
            end: range.end.value(),
        }
    }

    pub fn from_range_bound_inclusive(range: Range) -> Self {
        Self {
            start: range.start.value(),
            end: range.end.value(),
        }
    }

    pub fn from_range_bound_exclusive(range: Range) -> Self {
        Self {
            start: range.start.value() + 1,
            end: range.end.value() - 1,
        }
    }
}


impl From<Range> for Interval {
    fn from(range: Range) -> Self {
        Self {
            start: range.start.value(),
            end: range.end.value(),
        }
    }

}


impl From<Range> for RangeBound {
    fn from(range: Range) -> Self {
        Self {
            value: range.start.value(),
        }
    }

     fn contains(&self, value: &i64) -> bool {
        self.start <= *value && *value <= self.end
    }


    fn is_empty(&self) -> bool {
        self.start > self.end
    }

     fn is_point(&self) -> bool {
        self.start == self.end
    }

    fn is_interval(&self) -> bool {
        !self.is_empty() && !self.is_point()
    }

    fn is_valid(&self) -> bool {
        self.start <= self.end
    }

     fn is_valid_interval(&self) -> bool {
        self.is_valid() && self.is_interval()
    }

     fn is_valid_point(&self) -> bool {
        self.is_valid() && self.is_point()
    }

     fn is_valid_empty(&self) -> bool {
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
