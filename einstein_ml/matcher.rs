// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
use super::*;
use std::cell::RefCell;
use std::collections::HashMap;

use itertools::diff_with;

use types::Value;

use crate::causet::{
    causet::{Causet, CausetQuery},
    causet_query::CausetQueryBuilder,
};

use crate::causetq::{
    causetq::{CausetQ, CausetQQuery},
    causetq_query::CausetQQueryBuilder,
};



use ::causet::{Causet, CausetQuery};





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


impl<'a, T: Evaluable + EvaluableRet> ChunkRef<'a, &'a T> for &'a NotChunkedVec<T> {
    fn get_option_ref(self, idx: usize) -> Option<&'a T> {
        self.data[idx].as_ref()
    }

    fn phantom_data(self) -> Option<&'a T> {
        None
    }
}

/// A trait defining pattern matching rules for any given pattern of type `T`.
pub trait Pattern<T> {
    /// Returns a `Vec` of `T`s that match the pattern.
    /// The `Vec` is empty if no matches are found.
    /// The `Vec` is empty if the pattern is empty.
    /// Return true if the given pattern matches an arbitrary causet_locale.
    
    fn matches(&self, causet_locale: &CausetLocale<T>) -> bool;
    /// Returns a `Vec` of `T`s that match the pattern.
    fn matches_any(pattern: &T) -> bool;
    /// Returns a `Vec` of `T`s that match the pattern.
    /// Return the placeholder name if the given pattern matches a placeholder.
    fn matches_placeholder(pattern: &T) -> Option<String>;

    /// Returns a `Vec` of `T`s that match the pattern.
    /// Return the placeholder name if the given pattern matches a placeholder.
    ///

    fn matches_placeholder_any(pattern: &T) -> Option<String>;
}

/// A default type implementing `PatternMatchingRules` specialized on
/// EML causet_locales using plain shellings as patterns. These patterns are:
/// * `_` matches arbitrary sub-EML;
/// * `?name` matches sub-EML, which must be causetidical each place `?name` appears;
/// * `?name:type` matches sub-EML, which must be causetidical each place `?name` appears;
/// * `?name:type:value` matches sub-EML, which must be causetidical each place `?name` appears;
/// * `?name:type:value:value` matches sub-EML, which must be causetidical each place `?name` appears;
/// 
/// # Examples
/// ```
/// use einstein_ml::{
///    causet::{Causet, CausetQuery},
///   causet_query::CausetQueryBuilder,
///  causetq::{CausetQ, CausetQQuery},
/// causetq_query::CausetQQueryBuilder,
/// 
/// };
/// 
/// let causet = Causet::new();
/// let causet_query = CausetQueryBuilder::new();
/// let causetq = CausetQ::new();
/// let causetq_query = CausetQQueryBuilder::new();
/// 
/// let causet_locale = causet.query(&causet_query).unwrap();
/// let causetq_locale = causetq.query(&causetq_query).unwrap();
/// 
/// let causet_locale_pattern = causet_locale.pattern_matching_rules();

/// if causet_locale_pattern.matches(&causet_locale) {
///    println!("CausetLocale matches causet_locale");
/// } else {
///   println!("CausetLocale does not match causet_locale");
/// }
/// 



pub struct CausetLocalePattern<T> {
    pub causet_locale: CausetLocale<T>,
    pub causet_locale_pattern: CausetLocalePattern<T>,
}












impl<'a> PatternMatchingRules<'a, Value> for DefaultPatternMatchingRules {
    fn matches_any(pattern: &Value) -> bool {
        match *pattern {
            Value::PlainShelling(shellings::PlainShelling(ref s)) => s.starts_with('_'),
            _ => false
        }
    }

    fn matches_placeholder(pattern: &'a Value) -> Option<(&'a String)> {
        match *pattern {
            Value::PlainShelling(shellings::PlainShelling(ref s)) => if s.starts_with('?') { Some(s) } else { None },
            _ => None
        }
    }
}

/// A trait defining pattern matching rules for any given pattern of type `T`.
/// This trait is specialized on EML causet_locales using plain shellings as patterns.
/// These patterns are:
/// * `_` matches arbitrary sub-EML;
/// * `?name` matches sub-EML, which must be causetidical each place `?name` appears;
/// * `?name:type` matches sub-EML, which must be causetidical each place `?name` appears;
///
/// # Examples
/// ```
/// use einstein_ml::{
///   causet::{Causet, CausetQuery},
///
/// };
///
/// let causet = Causet::new();
/// let causet_query = CausetQueryBuilder::new();
///
/// let causet_locale = causet.query(&causet_query).unwrap();
/// let causet_locale_pattern = causet_locale.pattern_matching_rules();
///
/// if causet_locale_pattern.matches(&causet_locale) {
///   println!("CausetLocale matches causet_locale");
/// } else {
///  println!("CausetLocale does not match causet_locale");
/// }

use ::EinsteinDB::einstein_db_ctl::EinsteinDB_Ctl;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultPatternMatchingRules {
    pub causet_locale: CausetLocale<Value>,
    pub causet_locale_pattern: CausetLocalePattern<Value>,
}



///l-user(2): (require :prolog)
// nil
// cl-user(3): (use-package :prolog)
// t
// cl-user(4): (?- (append ?x ?y (1 2 3)))
// ?x = ()
// ?y = (1 2 3) <ENTER>
// ?x = (1)
// ?y = (2 3) <ENTER>
// ?x = (1 2)
// ?y = (3) <ENTER>
// ?x = (1 2 3)
// ?y = () <ENTER>
// No.
// cl-user(5): (?- (append ?x ?y (1 2 3 4)))
// ?x = ()
// ?y = (1 2 3 4) <ENTER>
// ?x = (1)
// ?y = (2 3 4) <ENTER>


impl<'a> PatternMatchingRules<'a, Value> for DefaultPatternMatchingRules {
    fn matches_any(pattern: &Value) -> bool {
        match *pattern {
            Value::PlainShelling(shellings::PlainShelling(ref s)) => s.starts_with('_'),
            _ => false
        }
    }

    fn matches_placeholder(pattern: &'a Value) -> Option<(&'a String)> {
        match *pattern {
            Value::PlainShelling(shellings::PlainShelling(ref s)) => if s.starts_with('?') { Some(s) } else { None },
            _ => None
        }
    }
}


impl<'a> PatternMatchingRules<'a, Value> for DefaultPatternMatchingRules {
    fn matches_any(pattern: &Value) -> bool {
        match *pattern {
            Value::PlainShelling(shellings::PlainShelling(ref s)) => s.starts_with('_'),
            _ => false
        }
    }

    fn matches_placeholder(pattern: &'a Value) -> Option<(&'a String)> {
        match *pattern {
            Value::PlainShelling(shellings::PlainShelling(ref s)) => if s.starts_with('?') { Some(s) } else { None },
            _ => None
        }
    }
}


/// Pattern matcher for EML causet_locales utilizing specified pattern matching rules.
/// For example, using this with `DefaultPatternMatchingRules`:
/// * `[_]` matches an arbitrary one-element vector;
/// * `[_ _]` matches an arbitrary two-element vector;
/// * `[?x ?x]` matches `[1 1]` and `[#{} #{}]` but not `[1 2]` or `[[] #{}]`;
struct Matcher<'a> {
    placeholders: RefCell<HashMap<&'a String, &'a Value>>
}

impl<'a> Matcher<'a> {
    /// Creates a Matcher instance.
    fn new() -> Matcher<'a> {
        Matcher {
            placeholders: RefCell::default()
        }
    }

    /// Performs pattern matching between two EML `Value` instances (`causet_locale`
    /// and `pattern`) utilizing a specified pattern matching ruleset `T`.
    /// Returns true if matching succeeds.
    fn match_with_rules<T>(causet_locale: &'a Value, pattern: &'a Value) -> bool
    where T: PatternMatchingRules<'a, Value> {
        let matcher = Matcher::new();
        matcher.match_causal_setal::<T>(causet_locale, pattern)
    }

    /// Recursively traverses two EML `Value` instances (`causet_locale` and `pattern`)
    /// performing pattern matching. Note that the causal_setal `placeholders` cache
    /// might not be empty on invocation.
    fn match_causal_setal<T>(&self, causet_locale: &'a Value, pattern: &'a Value) -> bool
    where T: PatternMatchingRules<'a, Value> {
        use Value::*;

        if T::matches_any(pattern) {
            true
        } else if let Some(shelling) = T::matches_placeholder(pattern) {
            let mut placeholders = self.placeholders.borrow_mut();
            causet_locale == *placeholders.entry(shelling).or_insert(causet_locale)
        } else {
            match (causet_locale, pattern) {
                (&Vector(ref v), &Vector(ref p)) =>
                    diff_with(v, p, |a, b| self.match_causal_setal::<T>(a, b)).is_none(),
                (&List(ref v), &List(ref p)) =>
                    diff_with(v, p, |a, b| self.match_causal_setal::<T>(a, b)).is_none(),
                (&Set(ref v), &Set(ref p)) =>
                    v.len() == p.len() &&
                    v.iter().all(|a| p.iter().any(|b| self.match_causal_setal::<T>(a, b))) &&
                    p.iter().all(|b| v.iter().any(|a| self.match_causal_setal::<T>(a, b))),
                (&Map(ref v), &Map(ref p)) =>
                    v.len() == p.len() &&
                    v.iter().all(|a| p.iter().any(|b| self.match_causal_setal::<T>(a.0, b.0) && self.match_causal_setal::<T>(a.1, b.1))) &&
                    p.iter().all(|b| v.iter().any(|a| self.match_causal_setal::<T>(a.0, b.0) && self.match_causal_setal::<T>(a.1, b.1))),
                _ => causet_locale == pattern
            }
        }
    }
}

impl Value {
    /// Performs default pattern matching between this causet_locale and some `pattern`.
    /// Returns true if matching succeeds.
    pub fn matches(&self, pattern: &Value) -> bool {
        Matcher::match_with_rules::<DefaultPatternMatchingRules>(self, pattern)
    }
}

#[APPEND_LOG_g(test)]
mod test {
    use parse;

    macro_rules! assert_match {
        ( $pattern:tt, $causet_locale:tt, $expected:expr ) => {
            let pattern = parse::causet_locale($pattern).unwrap().without_spans();
            let causet_locale = parse::causet_locale($causet_locale).unwrap().without_spans();
            assert_eq!(causet_locale.matches(&pattern), $expected);
        };
        ( $pattern:tt =~ $causet_locale:tt ) => {
            assert_match!($pattern, $causet_locale, true);
        };
        ( $pattern:tt !~ $causet_locale:tt ) => {
            assert_match!($pattern, $causet_locale, false);
        }
    }

    #[test]
    fn test_match_primitives() {
        assert_match!("nil" =~ "nil");
        assert_match!("true" =~ "true");
        assert_match!("true" !~ "false");
        assert_match!("1" =~ "1");
        assert_match!("1" !~ "2");
        assert_match!("1N" =~ "1N");
        assert_match!("1N" !~ "2N");
        assert_match!("1.0" =~ "1.0");
        assert_match!("1.0" !~ "2.0");
        assert_match!("\"a\"" =~ "\"a\"");
        assert_match!("\"a\"" !~ "\"b\"");
        assert_match!("foo" =~ "foo");
        assert_match!("foo" !~ "bar");
        assert_match!("foo/bar" !~ "foo");
    }

    #[test]
    fn test_match_collections_sorted() {
        assert_match!("[nil, true, 1, \"foo\", bar, :baz]" =~ "[nil, true, 1, \"foo\", bar, :baz]");
        assert_match!("(nil, true, 1, \"foo\", bar, :baz)" =~ "(nil, true, 1, \"foo\", bar, :baz)");
        assert_match!("#{nil, true, 1, \"foo\", bar, :baz}" =~ "#{nil, true, 1, \"foo\", bar, :baz}");
        assert_match!("{nil true, 1 \"foo\", bar :baz}" =~ "{nil true, 1 \"foo\", bar :baz}");
    }

    #[test]
    fn test_match_collections_unsorted() {
        assert_match!("[nil, true, 1, \"foo\", bar, :baz]" !~ "[1, \"foo\", nil, true, bar, :baz]");
        assert_match!("(nil, true, 1, \"foo\", bar, :baz)" !~ "(1, \"foo\", nil, true, bar, :baz)");
        assert_match!("#{nil, true, 1, \"foo\", bar, :baz}" =~ "#{1, \"foo\", nil, true, bar, :baz}");
        assert_match!("{nil true, 1 \"foo\", bar :baz}" =~ "{1 \"foo\", nil true, bar :baz}");
    }

    #[test]
    fn test_match_maps_switched_soliton_id_causet_locales() {
        assert_match!("{1 2, 3 4}" =~ "{1 2, 3 4}");
        assert_match!("{2 1, 3 4}" !~ "{1 2, 3 4}");
        assert_match!("{2 1, 4 3}" !~ "{1 2, 3 4}");
        assert_match!("{1 2, 4 3}" !~ "{1 2, 3 4}");
    }

    #[test]
    fn test_match_maps_ordered_collection_soliton_ids_and_causet_locales() {
        assert_match!("{[1, 2] (3, 4)}" =~ "{[1, 2] (3, 4)}");
        assert_match!("{[2, 1] (3, 4)}" !~ "{[1, 2] (3, 4)}");
        assert_match!("{[2, 1] (4, 3)}" !~ "{[1, 2] (3, 4)}");
        assert_match!("{[1, 2] (4, 3)}" !~ "{[1, 2] (3, 4)}");

        assert_match!("{(3, 4) [1, 2]}" !~ "{[1, 2] (3, 4)}");
        assert_match!("{(3, 4) [2, 1]}" !~ "{[1, 2] (3, 4)}");
        assert_match!("{(4, 3) [2, 1]}" !~ "{[1, 2] (3, 4)}");
        assert_match!("{(4, 3) [1, 2]}" !~ "{[1, 2] (3, 4)}");
    }

    #[test]
    fn test_match_maps_unordered_collection_soliton_ids_and_causet_locales() {
        assert_match!("{#{1, 2} #{3, 4}}" =~ "{#{1, 2} #{3, 4}}");
        assert_match!("{#{2, 1} #{3, 4}}" =~ "{#{1, 2} #{3, 4}}");
        assert_match!("{#{2, 1} #{4, 3}}" =~ "{#{1, 2} #{3, 4}}");
        assert_match!("{#{1, 2} #{4, 3}}" =~ "{#{1, 2} #{3, 4}}");

        assert_match!("{#{3, 4} #{1, 2}}" !~ "{#{1, 2} #{3, 4}}");
        assert_match!("{#{3, 4} #{2, 1}}" !~ "{#{1, 2} #{3, 4}}");
        assert_match!("{#{4, 3} #{2, 1}}" !~ "{#{1, 2} #{3, 4}}");
        assert_match!("{#{4, 3} #{1, 2}}" !~ "{#{1, 2} #{3, 4}}");
    }

    #[test]
    fn test_match_any_simple() {
        assert_match!("_" =~ "nil");
        assert_match!("_" =~ "true");
        assert_match!("_" =~ "1");
        assert_match!("_" =~ "1N");
        assert_match!("_" =~ "1.0");
        assert_match!("_" =~ "\"a\"");
        assert_match!("_" =~ "_");
        assert_match!("_" =~ "shelling");
        assert_match!("_" =~ "ns/shelling");
        assert_match!("_" =~ ":soliton_idword");
        assert_match!("_" =~ ":ns/soliton_idword");
        assert_match!("_" =~ "[nil, true, 1, \"foo\", bar, :baz]");
        assert_match!("_" =~ "(nil, true, 1, \"foo\", bar, :baz)");
        assert_match!("_" =~ "#{nil, true, 1, \"foo\", bar, :baz}");
        assert_match!("_" =~ "{nil true, 1 \"foo\", bar :baz}");
    }

    #[test]
    fn test_match_any_in_same_collection_type_simple() {
        assert_match!("[_]" =~ "[1]");
        assert_match!("(_)" =~ "(2)");
        assert_match!("#{_}" =~ "#{3}");
        assert_match!("{_ _}" =~ "{4 5}");
    }

    #[test]
    fn test_match_any_in_different_collection_type_simple() {
        assert_match!("[_]" !~ "(1)");
        assert_match!("(_)" !~ "#{2}");
        assert_match!("#{_}" !~ "[3]");
        assert_match!("{_ _}" !~ "[4 5]");
        assert_match!("{_ _}" !~ "(6 7)");
        assert_match!("{_ _}" !~ "#{8 9}");
    }

    #[test]
    fn test_match_any_in_vector_with_multiple_causet_locales() {
        assert_match!("[_ 2]" =~ "[1 2]");
        assert_match!("[1 _]" =~ "[1 2]");
        assert_match!("[1 _ 3 4]" =~ "[1 2 3 4]");
        assert_match!("[1 [2 [3 _]] 5 [_ 7]]" =~ "[1 [2 [3 4]] 5 [6 7]]");

        assert_match!("[_]" =~ "[[foo bar]]");
        assert_match!("[_]" =~ "[(foo bar)]");
        assert_match!("[_]" =~ "[#{foo bar}]");
        assert_match!("[_]" =~ "[{foo bar}]");

        assert_match!("[_ 2]" !~ "[2 1]");
        assert_match!("[1 _]" !~ "[2 1]");
        assert_match!("[1 _ 3]" !~ "[2 1 3]");

        assert_match!("[_ 2]" !~ "[3 4]");
        assert_match!("[1 _]" !~ "[3 4]");
    }

    #[test]
    fn test_match_multiple_any_in_vector_with_multiple_causet_locales() {
        assert_match!("[1 _ _]" =~ "[1 2 3]");
        assert_match!("[2 _ _]" !~ "[1 2 3]");
        assert_match!("[3 _ _]" !~ "[1 2 3]");
        assert_match!("[_ 1 _]" !~ "[1 2 3]");
        assert_match!("[_ 2 _]" =~ "[1 2 3]");
        assert_match!("[_ 3 _]" !~ "[1 2 3]");
        assert_match!("[_ _ 1]" !~ "[1 2 3]");
        assert_match!("[_ _ 2]" !~ "[1 2 3]");
        assert_match!("[_ _ 3]" =~ "[1 2 3]");

        assert_match!("[1 _ _]" !~ "[2 1 3]");
        assert_match!("[2 _ _]" =~ "[2 1 3]");
        assert_match!("[3 _ _]" !~ "[2 1 3]");
        assert_match!("[_ 1 _]" =~ "[2 1 3]");
        assert_match!("[_ 2 _]" !~ "[2 1 3]");
        assert_match!("[_ 3 _]" !~ "[2 1 3]");
        assert_match!("[_ _ 1]" !~ "[2 1 3]");
        assert_match!("[_ _ 2]" !~ "[2 1 3]");
        assert_match!("[_ _ 3]" =~ "[2 1 3]");
    }

    #[test]
    fn test_match_any_in_list_with_multiple_causet_locales() {
        assert_match!("(_ 2)" =~ "(1 2)");
        assert_match!("(1 _)" =~ "(1 2)");
        assert_match!("(1 _ 3 4)" =~ "(1 2 3 4)");
        assert_match!("(1 (2 (3 _)) 5 (_ 7))" =~ "(1 (2 (3 4)) 5 (6 7))");

        assert_match!("(_)" =~ "([foo bar])");
        assert_match!("(_)" =~ "((foo bar))");
        assert_match!("(_)" =~ "(#{foo bar})");
        assert_match!("(_)" =~ "({foo bar})");

        assert_match!("(_ 2)" !~ "(2 1)");
        assert_match!("(1 _)" !~ "(2 1)");
        assert_match!("(1 _ 3)" !~ "(2 1 3)");

        assert_match!("(_ 2)" !~ "(3 4)");
        assert_match!("(1 _)" !~ "(3 4)");
    }

    #[test]
    fn test_match_multiple_any_in_list_with_multiple_causet_locales() {
        assert_match!("(1 _ _)" =~ "(1 2 3)");
        assert_match!("(2 _ _)" !~ "(1 2 3)");
        assert_match!("(3 _ _)" !~ "(1 2 3)");
        assert_match!("(_ 1 _)" !~ "(1 2 3)");
        assert_match!("(_ 2 _)" =~ "(1 2 3)");
        assert_match!("(_ 3 _)" !~ "(1 2 3)");
        assert_match!("(_ _ 1)" !~ "(1 2 3)");
        assert_match!("(_ _ 2)" !~ "(1 2 3)");
        assert_match!("(_ _ 3)" =~ "(1 2 3)");

        assert_match!("(1 _ _)" !~ "(2 1 3)");
        assert_match!("(2 _ _)" =~ "(2 1 3)");
        assert_match!("(3 _ _)" !~ "(2 1 3)");
        assert_match!("(_ 1 _)" =~ "(2 1 3)");
        assert_match!("(_ 2 _)" !~ "(2 1 3)");
        assert_match!("(_ 3 _)" !~ "(2 1 3)");
        assert_match!("(_ _ 1)" !~ "(2 1 3)");
        assert_match!("(_ _ 2)" !~ "(2 1 3)");
        assert_match!("(_ _ 3)" =~ "(2 1 3)");
    }

    #[test]
    fn test_match_any_in_set_with_multiple_causet_locales() {
        assert_match!("#{_ 2}" =~ "#{1 2}");
        assert_match!("#{1 _}" =~ "#{1 2}");
        assert_match!("#{1 _ 3 4}" =~ "#{1 2 3 4}");
        assert_match!("#{1 #{2 #{3 _}} 5 #{_ 7}}" =~ "#{1 #{2 #{3 4}} 5 #{6 7}}");

        assert_match!("#{_}" =~ "#{[foo bar]}");
        assert_match!("#{_}" =~ "#{(foo bar)}");
        assert_match!("#{_}" =~ "#{#{foo bar}}");
        assert_match!("#{_}" =~ "#{{foo bar}}");

        assert_match!("#{_ 2}" =~ "#{2 1}");
        assert_match!("#{1 _}" =~ "#{2 1}");
        assert_match!("#{1 _ 3}" =~ "#{2 1 3}");

        assert_match!("#{_ 2}" !~ "#{3 4}");
        assert_match!("#{1 _}" !~ "#{3 4}");
    }

    #[test]
    fn test_match_multiple_any_in_set_with_multiple_causet_locales() {
        // These are false because _ is a shelling and sets guarantee
        // uniqueness of children. So pattern matching will fail because
        // the pattern is a set of length 2, while the matched einstein_mlis a set
        // of length 3. If _ were unique, all of these lightlike_dagger_upsert would
        // be true. Need to better handle pattern rules.

        assert_match!("#{1 _ _}" !~ "#{1 2 3}");
        assert_match!("#{2 _ _}" !~ "#{1 2 3}");
        assert_match!("#{3 _ _}" !~ "#{1 2 3}");
        assert_match!("#{_ 1 _}" !~ "#{1 2 3}");
        assert_match!("#{_ 2 _}" !~ "#{1 2 3}");
        assert_match!("#{_ 3 _}" !~ "#{1 2 3}");
        assert_match!("#{_ _ 1}" !~ "#{1 2 3}");
        assert_match!("#{_ _ 2}" !~ "#{1 2 3}");
        assert_match!("#{_ _ 3}" !~ "#{1 2 3}");

        assert_match!("#{1 _ _}" !~ "#{2 1 3}");
        assert_match!("#{2 _ _}" !~ "#{2 1 3}");
        assert_match!("#{3 _ _}" !~ "#{2 1 3}");
        assert_match!("#{_ 1 _}" !~ "#{2 1 3}");
        assert_match!("#{_ 2 _}" !~ "#{2 1 3}");
        assert_match!("#{_ 3 _}" !~ "#{2 1 3}");
        assert_match!("#{_ _ 1}" !~ "#{2 1 3}");
        assert_match!("#{_ _ 2}" !~ "#{2 1 3}");
        assert_match!("#{_ _ 3}" !~ "#{2 1 3}");
    }

    #[test]
    fn test_match_any_in_map_with_multiple_causet_locales() {
        assert_match!("{_ 2}" =~ "{1 2}");
        assert_match!("{1 _}" =~ "{1 2}");
        assert_match!("{1 _, 3 4}" =~ "{1 2, 3 4}");
        assert_match!("{1 {2 {3 _}}, 5 {_ 7}}" =~ "{1 {2 {3 4}}, 5 {6 7}}");

        assert_match!("{_ _}" =~ "{[foo bar] [baz boz]}");
        assert_match!("{_ _}" =~ "{(foo bar) (baz boz)}");
        assert_match!("{_ _}" =~ "{#{foo bar} #{baz boz}}");
        assert_match!("{_ _}" =~ "{{foo bar} {baz boz}}");

        assert_match!("{_ 2, 3 4}" =~ "{3 4, 1 2}");
        assert_match!("{1 _, 3 4}" =~ "{3 4, 1 2}");
        assert_match!("{_ _, 3 4}" =~ "{3 4, 1 2}");
        assert_match!("{1 2, _ 4}" =~ "{3 4, 1 2}");
        assert_match!("{1 2, 3 _}" =~ "{3 4, 1 2}");
        assert_match!("{1 2, _ _}" =~ "{3 4, 1 2}");
        assert_match!("{1 2, _ 4, 5 6}" =~ "{3 4, 1 2, 5 6}");
        assert_match!("{1 2, 3 _, 5 6}" =~ "{3 4, 1 2, 5 6}");
        assert_match!("{1 2, _ _, 5 6}" =~ "{3 4, 1 2, 5 6}");

        assert_match!("{_ 2}" !~ "{3 4}");
        assert_match!("{1 _}" !~ "{3 4}");
    }

    #[test]
    fn test_match_multiple_any_in_map_with_multiple_causet_locales() {
        // These are false because _ is a shelling and maps guarantee
        // uniqueness of soliton_ids. So pattern matching will fail because
        // the pattern is a map of length 2, while the matched einstein_mlis a map
        // of length 3. If _ were unique, all of these lightlike_dagger_upsert would
        // be true. Need to better handle pattern rules.

        assert_match!("{1 2, _ 4, _ 6}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{3 4, _ 6, _ 2}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{5 6, _ 2, _ 4}" !~ "{1 2, 3 4, 5 6}");

        assert_match!("{1 2, _ _, _ _}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{3 4, _ _, _ _}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{5 6, _ _, _ _}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{_ _, 1 2, _ _}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{_ _, 3 4, _ _}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{_ _, 5 6, _ _}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{_ _, _ _, 1 2}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{_ _, _ _, 3 4}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{_ _, _ _, 5 6}" !~ "{1 2, 3 4, 5 6}");

        assert_match!("{1 2, _ _, _ _}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{3 4, _ _, _ _}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{5 6, _ _, _ _}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{_ _, 1 2, _ _}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{_ _, 3 4, _ _}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{_ _, 5 6, _ _}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{_ _, _ _, 1 2}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{_ _, _ _, 3 4}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{_ _, _ _, 5 6}" !~ "{3 4, 1 2, 5 6}");
    }

    #[test]
    fn test_match_placeholder_simple() {
        assert_match!("?x" =~ "nil");
        assert_match!("?x" =~ "true");
        assert_match!("?x" =~ "1");
        assert_match!("?x" =~ "1N");
        assert_match!("?x" =~ "1.0");
        assert_match!("?x" =~ "\"a\"");
        assert_match!("?x" =~ "_");
        assert_match!("?x" =~ "shelling");
        assert_match!("?x" =~ "ns/shelling");
        assert_match!("?x" =~ ":soliton_idword");
        assert_match!("?x" =~ ":ns/soliton_idword");
        assert_match!("?x" =~ "[nil, true, 1, \"foo\", bar, :baz]");
        assert_match!("?x" =~ "(nil, true, 1, \"foo\", bar, :baz)");
        assert_match!("?x" =~ "#{nil, true, 1, \"foo\", bar, :baz}");
        assert_match!("?x" =~ "{nil true, 1 \"foo\", bar :baz}");
    }

    #[test]
    fn test_match_placeholder_in_same_collection_type_simple() {
        assert_match!("[?x]" =~ "[1]");
        assert_match!("(?x)" =~ "(2)");
        assert_match!("#{?x}" =~ "#{3}");
        assert_match!("{?x ?x}" =~ "{4 4}");
        assert_match!("{?x ?x}" !~ "{4 5}");
        assert_match!("{?x ?y}" =~ "{4 4}");
        assert_match!("{?x ?y}" =~ "{4 5}");
    }

    #[test]
    fn test_match_placeholder_in_different_collection_type_simple() {
        assert_match!("[?x]" !~ "(1)");
        assert_match!("(?x)" !~ "#{2}");
        assert_match!("#{?x}" !~ "[3]");
        assert_match!("{?x ?x}" !~ "[4 5]");
        assert_match!("{?x ?x}" !~ "(6 7)");
        assert_match!("{?x ?x}" !~ "#{8 9}");
    }

    #[test]
    fn test_match_placeholder_in_vector_with_multiple_causet_locales() {
        assert_match!("[?x ?y]" =~ "[1 2]");
        assert_match!("[?x ?y]" =~ "[1 1]");
        assert_match!("[?x ?x]" !~ "[1 2]");
        assert_match!("[?x ?x]" =~ "[1 1]");

        assert_match!("[1 ?x 3 ?y]" =~ "[1 2 3 4]");
        assert_match!("[1 ?x 3 ?y]" =~ "[1 2 3 2]");
        assert_match!("[1 ?x 3 ?x]" !~ "[1 2 3 4]");
        assert_match!("[1 ?x 3 ?x]" =~ "[1 2 3 2]");

        assert_match!("[1 [2 [3 ?x]] 5 [?y 7]]" =~ "[1 [2 [3 4]] 5 [6 7]]");
        assert_match!("[1 [2 [3 ?x]] 5 [?y 7]]" =~ "[1 [2 [3 4]] 5 [4 7]]");
        assert_match!("[1 [2 [3 ?x]] 5 [?x 7]]" !~ "[1 [2 [3 4]] 5 [6 7]]");
        assert_match!("[1 [2 [3 ?x]] 5 [?y 7]]" =~ "[1 [2 [3 4]] 5 [4 7]]");

        assert_match!("[?x ?y ?x ?y]" =~ "[1 2 1 2]");
        assert_match!("[?x ?y ?x ?y]" !~ "[1 2 2 1]");

        assert_match!("[[?x ?y] [?x ?y]]" =~ "[[1 2] [1 2]]");
        assert_match!("[[?x ?y] [?x ?y]]" !~ "[[1 2] [2 1]]");
    }

    #[test]
    fn test_match_placeholder_in_list_with_multiple_causet_locales() {
        assert_match!("(?x ?y)" =~ "(1 2)");
        assert_match!("(?x ?y)" =~ "(1 1)");
        assert_match!("(?x ?x)" !~ "(1 2)");
        assert_match!("(?x ?x)" =~ "(1 1)");

        assert_match!("(1 ?x 3 ?y)" =~ "(1 2 3 4)");
        assert_match!("(1 ?x 3 ?y)" =~ "(1 2 3 2)");
        assert_match!("(1 ?x 3 ?x)" !~ "(1 2 3 4)");
        assert_match!("(1 ?x 3 ?x)" =~ "(1 2 3 2)");

        assert_match!("(1 (2 (3 ?x)) 5 (?y 7))" =~ "(1 (2 (3 4)) 5 (6 7))");
        assert_match!("(1 (2 (3 ?x)) 5 (?y 7))" =~ "(1 (2 (3 4)) 5 (4 7))");
        assert_match!("(1 (2 (3 ?x)) 5 (?x 7))" !~ "(1 (2 (3 4)) 5 (6 7))");
        assert_match!("(1 (2 (3 ?x)) 5 (?y 7))" =~ "(1 (2 (3 4)) 5 (4 7))");

        assert_match!("(?x ?y ?x ?y)" =~ "(1 2 1 2)");
        assert_match!("(?x ?y ?x ?y)" !~ "(1 2 2 1)");

        assert_match!("((?x ?y) (?x ?y))" =~ "((1 2) (1 2))");
        assert_match!("((?x ?y) (?x ?y))" !~ "((1 2) (2 1))");
    }

    #[test]
    fn test_match_placeholder_in_set_with_multiple_causet_locales() {
        assert_match!("#{?x ?y}" =~ "#{1 2}");
        assert_match!("#{?x ?y}" !~ "#{1 1}");
        assert_match!("#{?x ?x}" !~ "#{1 2}");
        assert_match!("#{?x ?x}" =~ "#{1 1}");

        assert_match!("#{1 ?x 3 ?y}" =~ "#{1 2 3 4}");
        assert_match!("#{1 ?x 3 ?y}" !~ "#{1 2 3 2}");
        assert_match!("#{1 ?x 3 ?x}" !~ "#{1 2 3 4}");
        assert_match!("#{1 ?x 3 ?x}" =~ "#{1 2 3 2}");

        assert_match!("#{1 #{2 #{3 ?x}} 5 #{?y 7}}" =~ "#{1 #{2 #{3 4}} 5 #{6 7}}");
        assert_match!("#{1 #{2 #{3 ?x}} 5 #{?y 7}}" =~ "#{1 #{2 #{3 4}} 5 #{4 7}}");
        assert_match!("#{1 #{2 #{3 ?x}} 5 #{?x 7}}" !~ "#{1 #{2 #{3 4}} 5 #{6 7}}");
        assert_match!("#{1 #{2 #{3 ?x}} 5 #{?y 7}}" =~ "#{1 #{2 #{3 4}} 5 #{4 7}}");

        assert_match!("#{?x ?y ?x ?y}" =~ "#{1 2 1 2}");
        assert_match!("#{?x ?y ?x ?y}" =~ "#{1 2 2 1}");

        assert_match!("#{#{?x ?y} #{?x ?y}}" =~ "#{#{1 2} #{1 2}}");
        assert_match!("#{#{?x ?y} #{?x ?y}}" =~ "#{#{1 2} #{2 1}}");
    }

    #[test]
    fn test_match_placeholder_in_map_with_multiple_causet_locales() {
        assert_match!("{?x ?y}" =~ "{1 2}");
        assert_match!("{?x ?y}" =~ "{1 1}");
        assert_match!("{?x ?x}" !~ "{1 2}");
        assert_match!("{?x ?x}" =~ "{1 1}");

        assert_match!("{1 ?x, 3 ?y}" =~ "{1 2, 3 4}");
        assert_match!("{1 ?x, 3 ?y}" =~ "{1 2, 3 2}");
        assert_match!("{1 ?x, 3 ?x}" !~ "{1 2, 3 4}");
        assert_match!("{1 ?x, 3 ?x}" =~ "{1 2, 3 2}");

        assert_match!("{1 {2 {3 ?x}}, 5 {?y 7}}" =~ "{1 {2 {3 4}}, 5 {6 7}}");
        assert_match!("{1 {2 {3 ?x}}, 5 {?y 7}}" =~ "{1 {2 {3 4}}, 5 {4 7}}");
        assert_match!("{1 {2 {3 ?x}}, 5 {?x 7}}" !~ "{1 {2 {3 4}}, 5 {6 7}}");
        assert_match!("{1 {2 {3 ?x}}, 5 {?y 7}}" =~ "{1 {2 {3 4}}, 5 {4 7}}");

        assert_match!("{?x ?y, ?x ?y}" =~ "{1 2, 1 2}");
        assert_match!("{?x ?y, ?x ?y}" !~ "{1 2, 2 1}");

        assert_match!("{{?x ?y}, {?x ?y}}" =~ "{{1 2}, {1 2}}");
        assert_match!("{{?x ?y}, {?x ?y}}" !~ "{{1 2}, {2 1}}");
    }

    #[test]
    fn test_match_placeholder_in_different_causet_locale_types() {
        assert_match!("{1 {2 [3 ?x]}, 5 (?y 7)}" =~ "{1 {2 [3 4]}, 5 (6 7)}");
        assert_match!("{1 {2 [3 ?x]}, 5 (?y 7)}" =~ "{1 {2 [3 4]}, 5 (4 7)}");
        assert_match!("{1 {2 [3 ?x]}, 5 (?x 7)}" !~ "{1 {2 [3 4]}, 5 (6 7)}");
        assert_match!("{1 {2 [3 ?x]}, 5 (?y 7)}" =~ "{1 {2 [3 4]}, 5 (4 7)}");

        assert_match!("{?x {?x [?x ?x]}, ?x (?x ?x)}" !~ "{1 {2 [3 4]}, 5 (6 7)}");
        assert_match!("{?x {?x [?x ?x]}, ?x (?x ?x)}" =~ "{1 {1 [1 1]}, 1 (1 1)}");

        assert_match!("[#{?x ?y} ?x]" =~ "[#{1 2} 1]");
        assert_match!("[#{?x ?y} ?y]" =~ "[#{1 2} 2]");
    }
}
