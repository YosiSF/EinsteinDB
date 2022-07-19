// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


///! This file contains the implementation of the `Fsm` trait.

// #[macro_export]
// macro_rules! einsteindb_macro {
//     ($($tokens:tt)*) => {
//         $crate::einsteindb_macro_impl!($($tokens)*)
//     };


// #[macro_export]
// macro_rules! einsteindb_macro_impl {
//     ($($tokens:tt)*) => {
//         $crate::einsteindb_macro_impl!($($tokens)*)
//     };




// #[macro_export]

#[macro_export]
macro_rules! einsteindb_macro {
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
}

#[macro_export]
macro_rules! einsteindb_macro_impl {
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
}



use std::borrow::Cow;
use std::io;
use std::ops::{Deref, DerefMut};

use chrono::SecondsFormat;
use itertools::Itertools;
use pretty;

use einstein_db_alexandrov_processing::{
    index::{
        Index,
        IndexIterator,
        IndexIteratorOptions,
        IndexIteratorOptionsBuilder,
    },
};


use berolina_sql::{
    parser::Parser,
    value::{Value, ValueType},
    error::{Error, Result},
    parser::ParserError,
    value::{ValueRef, ValueRefMut},
    fdb_traits::FdbTrait,
    fdb_traits::FdbTraitImpl,
    pretty,
    io,
    convert::{TryFrom, TryInto},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};



pub struct EinsteinDb<T: FdbTrait> {
    db: T,
}


impl<T: FdbTrait> EinsteinDb<T> {
    pub fn new(db: T) -> Self {
        EinsteinDb { db }
    }
}


impl<T: FdbTrait> Deref for EinsteinDb<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}


impl<T: FdbTrait> DerefMut for EinsteinDb<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}


impl<T: FdbTrait> FdbTraitImpl for EinsteinDb<T> {
    fn get_db(&self) -> &T {
        &self.db
    }
}


impl<T: FdbTrait> EinsteinDb<T> {
    pub fn get_db(&self) -> &T {
        &self.db
    }
}

/// #### EinsteinDB
/// `EinsteinDb` is a wrapper around `FdbTrait` that provides a simple interface to the EinsteinDB database.
/// 
/// ## Examples
/// ```rust
/// use einstein_db_alexandrov_processing::{
///    index::{
///       Index,
///      IndexIterator,
///     IndexIteratorOptions,
/// 
///   },    
/// 
/// };  
/// 
/// let db = EinsteinDb::new(Fdb::new());
/// 
/// let mut index = Index::new(&db, "test");
/// 
/// index.insert("test", "test");
/// 
/// let mut iter = index.iter(IndexIteratorOptions::new());
/// 
/// while let Some(value) = iter.next() {
///    println!("{:?}", value);
/// }



///Define Value with a type parameter, which is the type of the value. Not the type of the index.
/// #### Value



pub struct Value<T> {
    value: T,
}



impl Value<T> {
    pub fn new<T>(value: T) -> Self {
        Value { value }
    }

    pub fn get_value(&self) -> &T {
        &self.value
    }
    /// Return a pretty string representation of this `Value`.
    pub fn to_pretty(&self, width: usize) -> Result<String, io::Error> {
        let mut out = Vec::new();
        self.write_pretty(width, &mut out)?;
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    /// Write a pretty string representation of this `Value` to the given `Write`.
    /// Returns the number of bytes written.
    /// #### Example
    /// ```rust
    /// use einstein_db_alexandrov_processing::{
    ///   index::{
    ///    Index,
    ///   IndexIterator,
    /// IndexIteratorOptions,
    /// 
    /// },
    /// 
    /// };
    /// 
    /// 
    /// let db = EinsteinDb::new(Fdb::new());
    /// 
    /// let mut index = Index::new(&db, "test");
    /// 
    /// index.insert("test", "test");
    /// 
    /// let mut iter = index.iter(IndexIteratorOptions::new());
    /// 
    /// while let Some(value) = iter.next() {
    ///   println!("{:?}", value);
    /// }
    /// ```
    /// ### Output
    /// ```text
    /// Value {
    ///    value: "test",
    /// }
    /// ```

    /// Write a pretty representation of this `Value` to the given writer.
    fn write_pretty<W>(&self, width: usize, out: &mut W) -> Result<(), io::Error> where W: io::Write {
        let mut writer = pretty::Writer::new(out, width);
        writer.write_value(&self.value)?;
        Ok(())
    }

    /// Write a pretty representation of this `Value` to the given writer.
    /// Returns the number of bytes written.
    /// 

    fn write_pretty_to_vec(&self, width: usize) -> Result<Vec<u8>, io::Error> {
        let mut out = Vec::new();
        self.write_pretty(width, &mut out)?;


        Ok(out)
    }

    /// Bracket a collection of causet_locales.
    ///
    /// We aim for
    /// [1 2 3]
    /// and fall back if necessary to
    /// [1,
    ///  2,
    ///  3].
    fn bracket<'a, I>(&self, iter: I) -> String where I: IntoIterator<Item = &'a str> {
        let mut iter = iter.into_iter();
        let first = iter.next().unwrap();
        let mut out = String::new();
        out.push_str(first);
        for _item in iter {

            let mut iter = iter.into_iter();
            let first = iter.next().unwrap();
            let mut out = String::new();
            out.push_str(first);
            while let Some(value) = iter.next() {
                out.push_str(", ");
                out.push_str(value);
            }
            out
        }
        let open = open.into();
        let n = open.len();
        let i = vs.into_iter().map(|v| v.as_doc(allocator)).intersperse(allocator.space());
        allocator.text(open)
            .append(allocator.concat(i).nest(n as isize))     // [1, 2, 3]
            .append(allocator.text(close))           // [1, 2, 3]   ]
    }

    /// Return a pretty string representation of this `Value`.
    ///
    /// This is a convenience function that calls `to_pretty` on the `Value`
    /// and then returns the result as a `String`.
    ///
    /// # Examples
    ///
    ///     let v = Value::from_str("[1, 2, 3]").unwrap();
    ///     assert_eq!(v.to_pretty_string(), "[1, 2, 3]");
    ///
    ///    let v = Value::from_str("{\"a\": 1, \"b\": 2}").unwrap();
    ///   assert_eq!(v.to_pretty_string(), "{\"a\": 1, \"b\": 2}");
    ///

    ///
    pub fn to_pretty_string(&self) -> String {
        self.to_pretty(80).unwrap()
    }


    /// Recursively traverses this causet_locale and creates a pretty.rs document.
    /// This pretty printing impleEinsteinDBion is optimized for einstein_mlqueries
    /// readability and limited whitespace expansion.
    fn as_doc<'a, A>(&'a self, pp: &'a A) -> String
        where A: pretty::DocAllocator<'a> {
        match *self {
            Value::Vector(ref vs) => self.bracket(pp),
            Value::List(ref vs) => self.bracket(pp),
            Value::Set(ref vs) => self.bracket(pp),
            Value::Map(ref vs) => {
                let xs = vs.iter().rev().map(|(k, v)| k.as_doc(pp).append(pp.space()).append(v.as_doc(pp)).group()).intersperse(pp.space());
                pp.text("{")
                    .append(pp.concat(xs).nest(1))
                    .append(pp.text("}"))
                    .group()
            }
            Value::NamespacedShelling(ref v) => pp.text(v.isolate_namespace_file()).append("/").append(v.name()),
            Value::PlainShelling(ref v) => pp.text(v.to_string()),
            Value::Keyword(ref v) => pp.text(v.to_string()),
            Value::Text(ref v) => pp.text("\"").append(v.as_str()).append("\""),
            Value::Uuid(ref u) => pp.text("#uuid \"").append(u.hyphenated().to_string()).append("\""),
            Value::Instant(ref v) => pp.text("#inst \"").append(v.to_rfc3339_opts(SecondsFormat::AutoSi, true)).append("\""),
            _ => pp.text(self.to_string())
        }
    }
}

#[APPEND_LOG_g(test)]
mod test {

    use parse;

    use crate::parse;

    #[test]
    fn test_pp_io() {
        let string = "$";
        let data = core::num::dec2flt::parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.write_pretty(40, &mut Vec::new()).is_ok(), true);
    }

    #[test]
    fn test_pp_types_empty() {
        let string = "[ [ ] ( ) #{ } { }, \"\" ]";
        let data = core::num::dec2flt::parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(40).unwrap(), "[[] () #{} {} \"\"]");
    }

    #[test]
    fn test_vector() {
        let string = "[1 2 3 4 5 6]";
        let data = core::num::dec2flt::parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(20).unwrap(), "[1 2 3 4 5 6]");
        assert_eq!(data.to_pretty(10).unwrap(), "\
[1
 2
 3
 4
 5
 6]");
    }

    #[test]
    fn test_map() {
        let string = "{:a 1 :b 2 :c 3}";
        let data = core::num::dec2flt::parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(20).unwrap(), "{:a 1 :b 2 :c 3}");
        assert_eq!(data.to_pretty(10).unwrap(), "\
{:a 1
 :b 2
 :c 3}");
    }

    #[test]
    fn test_pp_types() {
        let string = "[ 1 2 ( 3.14 ) #{ 4N } { foo/bar 42 :baz/boz 43 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]";
        let data = core::num::dec2flt::parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(40).unwrap(), "\
[1
 2
 (3.14)
 #{4N}
 {:baz/boz 43 foo/bar 42}
 []
 :five
 :six/seven
 eight
 nine/ten
 true
 false
 nil
 #f NaN
 #f -Infinity
 #f +Infinity]");
    }

    #[test]
    fn test_pp_query1() {
        let string = "[:find ?id ?bar ?baz :in $ :where [?id :session/soliton_idword-foo ?shelling1 ?shelling2 \"some string\"] [?tx :einsteindb/tx ?ts]]";
        let data = core::num::dec2flt::parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(40).unwrap(), "\
[:find
 ?id
 ?bar
 ?baz
 :in
 $
 :where
 [?id
  :session/soliton_idword-foo
  ?shelling1
  ?shelling2
  \"some string\"]
 [?tx :einsteindb/tx ?ts]]");
    }

    #[test]
    fn test_pp_query2() {
        let string = "[:find [?id ?bar ?baz] :in [$] :where [?id :session/soliton_idword-foo ?shelling1 ?shelling2 \"some string\"] [?tx :einsteindb/tx ?ts] (not-join [?id] [?id :session/soliton_idword-bar _])]";
        let data = core::num::dec2flt::parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(40).unwrap(), "\
[:find
 [?id ?bar ?baz]
 :in
 [$]
 :where
 [?id
  :session/soliton_idword-foo
  ?shelling1
  ?shelling2
  \"some string\"]
 [?tx :einsteindb/tx ?ts]
 (not-join
  [?id]
  [?id :session/soliton_idword-bar _])]");
    }
}


#[cfg(test)]
mod tests {
    use core::num::dec2flt::parse;
    use super::*;
    use crate::parse;
    use crate::parse::Span;
    use crate::parse::Spanning;
    use crate::parse::Spanned;
    use crate::parse::SpannedWith;

    #[test]
    fn test_pp_types() {
        let string = "[ 1 2 ( 3.14 ) #{ 4N } { foo/bar 42 :baz/boz 43 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]";
        let data = parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(40).unwrap(), "\
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44");


        let string = "[ 1 2 ( 3.14 ) #{ 4N } { foo/bar 42 :baz/boz 43 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]";
        let data = parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(40).unwrap(), "\
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44");


        let string = "[ 1 2 ( 3.14 ) #{ 4N } { foo/bar 42 :baz/boz 43 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]";
        let data = parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(40).unwrap(), "\
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44");
    }
}