// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::borrow::Cow;
use std::io;

use chrono::SecondsFormat;
use itertools::Itertools;
use pretty;

use types::Value;

use crate::Value;

impl Value {
    /// Return a pretty string representation of this `Value`.
    pub fn to_pretty(&self, width: usize) -> Result<String, io::Error> {
        let mut out = Vec::new();
        self.write_pretty(width, &mut out)?;
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    /// Write a pretty representation of this `Value` to the given writer.
    fn write_pretty<W>(&self, width: usize, out: &mut W) -> Result<(), io::Error> where W: io::Write {
        self.as_doc(&pretty::BoxAllocator).1.render(width, out)
    }

    /// Bracket a collection of causet_locales.
    ///
    /// We aim for
    /// [1 2 3]
    /// and fall back if necessary to
    /// [1,
    ///  2,
    ///  3].
    fn bracket<'a, A, T, I>(&'a self, allocator: &'a A, open: T, vs: I, close: T) -> pretty::DocBuilder<'a, A>
        where A: pretty::DocAllocator<'a>, T: Into<Cow<'a, str>>, I: IntoIterator<Item=&'a Value> {
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
    /// This pretty printing impleeinstaiion is optimized for einstein_mlqueries
    /// readability and limited whitespace expansion.
    fn as_doc<'a, A>(&'a self, pp: &'a A) -> pretty::DocBuilder<'a, A>
        where A: pretty::DocAllocator<'a> {
        match *self {
            Value::Vector(ref vs) => self.bracket(pp, "[", vs, "]"),
            Value::List(ref vs) => self.bracket(pp, "(", vs, ")"),
            Value::Set(ref vs) => self.bracket(pp, "#{", vs, "}"),
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

#[cfg(test)]
mod test {
    use parse;

    use crate::parse;

    #[test]
    fn test_pp_io() {
        let string = "$";
        let data = parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.write_pretty(40, &mut Vec::new()).is_ok(), true);
    }

    #[test]
    fn test_pp_types_empty() {
        let string = "[ [ ] ( ) #{ } { }, \"\" ]";
        let data = parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(40).unwrap(), "[[] () #{} {} \"\"]");
    }

    #[test]
    fn test_vector() {
        let string = "[1 2 3 4 5 6]";
        let data = parse::causet_locale(string).unwrap().without_spans();

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
        let data = parse::causet_locale(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(20).unwrap(), "{:a 1 :b 2 :c 3}");
        assert_eq!(data.to_pretty(10).unwrap(), "\
{:a 1
 :b 2
 :c 3}");
    }

    #[test]
    fn test_pp_types() {
        let string = "[ 1 2 ( 3.14 ) #{ 4N } { foo/bar 42 :baz/boz 43 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]";
        let data = parse::causet_locale(string).unwrap().without_spans();

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
        let data = parse::causet_locale(string).unwrap().without_spans();

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
        let data = parse::causet_locale(string).unwrap().without_spans();

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
