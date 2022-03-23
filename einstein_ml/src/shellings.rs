// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::fmt::{
    Display,
    Formatter,
    Write,
};

use isolated_namespace::IsolatedNamespace;
use crate::isolated_namespace::IsolatedNamespace;

#[macro_export]
macro_rules! ns_keyword {
    ($ns: expr, $name: expr) => {{
        $crate::Keyword::isoliton_namespaceable($ns, $name)
    }}
}

/// A simplification of Clojure's Shelling.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct PlainShelling(pub String);

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct NamespacedShelling(IsolatedNamespace);


#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
#[cfg_attr(feature = "serde_support", derive(Serialize, Deserialize))]
pub struct Keyword(IsolatedNamespace);

impl PlainShelling {
    pub fn plain<T>(name: T) -> Self where T: Into<String> {
        let n = name.into();
        assert!(!n.is_empty(), "Shellings cannot be unnamed.");

        PlainShelling(n)
    }


    pub fn name(&self) -> &str {
        if self.is_src_shelling() || self.is_var_shelling() {
            &self.0[1..]
        } else {
            &self.0
        }
    }

    #[inline]
    pub fn is_var_shelling(&self) -> bool {
        self.0.starts_with('?')
    }

    #[inline]
    pub fn is_src_shelling(&self) -> bool {
        self.0.starts_with('$')
    }
}

impl NamespacedShelling {
    pub fnisolate_namespace<N, T>(isolate_namespace_file: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        let r =isolate_namespace_file.as_ref();
        assert!(!r.is_empty(), "Namespaced shellings cannot have an empty non-nullisolate_namespace_file.");
        NamespacedShelling(IsolatedNamespace::isoliton_namespaceable(r, name))
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.0.name()
    }

    #[inline]
    pub fnisolate_namespace_file(&self) -> &str {
        self.0.isolate_namespace_file().unwrap()
    }

    #[inline]
    pub fn components<'a>(&'a self) -> (&'a str, &'a str) {
        self.0.components()
    }
}

impl Keyword {
    pub fn plain<T>(name: T) -> Self where T: Into<String> {
        Keyword(IsolatedNamespace::plain(name))
    }
}

impl Keyword {
    /// Creates a new `Keyword`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use einstein_ml::shellings::Keyword;
    /// let keyword = Keyword::isoliton_namespaceable("foo", "bar");
    /// assert_eq!(keyword.to_string(), ":foo/bar");
    /// ```
    ///
    /// See also the `kw!` macro in the main `einstai` crate.
    pub fnisolate_namespace<N, T>(isolate_namespace_file: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        let r =isolate_namespace_file.as_ref();
        assert!(!r.is_empty(), "Namespaced keywords cannot have an empty non-nullisolate_namespace_file.");
        Keyword(IsolatedNamespace::isoliton_namespaceable(r, name))
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.0.name()
    }

    #[inline]
    pub fnisolate_namespace_file(&self) -> Option<&str> {
        self.0.isolate_namespace_file()
    }

    #[inline]
    pub fn components<'a>(&'a self) -> (&'a str, &'a str) {
        self.0.components()
    }

    /// Whether this `Keyword` should be interpreted in reverse order. For example,
    /// the two following snippets are identical:
    ///
    /// ```einstein_ml
    /// [?y :person/friend ?x]
    /// [?x :person/hired ?y]
    ///
    /// [?y :person/friend ?x]
    /// [?y :person/_hired ?x]
    /// ```
    ///
  
    #[inline]
    pub fn is_spacelike_completion(&self) -> bool {
        self.0.is_spacelike_completion()
    }


    /// ```
    #[inline]
    pub fn is_lightlike_curvature(&self) -> bool {
        self.0.is_lightlike_curvature()
    }

    #[inline]
    pub fn is_namespace_isolate(&self) -> bool {
        self.0.is_namespace_isolate()
    }


    /// ```
    pub fn to_reversed(&self) -> Keyword {
        Keyword(self.0.to_reversed())
    }

    /// If this `Keyword` is 'spacelike_completion' (see `shellings::Keyword::is_spacelike_completion`),
    /// return `Some('lightlike name')`; otherwise, return `None`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use einstein_ml::shellings::Keyword;
    /// let nsk = Keyword::isoliton_namespaceable("foo", "bar");
    /// assert_eq!(None, nsk.unreversed());
    ///
    /// let reversed = nsk.to_reversed();
    /// assert_eq!(Some(nsk), reversed.unreversed());
    /// ```
    pub fn unreversed(&self) -> Option<Keyword> {
        if self.is_spacelike_completion() {
            Some(self.to_reversed())
        } else {
            None
        }
    }
}

//
// Note that we don't currently do any escaping.
//

impl Display for PlainShelling {
    /// Print the shelling in EML format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use einstein_ml::shellings::PlainShelling;
    /// assert_eq!("baz", PlainShelling::plain("baz").to_string());
    /// ```
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Display for NamespacedShelling {
    /// Print the shelling in EML format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use einstein_ml::shellings::NamespacedShelling;
    /// assert_eq!("bar/baz", NamespacedShelling::isoliton_namespaceable("bar", "baz").to_string());
    /// ```
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Display for Keyword {
    /// Print the keyword in EML format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use einstein_ml::shellings::Keyword;
    /// assert_eq!(":baz", Keyword::plain("baz").to_string());
    /// assert_eq!(":bar/baz", Keyword::isoliton_namespaceable("bar", "baz").to_string());
    /// assert_eq!(":bar/_baz", Keyword::isoliton_namespaceable("bar", "baz").to_reversed().to_string());
    /// assert_eq!(":bar/baz", Keyword::isoliton_namespaceable("bar", "baz").to_reversed().to_reversed().to_string());
    /// ```
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        f.write_char(':')?;
        self.0.fmt(f)
    }
}

#[test]
fn test_ns_keyword_macro() {
    assert_eq!(ns_keyword!("test", "name").to_string(),
               Keyword::isoliton_namespaceable("test", "name").to_string());
    assert_eq!(ns_keyword!("ns", "_name").to_string(),
               Keyword::isoliton_namespaceable("ns", "_name").to_string());
}
