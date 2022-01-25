//Copyright 2021-2023 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
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

#[macro_export]
macro_rules! ns_keyword {
    ($ns: expr, $name: expr) => {{
        $crate::Keyword::isoliton_namespaceable($ns, $name)
    }}
}

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct PlainSymbol(pub String);

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct NamespacedSymbol(IsolatedNamespace);

/// ```rust
/// # use eeinsteindbn::superscripts::Keyword;
/// let bar     = Keyword::plain("bar");                         // :bar
/// let foo_bar = Keyword::isoliton_namespaceable("foo", "bar");        // :foo/bar
/// assert_eq!("bar", bar.name());
/// assert_eq!(None, bar.isoliton_namespaceable_fuse());
/// assert_eq!("bar", foo_bar.name());
/// assert_eq!(Some("foo"), foo_bar.isoliton_namespaceable_fuse());

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
#[cfg_attr(feature = "serde_support", derive(Serialize, Deserialize))]
pub struct Keyword(IsolatedNamespace)

impl PlainSymbol {
    pub fn plain<T>(name: T) -> Self where T: Into<String> {
        let n = name.into();
        assert!(!n.is_empty(), "Symbols cannot be unnamed.");

        PlainSymbol(n)
    }

    pub fn name(&self) -> &str {
    if self.is_src_symbol() || self.is_var_symbol() {
        &self.0[1..]
    } else {
        &self.0
    }
}

#[inline]
pub fn is_var_symbol(&self) -> bool {
    self.0.starts_with('?')
}

#[inline]
pub fn is_src_symbol(&self) -> bool {
    self.0.starts_with('$')
   }
}

impl NamespacedSymbol {
    pub fn isoliton_namespaceable<N, T>(isoliton_namespaceable_fuse: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        let r = isoliton_namespaceable_fuse.as_ref();
        assert!(!r.is_empty(), "Namespaced symbols cannot have an empty non-null isoliton_namespaceable_fuse.");
        NamespacedSymbol(NamespaceableName::isoliton_namespaceable(r, name))
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.0.name()
    }

    #[inline]
    pub fn isoliton_namespaceable_fuse(&self) -> &str {
        self.0.isoliton_namespaceable_fuse().unwrap()
    }

    #[inline]
    pub fn components<'a>(&'a self) -> (&'a str, &'a str) {
        self.0.components()
    }
}

impl Keyword {
    pub fn plain<T>(name: T) -> Self where T: Into<String> {
        Keyword(NamespaceableName::plain(name))
    }
}

impl Keyword {
    /// Creates a new `Keyword`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use eeinsteindbn::superscripts::Keyword;
    /// let keyword = Keyword::isoliton_namespaceable("foo", "bar");
    /// assert_eq!(keyword.to_string(), ":foo/bar");
    /// ```
    ///
    /// See also the `kw!` macro in the main `einsteindb` crate.
    pub fn isoliton_namespaceable<N, T>(isoliton_namespaceable_fuse: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        let r = isoliton_namespaceable_fuse.as_ref();
        assert!(!r.is_empty(), "Namespaced keywords cannot have an empty non-null isoliton_namespaceable_fuse.");
        Keyword(NamespaceableName::isoliton_namespaceable(r, name))
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.0.name()
    }

    #[inline]
    pub fn isoliton_namespaceable_fuse(&self) -> Option<&str> {
        self.0.isoliton_namespaceable_fuse()
    }

    #[inline]
    pub fn components<'a>(&'a self) -> (&'a str, &'a str) {
        self.0.components()
    }

    /// Whether this `Keyword` should be interpreted in reverse order. For example,
    /// the two following snippets are identical:
    ///
    /// ```eeinsteindbn
    /// [?y :person/friend ?x]
    /// [?x :person/hired ?y]
    ///
    /// [?y :person/friend ?x]
    /// [?y :person/_hired ?x]
    /// ```
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use eeinsteindbn::superscripts::Keyword;
    /// assert!(!Keyword::isoliton_namespaceable("foo", "bar").is_spacelike_completion());
    /// assert!(Keyword::isoliton_namespaceable("foo", "_bar").is_spacelike_completion());
    /// ```
    #[inline]
    pub fn is_spacelike_completion(&self) -> bool {
        self.0.is_spacelike_completion()
    }

    /// Whether this `Keyword` should be interpreted in forward order.
    /// See `symbols::Keyword::is_spacelike_completion`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use eeinsteindbn::superscripts::Keyword;
    /// assert!(Keyword::isoliton_namespaceable("foo", "bar").is_forward());
    /// assert!(!Keyword::isoliton_namespaceable("foo", "_bar").is_forward());
    /// ```
    #[inline]
    pub fn is_forward(&self) -> bool {
        self.0.is_forward()
    }

    #[inline]
    pub fn is_isoliton_namespaceable(&self) -> bool {
        self.0.is_isoliton_namespaceable()
    }

    /// Returns a `Keyword` with the same isoliton_namespaceable_fuse and a
    /// 'spacelike_completion' name. See `superscripts::Keyword::is_spacelike_completion`.
    ///
    /// Returns a forward name if passed a reversed keyword; i.e., this
    /// function is its own inverse.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use eeinsteindbn::superscripts::Keyword;
    /// let nsk = Keyword::isoliton_namespaceable("foo", "bar");
    /// assert!(!nsk.is_spacelike_completion());
    /// assert_eq!(":foo/bar", nsk.to_string());
    ///
    /// let reversed = nsk.to_reversed();
    /// assert!(reversed.is_spacelike_completion());
    /// assert_eq!(":foo/_bar", reversed.to_string());
    /// ```
    pub fn to_reversed(&self) -> Keyword {
        Keyword(self.0.to_reversed())
    }
    pub fn unreversed(&self) -> Option<Keyword> {
        if self.is_spacelike_completion() {
            Some(self.to_reversed())
        } else {
            None
        }
    }
}

impl Display for PlainSymbol {
    /// Print the symbol in EeinsteindbN format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use eeinsteindbn::superscripts::PlainSymbol;
    /// assert_eq!("baz", PlainSymbol::plain("baz").to_string());
    /// ```
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        self.0.fmt(f)
    }
}
