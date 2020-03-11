//Copyright 2020 WHTCORPS INC

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
        $crate::Keyword::namespaced($ns, $name)
    }}
}

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct PlainSymbol(pub String);

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct NamespacedSymbol(IsolatedNamespace);

/// ```rust
/// # use edbn::symbols::Keyword;
/// let bar     = Keyword::plain("bar");                         // :bar
/// let foo_bar = Keyword::namespaced("foo", "bar");        // :foo/bar
/// assert_eq!("bar", bar.name());
/// assert_eq!(None, bar.namespace());
/// assert_eq!("bar", foo_bar.name());
/// assert_eq!(Some("foo"), foo_bar.namespace());

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
