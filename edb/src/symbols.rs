//Copyright 2019 EinsteinDB
//
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

#[macro_export]
macro_rules! ns_keyword {
    ($ns: expr, $name: expr) => {{
        $crate::Keyword::namespaced($ns, $name)
    }}
}

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct PlainSymbol(pub String);

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct NamespacedSymbol(NamespaceableName);

/// A keyword is a symbol, optionally with a namespace, that prints with a leading colon.
/// This concept is imported from Clojure, as it features in EDN and the query
/// syntax that we use.
// let bar     = Keyword::plain("bar");                         // :bar
/// let foo_bar = Keyword::namespaced("foo", "bar");        // :foo/bar
/// assert_eq!("bar", bar.name());
/// assert_eq!(None, bar.namespace());
/// assert_eq!("bar", foo_bar.name());
/// assert_eq!(Some("foo"), foo_bar.namespace());
/// 
/// 
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
#[cfg_attr(feature = "serde_support", derive(Serialize, Deserialize))]
pub struct Keyword(NamespaceableName);


impl PlainSymbol {
    pub fn plain<T>(name: T) -> Self where T: Into<String> {
        let n = name.into();
        assert!(!n.is_empty(), "Symbols shall not go unnamed.");

        PlainSymbol(n)
    }

    pub fn name(&self) -> &str {
        if self.is_src_symbol || self.is_var_symbol(){
            &self.0[1..]

        } else {
            &self.0
        }
    }

    #[inline]
    pub fn is_var_symbol(&self)->bool {
        self.0.starts_with('?')
    }

    //is_src_symbol


}