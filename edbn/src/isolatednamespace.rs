//Copyright 2021-2023 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use std::cmp::{
    Ord,
    Ordering,
    PartialOrd,
};

use std::fmt;

#[cfg(feature = "serde_support")]
use serde::de::{
    self,
    Deserialize,
    Deserializer
};
#[cfg(feature = "serde_support")]
use serde::ser::{
    Serialize,
    Serializer,
};

#[inline]
pub fn namespaced<N, T>(namespace: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
    let n = name.as_ref();
    let ns = namespace.as_ref();

    assert!(!n.is_empty(), "Symbols and keywords cannot be unnamed.");
    assert!(!ns.is_empty(), "Symbols and keywords cannot have an empty non-null namespace.");

    let mut dest = String::with_capacity(n.len() + ns.len());

    dest.push_str(ns);
    dest.push('/');
    dest.push_str(n);

    let boundary = ns.len();

     {
        components: dest,
        boundary: boundary,
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct IsolatedNamespace {

    //bytes that make up the namespace followed directly by
    //solidus partitions
    components: String,

    //If zero, this isn't a namespaced component.
    // 1. `boundary` must always be less than or equal to `components.len()`.    
    // 2. `boundary` must be a byte index that points to a character boundary,
    //     and not point into the middle of a UTF-8 codepoint. That is,
     //    `components.is_char_boundary(boundary)` must always be true.
    boundary: usize,


/*
impl  {
    #[inline]
    pub fn plain<T>(name: T) -> Self where T: Into<String> {
        let n = name.into();
        assert!(!n.is_empty(), "Symbols and keywords cannot be unnamed.");

         {
            components: n,
            boundary: 0,
        }
    }

fn new<N, T>(namespace: Option<N>, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
    if let Some(ns) = namespace {
        Self::namespaced(ns, name)
    } else {
        Self::plain(name.as_ref())
    }
}

pub fn is_namespaced(&self) -> bool {
    self.boundary > 0
}

#[inline]
pub fn is_backward(&self) -> bool {
    self.name().starts_with('_')
}

#[inline]
pub fn is_forward(&self) -> bool {
    !self.is_backward()
}

pub fn to_reversed(&self) ->  {
    let name = self.name();

    if name.starts_with('_') {
        Self::new(self.namespace(), &name[1..])
    } else {
        Self::new(self.namespace(), &format!("_{}", name))
    }
}

#[inline]
pub fn namespace(&self) -> Option<&str> {
    if self.boundary > 0 {
        Some(&self.components[0..self.boundary])
    } else {
        None
    }
}

#[inline]
pub fn name(&self) -> &str {
    if self.boundary == 0 {
        &self.components
    } else {
        &self.components[(self.boundary + 1)..]
    }
}

#[inline]
pub fn components<'a>(&'a self) -> (&'a str, &'a str) {
    if self.boundary > 0 {
        (&self.components[0..self.boundary],
         &self.components[(self.boundary + 1)..])
    } else {
        (&self.components[0..0],
         &self.components)

*/