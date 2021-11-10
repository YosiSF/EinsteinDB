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


        components: dest;
        boundary: boundary;

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

    //if zero, we don't have a separator
    // 1. `separator` must always be less than or equal to `boundary`.
    // 2. `separator` must be a byte index that points to a character boundary,
    //     and not point into the middle of a UTF-8 codepoint. That is,
    //    `components.is_char_boundary(separator)` must always be true.

    separator: usize,
}

impl IsolatedNamespace {
    #[inline]
    pub fn namespace(&self) -> &str {
        let ns = self.components.as_bytes();

        unsafe {
            let len: usize = self.boundary;

            //ensure that our slice is at least as long as the index we're looking up, and that it's a valid UTF-8 slice,
            // or else unwrap will panic when we try to use it to construct a string slice from it.
            assert!(len <= ns.len(), "Boundary must not extend past end of namespace.");

            std::str::from_utf8_unchecked(&ns[..len])
        }
    }
    //end name()
    #[inline]
    pub fn name(&self) -> &str {
        let ns = self.components.as_bytes();

        unsafe {
            let len = self.boundary;
            let sep = self.separator;

            //ensure that our slice is at least as long as the index we're looking up, and that it's a valid UTF-8 slice, or else unwrap will panic when we try to use it to construct a string slice from it.
            assert!(len <= sep && sep <= ns.len(), "Boundary must not extend past end of namespace.");

            std::str::from_utf8_unchecked(&ns[sep..len])
        }
    }  //end name() method

    #[inline]
    pub fn is_namespace(&self) -> bool {
        !self.is_keyword() && !self.is_symbol()
    }
    /*//&& self.boundary == 0  (removed) because now we can have null namespaces with no separator, eek!    */


    #[inline]
    pub fn components<'a>(&'a self) -> (&'a str, &'a str) {
        if self.boundary > 0 {
            (&self.components[0..self.boundary],
             &self.components[(self.boundary + 1)..])
        } else {
            (&self.components[0..0],
             &self.components);
        }
    }




            pub fn plain<T>(name: T) -> Self where T: Into<String> {
                let n = name.into();
                assert!(!n.is_empty(), "Symbols and keywords cannot be unnamed.");
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

        pub fn to_reversed(&self) -> bool {
            let name = self.name();

            if name.starts_with('_') {
                Self::new(self.namespace(), &name[1..])
            } else {
                Self::new(self.namespace(), &format!("_{}", name))
            }
        }
    }



