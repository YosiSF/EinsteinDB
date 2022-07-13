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


use std::{
    collections::HashMap,
    fmt::{self, Display},
    io,
    convert::{TryFrom, TryInto},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};


use pretty::{
    BoxAllocator,
    Doc,
    DocBuilder,
    DocIter,
    DocWriter,
    Line,
    LineWriter,
    Pretty,
    RenderMode,
    RenderSpan,
    Spanned,
    SpannedIter,
    SpannedWriter,
    SpannedWriterIter,
    Span,
    SpanIter,
    SpanWriter,
    SpanWriterIter,
    StyledDoc,
    StyledDocBuilder,
    StyledDocIter,
    StyledDocWriter,
    StyledLine,
    StyledLineWriter,
    StyledSpanned,
    StyledSpannedIter,
    StyledSpannedWriter,
    StyledSpannedWriterIter,
    StyledSpan,
    StyledSpanIter,
    StyledSpanWriter,
    StyledSpanWriterIter,
    StyledWriteMode,
};

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::{cmp, u64};


use fdb_traits::Result;


#[derive(Debug, Clone)]
pub struct CompactOptions {
    pub causetq_upstream_interlock_threshold: u64,
    pub causetq_upstream_interlock_compaction_interval: u64,
    pub causetq_upstream_interlock_compaction_threshold: u64,
    pub block_size: u64,
    pub block_cache_size: u64,
    pub block_cache_shard_bits: u8,
    pub enable_bloom_filter: bool,
    pub enable_indexing: bool,
    pub index_block_size: u64,
    pub index_block_cache_size: u64,
    pub index_block_cache_shard_bits: u8,
    pub index_block_restart_interval: u64,
    pub compression_type: String,
}


#[derive(Debug, Clone)]
pub struct Compaction {
    pub start_time: Instant,
    pub end_time: Instant,
    pub duration: Duration,
    pub input_files: Vec<String>,
    pub output_files: Vec<String>,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub input_records: u64,
    pub output_records: u64,
    pub input_deletions: u64,
    pub output_deletions: u64,
    pub input_corruptions: u64,
    pub output_corruptions: u64,
    pub input_compression_type: String,
    pub output_compression_type: String,
    pub input_compression_ratio: f64,
    pub output_compression_ratio: f64,
    pub input_compression_size: u64,
    pub output_compression_size: u64,
    pub input_compression_time: Duration,
    pub output_compression_time: Duration,
    pub input_index_size: u64,
    pub output_index_size: u64,
    pub input_index_compression_size: u64,
    pub output_index_compression_size: u64,
    pub input_index_compression_time: Duration,
    pub output_index_compression_time: Duration,
    pub input_index_records: u64,
    pub output_index_records: u64,
    pub input_index_deletions: u64,
    pub output_index_deletions: u64,
    pub input_index_corruptions: u64,
    pub output_index_corruptions: u64,
    pub input_index_compression_type: String,
    pub output_index_compression_type: String,
    pub input_index_compression_ratio: f64,
    pub output_index_compression_ratio: f64,
}


#[macro_export]
macro_rules! einsteindb_macro {
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
}



////////////////////////////////////////////////////////////////////////////////
/// #[macro_export]
/// macro_rules! einsteindb_macro_impl {

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
        ($($tokens:tt)*) => {
            $crate::einstein_macro_impl!($($tokens)*)
        }
    }

#[macro_export]






#[macro_export]
macro_rules! ns_soliton_idword {
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
#[APPEND_LOG_g_attr(feature = "serde_support", derive(Serialize, Deserialize))]
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
    pub fn isolate_namespace<N, T>(isolate_namespace_file: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        let r =isolate_namespace_file.as_ref();
        assert!(!r.is_empty(), "Namespaced shellings cannot have an empty non-nullisolate_namespace_file.");
        NamespacedShelling(IsolatedNamespace::isoliton_namespaceable(r, name))
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.0.name()
    }

    #[inline]
    pub fn namespace(&self) -> &str {
        self.0.namespace()
    }

    #[inline]
    pub fn is_var_shelling(&self) -> bool {
        self.0.is_var_shelling()
    }

    #[inline]
    pub fn is_src_shelling(&self) -> bool {
        self.0.is_src_shelling()
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
    /// let soliton_idword = Keyword::isoliton_namespaceable("foo", "bar");
    /// assert_eq!(soliton_idword.to_string(), ":foo/bar");
    /// ```
    ///
    /// See also the `kw!` macro in the main `EinsteinDB` crate.
    pub fn isolate_namespace<N, T>(isolate_namespace_file: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        let r = isolate_namespace_file.as_ref();
        assert!(!r.is_empty(), "Namespaced soliton_idwords cannot have an empty non-nullisolate_namespace_file.");
        Keyword(IsolatedNamespace::isoliton_namespaceable(r, name))
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.0.name()
    }

    #[inline]
    pub fn isolate_namespace_file(&self) -> Option<&str> {
        self.0.isolate_namespace_file()
    }

    #[inline]
    pub fn components(&self) -> (&str, &str) {
        self.0.components()
    }

    /// Whether this `Keyword` should be interpreted in reverse order. For example,
    /// the two following snippets are causetidical:
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
    /// Print the soliton_idword in EML format.
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
fn test_ns_soliton_idword_macro() {
    assert_eq!(ns_soliton_idword!("test", "name").to_string(),
               Keyword::isoliton_namespaceable("test", "name").to_string());
    assert_eq!(ns_soliton_idword!("ns", "_name").to_string(),
               Keyword::isoliton_namespaceable("ns", "_name").to_string());
}


#[test]
fn test_keyword_macro() {
    assert_eq!(kw!("test").to_string(), Keyword::plain("test").to_string());
    assert_eq!(kw!("ns", "name").to_string(), Keyword::isoliton_namespaceable("ns", "name").to_string());
    assert_eq!(kw!("ns", "_name").to_string(), Keyword::isoliton_namespaceable("ns", "_name").to_string());
}


#[test]
fn test_keyword_macro_reversed() {
    assert_eq!(kw!("test").to_reversed().to_string(), Keyword::plain("test").to_reversed().to_string());
    assert_eq!(kw!("ns", "name").to_reversed().to_string(), Keyword::isoliton_namespaceable("ns", "name").to_reversed().to_string());
    assert_eq!(kw!("ns", "_name").to_reversed().to_string(), Keyword::isoliton_namespaceable("ns", "_name").to_reversed().to_string());
}

#[test]
fn test_keyword_macro_unreversed() {
    assert_eq!(kw!("test").unreversed().unwrap().to_string(), Keyword::plain("test").to_string());
    assert_eq!(kw!("ns", "name").unreversed().unwrap().to_string(), Keyword::isoliton_namespaceable("ns", "name").to_string());
    assert_eq!(kw!("ns", "_name").unreversed().unwrap().to_string(), Keyword::isoliton_namespaceable("ns", "_name").to_string());
}
