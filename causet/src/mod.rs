//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Co-Authors: FoundationDB Inc. Apple Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


/// A trait for converting a value to a reference to the same value.
/// This is implemented for all `T` which implement `AsRef<U>` for some `U`.
/// This is a specialization of [`AsRef`] for references.
/// [`AsRef`]: trait.AsRef.html
/// [`From`]: trait.From.html

use EinsteinDB::{ Database, DatabaseConfig, DatabaseType };
use EinsteinDB::Database::Storage::Memtable::Memtable;
use einstein_ml::{ Dataset, DatasetConfig, DatasetType };
use allegro_poset::{ Poset, PosetConfig, PosetType };
use allegro_graph::{ Graph, GraphConfig, GraphType };
use allegro_graph::{ Vertex, VertexConfig, VertexType };
use gremlin_client::{ GremlinClient, GremlinClientConfig, GremlinClientType };
use fdb_traits::{ FdbDatabase, FdbDatabaseConfig, FdbDatabaseType };
use fdb_traits::{ FdbKV, FdbKVConfig, FdbKVType };
use crate::fmt;
use crate::hash::{Hash, Hasher};
use crate::marker::{PhantomData, Unsize};
use crate::mem;
use crate::ops::CoerceUnsized;


use std::borrow::Borrow;
use std::fmt::{self, Debug, Display};


use std::hash::{Hash, Hasher};



#[unstable(feature = "convert_float_to_int", issue = "67057")]
pub use num::FloatToInt;


use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;


#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};




#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};


pub use hex::{FromHex, ToHex};
pub use std::collections::HashMap;
pub use std::collections::HashSet;
pub use std::collections::VecDeque;




pub use std::collections::BTreeMap;
pub use std::collections::BTreeSet;
pub use std::collections::HashMap;


mod causet;
mod causet_of_causets;
mod eval_type;
mod range;


pub use self::causet::Causet;
pub use self::causet_of_causets::CausetOfCausets;
pub use self::eval_type::EvalType;
pub use self::range::Range;


use std::fmt::{self, Display, Formatter};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::collections::hash_set::Iter as HashSetIter;
use std::collections::hash_set::IterMut as HashSetIterMut;
use std::collections::hash_set::IntoIter as HashSetIntoIter;


use crate::einsteindb_macro::einsteindb_macro_impl;


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key(Arc<KeyInner>);


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct KeyInner {
    pub(crate) id: usize,
    pub(crate) name: String,
    pub(crate) causet: Causet,
    pub(crate) causet_of_causets: CausetOfCausets,
    pub(crate) eval_type: EvalType,
    pub(crate) range: Range,
}




impl Key {
    pub fn new(id: usize, name: String, causet: Causet, causet_of_causets: CausetOfCausets, eval_type: EvalType, range: Range) -> Self {
        Key(Arc::new(KeyInner {
            id,
            name,
            causet,
            causet_of_causets,
            eval_type,
            range,
        }))
    }
}


impl Deref for Key {
    type Target = KeyInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


impl DerefMut for Key {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


#[derive(Clone, Debug, PartialEq)]
pub struct JsonRef<'a> {
    pub json: &'a Json,
    pub path: Vec<JsonRef<'a>>,
}




impl<'a> JsonRef<'a> {
    pub fn new(json: &'a Json, path: Vec<JsonRef<'a>>) -> Self {
        Self { json, path }
    }

    pub fn get_type(&self) -> JsonType {
        self.json.get_type()
    }

    pub fn get_elem_count(&self) -> usize {
        self.json.get_elem_count()
    }

    pub fn get_elem(&self, index: usize) -> Option<JsonRef<'a>> {
        self.json.get_elem(index).map(|json| JsonRef::new(json, self.path.clone()))
    }

    pub fn get_elem_by_key(&self, key: &str) -> Option<JsonRef<'a>> {
        self.json.get_elem_by_key(key).map(|json| JsonRef::new(json, self.path.clone()))
    }

    pub fn get_elem_by_key_path(&self, key_path: &[&str]) -> Option<JsonRef<'a>> {
        self.json.get_elem_by_key_path(key_path).map(|json| JsonRef::new(json, self.path.clone()))
    }

    pub fn get_elem_by_path(&self, path: &[&str]) -> Option<JsonRef<'a>> {
        self.json.get_elem_by_path(path).map(|json| JsonRef::new(json, self.path.clone()))
    }

    pub fn get_elem_by_path_expr(&self, path_expr: &str) -> Option<JsonRef<'a>> {
        self.json.get_elem_by_path_expr(path_expr).map(|json| JsonRef::new(json, self.path.clone()))
    }

    pub fn get_elem_by_path_exp(&self, path_exp: &str) -> Option<JsonRef<'a>> {
        self.json.get_elem_by_path_exp(path_exp).map(|json| JsonRef::new(json, self.path.clone()))
    }
}




impl<'a> fmt::Display for JsonRef<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.json.fmt(f)
    }
}








#[derive(Clone, Debug, PartialEq)]
pub struct Json {
    pub json_type: JsonType,
    pub json_value: JsonValue,
}


pub type JsonValue = JsonValue_<Json>;


#[derive(Clone, Debug, PartialEq)]
pub enum JsonValue_<Json> {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Json>),
    Object(HashMap<String, Json>),
    Ref(Arc<Json>),
}




impl Json {
    pub fn new(json_type: JsonType, json_value: JsonValue) -> Self {
        Self { json_type, json_value }
    }
}






//! This module contains the causet-specific types and functions.
//! Traits for conversions between types.
//!
//! The traits in this module provide a way to convert from one type to another type.
//! Each trait serves a different purpose:
//!
//! - Implement the [`AsRef`] trait for cheap reference-to-reference conversions
//! - Implement the [`AsMut`] trait for cheap mutable-to-mutable conversions
//! - Implement the [`From`] trait for consuming value-to-value conversions
//! - Implement the [`Into`] trait for consuming value-to-value conversions to types
//!   outside the current crate
//! - The [`TryFrom`] and [`TryInto`] traits behave like [`From`] and [`Into`],
//!   but should be implemented when the conversion can fail.
//!
//! The traits in this module are often used as trait bounds for generic functions such that to
//! arguments of multiple types are supported. See the documentation of each trait for examples.
//!
//! As a library author, you should always prefer implementing [`From<T>`][`From`] or
//! [`TryFrom<T>`][`TryFrom`] rather than [`Into<U>`][`Into`] or [`TryInto<U>`][`TryInto`],
//! as [`From`] and [`TryFrom`] provide greater flexibility and offer
//! equivalent [`Into`] or [`TryInto`] implementations for free, thanks to a
//! blanket implementation in the standard library. When targeting a version prior to Rust 1.41, it
//! may be necessary to implement [`Into`] or [`TryInto`] directly when converting to a type
//! outside the current crate.
//!
//! # Generic Implementations
//!
//! - [`AsRef`] and [`AsMut`] auto-dereference if the inner type is a reference
//! - [`From`]`<U> for T` implies [`Into`]`<T> for U`
//! - [`TryFrom`]`<U> for T` implies [`TryInto`]`<T> for U`
//! - [`From`] and [`Into`] are reflexive, which means that all types can
//!   `into` themselves and `from` themselves
//!
//! See each trait for usage examples.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};






/// The identity function.
///
/// Two things are important to note about this function:
///
/// - It is not always equivalent to a closure like `|x| x`, since the
///   closure may coerce `x` into a different type.
///
/// - It moves the input `x` passed to the function.
///
/// While it might seem strange to have a function that just returns back the
/// input, there are some interesting uses.
///
/// # Examples
///
/// Using `identity` to do nothing in a sequence of other, interesting,
/// functions:
///
/// ```rust
/// use std::convert::identity;
///
/// fn manipulation(x: u32) -> u32 {
///     // Let's pretend that adding one is an interesting function.
///     x + 1
/// }
///
/// let _arr = &[identity, manipulation];
/// ```
///
/// Using `identity` as a "do nothing" base case in a conditional:
///
/// ```rust
/// use std::convert::identity;
///
/// # let condition = true;
/// #
/// # fn manipulation(x: u32) -> u32 { x + 1 }
/// #
/// let do_stuff = if condition { manipulation } else { identity };
///
/// // Do more interesting stuff...
///
/// let _results = do_stuff(42);
/// ```
///
/// Using `identity` to keep the `Some` variants of an iterator of `Option<T>`:
///
/// ```rust
/// use std::convert::identity;
///
/// let iter = [Some(1), None, Some(3)].into_iter();
/// let filtered = iter.filter_map(identity).collect::<Vec<_>>();
/// assert_eq!(vec![1, 3], filtered);
/// ```
#[stable(feature = "convert_id", since = "1.33.0")]
#[rustc_const_stable(feature = "const_identity", since = "1.33.0")]
#[inline]
pub const fn identity<T>(x: T) -> T {
    x
}

struct Identity<T> {
    _marker: PhantomData<T>,

}








/// Used to do a cheap reference-to-reference conversion.
///
/// This trait is similar to [`AsMut`] which is used for converting between mutable references.
/// If you need to do a costly conversion it is better to implement [`From`] with type
/// `&T` or write a custom function.
///
/// `AsRef` has the same signature as [`Borrow`], but [`Borrow`] is different in a few aspects:
///
/// - Unlike `AsRef`, [`Borrow`] has a blanket impl for any `T`, and can be used to accept either
///   a reference or a value.
/// - [`Borrow`] also requires that [`Hash`], [`Eq`] and [`Ord`] for a borrowed value are
///   equivalent to those of the owned value. For this reason, if you want to
///   borrow only a single field of a struct you can implement `AsRef`, but not [`Borrow`].
///
/// **Note: This trait must not fail**. If the conversion can fail, use a
/// dedicated method which returns an [`Option<T>`] or a [`Result<T, E>`].
///
/// # Generic Implementations
///
/// - `AsRef` auto-dereferences if the inner type is a reference or a mutable
///   reference (e.g.: `foo.as_ref()` will work the same if `foo` has type
///   `&mut Foo` or `&&mut Foo`)
///
/// # Examples
///
/// By using trait bounds we can accept arguments of different types as long as they can be
/// converted to the specified type `T`.
///
/// For example: By creating a generic function that takes an `AsRef<str>` we express that we
/// want to accept all references that can be converted to [`&str`] as an argument.
/// Since both [`String`] and [`&str`] implement `AsRef<str>` we can accept both as input argument.
///
/// [`&str`]: primitive@str
/// [`Borrow`]: crate::borrow::Borrow
/// [`Eq`]: crate::cmp::Eq
/// [`Ord`]: crate::cmp::Ord
/// [`String`]: ../../std/string/struct.String.html
///
/// ```
/// fn is_hello<T: AsRef<str>>(s: T) {
///    assert_eq!("hello", s.as_ref());
/// }
///
/// let s = "hello";
/// is_hello(s);
///
/// let s = "hello".to_string();
/// is_hello(s);
/// ```
#[stable(feature = "rust1", since = "1.0.0")]
#[cfg_attr(not(test), rustc_diagnostic_item = "AsRef")]
pub trait AsRef<T: ?Sized> {
    /// Converts this type into a shared reference of the (usually inferred) input type.
    #[stable(feature = "rust1", since = "1.0.0")]
    fn as_ref(&self) -> &T;
}



#[stable(feature = "rust1", since = "1.0.0")]
impl<T: ?Sized> AsRef<T> for T {
    fn as_ref(&self) -> &T {
        self
    }
}



/// Used to do a cheap mutable-to-mutable reference conversion.
///
/// This trait is similar to [`AsRef`] but used for converting between mutable
/// references. If you need to do a costly conversion it is better to
/// implement [`From`] with type `&mut T` or write a custom function.
///
/// **Note: This trait must not fail**. If the conversion can fail, use a
/// dedicated method which returns an [`Option<T>`] or a [`Result<T, E>`].
///
/// # Generic Implementations
///
/// - `AsMut` auto-dereferences if the inner type is a mutable reference
///   (e.g.: `foo.as_mut()` will work the same if `foo` has type `&mut Foo`
///   or `&mut &mut Foo`)
///
/// # Examples
///
/// Using `AsMut` as trait bound for a generic function we can accept all mutable references
/// that can be converted to type `&mut T`. Because [`Box<T>`] implements `AsMut<T>` we can
/// write a function `add_one` that takes all arguments that can be converted to `&mut u64`.
/// Because [`Box<T>`] implements `AsMut<T>`, `add_one` accepts arguments of type
/// `&mut Box<u64>` as well:
///
/// ```
/// fn add_one<T: AsMut<u64>>(num: &mut T) {
///     *num.as_mut() += 1;
/// }
///
/// let mut boxed_num = Box::new(0);
/// add_one(&mut boxed_num);
/// assert_eq!(*boxed_num, 1);
/// ```
///
/// [`Box<T>`]: ../../std/boxed/struct.Box.html
#[stable(feature = "rust1", since = "1.0.0")]
#[cfg_attr(not(test), rustc_diagnostic_item = "AsMut")]
pub trait AsMut<T: ?Sized> {
    /// Converts this type into a mutable reference of the (usually inferred) input type.
    #[stable(feature = "rust1", since = "1.0.0")]
    fn as_mut(&mut self) -> &mut T;
}

/// A value-to-value conversion that consumes the input value. The
/// opposite of [`From`].
///
/// One should avoid implementing [`Into`] and implement [`From`] instead.
/// Implementing [`From`] automatically provides one with an implementation of [`Into`]
/// thanks to the blanket implementation in the standard library.
///
/// Prefer using [`Into`] over [`From`] when specifying trait bounds on a generic function
/// to ensure that types that only implement [`Into`] can be used as well.
///
/// **Note: This trait must not fail**. If the conversion can fail, use [`TryInto`].
///
/// # Generic Implementations
///
/// - [`From`]`<T> for U` implies `Into<U> for T`
/// - [`Into`] is reflexive, which means that `Into<T> for T` is implemented
///
/// # Implementing [`Into`] for conversions to external types in old versions of Rust
///
/// Prior to Rust 1.41, if the destination type was not part of the current crate
/// then you couldn't implement [`From`] directly.
/// For example, take this code:
///
/// ```
/// struct Wrapper<T>(Vec<T>);
/// impl<T> From<Wrapper<T>> for Vec<T> {
///     fn from(w: Wrapper<T>) -> Vec<T> {
///         w.0
///     }
/// }
/// ```
/// This will fail to compile in older versions of the language because Rust's orphaning rules
/// used to be a little bit more strict. To bypass this, you could implement [`Into`] directly:
///
/// ```
/// struct Wrapper<T>(Vec<T>);
/// impl<T> Into<Vec<T>> for Wrapper<T> {
///     fn into(self) -> Vec<T> {
///         self.0
///     }
/// }
/// ```
///
/// It is important to understand that [`Into`] does not provide a [`From`] implementation
/// (as [`From`] does with [`Into`]). Therefore, you should always try to implement [`From`]
/// and then fall back to [`Into`] if [`From`] can't be implemented.
///
/// # Examples
///
/// [`String`] implements [`Into`]`<`[`Vec`]`<`[`u8`]`>>`:
///
/// In order to express that we want a generic function to take all arguments that can be
/// converted to a specified type `T`, we can use a trait bound of [`Into`]`<T>`.
/// For example: The function `is_hello` takes all arguments that can be converted into a
/// [`Vec`]`<`[`u8`]`>`.
///
/// ```
/// fn is_hello<T: Into<Vec<u8>>>(s: T) {
///    let bytes = b"hello".to_vec();
///    assert_eq!(bytes, s.into());
/// }
///
/// let s = "hello".to_string();
/// is_hello(s);
/// ```
///
/// [`String`]: ../../std/string/struct.String.html
/// [`Vec`]: ../../std/vec/struct.Vec.html
#[rustc_diagnostic_item = "Into"]
#[stable(feature = "rust1", since = "1.0.0")]
pub trait Into<T>: Sized {
    /// Converts this type into the (usually inferred) input type.
    #[must_use]
    #[stable(feature = "rust1", since = "1.0.0")]
    fn into(self) -> T;
}

/// Used to do value-to-value conversions while consuming the input value. It is the reciprocal of
/// [`Into`].
///
/// One should always prefer implementing `From` over [`Into`]
/// because implementing `From` automatically provides one with an implementation of [`Into`]
/// thanks to the blanket implementation in the standard library.
///
/// Only implement [`Into`] when targeting a version prior to Rust 1.41 and converting to a type
/// outside the current crate.
/// `From` was not able to do these types of conversions in earlier versions because of Rust's
/// orphaning rules.
/// See [`Into`] for more details.
///
/// Prefer using [`Into`] over using `From` when specifying trait bounds on a generic function.
/// This way, types that directly implement [`Into`] can be used as arguments as well.
///
/// The `From` is also very useful when performing error handling. When constructing a function
/// that is capable of failing, the return type will generally be of the form `Result<T, E>`.
/// The `From` trait simplifies error handling by allowing a function to return a single error type
/// that encapsulate multiple error types. See the "Examples" section and [the book][book] for more
/// details.
///
/// **Note: This trait must not fail**. The `From` trait is intended for perfect conversions.
/// If the conversion can fail or is not perfect, use [`TryFrom`].
///
/// # Generic Implementations
///
/// - `From<T> for U` implies [`Into`]`<U> for T`
/// - `From` is reflexive, which means that `From<T> for T` is implemented
///
/// # Examples
///
/// [`String`] implements `From<&str>`:
///
/// An explicit conversion from a `&str` to a String is done as follows:
///
/// ```
/// let string = "hello".to_string();
/// let other_string = String::from("hello");
///
/// assert_eq!(string, other_string);
/// ```
///
/// While performing error handling it is often useful to implement `From` for your own error type.
/// By converting underlying error types to our own custom error type that encapsulates the
/// underlying error type, we can return a single error type without losing information on the
/// underlying cause. The '?' operator automatically converts the underlying error type to our
/// custom error type by calling `Into<CliError>::into` which is automatically provided when
/// implementing `From`. The compiler then infers which implementation of `Into` should be used.


pub struct RustAlgebrizer;



impl RustAlgebrizer {
    pub fn new() -> Self {
        for x in 0..turing_automata.get_states().len() {
            let mut state = turing_automata.get_states()[x].clone();
            state.set_id(x);
            turing_automata.get_states_mut()[x] = state;
        }

        for x in 0..turing_automata.get_transitions().len() {
            let mut transition = turing_automata.get_transitions()[x].clone();
            transition.set_id(x);
            turing_automata.get_transitions_mut()[x] = transition;
        }
        RustAlgebrizer {}
    }

    pub fn algebra(&self, turing_automata: &mut FSM) {
        let mut algebrizer = Algebrizer::new();
        algebrizer.algebrize(turing_automata);
    }
}

//conjoining the two states_mut
//conjoining the two transitions_mut
//conjoining the two states


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FSM {
    pub states: Vec<State>,
    pub transitions: Vec<Transition>,
    pub initial_state: State,
    pub final_states: Vec<State>,
}

pub struct ConjoiningCausets {


    pub states: Vec<State>,
    pub transitions: Vec<Transition>,
    pub initial_state: State,
    pub final_states: Vec<State>,
}

pub use causet::{
    error::{Error, Result},
    types::{
        Key, KeyRef, KeyValue, KeyValueRef, KvPair, KvPairRef, KvPairs, KvPairsRef, Value,
        ValueRef,
    },
    util::{
        self,
        bytes::{Bytes, BytesRef},
        hash::{Hash, Hasher},
        iter::{
            Iter, IterMut, IteratorExt, IteratorExtMut, Peekable, PeekableExt, PeekableExtMut,
        },
        slice::{Slice, SliceMut},
        str::{Str, StrRef},
    },
};





#[macro_export]
macro_rules! fdb_try {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return Err(e.into()),
        }
    };
}


pub fn causet_algebrize(turing_automata: &mut FSM) {
    let mut algebrizer = Algebrizer::new();
    algebrizer.algebrize(turing_automata);
}



#[macro_export]
macro_rules! fdb_try_opt {
    ($e:expr) => {
        match $e {
            Some(v) => v,
            None => return Err(Error::NotFound),
        }
    };
}


#[macro_export]
macro_rules! fdb_try_opt_ref {
    ($e:expr) => {
        match $e {
            Some(v) => v,
            None => return Err(Error::NotFound),
        }
    };
}



//! This module provides a causet-based implementation of the `CausalContext` trait.

pub type Result<T> = std::result::Result<T, crate::error::StorageError>;

pub type OwnedParityFilter = (Vec<u8>, Vec<u8>);



#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KV {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}


impl KV {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        KV { key, value }
    }
}


impl KV {
    pub fn from_key_value(key: &[u8], value: &[u8]) -> Self {
        KV {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }
}

/// The abstract storage interface. The table mutant_search and Index mutant_search interlocking_directorate relies on a `Storage`
/// implementation to provide source data.
pub trait Storage: Send {
    type Metrics;

    fn begin_mutant_search(
        &mut self,
        is_spacelike_completion_mutant_search: bool,
        is_soliton_id_only: bool,
        range: Interval,
    ) -> Result<()>;

    fn mutant_search_next(&mut self) -> Result<Option<OwnedParityFilter>>;

    // TODO: Use const generics.
    // TODO: Use reference is better.
    fn get(&mut self, is_soliton_id_only: bool, range: Point) -> Result<Option<OwnedParityFilter>>;

    fn met_unreachable_data(&self) -> Option<bool>;

    fn collect_statistics(&mut self, dest: &mut Self::Metrics);
}

impl<T: Storage + ?Sized> Storage for Box<T> {
    type Metrics = T::Metrics;

    fn begin_mutant_search(
        &mut self,
        is_spacelike_completion_mutant_search: bool,
        is_soliton_id_only: bool,
        range: Interval,
    ) -> Result<()> {
        (**self).begin_mutant_search(is_spacelike_completion_mutant_search, is_soliton_id_only, range)
    }

    fn mutant_search_next(&mut self) -> Result<Option<OwnedParityFilter>> {
        (**self).mutant_search_next()
    }

    fn get(&mut self, is_soliton_id_only: bool, range: Point) -> Result<Option<OwnedParityFilter>> {
        (**self).get(is_soliton_id_only, range)
    }

    fn met_unreachable_data(&self) -> Option<bool> {
        (**self).met_uncacheable_data()
    }

    fn collect_statistics(&mut self, dest: &mut Self::Metrics) {
        (**self).collect_statistics(dest);
    }
}
