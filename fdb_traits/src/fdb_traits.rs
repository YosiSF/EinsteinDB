/// protobuf-like traits for fdb_traits
/// Language: rust
/// Path: fdb_traits/src/fdb_traits.rs
/// Description: These traits are used to implement the trait traits_graph  interfaces format!("{}", self.to_string())  as fdb_traits::traits_graph::TraitGraph.to_string() in fdb_traits/src/lib.rstr
use std::{
    cmp,
    fmt,
    hash,
    marker::PhantomData,
    mem,
    ptr,
    slice,
};






use std::borrow::Cow;
use std::collections::{
    BTreeMap,
    BTreeSet,
};
use std::collections::HashMap;
use std::fmt::{
    Display,
    Formatter,
    Result as FmtResult,
};

use std::iter::FromIterator;
use std::ops::Deref;



/// A trait for EinsteinMerkleTrees that support setting global options
/// This is a simple wrapper around the `std::error::Error` trait.
/// The `EinsteinOptions` trait is implemented for `EinsteinOptionsError` and `einstein_options_error_kind`.
/// Here we define the type of the errors that can be returned by the `EinsteinOptions` trait.
/// The `EinsteinOptions` trait is implemented for `EinsteinOptionsError` and `einstein_options_error_kind`.
///
/// Here we define the type of the errors that can be returned by the `EinsteinOptions` trait.
///
///
/// #[derive(Debug, Clone, PartialEq, Eq)]
/// pub struct EinsteinOptionsError {
///    pub kind: EinsteinOptionsErrorKind,
/// }
///
/// let
///    einstein_options_error_kind::EinsteinOptionsErrorKind::{


/// Here we define the type of the errors that can be returned by the `EinsteinOptions` trait.
/// The `EinsteinOptions` trait is implemented for `EinsteinOptionsError` and `einstein_options_error_kind`.
///
///




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CausetQErrorKind {
    /// Here we define the type of the errors that can be returned by the `EinsteinOptions` trait.
    /// The `EinsteinOptions` trait is implemented for `EinsteinOptionsError` and `einstein_options_error_kind`.
    /// Here we define the type of the errors that can be returned by the `EinsteinOptions` trait.
    ///
    ///



    pub kind: CausetQErrorKind,
    /// Here we define the type of the errors that can be returned by the `EinsteinOptions` trait.



    pub causet: Option<Box<dyn std::error::Error + Send + Sync>>,
}




