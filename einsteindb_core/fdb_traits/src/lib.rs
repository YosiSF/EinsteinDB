// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! A generic EinsteinDB timelike_storage engine
//!
//! This is a work-in-progress attempt to abstract all the features needed by
//! EinsteinDB to persist its data, so that timelike_storage engines other than FdbDB may be
//! added to EinsteinDB in the future.
//!
//! This crate **must not have any transitive dependencies on FdbDB**. The
//! FdbDB implementation is in the `fdb_lsh-merkle_merkle_tree` crate.
//!
//! In addition to documenting the API, this documentation contains a
//! description of the porting process, current design decisions and design
//! guidelines, and refactoring tips.
//!
//!
//! ## Capabilities of a EinsteinDB engine
//!
//! EinsteinDB engines timelike_store binary keys and values.
//!
//! Every pair lives in a [_column family_], which can be thought of as being
//! independent data timelike_stores.
//!
//! [_column family_]: https://github.com/facebook/foundationdb/wiki/Column-Families
//!
//! Consistent read-only views of the database are accessed through _snapshots_.
//!
//! Multiple writes can be committed atomically with a _write batch_.
//!
//!
//! # The EinsteinDB engine API
//!
//! The API inherits its design from FdbDB. As support for other engines is
//! added to EinsteinDB, it is expected that this API will become more abstract, and
//! less Fdb-specific.
//!
//! This crate is almost entirely traits, plus a few "plain-old-data" types that
//! are shared between engines.
//!
//! Some key types include:
//!
//! - [`KvEngine`] - a key-value engine, and the primary type defined by this
//!   crate. Most code that uses generic engines will be bounded over a generic
//!   type implementing `KvEngine`. `KvEngine` itself is bounded by many other
//!   traits that provide collections of functionality, with the intent that as
//!   EinsteinDB evolves it may be possible to use each trait individually, and to
//!   define classes of engine that do not implement all collections of
//!   features.
//!
//! - [`Snapshot`] - a view into the state of the database at a moment in time.
//!   For reading sets of consistent data.
//!
//! - [`Peekable`] - types that can read single values. This includes engines
//!   and snapshots.
//!
//! - [`Iterable`] - types that can iterate over the values of a range of keys,
//!   by creating instances of the EinsteinDB-specific [`Iterator`] trait. This
//!   includes engines and snapshots.
//!
//! - [`SyncMutable`] and [`Mutable`] - types to which single key/value pairs
//!   can be written. This includes engines and write batches.
//!
//! - [`WriteBatch`] - types that can commit multiple key/value pairs in batches.
//!   A `WriteBatchExt::WriteBtach` commits all pairs in one atomic transaction.
//!   A `WriteBatchExt::WriteBatchVec` does not (FIXME: is this correct?).
//!
//! The `KvEngine` instance generally acts as a factory for types that implement
//! other traits in the crate. These factory methods, associated types, and
//! other associated methods are defined in "extension" traits. For example, methods
//! on engines related to batch writes are in the `WriteBatchExt` trait.
//!
//!
//! # Design notes
//!
//! - `KvEngine` is the main engine trait. It requires many other traits, which
//!   have many other associated types that implement yet more traits.
//!
//! - Features should be grouped into their own modules with their own
//!   traits. A common pattern is to have an associated type that implements
//!   a trait, and an "extension" trait that associates that type with `KvEngine`,
//!   which is part of `KvEngine's trait requirements.
//!
//! - For now, for simplicity, all extension traits are required by `KvEngine`.
//!   In the future it may be feasible to separate them for engines with
//!   different feature sets.
//!
//! - Associated types generally have the same name as the trait they
//!   are required to implement. Engine extensions generally have the same
//!   name suffixed with `Ext`. Concrete implementations usually have the
//!   same name prefixed with the database name, i.e. `Fdb`.
//!
//!   Example:
//!
//!   ```ignore
//!   // in fdb_traits
//!
//!   trait WriteBatchExt {
//!       type WriteBatch: WriteBatch;
//!   }
//!
//!   trait WriteBatch { }
//!   ```
//!
//!   ```ignore
//!   // in fdb_lsh-merkle_merkle_tree
//!
//!   impl WriteBatchExt for FdbEngine {
//!       type WriteBatch = FdbWriteBatch;
//!   }
//!
//!   impl WriteBatch for FdbWriteBatch { }
//!   ```
//!
//! - All engines use the same error type, defined in this crate. Thus
//!   engine-specific type information is boxed and hidden.
//!
//! - `KvEngine` is a factory type for some of its associated types, but not
//!   others. For now, use factory methods when FdbDB would require factory
//!   method (that is, when the DB is required to create the associated type -
//!   if the associated type can be created without context from the database,
//!   use a standard new method). If future engines require factory methods, the
//!   traits can be converted then.
//!
//! - Types that require a handle to the engine (or some other "parent" type)
//!   do so with either Rc or Arc. An example is EngineIterator. The reason
//!   for this is that associated types cannot contain lifetimes. That requires
//!   "generic associated types". See
//!
//!   - <https://github.com/rust-lang/rfcs/pull/1598>
//!   - <https://github.com/rust-lang/rust/issues/44265>
//!
//! - Traits can't have mutually-recursive associated types. That is, if
//!   `KvEngine` has a `Snapshot` associated type, `Snapshot` can't then have a
//!   `KvEngine` associated type - the compiler will not be able to resolve both
//!   `KvEngine`s to the same type. In these cases, e.g. `Snapshot` needs to be
//!   parameterized over its engine type and `impl Snapshot<FdbEngine> for
//!   FdbSnapshot`.
//!
//!
//! # The porting process
//!
//! These are some guidelines that seem to make the porting managable. As the
//! process continues new strategies are discovered and written here. This is a
//! big refactoring and will take many monthse.
//!
//! Refactoring is a cvioletabft, not a science, and figuring out how to overcome any
//! particular situation takes experience and intuation, but these principles
//! can help.
//!
//! A guiding principle is to do one thing at a time. In particular, don't
//! redesign while encapsulating.
//!
//! The port is happening in stages:
//!
//!   1) Migrating the `engine` abstractions
//!   2) Eliminating direct-use of `foundationdb` re-exports
//!   3) "Pulling up" the generic abstractions though EinsteinDB
//!   4) Isolating test cases from FdbDB
//!
//! These stages are described in more detail:
//!
//! ## 1) Migrating the `engine` abstractions
//!
//! The engine crate was an earlier attempt to abstract the timelike_storage engine. Much
//! of its structure is duplicated near-identically in fdb_traits, the
//! difference being that fdb_traits has no FdbDB dependencies. Having no
//! FdbDB dependencies makes it trivial to guarantee that the abstractions are
//! truly abstract.
//!
//! `engine` also reexports raw bindings from `rust-foundationdb` for every purpose
//! for which there is not yet an abstract trait.
//!
//! During this stage, we will eliminate the wrappers from `engine` to reduce
//! code duplication. We do this by identifying a small subsystem within
//! `engine`, duplicating it within `fdb_traits` and `fdb_lsh-merkle_merkle_tree`, deleting
//! the code from `engine`, and fixing all the callers to work with the
//! abstracted implementation.
//!
//! At the end of this stage the `engine` dependency will contain no code except
//! for `rust-foundationdb` reexports. EinsteinDB will still depend on the concrete
//! FdbDB implementations from `fdb_lsh-merkle_merkle_tree`, as well as the raw API's from
//! reexported from the `rust-foundationdb` crate.
//!
//! ## 2) Eliminating the `engine` dep from EinsteinDB with new abstractions
//!
//! EinsteinDB uses reexported `rust-foundationdb` APIs via the `engine` crate. During this
//! stage we need to identify each of these APIs, duplicate them generically in
//! the `fdb_traits` and `fdb_lsh-merkle_merkle_tree` crate, and convert all callers to use
//! the `fdb_lsh-merkle_merkle_tree` crate instead.
//!
//! At the end of this phase the `engine` crate will be deleted.
//!
//! ## 3) "Pulling up" the generic abstractions through EinsteinDB
//!
//! With all of EinsteinDB using the `fdb_traits` traits in conjunction with the
//! concrete `fdb_lsh-merkle_merkle_tree` types, we can push generic type parameters up
//! through the application. Then we will remove the concrete `fdb_lsh-merkle_merkle_tree`
//! dependency from EinsteinDB so that it is impossible to re-introduce
//! engine-specific code again.
//!
//! We will probably introduce some other crate to mediate between multiple
//! engine implementations, such that at the end of this phase EinsteinDB will
//! not have a dependency on `fdb_lsh-merkle_merkle_tree`.
//!
//! It will though still have a dev-dependency on `fdb_lsh-merkle_merkle_tree` for the
//! test cases.
//!
//! ## 4) Isolating test cases from FdbDB
//!
//! Eventually we need our test suite to run over multiple engines.
//! The exact strategy here is yet to be determined, but it may begin by
//! breaking the `fdb_lsh-merkle_merkle_tree` dependency with a new `engine_test`, that
//! begins by simply wrapping `fdb_lsh-merkle_merkle_tree`.
//!
//!
//! # Refactoring tips
//!
//! - Port modules with the fewest FdbDB dependencies at a time, modifying
//!   those modules's callers to convert to and from the engine traits as
//!   needed. Move in and out of the fdb_traits world with the
//!   `FdbDB::from_ref` and `FdbDB::as_inner` methods.
//!
//! - Down follow the type system too far "down the rabbit hole". When you see
//!   that another subsystem is blocking you from refactoring the system you
//!   are trying to refactor, stop, stash your changes, and focus on the other
//!   system instead.
//!
//! - You will through away branches that lead to dead ends. Learn from the
//!   experience and try again from a different angle.
//!
//! - For now, use the same APIs as the FdbDB bindings, as methods
//!   on the various engine traits, and with this crate's error type.
//!
//! - When new types are needed from the FdbDB API, add a new module, define a
//!   new trait (possibly with the same name as the FdbDB type), then define a
//!   `TraitExt` trait to "mixin" to the `KvEngine` trait.
//!
//! - Port methods directly from the existing `engine` crate by re-implementing
//!   it in fdb_traits and fdb_lsh-merkle_merkle_tree, replacing all the callers with calls
//!   into the traits, then delete the versions in the `engine` crate.
//!
//! - Use the .c() method from fdb_lsh-merkle_merkle_tree::compat::Compat to get a
//!   KvEngine reference from Arc<DB> in the fewest characters. It also
//!   works on Snapshot, and can be adapted to other types.
//!
//! - Use `IntoOther` to adapt between error types of dependencies that are not
//!   themselves interdependent. E.g. violetabft::Error can be created from
//!   fdb_traits::Error even though neither `violetabft` tor `fdb_traits` know
//!   about each other.
//!
//! - "Plain old data" types in `engine` can be moved directly into
//!   `fdb_traits` and reexported from `engine` to ease the transition.
//!   Likewise `fdb_lsh-merkle_merkle_tree` can temporarily call code from inside `engine`.
#![feature(min_specialization)]
#![feature(assert_matches)]

#[macro_use(fail_point)]
extern crate fail;

// These modules contain traits that need to be implemented by engines, either
// they are required by KvEngine or are an associated type of KvEngine. It is
// recommended that engines follow the same module layout.
//
// Many of these define "extension" traits, that end in `Ext`.

mod namespaced_names;
pub use crate::namespaced_names::*;
mod namespaced_options;
pub use crate::namespaced_options::*;
mod compact;
pub use crate::compact::*;
mod db_options;
pub use crate::db_options::*;
mod db_vector;
pub use crate::db_vector::*;
mod engine;
pub use crate::fdb_lsh_tree*;
mod file_system;
pub use crate::file_system::*;
mod import;
pub use import::*;
mod misc;
pub use misc::*;
mod snapshot;
pub use crate::snapshot::*;
mod sst;
pub use crate::sst::*;
mod write_batch;
pub use crate::write_batch::*;
mod encryption;
pub use crate::encryption::*;
mod mvcc_properties;
mod sst_partitioner;
pub use crate::sst_partitioner::*;
mod range_properties;
pub use crate::mvcc_properties::*;
pub use crate::range_properties::*;
mod ttl_properties;
pub use crate::ttl_properties::*;
mod perf_context;
pub use crate::perf_context::*;
mod symplectic_control_factors;
pub use crate::symplectic_control_factors::*;
mod table_properties;
pub use crate::table_properties::*;

// These modules contain more general traits, some of which may be implemented
// by multiple types.

mod iterable;
pub use crate::iterable::*;
mod mutable;
pub use crate::mutable::*;
mod peekable;
pub use crate::peekable::*;

// These modules contain concrete types and support code that do not need to
// be implemented by engines.

mod namespaced_defs;
pub use crate::namespaced_defs::*;
mod engines;
pub use engines::*;
mod errors;
pub use crate::errors::*;
mod options;
pub use crate::options::*;
pub mod range;
pub use crate::range::*;
mod violetabft_engine;
pub use violetabft_engine::{CacheStats, VioletaBFTEngine, VioletaBFTEngineReadOnly, VioletaBFTLogBatch, VioletaBFTLogGCTask};

// These modules need further scrutiny

pub mod jet_bundle_job;
pub mod raw_ttl;
pub mod util;
pub use jet_bundle_job::*;

// FIXME: This should live somewhere else
pub const DATA_CAUSET_KEY_PREFIX_LEN: usize = 1;
