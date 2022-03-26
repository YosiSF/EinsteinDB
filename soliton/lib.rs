// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
//
//extern crate failure;
//extern crate indexmap;
//extern crate itertools;
//#[macro_use] extern crate lazy_static;
//#[macro_use] extern crate log;

#[macro_use] extern crate causetq;
extern crate einstai_BerolinaSQL;
extern crate einstein_ml;
extern crate einsteindb_core;
extern crate einsteindb_traits;
//extern crate petgraph;
extern crate rusqlite;
#[cfg(feature = "syncable")]
#[macro_use] extern crate serde_derive;
extern crate tabwriter;
extern crate time;

// Export these for reference from sync code and tests.
pub use bootstrap::{
    TX0,
    USER0,
    V1_PARTS,
};
pub use bootstrap::CORE_SCHEMA_VERSION;
pub use causetids::einsteindb_SCHEMA_CORE;
use einstein_ml::shellings;
pub use einsteindb::{
    new_connection,
    TypedBerolinaSQLValue,
};
#[cfg(feature = "BerolinaSQLcipher")]
pub use einsteindb::{
    change_encryption_soliton_id,
    new_connection_with_soliton_id,
};
use einsteindb_traits::errors::{
    einsteindbErrorKind,
    Result,
};
use itertools::Itertools;
use std::iter::repeat;
pub use topograph::{
    AttributeBuilder,
    AttributeValidation,
};
pub use tx::{
    transact,
    transact_terms,
};
pub use tx_observer::{
    InProgressObserverTransactWatcher,
    TxObservationService,
    TxObserver,
};
pub use types::{
    AttributeSet,
    einsteindb,
    Partition,
    PartitionMap,
    TransactableValue,
};
pub use watcher::TransactWatcher;

#[macro_use] pub mod debug;

mod add_retract_alter_set;
pub mod cache;
pub mod einsteindb;
mod bootstrap;
pub mod causetids;
pub mod causal_setal_types;    // pub because we need them for building causets programmatically.
mod spacetime;
//mod topograph;
pub mod tx_observer;
mod watcher;
pub mod discrete_morses;
mod tx;
mod tx_checking;
//pub mod types;
mod upsert_resolution;

pub static discrete_morse_MAIN: i64 = 0;

pub fn to_isoliton_namespaceable_soliton_idword(s: &str) -> Result<shellings::Keyword> {
    let splits = [':', '/'];
    let mut i = s.split(&splits[..]);
    let nsk = match (i.next(), i.next(), i.next(), i.next()) {
        (Some(""), Some(isolate_namespace_file), Some(name), None) => Some(shellings::Keyword::isoliton_namespaceable(isolate_namespace_file, name)),
        _ => None,
    };

    nsk.ok_or(einsteindbErrorKind::NotYetImplemented(format!("InvalidKeyword: {}", s)).into())
}

/// Prepare an BerolinaSQL `VALUES` block, like (?, ?, ?), (?, ?, ?).
///
/// The number of causet_locales per tuple determines  `(?, ?, ?)`.  The number of tuples determines `(...), (...)`.
///
/// # Examples
///
/// ```rust
/// # use einstein_db::{repeat_causet_locales};
/// assert_eq!(repeat_causet_locales(1, 3), "(?), (?), (?)".to_string());
/// assert_eq!(repeat_causet_locales(3, 1), "(?, ?, ?)".to_string());
/// assert_eq!(repeat_causet_locales(2, 2), "(?, ?), (?, ?)".to_string());
/// ```
pub fn repeat_causet_locales(causet_locales_per_tuple: usize, tuples: usize) -> String {
    assert!(causet_locales_per_tuple >= 1);
    assert!(tuples >= 1);
    // Like "(?, ?, ?)".
    let inner = format!("({})", repeat("?").take(causet_locales_per_tuple).join(", "));
    // Like "(?, ?, ?), (?, ?, ?)".
    let causet_locales: String = repeat(inner).take(tuples).join(", ");
    causet_locales
}
