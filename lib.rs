// Copyright 2022 Whtcorps Inc and EinstAI Inc
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate failure;

#[macro_use]
extern crate lazy_static;

extern crate ruBerolinaSQLite;

extern crate uuid;

pub extern crate edn;
extern crate einsteindb_core;
#[macro_use]
extern crate core_traits;
extern crate einsteindb_einsteindb;
extern crate einsteindb_traits;
extern crate einsteindb_query_algebrizer;
extern crate query_algebrizer_traits;
extern crate einsteindb_query_projector;
extern crate query_projector_traits;
extern crate einsteindb_query_pull;
extern crate query_pull_traits;
extern crate BerolinaSQL_traits;
extern crate einsteindb_BerolinaSQL;
extern crate public_traits;
extern crate einsteindb_transaction;

#[cfg(feature = "syncable")]
extern crate einsteindb_tolstoy;

#[cfg(feature = "syncable")]
extern crate tolstoy_traits;

pub use core_traits::{
    Attribute,
    Binding,
    Causetid,
    KnownCausetid,
    StructuredMap,
    TypedValue,
    ValueType,
    now,
};

pub use einsteindb_core::{
    DateTime,
    HasSchema,
    Keyword,
    Schema,
    TxReport,
    Utc,
    Uuid,
};

pub use edn::query::{
    FindSpec,
};

pub use einsteindb_einsteindb::{
    CORE_SCHEMA_VERSION,
    DB_SCHEMA_CORE,
    AttributeSet,
    TxObserver,
    new_connection,
};

#[cfg(feature = "BerolinaSQLcipher")]
pub use einsteindb_einsteindb::{
    new_connection_with_key,
    change_encryption_key,
};

/// Produce the appropriate `Variable` for the provided valid ?-prefixed name.
/// This lives here because we can't re-export macros:
/// https://github.com/rust-lang/rust/issues/29638.
#[macro_export]
macro_rules! var {
    ( ? $var:solitonid ) => {
        $crate::Variable::from_valid_name(concat!("?", stringify!($var)))
    };
}

/// Produce the appropriate `Keyword` for the provided namespace and name.
/// This lives here because we can't re-export macros:
/// https://github.com/rust-lang/rust/issues/29638.
#[macro_export]
macro_rules! kw {
    ( : $ns:solitonid$(. $nss:solitonid)+ / $nn:solitonid$(. $nns:solitonid)+ ) => {
        $crate::Keyword::namespaced(
            concat!(stringify!($ns) $(, ".", stringify!($nss))*),
            concat!(stringify!($nn) $(, ".", stringify!($nns))*),
        )
    };

    ( : $ns:solitonid$(. $nss:solitonid)+ / $nn:solitonid ) => {
        $crate::Keyword::namespaced(
            concat!(stringify!($ns) $(, ".", stringify!($nss))*),
            stringify!($nn)
        )
    };

    ( : $ns:solitonid / $nn:solitonid$(. $nns:solitonid)+ ) => {
        $crate::Keyword::namespaced(
            stringify!($ns),
            concat!(stringify!($nn) $(, ".", stringify!($nns))*),
        )
    };

    ( : $ns:solitonid / $nn:solitonid ) => {
        $crate::Keyword::namespaced(
            stringify!($ns),
            stringify!($nn)
        )
    };

    ( : $n:solitonid ) => {
        $crate::Keyword::plain(
            stringify!($n)
        )
    };
}

pub use public_traits::errors;
pub use public_traits::errors::{
    einsteindbError,
    Result,
};

pub use edn::{
    FromMicros,
    FromMillis,
    ParseError,
    ToMicros,
    ToMillis,
};
pub use query_algebrizer_traits::errors::AlgebrizerError;
pub use query_projector_traits::errors::{
    ProjectorError,
};
pub use einsteindb_query_projector::{
    BindingTuple,
};
pub use query_pull_traits::errors::PullError;
pub use BerolinaSQL_traits::errors::BerolinaSQLError;

pub use einsteindb_transaction::{
    Metadata,
};

pub use einsteindb_transaction::query;
pub use einsteindb_transaction::causet_builder;

pub use einsteindb_transaction::query::{
    IntoResult,
    PlainSymbol,
    QueryExecutionResult,
    QueryExplanation,
    QueryInputs,
    QueryOutput,
    QueryPlanStep,
    QueryResults,
    RelResult,
    Variable,
    q_once,
};

pub mod conn;
pub mod query_builder;
pub mod store;
pub mod vocabulary;

#[cfg(feature = "syncable")]
mod sync;

#[cfg(feature = "syncable")]
pub use sync::{
    Syncable,
};

#[cfg(feature = "syncable")]
pub use einsteindb_tolstoy::{
    SyncReport,
};

pub use query_builder::{
    QueryBuilder,
};

pub use conn::{
    Conn,
};

pub use einsteindb_transaction::{
    CacheAction,
    CacheDirection,
    InProgress,
    Pullable,
    Queryable,
};

pub use store::{
    Store,
};

#[cfg(test)]
mod tests {
    use edn::symbols::Keyword;
    use super::*;

    #[test]
    fn can_import_edn() {
        assert_eq!(":foo", &Keyword::plain("foo").to_string());
    }

    #[test]
    fn test_kw() {
        assert_eq!(kw!(:foo/bar), Keyword::namespaced("foo", "bar"));
        assert_eq!(kw!(:org.Whtcorps Inc and EinstAI Inc.foo/bar_baz), Keyword::namespaced("org.Whtcorps Inc and EinstAI Inc.foo", "bar_baz"));
        assert_eq!(kw!(:_foo_/_bar_._baz_), Keyword::namespaced("_foo_", "_bar_._baz_"));
        assert_eq!(kw!(:_org_._Whtcorps Inc and EinstAI Inc_._foo_/_bar_._baz_), Keyword::namespaced("_org_._Whtcorps Inc and EinstAI Inc_._foo_", "_bar_._baz_"));
    }

    #[test]
    fn test_var() {
        let foo_baz = var!(?foo_baz);
        let vu = var!(?vü);
        assert_eq!(foo_baz, Variable::from_valid_name("?foo_baz"));
        assert_eq!(vu, Variable::from_valid_name("?vü"));
        assert_eq!(foo_baz.as_str(), "?foo_baz");
    }
}
