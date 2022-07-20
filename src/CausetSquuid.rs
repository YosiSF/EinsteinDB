//Copyright 2022 Whtcorps Inc. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
//BSD 3-Clause License (https://opensource.org/licenses/BSD-3-Clause)
//==============================================================================

//Macro the collections as instances of causets; called at compile time.
//This is a macro because it is called at compile time.


use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Partitioning};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::{thread, time};


use std::sync::mpsc::{RecvError, RecvTimeoutError};
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::TrySendError;
use std::sync::mpsc::SendError;

use std::sync::mpsc::{RecvError, RecvTimeoutError};
use std::sync::mpsc::TryRecvError;


use std::sync::mpsc::{RecvError, RecvTimeoutError};







//==============================================================================

/// A `Sync` implementation for `AllegroPoset`.
/// This implementation is thread-safe.
/// # Examples
/// ```
/// use einsteindb::causetq::sync::new_sync;
/// use einsteindb::causetq::sync::Sync;
///
/// let poset = new_sync();
/// let sync = Sync::new(poset);
///
/// ```

//==============================================================================


///




//==============================================================================

#[derive(Debug)]
pub struct Sync {
    poset: Arc<Mutex<AllegroPoset>>,
}



#[macro_export]
macro_rules! causet {
    ($name:ident, $($key:expr => $value:expr),*) => {
        pub struct $name {
            inner: CausalSet<Arc<HashMap<$($key),*>>>,
        }
        impl $name {
            pub fn new() -> $name {
                $name {
                    inner: CausalSet::new(),
                }
            }
            pub fn get(&self, $($key: $value),*) -> Option<Arc<HashMap<$($key),*>>> {
                self.inner.get($($key),*)
            }
            pub fn insert(&mut self, $($key: $value),*) {
                self.inner.insert($($key),*);
            }
            pub fn remove(&mut self, $($key: $value),*) {
                self.inner.remove($($key),*);
            }
        }
    };
}


#[macro_export]
macro_rules! causet_sync {
    ($name:ident, $($key:expr => $value:expr),*) => {
        pub struct $name {
            inner: CausalSet<Arc<Mutex<HashMap<$($key),*>>>>,
        }
        impl $name {
            pub fn new() -> $name {
                $name {
                    inner: CausalSet::new(),
                }
            }
            pub fn get(&self, $($key: $value),*) -> Option<Arc<Mutex<HashMap<$($key),*>>>> {
                self.inner.get($($key),*)
            }
            pub fn insert(&mut self, $($key: $value),*) {
                self.inner.insert($($key),*);
            }
            pub fn remove(&mut self, $($key: $value),*) {
                self.inner.remove($($key),*);
            }
        }
    };
}


#[macro_export]
macro_rules! causet_test {
    ($name:ident, $($key:expr => $value:expr),*) => {
        pub struct $name {
            inner: CausalSet<Arc<HashMap<$($key),*>>>,
        }
        impl $name {
            pub fn new() -> $name {
                $name {
                    inner: CausalSet::new(),
                }
            }
            pub fn get(&self, $($key: $value),*) -> Option<Arc<HashMap<$($key),*>>> {
                self.inner.get($($key),*)
            }
            pub fn insert(&mut self, $($key: $value),*) {
                self.inner.insert($($key),*);
            }
            pub fn remove(&mut self, $($key: $value),*) {
                self.inner.remove($($key),*);
            }
        }
    };
}



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
}


//==============================================================================
//==============================================================================
//==============================================================================
// Here we define the macro. For each collection, we define a macro that takes
// the collection name and the collection type. The macro then defines a new
// type that is an instance of the collection type. The macro then defines a
// new function that takes the collection name and the collection type. The
// function then returns a new instance of the collection type.



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
}


#[macro_export]
macro_rules! causets {
    ($($name:soliton_id),*) => {
        $(
            pub mod $name {
                use std::collections::HashMap;
                use std::collections::HashSet;
                use std::collections::BTreeSet;
                use std::collections::BTreeMap;
                use std::collections::VecDeque;
                use std::collections::LinkedList;
                use std::collections::BinaryHeap;
                use std::collections::HashSet;
                use std::collections::BTreeMap;
                use std::collections::HashMap;
                use std::collections::BTreeSet;
                use std::collections::VecDeque;
                use std::collections::LinkedList;
                use std::collections::BinaryHeap;
                use std::collections::HashSet;
                use std::collections::BTreeMap;
                use std::collections::HashMap;
                use std::collections::BTreeSet;
                use std::collections::VecDeque;
                use std::collections::LinkedList;
                use std::collections::BinaryHeap;
                use std::collections::HashSet;
                use std::collections::BTreeMap;
                use std::collections::HashMap;
                use std::collections::BTreeSet;
                use std::collections::VecDeque;
                use std::collections::LinkedList;
                use std::collections::BinaryHeap;
                use std::collections::HashSet;
                use std::collections::BTreeMap;
                use std::collections::HashMap;
                use std::collections::BTreeSet;
                use std::collections::VecDeque;
                use std::collections::LinkedList;
                use std::collections::BinaryHeap;
                use std::collections::HashSet;
                use std::collections::BTreeMap;
                use std::collections::HashMap;
                use std::collections::BTreeSet;
                use std::collections::VecDeque;
                //DateTime
                use chrono::{DateTime, Utc};
                use chrono::offset::TimeZone;
                use chrono::offset::Local;
                use chrono::offset::FixedOffset;
                use chrono::offset::Utc;
                //Causetid
                use causetid::Causetid;
                //solitonid
                use solitonid::Solitonid;
                //causetq
                use causetq::Causetq;
                //soliton
                use soliton::Soliton;


        }
        )*  //end of macro
    }
}

use::{
    SchemaReplicator,
    SchemaReplicatorError,
    CausetQqueryableResult,
    CausetQInputStream,
    CausetQOutputStream,
};

//Maintains the mapping between string idents and positive integer causetids
pub struct CausetSquuidQueryBuilder<'a> {
    //mutable store
    pub causet_store: &'a mut CausetSquuidStore,
    pub values : BTreeMap<Variable, causetq_TV>,
    //foundationdb store
    pub store: &'a mut fdb::FdbStore,
    pub merkle_store_fdb_2_einstein_db: BTreeMap<fdb::Fdb, fdb::FdbStore>,
    //The query string
    pub query: String,
    pub causet_squuid: &'a str, //secondary index should be a string, when we have a string index, but we need to be able to use a causetid as a key
    pub causet_squuid_query_builder: CausetSquuidQueryBuilderType,
}

pub struct CausetSquuidQueryBuilderType {
    pub causet_squuid: String,
    pub causet_squuid_query_builder: CausetSquuidQueryBuilderType,

}

impl <'a> CausetSquuidQueryBuilder<'a> {
    pub fn new(
        causet_store: &'a mut CausetSquuidStore,
        store: &'a mut fdb::FdbStore,
        merkle_store_fdb_2_einstein_db: BTreeMap<fdb::Fdb, fdb::FdbStore>,
        causet_squuid: &'a str,
        causet_squuid_query_builder: CausetSquuidQueryBuilderType,
    ) -> CausetSquuidQueryBuilder<'a> {
        CausetSquuidQueryBuilder {
            causet_store,
            values: BTreeMap::new(),
            store,
            merkle_store_fdb_2_einstein_db,
            query: String::new(),
            causet_squuid,
            causet_squuid_query_builder,
        }
    }

        pub fn get_query(&self) -> String {
        self.query.clone()
    }

        pub fn get_causet_squuid(&self) -> String {
        self.causet_squuid.clone()
    }

        pub fn get_causet_squuid_query_builder(&self) -> CausetSquuidQueryBuilderType {
        self.causet_squuid_query_builder.clone()
    }

    pub fn get_causet_store_and_bind<CausetqTv>(
            &mut self,
            variable: &str,
            value: CausetqTv,
        ) -> Result<(), CausetSquuidQueryBuilderError> {

                let causet_squuid_query_builder = self.get_causet_squuid_query_builder();
            let causet_squuid = self.get_causet_squuid();
            let causet_store = self.causet_store;
            let store = self.store;
            let merkle_store_fdb_2_einstein_db = self.merkle_store_fdb_2_einstein_db;
            let values = &mut self.values;
            let query = &mut self.query;
            let causet_squuid_query_builder = &mut self.causet_squuid_query_builder;
            let causet_squuid_query_builder = CausetSquuidQueryBuilder::<'a>::new(
                causet_store,
                store,
                merkle_store_fdb_2_einstein_db,
                causet_squuid,
                causet_squuid_query_builder,
            );
            let causet_squuid_query_builder = causet_squuid_query_builder.bind(variable, value);
            let causet_squuid_query_builder = causet_squuid_query_builder.get_causet_squuid_query_builder();
            let causet_squuid_query_builder = causet_squuid_query_builder.get_query();
            let causet_squuid_query_builder = causet_squuid_query_builder.get_causet_squuid();
            let causet_squuid_query_builder = causet_squuid_query_builder.get_causet_store_and_bind(variable, value);
            let causet_squuid_query_builder = causet_squuid_query_builder.get_causet_squuid_query_builder();
            let causet_squuid_query_builder = causet_squuid_query_builder.get_query();
            let causet_squuid_query_builder = causet_squuid_query_builder.get_causet_squuid();

                Ok(())
        }

        pub fn get_store_bind_ref_from_kw(&mut self, var: &str, value:keyword, index:T) -> Result<&mut Self> where T: into<causetq_TV>{
        self.values.insert(var.to_string(), value.into());
        self.query = format!("{} {} {}", self.query, var, index);
        Ok(self)

        }


        pub fn get_merkle_store_fdb_2_einstein_db(&self) -> &BTreeMap<fdb::Fdb, fdb::FdbStore> {
        &self.merkle_store_fdb_2_einstein_db
            }

        pub fn get_values(&self) -> &BTreeMap<Variable, causetq_TV> {
        &self.values
            }

        pub fn set_bind_ref_from_causetq_values(&mut self, values: KeywordValues) -> Result<(), CausetSquuidQueryBuilderError> {
            let mut values = values;
            let mut values_iter = values.iter();
            let mut values_iter = values_iter.next();
            let mut values_iter = values_iter.unwrap();
            let mut values_iter = values_iter.1;
            let mut values_iter = values_iter.iter();
            let mut values_iter = values_iter.next();
            let mut values_iter = values_iter.unwrap();
            let mut values_iter = values_iter.1;
            let mut values_iter = values_iter.iter();
            let mut values_iter = values_iter.next();
            let mut values_iter = values_iter.unwrap();
            let mut values_iter = values_iter.1;
            let mut values_iter = values_iter.iter();
            let mut values_iter = values_iter.next();
            let mut values_iter = values_iter.unwrap();
            let mut values_iter = values_iter.1;
            let mut values_iter = values_iter.iter();
            let mut values_iter = values_iter.next();
            let mut values_iter = values_iter.unwrap();
            let mut values_iter = values_iter.1;
            let mut values_iter = values_iter.iter();
            let mut values_iter = values_iter.next();
            let mut values_iter = values_iter.unwrap();
            let mut values_iter = values_iter.1;
            let mut values_iter = values_iter.iter();
            let mut values_iter = values_iter.next();
            let mut values_iter = values_iter.unwrap();
            let mut values_iter = values_iter.1;
            let mut values_iter = values_iter.iter();
            let mut values_iter = values_iter.next();
            let mut values_iter = values_iter.unwrap();
            let mut values_iter = values_iter.1;
            let mut values_iter = values_iter.iter();
            let mut values_iter = values_iter.next();
            let mut values_iter = values_iter.unwrap();

        self.values = values;
        self.query = format!("{} {} {}", self.query, values_iter.0, values_iter.1);
        Ok(())
        }


        pub fn set_query(&mut self, query: String) {
        self.query = query;
                }

        pub fn set_causet_squuid(&mut self, causet_squuid: String) {
        self.causet_squuid = causet_squuid;
        self
    }

    pub fn set_causet_squuid_from_kw(&mut self, causet_squuid: keyword) {
        self.causet_squuid = causet_squuid.to_string();
        self
    }


        pub fn set_causet_squuid_query_builder(&mut self, causet_squuid_query_builder: CausetSquuidQueryBuilderType) {
        self.causet_squuid_query_builder = causet_squuid_query_builder;
        self
    }

    //bind
        pub fn bind_variable<CausetqTv>(&mut self, variable: Variable, value: CausetqTv, causet_squuid: &str) -> Result<(), CausetQqueryableResult> {
        self.values.insert(CausetqTv::from_valid_name(variable), value.into(causet_squuid));
        self.causet_squuid = causet_squuid;

        Ok(())
    }
        pub fn execute_causet_tuples(&mut self) -> Result<(), CausetQqueryableResult> {
            let causet_squuid = self.causet_squuid.clone();
            let causet_squuid_query_builder = self.causet_squuid_query_builder.clone();
        }
    }





