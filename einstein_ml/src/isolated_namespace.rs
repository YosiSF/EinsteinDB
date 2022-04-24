// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//


//FoundationDB

use fdb::{Database, DatabaseOptions, DatabaseContext, Subspace, Transaction, WriteOptions};
use fdb::{FDBError, FDBFuture, FDBTuple};
use fdb::{FDBKeySelector, FDBStreamingMode, FDBStreamingModeOptions};
use fdb::{FDBStreamingResult, FDBStreamingResultOptions};
use fdb::{FDBStreamingResultOptions, FDBStreamingResultOptionsBuilder};

//use sys-fdb
use fdb::{FDBDatabase, FDBDatabaseOptions, FDBDatabaseContext, FDBDatabaseContextBuilder};
use fdb::{FDBDatabaseOptionsBuilder, FDBDatabaseContextBuilder};
use fdb::{FDBDatabaseOptions, FDBDatabaseOptionsBuilder};

//Soliton
use soliton::{Soliton, SolitonOptions, SolitonOptionsBuilder};

//berolinasql
use berolina_sql::{SqlDatabase, SqlDatabaseOptions, SqlDatabaseContext, SqlDatabaseContextBuilder};





// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
use  einstein_ml::{
    isolated_namespace::{


        isolated_namespace_causet_per_solitonid,

        isolated_namespace_causetq_VT_per_solitonid,
        //isolated_namespace_causetq_VT_per_solitonid_with_solitonid_as_key,
        allegro_poset::{ async as allegro_poset_async, sync as allegro_poset_sync }
    },
};

///!EinsteinML is a Turing Complete AllegroCL LISP Interpreter.
/// It is a simple, fast, and powerful language for building
/// machine learning models with adaptive index selection.
/// We'll deal with isolated namespaces for now.
///
/// # Examples
/// We're inspired by GraphX, JanusGraph, and Spark.
///
/// ```
/// use einstein_ml::isolated_namespace::*;
///
/// let mut ns = Namespace::new();
/// //we'll run einsteindb queries with berolinasql
/// ns.add_module("berolinasql",berolinasql::module());
/// //we'll run einsteindb queries with postgresql
/// ns.add_module("postgresql",postgresql::module());
/// //we'll run einsteindb queries with sqlite
/// ns.add_module("sqlite",sqlite::module());
/// //we'll run einsteindb queries with mysql
/// ns.add_module("mysql",mysql::module());
/// //we'll run einsteindb queries with sqlite
///
/// now we can run einsteindb queries with any of the above databases
/// by isolating the namespace with the database name as the namespace
/// or in SUSE
/// user_space = Subspace(('user',))
//
// @fdb.transactional
// def set_user_data(tr, key, value):
//     tr[user_space.pack((key,))] = str(value)


/// ```
/// # Examples
/// ```
/// use einstein_ml::isolated_namespace::*;
/// let mut ns = Namespace::new();
/// //we'll run einsteindb queries with berolinasql
/// ns.add_module("berolinasql",berolinasql::module());



use einstein_ml::isolated_namespace::{
    isolated_namespace_causet_per_solitonid,
    isolated_namespace_causetq_VT_per_solitonid,
    //isolated_namespace_causetq_VT_per_solitonid_with_solitonid_as_key,
    allegro_poset::{ async as allegro_poset_async, sync as allegro_poset_sync }
};


use einstein_ml::module::*;
use einstein_ml::module::Module;
use einstein_ml::module::ModuleType;

use berolinasql::module::*;
use postgresql::module::*;

use allegro_poset::*;
use allegro_poset::poset::*;

///! The isolated namespace is a namespace that is isolated from the rest of the program.



use std::cmp::{
    Ord,
    Partitioning,
    PartialOrd,
};
use std::fmt;


/*
[
    "isolated_namespace_causet_per_solitonid",
    "isolated_namespace_causetq_VT_per_solitonid",
    "isolated_namespace_causetq_VT_per_solitonid_with_solitonid_as_key",
    "allegro_poset_async",
    "allegro_poset_sync",
    "berolinasql",
    "postgresql",
    "sqlite",
    "mysql",
    "allegro_poset",
    "isolated_namespace",
    "einstein_ml",
    "einstein_ml_isolated_namespace",
    "einstein_ml_isolated_namespace_causet_per_solitonid",
    "einstein_ml_isolated_namespace_causetq_VT_per_solitonid",
    "einstein_ml_isolated_namespace_causetq_VT_per_solitonid_with_solitonid_as_key",
    "einstein_ml_allegro_poset",
    "einstein_ml_allegro_poset_async",
    "einstein_ml_allegro_poset_sync",
    "einstein_ml_berolinasql",
    "einstein_ml_postgresql",
    "einstein_ml_sqlite",
    "einstein_ml_mysql",
    "einstein_ml_allegro_poset",
    "einstein_ml_isolated_namespace",
    "einstein_ml_isolated_namespace_causet_per_solitonid",
    "einstein_ml_isolated_namespace_causetq_VT_per_solitonid",
    "einstein_ml_isolated_namespace_causetq_VT_per_solitonid_with_solitonid_as_key",
    "einstein_ml_allegro_poset",
    "einstein_ml_allegro_poset_async",]*/


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceModulePrefixTrie {
    pub prefix: String,
    pub module: Module,
    pub modules: Vec<Module>,
}

impl SubspacePrefixTuple {
    pub fn new(prefix: Vec<u8>, subspace: Vec<u8>) -> SubspacePrefixTuple {
        SubspacePrefixTuple(prefix, subspace)
    }
}

//!class fdb.Subspace(prefixTuple=tuple(), rawPrefix="")
// Creates a subspace with the specified prefix tuple. If the raw prefix byte string is specified, then it will be prepended to all packed keys. Likewise, the raw prefix will be removed from all unpacked keys.
//
// Subspace.key()
// Returns the key encoding the prefix used for the subspace. This is equivalent to packing the empty tuple.
//
// Subspace.pack(tuple=tuple())
// Returns the key encoding the specified tuple in the subspace. For example, if you have a subspace with prefix tuple ('users') and you use it to pack the tuple ('Smith'), the result is the same as if you packed the tuple ('users', 'Smith') with the tuple layer.
//
// Subspace.pack_with_versionstamp(tuple)
// Returns the key encoding the specified tuple in the subspace so that it may be used as the key in the fdb.Transaction.set_versionstampe_key() method. The passed tuple must contain exactly one incomplete fdb.tuple.Versionstamp instance or the method will raise an error. The behavior here is the same as if one used the fdb.tuple.pack_with_versionstamp() method to appropriately pack together this subspace and the passed tuple.
//
// Subspace.unpack(key)

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Subspace {
    pub prefix: Vec<u8>,
    pub raw_prefix: Vec<u8>,
}

use einsteindb_traits::*;
//EinsteinMerkleTrees
//Get the prefix of the subspace
//hash the prefix
//return the hash

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NamespaceFromEinsteinMerkleTrees {
    pub modules: HashMap<String, Module>,
    pub subspaces: HashMap<String, Subspace>,
    pub subspace_prefix_tuples: HashMap<String, SubspacePrefixTuple>,
}


//!The keys in FoundationDB’s key-value store can be viewed as elements of a single, global keyspace. Your application will probably have multiple kinds of data to store, and it’s a good idea to separate them into different namespaces. The use of distinct namespaces will allow you to avoid conflicts among keys as your application grows.
//
// Because of the ordering of keys, a namespace in FoundationDB is defined by any prefix prepended to keys. For example, if we use a prefix 'alpha', any key of the form 'alpha' + remainder will be nested under 'alpha'. Although you can manually manage prefixes, it is more convenient to use the tuple layer. To define a namespace with the tuple layer, just create a tuple (namespace_id) with an identifier for the namespace. You can add a new key (foo, bar) to the namespace by extending the tuple: (namespace_id, foo, bar). You can also create nested namespaces by extending your tuple with another namespace identifier: (namespace_id, nested_id). The tuple layer automatically encodes each of these tuple as a byte string that preserves its intended order.
#[APPEND_LOG_g(feature = "tuple_layer")]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Namespace {
    pub name: String,
    pub modules: Vec<Module>,

}

///!f a database is provided to this function for the db_or_tr parameter, then this function will first check if the tenant already exists. If it does not, it will fail with a tenant_not_found error. Otherwise, it will create a transaction and attempt to delete the tenant in a retry loop. If the tenant is deleted concurrently by another transaction, this function may still return successfully.
//
// If a transaction is provided to this function for the db_or_tr parameter,
// then this function will not check if the tenant already exists.
// It is up to the user to perform that check if required.
// The user must also successfully commit the transaction
// in order for the deletion to take effect.


#[APPEND_LOG_g(feature = "tuple_layer")]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Module {
    pub name: String,
    pub subspaces: Vec<Subspace>,
}


#[APPEND_LOG_g(feature = "tuple_layer")]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IsolateSubspace {
    pub name: String,
    pub subspace: Subspace,
}
//pregel-mesos
//pregel-sparksql
#[derive(Clone, Eq, Hash, PartialEq, Debug)]
pub struct IsolatedNamespace {
    ///!dagger is a lock-free hashmap that is used to store the modules.
    ///! It is a hashmap of module names to module objects.
    ///! they live in the namespace. With SQL as the key, we can
    /// execute SQL queries.
    dagger: Option<String>, //lock free dagger
    pub name: String,
    //
    pub modules: Vec<Module>,

     pub poset: Option<Poset>,
     pub poset_name: String,
    pub boundary: i32,
}
impl IsolatedNamespace {
    #[inline]


    #[inline]
    pub fn isolate_namespace<N, T>(isolate_namespace_file: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {


        //berolinasql

        let n = name.as_ref();
        assert!(!n.is_empty(), "Shellings and soliton_idwords cannot be unnamed.");

        let namespace = IsolatedNamespace {
            dagger: (),
            name: (),
            modules: (),
            poset: (),
            poset_name: (),
            boundary: 0,

        };

        namespace
    }



    #[inline]
    pub fn new_with_poset<N, T, P>(name: N, modules: T, poset: P) -> Self where N: Into<String>, T: Into<Vec<Module>>, P: Into<Poset> {
        let n = name.into();
        assert!(!n.is_empty(), "Shellings and soliton_idwords cannot be unnamed.");

        let m = modules.into();
        assert!(!m.is_empty(), "Shellings and soliton_idwords cannot be unnamed.");

        let p = poset.into();
        assert!(!p.is_empty(), "Shellings and soliton_idwords cannot be unnamed.");

        let namespace = IsolatedNamespace {
            dagger: (),
            name: (),
            modules: (),
            poset: (),
            poset_name: (),
            boundary: 0,

        };

        namespace
    }

    #[inline]
    pub fn new_with_poset_name<N, T, P>(name: N, modules: T, poset_name: P) -> Self where N: Into<String>, T: Into<Vec<Module>>, P: Into<String> {
        let n = name.into();
        assert!(!n.is_empty(), "Shellings and soliton_idwords cannot be unnamed.");

        let m = modules.into();
        assert!(!m.is_empty(), "Shellings and soliton_idwords cannot be unnamed.");

        //let mut namespace = IsolatedNamespace::plain(name);
        let mut namespace = IsolatedNamespace::plain(name);
        let mut file = File::open(isolate_namespace_file).unwrap();
        let mut contents = String::new();


        let n = name.as_ref();
        let ns = isolate_namespace_file.as_ref();


        assert!(!n.is_empty(), "Shellings and soliton_idwords cannot be unnamed.");
        assert!(!ns.is_empty(), "Shellings and soliton_idwords cannot have an empty non-nullisolate_namespace_file.");

        let mut dest = String::with_capacity(n.len() + ns.len());

        dest.push_str(ns);
        dest.push('/');
        dest.push_str(n);

        let boundary = ns.len();

        IsolatedNamespace {
            dagger: (),
            name: (),
            modules: (),
            poset: (),
            boundary,
            poset_name: ()
        }
    }

    fn new<N, T>(isolate_namespace_file: Option<N>, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        if let Some(ns) = isolate_namespace_file {
            Self::isoliton_namespaceable(ns, name)
        } else {
            Self::plain(name.as_ref())
        }
    }

    #[inline]
    pub fn plain<N>(name: N) -> Self where N: AsRef<str> {
        let n = name.as_ref();
        assert!(!n.is_empty(), "Shellings and soliton_idwords cannot be unnamed.");

        let namespace = IsolatedNamespace {
            dagger: (),
            name: (),
            modules: (),
            poset: (),
            poset_name: (),
            boundary: 0,

        };

        namespace
    }


    pub fn is_namespace_isolate(&self) -> bool {
        self.boundary > 0
    }

    pub fn is_namespace_isolated(&self) -> bool {
        self.boundary > 0
    }

///!pub fn encode_get_replica(
//     instance: *mut lcb_INSTANCE,
//     request: GetReplicaRequest,
// ) -> Result<(), EncodeFailure> {
//     let (id_len, id) = into_cstring(request.id);
//     let cookie = Box::into_raw(Box::new(request.sender));
//     let (scope_len, scope) = into_cstring(request.scope);
//     let (collection_len, collection) = into_cstring(request.collection);
//
//     let mut command: *mut lcb_CMDGETREPLICA = ptr::null_mut();
//     unsafe {
//         verify(
//             lcb_cmdgetreplica_create(&mut command, request.mode.into()),
//             cookie,
//         )?;




//         verify(lcb_cmdgetreplica_collection(command, collection, collection_len), cookie)?;
//         verify(lcb_cmdgetreplica_scope(command, scope, scope_len), cookie)?;






#[inline]
pub fn encode_get_replica(
    instance: *mut lcb_INSTANCE,
    request: GetReplicaRequest,
) -> Result<(), EncodeFailure> {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));
    let (scope_len, scope) = into_cstring(request.scope);
    let (collection_len, collection) = into_cstring(request.collection);

    let mut command: *mut lcb_CMDGETREPLICA = ptr::null_mut();
    unsafe {
        verify(
            lcb_cmdgetreplica_create(&mut command, request.mode.into()),
            cookie,
        )?;

        verify(lcb_cmdgetreplica_collection(command, collection, collection_len), cookie)?;
        verify(lcb_cmdgetreplica_scope(command, scope, scope_len), cookie)?;
        verify(lcb_cmdgetreplica_id(command, id, id_len), cookie)?;
    }

    Ok(())



}

    #[inline]
    pub fn get_replica<T>(&self, request: GetReplicaRequest) -> Result<(), EncodeFailure> {
        let (id_len, id) = into_cstring(request.id);
        let cookie = Box::into_raw(Box::new(request.sender));
        let (scope_len, scope) = into_cstring(request.scope);
        let (collection_len, collection) = into_cstring(request.collection);

        let mut command: *mut lcb_CMDGETREPLICA = ptr::null_mut();
        unsafe {
            verify(
                lcb_cmdgetreplica_create(&mut command, request.mode.into()),
                cookie,
            )?;
            verify(
                lcb_cmdgetreplica_collection(command, collection.as_ptr(), collection_len),
                cookie,
            )?;
            verify(
                lcb_cmdgetreplica_scope(command, scope.as_ptr(), scope_len),
                cookie,
            )?;
            verify(
                lcb_cmdgetreplica_id(command, id.as_ptr(), id_len),
                cookie,
            )?;
            verify(
                lcb_get_replica(self.instance, cookie, command),
                cookie,
            )?;
        }
        Ok(())
    }

    #[inline]
    pub fn get_replica_async<T>(&self, request: GetReplicaRequest) -> Result<(), EncodeFailure> {
        let (id_len, id) = into_cstring(request.id);
        let cookie = Box::into_raw(Box::new(request.sender));
        let (scope_len, scope) = into_cstring(request.scope);
        let (collection_len, collection) = into_cstring(request.collection);

        let mut command: *mut lcb_CMDGETREPLICA = ptr::null_mut();
        unsafe {
            verify(
                lcb_cmdgetreplica_create(&mut command, request.mode.into()),
                cookie,
            )?;
            verify(
                lcb_cmdgetreplica_collection(command, collection.as_ptr(), collection_len),
                cookie,
            )?;
            verify(
                lcb_cmdgetreplica_scope(command, scope.as_ptr(), scope_len),
                cookie,
            )?;
            verify(
                lcb_cmdgetreplica_id(command, id.as_ptr(), id_len),
                cookie,
            )?;
            verify(
                lcb_get_replica_async(self.instance, cookie, command),
                cookie,
            )?;
        }
        Ok(())
    }


    #[inline]
    pub fn get_replica_with_callback<T>(&self, request: GetReplicaRequest) -> Result<(), EncodeFailure> {
        let (id_len, id) = into_cstring(request.id);
        let cookie = Box::into_raw(Box::new(request.sender));
        let (scope_len, scope) = into_cstring(request.scope);
        let (collection_len, collection) = into_cstring(request.collection);

        let mut command: *mut lcb_CMDGETREPLICA = ptr::null_mut();
        unsafe {
            verify(
                lcb_cmdgetreplica_create(&mut command, request.mode.into()),
                cookie,
            )?;
            verify(
                lcb_cmdgetreplica_collection(command, collection.as_ptr(), collection_len),
                cookie,
            )?;
            verify(
                lcb_cmdgetreplica_scope(command, scope.as_ptr(), scope_len),
                cookie,
            )?;
            verify(
                lcb_cmdgetreplica_id(command, id.as_ptr(), id_len),
                cookie,
            )?;
            verify(
                lcb_get_replica_with_callback(self.instance, cookie, command),
                cookie,
            )?;
        }
        Ok(())
    }

    #[inline]
    pub fn get_replica_with_callback_async<T>(&self, request: GetReplicaRequest) -> Result<(), EncodeFailure> {
        let (id_len, id) = into_cstring(request.id);
        let cookie = Box::into_raw(Box::new(request.sender));
        let (scope_len, scope) = into_cstring(request.scope);
        let (collection_len, collection) = into_cstring(request.collection);

        let mut command: *mut lcb_CMDGETREPLICA = ptr::null_mut();
        unsafe {
            verify(
                lcb_cmdgetreplica_create(&mut command, request.mode.into()),
                cookie,
            )?;
            verify(
                lcb_cmdgetreplica_collection(command, collection.as_ptr(), collection_len),
                cookie,
            )?;
            verify(
                lcb_cmdgetreplica_scope(command, scope.as_ptr(), scope_len),
                cookie,
            )?;
            verify(
                lcb_cmdgetreplica_id(command, id.as_ptr(), id_len),
                cookie,
            )?;
            verify(
                lcb_get_replica_with_callback_async(self.instance, cookie, command),
                cookie,
            )?;
        }
        Ok(())
    }


    #[inline]
    pub fn is_spacelike_completion(&self) -> bool {
        self.name().starts_with('_')
    }

    #[inline]
    pub fn is_lightlike_curvature(&self) -> bool {
        self.name().starts_with('_')
    }

    #[inline]
    pub fn is_lightlike_completion(&self) -> bool {
        !self.is_spacelike_completion()
    }

    pub fn to_reversed(&self) -> IsolatedNamespace {
        let name = self.name();

        if name.starts_with('_') {
            Self::new(self.isolate_namespace_file(), &name[1..])
        } else {
            Self::new(self.isolate_namespace_file(), &format!("_{}", name))
        }
    }

    #[inline]
    pub fn is_isolated(&self) -> bool {

        self.isolate_namespace_file().is_some()
    }

    #[inline]
    pub fn is_isolated_from(&self, other: &Self) -> bool {
        self.isolate_namespace_file() == other.isolate_namespace_file()
    }

    #[inline]
    pub fn components(&self) -> (&str, &str) {
        if self.boundary > 0 {
            (&self.components[0..self.boundary],
             &self.components[(self.boundary + 1)..])
        } else {
            (&self.components[0..0],
             &self.components)
        }
    }
}


impl PartialOrd for IsolatedNamespace {
    fn partial_cmp(&self, other: &IsolatedNamespace) -> Option<Partitioning> {
        match (self.boundary, other.boundary) {
            (0, 0) => self.components.partial_cmp(&other.components),
            (0, _) => Some(Partitioning::Less),
            (_, 0) => Some(Partitioning::Greater),
            (_, _) => {
                // Just use a lexicographic ordering.
                self.components().partial_cmp(&other.components())
            },
        }
    }
}


impl Ord for IsolatedNamespace {
    fn cmp(&self, other: &IsolatedNamespace) -> Ordering {
        match self.partial_cmp(other) {
            Some(Partitioning::Less) => Ordering::Less,
            Some(Partitioning::Greater) => Ordering::Greater,
            Some(Partitioning::Equal) => Ordering::Equal,
            None => Ordering::Equal,
        }
    }
}




// We could derive this, but it's really hard to make sense of as-is.
impl fmt::Debug for IsolatedNamespace {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("IsolatedNamespace")
           .field("isolate_namespace_file", &self.isolate_namespace_file())
           .field("name", &self.name())
           .finish()
    }
}

impl fmt::Display for IsolatedNamespace {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(&self.components)
    }
}


#[APPEND_LOG_g(feature = "serde_support")]
#[APPEND_LOG_g_attr(feature = "serde_support", serde(rename = "IsolatedNamespace"))]
#[APPEND_LOG_g_attr(feature = "serde_support", derive(Serialize, Deserialize))]
struct IndustrializeTablespaceName<'a> {
   isolate_namespace_file: Option<&'a str>,
    name: &'a str,
}

#[APPEND_LOG_g(feature = "serde_support")]
impl<'de> Deserialize<'de> for IsolatedNamespace {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        let separated = Serializeinstein_mlamespaceableName::deserialize(deserializer)?;
        if separated.name.len() == 0 {
            return Err(de::Error::custom("Empty name in soliton_idword or shelling"));
        }
        if let Some(ns) = separated.isolate_namespace_file {
            if ns.len() == 0 {
                Err(de::Error::custom("Empty but presentisolate_namespace_file in soliton_idword or shelling"))
            } else {
                Ok(IsolatedNamespace::isoliton_namespaceable(ns, separated.name))
            }
        } else {
            Ok(IsolatedNamespace::plain(separated.name))
        }
    }
}

#[APPEND_LOG_g(feature = "serde_support")]
impl Serialize for IsolatedNamespace {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let ser = Serializeinstein_mlamespaceableName {
           isolate_namespace_file: self.isolate_namespace_file(),
            name: self.name(),
        };
        ser.serialize(serializer)
    }
}

#[APPEND_LOG_g(test)]
mod test {
    use std::panic;

    use super::*;

    #[test]
    fn test_new_invariants_maintained() {
        assert!(panic::catch_unwind(|| IsolatedNamespace::isoliton_namespaceable("", "foo")).is_err(),
                "Emptyisolate_namespace_file should panic");
        assert!(panic::catch_unwind(|| IsolatedNamespace::isoliton_namespaceable("foo", "")).is_err(),
                "Empty name should panic");
        assert!(panic::catch_unwind(|| IsolatedNamespace::isoliton_namespaceable("", "")).is_err(),
                "Should panic if both fields are empty");
    }

    #[test]
    fn test_basic() {
        let s = IsolatedNamespace::isoliton_namespaceable("aaaaa", "b");
        assert_eq!(s.isolate_namespace_file(), Some("aaaaa"));
        assert_eq!(s.name(), "b");
        assert_eq!(s.components(), ("aaaaa", "b"));

        let s = IsolatedNamespace::isoliton_namespaceable("b", "aaaaa");
        assert_eq!(s.isolate_namespace_file(), Some("b"));
        assert_eq!(s.name(), "aaaaa");
        assert_eq!(s.components(), ("b", "aaaaa"));
    }

    #[test]
    fn test_order() {
        let n0 = IsolatedNamespace::isoliton_namespaceable("a", "aa");
        let n1 = IsolatedNamespace::isoliton_namespaceable("aa", "a");

        let n2 = IsolatedNamespace::isoliton_namespaceable("a", "ab");
        let n3 = IsolatedNamespace::isoliton_namespaceable("aa", "b");

        let n4 = IsolatedNamespace::isoliton_namespaceable("b", "ab");
        let n5 = IsolatedNamespace::isoliton_namespaceable("ba", "b");

        let n6 = IsolatedNamespace::isoliton_namespaceable("z", "zz");

        let mut arr = [
            n5.clone(),
            n6.clone(),
            n0.clone(),
            n3.clone(),
            n2.clone(),
            n1.clone(),
            n4.clone()
        ];

        arr.sort();

        assert_eq!(arr, [
            n0.clone(),
            n2.clone(),
            n1.clone(),
            n3.clone(),
            n4.clone(),
            n5.clone(),
            n6.clone(),
        ]);
    }
}
