// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
// -----------------------------------------------------------------------------
//! # EinsteinDB
//! #################################
//!
//!  EinsteinDB engages in a semi-lock-free concurrency control system for the EinsteinDB.
//!  While it boasts optimistic concurrency control, it is not a lock-free concurrency control system.
//! It is a semi-lock-free concurrency control system for the EinsteinDB.
//!
//! We use a combination of HoneyBadger Epaxos and MVRSI to implement MVRSI.
//! The MVRSI is a concurrency control system for the EinsteinDB.
//! Alexandrov Topologies are a concurrency control system for Causets of the EinsteinDB.
//! Their connected components provide CAP (Causal Partitioning) and CSP (Causet Partitioning) capabilities.
use fdb_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use std::collections::HashSet;
use std::collections::hash_set::Iter as HashSetIter;
use std::collections::hash_set::IterMut as HashSetIterMut;


use std::collections::BTreeSet;
use std::collections::btree_set::Iter as BTreeSetIter;
use std::collections::btree_set::IterMut as BTreeSetIterMut;


use std::collections::HashMap;
use std::collections::hash_map::Entry;


///! A structure that represents a node in the graph.
/// A node is a vertex in the graph.
/// Unlike a vertex, a node does not have a label.
/// We deal mostly with DAGs and within this DAG, we deal with vertices
/// and edges which are represented by the Node struct.
/// We elevate the notion of a Node and discuss it as a simplicial complex.
/// A simplicial complex is a set of simplices.
/// A simplex is a set of vertices.
/// A Causet could be thought of us as a simplicial norm of a DAG.
///
/// A Causet is a set of vertices.
/// A Causet is a set of edges.
/// A Causet is a set of simplices.



trait Node {
    /// Returns the id of the node.
    /// The id is a unique identifier for the node.
    /// The id is a non-negative integer.
    /// The id is unique for the node.

    fn id(&self) -> u64;

    /// Returns the label of the node.
    /// The label is a string.
    /// The label is a non-negative integer.
    ///
    /// The label is a string.

    fn label(&self) -> String;

    fn set_label(&mut self, label: String);

    async fn get_label(&self) -> String;

    async fn set_label_async(&self, label: String);

    //thread control via interlocking directorate
    fn set_thread_control(&mut self, thread_control: Arc<AtomicBool>);
}



use std::collections::hash_map::Iter;

pub use crate::fdb_traits::FdbTrait;
pub use crate::fdb_traits::FdbTraitImpl;
use crate::fdb_traits::FdbTrait;


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrimitiveTtl {
    pub name: String,
    pub value: Value,
    pub ttl: i64,
}


impl PrimitiveTtl {
    pub fn new(name: String, value: Value, ttl: i64) -> Self {
        PrimitiveTtl {
            name,
            value,
            ttl,
        }
    }
}


impl Display for PrimitiveTtl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrimitiveTtlSet {
    pub name: String,
    pub value: Value,
    pub ttl: i64,
}


impl PrimitiveTtlSet {
    pub fn new(name: String, value: Value, ttl: i64) -> Self {
        PrimitiveTtlSet {
            name,
            value,
            ttl,
        }
    }
}


impl Display for PrimitiveTtlSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}






#[derive(Clone)]
pub struct AlexandrovTopology {
    pub config: Config,
    pub db: Arc<FdbTransactional>,
    pub poset: Arc<Poset>,
    pub db_name: String,
    pub db_path: String,
    pub db_config: String,
    pub db_config_path: String,
    pub db_config_name: String,
    pub db_config_file: String,
    pub db_config_file_path: String,
    pub db_config_file_name: String,
    pub db_config_file_content: String,
}


///! AlexandrovTopology is a struct that contains the following:
/// - config: the configuration of the soliton_panic
/// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
/// -----------------------------------------------------------------------------
/// # EinsteinDB
/// #################################



#[derive(Clone)]
#[derive(Debug)]
pub struct AlexandrovTopologyConfig {
    pub config: Config,
    pub db: Arc<FdbTransactional>,
    pub poset: Arc<Poset>,
    pub db_name: String,
    pub db_path: String,
    pub db_config: String,
    pub db_config_path: String,
    pub db_config_name: String,
    pub db_config_file: String,
    pub db_config_file_path: String,
    pub db_config_file_name: String,
    pub db_config_file_content: String,
}
//now we abstract the lshp-tree into a poset so that it is lisp-like
//source: https://franz.com/support/documentation/ansicl.94/section/dictio19.htm


//the poset is a tree with a root and a set of leaves
//the leaves are the nodes of the poset
//the root is the root of the tree
//the root is the root of the poset
//the leaves are the leaves of the poset
//the leaves are the leaves of the tree
//the leaves are the nodes of the tree


//we shall use guillaume's interpretation of a stateless hash tree as featured on the NIST2019 paper
//SPHINCS-Gravity and its rust implementations
//





pub struct Poset {

    fdb: Arc<dyn Mutable>,

    //the root of the tree
    root: Arc<PosetNode>,
    //the leaves of the tree
    leaves: HashMap<u64, Arc<PosetNode>>,
    //sqlite secondary Index
    soft_index: HashMap<u64, Arc<PosetNode>>,

}

#[derive(Clone, Debug)]
pub struct AllegroPosetAlexandrovProcess {
    //the root of the tree
    root: Arc<PosetNode>,
    //the leaves of the tree
    leaves: HashMap<u64, Arc<PosetNode>>,
    //the leaves of the poset
    poset_leaves: HashMap<u64, Arc<PosetNode>>,
    //the leaves of the tree
    tree_leaves: HashMap<u64, Arc<PosetNode>>,
    //the leaves of the tree
    tree_leaves_reverse: HashMap<u64, Arc<PosetNode>>,
}



pub fn alexandrov_process(
    engine: Arc<dyn Engine>,
    snapshot: Arc<dyn Snapshot>,
    options: Arc<CompactOptions>,
    mutable: Arc<dyn Mutable>,
    mutable_options: Arc<WriteOptions>,
    ) -> Result<()> {
    let mut engine_iterator = engine.iterator(
        EngineIteratorOptions::new()
            .set_prefix(b"alexandrov_process")
            .set_reverse(true),
    );
    let mut snapshot_iterator = snapshot.iterator(
        SnapshotIteratorOptionsBuilder::new()
            .set_prefix(b"alexandrov_process")
            .set_reverse(true),
    );




    pub struct AlexandrovProcess<K, V> {
        pub(crate) db: Arc<dyn Mutable>,
        pub(crate) prefix: Vec<u8>,
        pub(crate) key_type: K,
        pub(crate) value_type: V,
        pub(crate) write_options: WriteOptions,

    }

    impl<K, V> AlexandrovProcess<K, V> {
        pub fn new(db: Arc<dyn Mutable>, prefix: Vec<u8>, key_type: K, value_type: V, write_options: WriteOptions) -> Self {
            AlexandrovProcess {
                db,
                prefix,
                key_type,
                value_type,
                write_options,
            }
        }
    }

    impl<K, V> Drop for AlexandrovProcess<K, V> {
        fn drop(&mut self) {
            //self.db.drop_prefix(self.prefix.clone());
        }
    }

    impl<K, V> AlexandrovProcess<K, V> {
        pub fn get_db(&self) -> Arc<dyn Mutable> {
            self.db.clone()
        }
        pub fn get_prefix(&self) -> Vec<u8> {
            self.prefix.clone()
        }
        pub fn get_key_type(&self) -> K {
            self.key_type.clone()
        }
        pub fn get_value_type(&self) -> V {
            self.value_type.clone()
        }
        pub fn get_write_options(&self) -> WriteOptions {
            self.write_options.clone()
        }
    }


//now we abstract the lshp-tree into a poset so that it is lisp-like
//source: https://franz.com/support/documentation/ansicl.94/section/dictio19.htm

    #[derive(Clone, Debug)]
    pub struct PosetNode {
        //the id of the node
        id: u64,
        //the hash of the node
        hash: Arc<BlockHash>,
        //the body of the node
        body: Arc<BlockBody>,
        //the parent of the node
        parent: Option<Arc<PosetNode>>,
        //the children of the node
        children: HashMap<u64, Arc<PosetNode>>,
        //the children of the node
        children_reverse: HashMap<u64, Arc<PosetNode>>,
        //the children of the node
        children_reverse_reverse: HashMap<u64, Arc<PosetNode>>,
        //the children of the node
        children_reverse_reverse_reverse: HashMap<u64, Arc<PosetNode>>,
        //the children of the node
    }

    impl PosetNode {
        pub fn new(id: u64, hash: Arc<BlockHash>, body: Arc<BlockBody>) -> Self {
            Self {
                id,
                hash,
                body,
                parent: None,
                children: HashMap::new(),
                children_reverse: HashMap::new(),
                children_reverse_reverse: HashMap::new(),
                children_reverse_reverse_reverse: HashMap::new(),
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct BlockHash {
        //the id of the node
        id: u64,
        //the hash of the node
        hash: Arc<Vec<u8>>,
        //the body of the node
        body: Arc<BlockBody>,
        //the parent of the node
        parent: Option<Arc<PosetNode>>,
        //the children of the node
        children: HashMap<u64, Arc<PosetNode>>,
        //the children of the node
        children_reverse: HashMap<u64, Arc<PosetNode>>,
        //the children of the node
        children_reverse_reverse: HashMap<u64, Arc<PosetNode>>,
        //the children of the node
        children_reverse_reverse_reverse: HashMap<u64, Arc<PosetNode>>,
        //the children of the node
    }

    impl BlockHash {
        pub fn new(id: u64, hash: Arc<Vec<u8>>, body: Arc<BlockBody>) -> Self {
            Self {
                id,
                hash,
                body,
                parent: None,
                children: HashMap::new(),
                children_reverse: HashMap::new(),
                children_reverse_reverse: HashMap::new(),
                children_reverse_reverse_reverse: HashMap::new(),
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct BlockBody {
        //the id of the node
        id: u64,
        //the hash of the node
        hash: Arc<Vec<u8>>,
        //the body of the node
        body: Arc<Vec<u8>>,
        //the parent of the node
        parent: Option<Arc<PosetNode>>,
        //the children of the node
        children: HashMap<u64, Arc<PosetNode>>,
        //the children of the node
        children_reverse: HashMap<u64, Arc<PosetNode>>,
        //the children of the node
        children_reverse_reverse: HashMap<u64, Arc<PosetNode>>,
        //the children of the node
        children_reverse_reverse_reverse: HashMap<u64, Arc<PosetNode>>,
        //the children of the node
    }

    pub fn new_alexandrov_process<K, V>(db: Arc<dyn Mutable>, prefix: Vec<u8>, key_type: K, value_type: V) -> Result<AlexandrovProcess<K, V>> {
        let write_options = WriteOptions::new();
        Ok(AlexandrovProcess {
            db,
            prefix,
            key_type,
            value_type,
            write_options,
        })
    }

    impl<K, V> AlexandrovProcess<K, V> {
        pub fn new_alexandrov_process<K, V>(db: Arc<dyn Mutable>, prefix: Vec<u8>, key_type: K, value_type: V) -> Result<AlexandrovProcess<K, V>> {
            let write_options = WriteOptions::new();
            Ok(AlexandrovProcess {
                db,
                prefix,
                key_type,
                value_type,
                write_options,
            })
        }
        pub fn new_alexandrov_process_with_write_options<K, V>(db: Arc<dyn Mutable>, prefix: Vec<u8>, key_type: K, value_type: V, write_options: WriteOptions) -> Result<AlexandrovProcess<K, V>> {
            Ok(AlexandrovProcess {
                db,
                prefix,
                key_type,
                value_type,
                write_options,
            })
        }
        pub fn new_alexandrov_process_with_write_options_and_prefix<K, V>(db: Arc<dyn Mutable>, prefix: Vec<u8>, key_type: K, value_type: V, write_options: WriteOptions) -> Result<AlexandrovProcess<K, V>> {
            Ok(AlexandrovProcess {
                db,
                prefix,
                key_type,
                value_type,
                write_options,
            })
        }
        pub fn new_alexandrov_process_with_write_options_and_prefix_and_key_type<K, V>(db: Arc<dyn Mutable>, prefix: Vec<u8>, key_type: K, value_type: V, write_options: WriteOptions) -> Result<AlexandrovProcess<K, V>> {
            Ok(AlexandrovProcess {
                db,
                prefix,
                key_type,
                value_type,
                write_options,
            })
        }
    }

    impl<K, V> AlexandrovProcess<K, V> {
        pub fn get_db(&self) -> Arc<dyn Mutable> {
            self.db.clone()
        }
        pub fn get_prefix(&self) -> Vec<u8> {
            self.prefix.clone()
        }
        pub fn get_key_type(&self) -> K {
            self.key_type.clone()
        }
        pub fn get_value_type(&self) -> V {
            self.value_type.clone()
        }
        pub fn get_write_options(&self) -> WriteOptions {
            self.write_options.clone()
        }
        pub fn get_write_options_with_prefix(&self) -> WriteOptions {
            self.write_options.clone()
        }
        pub fn get_write_options_with_prefix_and_key_type(&self) -> WriteOptions {
            self.write_options.clone()
        }
        pub fn get_write_options_with_prefix_and_key_type_and_value_type(&self) -> WriteOptions {
            self.write_options.clone()
        }
        pub fn get_write_options_with_prefix_and_key_type_and_value_type_and_write_options(&self) -> WriteOptions {
            self.write_options.clone()
        }
        pub fn get_write_options_with_prefix_and_key_type_and_value_type_and_write_options_and_db(&self) -> WriteOptions {
            self.write_options.clone()
        }
        pub fn get_write_options_with_prefix_and_key_type_and_value_type_and_write_options_and_db_and_prefix(&self) -> WriteOptions {
            self.write_options.clone()
        }
        pub fn get_write_options_with_prefix_and_key_type_and_value_type_and_write_options_and_db_and_prefix_and_key_type(&self) -> WriteOptions {
            self.write_options_and_prefix_and_key_type.clone()
        }
    }

    impl Poset {
        pub fn get_node(&self, id: u64) -> Result<Arc<PosetNode>> {
            let node = self.leaves.get(&id);
            if node.is_none() {
                return Err(Error::new(ErrorKind::NotFound, "Node not found"));
            }
            Ok(node.unwrap().clone())
        }
    }

    pub fn create_poset(db: Arc<dyn Mutable>, prefix: Vec<u8>, write_options: WriteOptions) -> Result<Poset> {
        let poset = Poset::new(db);
        let poset = Poset {
            //the database that is used to store the poset
            fdb: db,
            //the root of the poset
            root: Arc::new(PosetNode::new(db, prefix, write_options)?),
            leaves: HashMap::new(),
            soft_index: HashMap::new(),
        };
        Ok(poset)
    }



    ///The main function of the poset. It is used to create a new poset.
    /// # Arguments
    /// * `db` - The database that is used to store the poset.
    /// * `prefix` - The prefix that is used to store the poset.
    /// * `write_options` - The write options that are used to store the poset.
    /// # Returns
    /// * `Result` - A `Result` containing the `Poset` or an `Error` if an error occured.
    /// # Example
    async fn create_alexandrov_poset_processv_process_async<K, V>(db: Arc<dyn Mutable>, prefix: Vec<u8>, key_type: K, value_type: V, write_options: WriteOptions) -> Result<AlexandrovProcess<K, V>> {
        let alexandrov_poset_processv_process = AlexandrovProcess {
            db,
            prefix,
            key_type,
            value_type,
            write_options,
        };
        Ok(alexandrov_poset_processv_process)
    }

    /*
impl WriteBatchExt for soliton_panic_merkle_tree {
    type WriteBatch = PanicWriteBatch;
    type WriteBatchVec = PanicWriteBatch;

    const WRITE_BATCH_MAX_CAUSET_KEYS: usize = 1;

    fn support_write_alexandrov_poset_process_vec(&self) -> bool {
        panic!()
    }

    fn write_alexandrov_poset_process(&self) -> Self::WriteBatch {
        panic!()
    }
    fn write_alexandrov_poset_process_with_cap(&self, cap: usize) -> Self::WriteBatch {
        panic!()
    }
}

pub struct PanicWriteBatch;

impl WriteBatch<soliton_panic_merkle_tree> for PanicWriteBatch {
    fn with_capacity(_: &soliton_panic_merkle_tree, _: usize) -> Self {
        panic!()
    }

    fn write_opt(&self, _: &WriteOptions) -> Result<()> {
        panic!()
    }

    fn data_size(&self) -> usize {
        panic!()
    }
    fn count(&self) -> usize {
        panic!()
    }
    fn is_empty(&self) -> bool {
        panic!()
    }
    fn should_write_to_einstein_merkle_tree(&self) -> bool {
        panic!()
    }

    fn clear(&mut self) {
        panic!()
    }
    fn set_save_point(&mut self) {
        panic!()
    }
    fn pop_save_point(&mut self) -> Result<()> {
        panic!()
    }
    fn rollback_to_save_point(&mut self) -> Result<()> {
        panic!()
    }
    fn merge(&mut self, src: Self) {
        panic!()
    }
}

impl Mutable for PanicWriteBatch {
    fn put(&mut self, soliton_id: &[u8], causet_locale: &[u8]) -> Result<()> {
        panic!()
    }
    fn put_namespaced(&mut self, namespaced: &str, soliton_id: &[u8], causet_locale: &[u8]) -> Result<()> {
        panic!()
    }

    fn delete(&mut self, soliton_id: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_namespaced(&mut self, namespaced: &str, soliton_id: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_range(&mut self, begin_soliton_id: &[u8], end_soliton_id: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_range_namespaced(&mut self, namespaced: &str, begin_soliton_id: &[u8], end_soliton_id: &[u8]) -> Result<()> {
        panic!()
    }
}
*/

//Wrap the Tuplefied Index byte slice from the tree with a semantic encoder hexadecimal soliton
//id and causet_locale

    pub fn wrap_index_bytes(index_bytes: &[u8], soliton_id: &[u8], causet_locale: &[u8]) -> Vec<u8> {
        let mut index_bytes_vec = index_bytes.to_vec();
        index_bytes_vec.extend_from_slice(soliton_id);
        index_bytes_vec.extend_from_slice(causet_locale);
        index_bytes_vec
    }

    pub fn unwrap_index_bytes(index_bytes: &[u8]) -> (&[u8], &[u8]) {
        let mut index_bytes_vec = index_bytes.to_vec();
        let soliton_id = index_bytes_vec.split_off(index_bytes.len());
        let causet_locale = index_bytes_vec;
        (soliton_id, causet_locale)
    }

    pub fn wrap_index_bytes_with_namespaced(index_bytes: &[u8], namespaced: &str, soliton_id: &[u8], causet_locale: &[u8]) -> Vec<u8> {
        let mut index_bytes_vec = index_bytes.to_vec();
        index_bytes_vec.extend_from_slice(soliton_id);
        index_bytes_vec.extend_from_slice(causet_locale);
        index_bytes_vec.extend_from_slice(namespaced.as_bytes());
        index_bytes_vec
    }

    pub fn unwrap_index_bytes_with_namespaced(index_bytes: &[u8]) -> (&[u8], &[u8], &str) {
        let mut index_bytes_vec = index_bytes.to_vec();
        let soliton_id = index_bytes_vec.split_off(index_bytes.len());
        let causet_locale = index_bytes_vec.split_off(index_bytes.len());
        let namespaced = std::str::from_utf8(&index_bytes_vec).unwrap();
        (soliton_id, causet_locale, namespaced)
    }

    pub fn wrap_index_bytes_with_namespaced_and_key(index_bytes: &[u8], namespaced: &str, key: &[u8]) -> Vec<u8> {
        let mut index_bytes_vec = index_bytes.to_vec();
        index_bytes_vec.extend_from_slice(key);
        index_bytes_vec.extend_from_slice(namespaced.as_bytes());
        index_bytes_vec
    }

    pub fn unwrap_index_bytes_with_namespaced_and_key(index_bytes: &[u8]) -> (&[u8], &[u8], &str) {
        let mut index_bytes_vec = index_bytes.to_vec();
        let key = index_bytes_vec.split_off(index_bytes.len());
        let namespaced = index_bytes_vec.split_off(index_bytes.len());
        let namespaced = std::str::from_utf8(&namespaced).unwrap();
        (key, namespaced)
    }

    pub fn wrap_index_bytes_with_namespaced_and_key_and_value(index_bytes: &[u8], namespaced: &str, key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut index_bytes_vec = index_bytes.to_vec();
        index_bytes_vec.extend_from_slice(key);
        index_bytes_vec.extend_from_slice(value);
        index_bytes_vec.extend_from_slice(namespaced.as_bytes());
        index_bytes_vec
    }

    pub fn unwrap_index_bytes_with_namespaced_and_key_and_value(index_bytes: &[u8]) -> (&[u8], &[u8], &str, &[u8]) {
        let mut index_bytes_vec = index_bytes.to_vec();
        let key = index_bytes_vec.split_off(index_bytes.len());
        let value = index_bytes_vec.split_off(index_bytes.len());
        let namespaced = index_bytes_vec.split_off(index_bytes.len());
        let namespaced = std::str::from_utf8(&namespaced).unwrap();
        (key, value, namespaced)
    }

    pub fn wrap_index_bytes_with_namespaced_and_key_and_value_and_timestamp(index_bytes: &[u8], namespaced: &str, key: &[u8], value: &[u8], timestamp: u64) -> Vec<u8> {
        let mut index_bytes_vec = index_bytes.to_vec();
        index_bytes_vec.extend_from_slice(key);
        index_bytes_vec.extend_from_slice(value);
        index_bytes_vec.extend_from_slice(namespaced.as_bytes());
        index_bytes_vec.extend_from_slice(&timestamp.to_be_bytes());
        index_bytes_vec
    }
}


///CHANGELOG for soliton_panic_merkle_tree
///
///1.0.0:
///    - Initial version
///    - Added support for namespaced keys
///    - Added support for namespaced keys and namespaced valuesource
///    - Added support for namespaced keys and namespaced valuesource and namespaced valuesink


///CHANGELOG for soliton_panic_merkle_tree
///
///1.0.0:
///    - Initial version
///    - Added support for namespaced keys and namespaced valuesource