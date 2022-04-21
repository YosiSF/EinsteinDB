// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions};
use std::collections::HashMap;
use std::sync::Arc;

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


pub struct AlexandrovProcess<K, V> {
    pub(crate) db: Arc<dyn Mutable>,
    pub(crate) prefix: Vec<u8>,
    pub(crate) key_type: K,
    pub(crate) value_type: V,
    pub(crate) write_options: WriteOptions,

}

//now we abstract the lshp-tree into a poset so that it is lisp-like
//source: https://franz.com/support/documentation/ansicl.94/section/dictio19.htm





pub fn create_alexandrov_process<K, V>(db: Arc<dyn Mutable>, prefix: Vec<u8>, key_type: K, value_type: V, write_options: WriteOptions) -> Result<AlexandrovProcess<K, V>> {
    let alexandrov_process = AlexandrovProcess {
        db,
        prefix,
        key_type,
        value_type,
        write_options,
    };
    Ok(alexandrov_process)
}

pub fn create_poset(db: Arc<dyn Mutable>, prefix: Vec<u8>, write_options: WriteOptions) -> Result<Poset> {
    let poset = Poset {
        fdb: db,
        root: Arc::new(PosetNode::new(db, prefix, write_options)?),
        leaves: HashMap::new(),
        soft_index: HashMap::new(),
    };
    Ok(poset)
}

async fn create_alexandrov_process_async<K, V>(db: Arc<dyn Mutable>, prefix: Vec<u8>, key_type: K, value_type: V, write_options: WriteOptions) -> Result<AlexandrovProcess<K, V>> {
    let alexandrov_process = AlexandrovProcess {
        db,
        prefix,
        key_type,
        value_type,
        write_options,
    };
    Ok(alexandrov_process)
}

/*
impl WriteBatchExt for Paniceinstein_merkle_tree {
    type WriteBatch = PanicWriteBatch;
    type WriteBatchVec = PanicWriteBatch;

    const WRITE_BATCH_MAX_CAUSET_KEYS: usize = 1;

    fn support_write_alexandro_vec(&self) -> bool {
        panic!()
    }

    fn write_alexandro(&self) -> Self::WriteBatch {
        panic!()
    }
    fn write_alexandro_with_cap(&self, cap: usize) -> Self::WriteBatch {
        panic!()
    }
}

pub struct PanicWriteBatch;

impl WriteBatch<Paniceinstein_merkle_tree> for PanicWriteBatch {
    fn with_capacity(_: &Paniceinstein_merkle_tree, _: usize) -> Self {
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











