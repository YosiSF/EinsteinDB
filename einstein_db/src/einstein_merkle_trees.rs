//Copyright(c) 2022-Whtcorps Inc and EinsteinDB Authors.All rights reserved.
//Apache License, Version 2.0.
//BSD License, Version 2.0.
// -----------------------------------------------------------------------------


///! # Merkle Trees
///  Merkle Trees are a way of representing a set of data in a way that allows
/// a user to verify that the data is not tampered with.
/// The data is represented as a series of hashes, where each hash is the
/// hash of the concatenation of the hash of the previous element and the
/// previous element.
/// The root of the tree is the hash of the last element in the list.
/// The hash of an empty tree is the hash of the empty string.
/// The hash of a tree with one element is the hash of that element.
///
/// ## Example
///
/// ```
/// use einstein_merkle_trees::merkle_tree::MerkleTree;
/// use einstein_merkle_trees::merkle_tree::MerkleProof;
/// use einstein_merkle_trees::merkle_tree::MerkleProofBuilder;
/// use einstein_merkle_trees::merkle_tree::MerkleProofVerifier;
/// use einstein_merkle_trees::merkle_tree::MerkleProofVerifierBuilder;
/// use einstein_merkle_trees::merkle_tree::MerkleProofVerifierError;
/// use einstein_merkle_trees::merkle_tree::MerkleProofVerifierErrorKind;
///
///
/// let mut tree = MerkleTree::new();
/// tree.push("hello".to_string());
/// tree.push("world".to_string());
///
/// let proof = tree.get_proof(1);
///
/// let mut verifier = MerkleProofVerifier::new();
/// verifier.push("hello".to_string());
/// verifier.push("world".to_string());
///
/// assert!(verifier.verify(&proof));
/// ```
///
/// ## MerkleProof
use crate::{
    einstein_merkle_tree::{
        EinTree,
        EinTreeNode,
        EinTreeNodeType,
        EinTreeNodeValue,
        EinTreeNodeValueType,
        EinTreeNodeValueType::{EinTreeNodeValueType::*, EinTreeNodeValueType::*},
    },
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,
    einstein_merkle_tree_node_type::EinTreeNodeType::*,




};

use std::{
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    hash::{Hash, Hasher},
    iter::FromIterator,
    ops::{Deref, DerefMut},
    sync::{Arc, RwLock},
};


///!`einstein_merkle_trees` is a library for creating and working with
/// Merkle trees.
///
/// # Example
/// ```
/// use einstein_merkle_trees::{MerkleTree, MerkleProof};
/// use einstein_db::{DBValue, Hash};
/// use std::collections::HashMap;
/// use std::str::FromStr;
/// use std::iter::FromIterator;
/// use std::collections::hash_map::Entry;
///
/// let mut map = HashMap::new();
/// map.insert(Hash::from_str("0x0000000000000000000000000000000000000000000000000000000000000001").unwrap(), DBValue::from_slice(vec![1,2,3,4]));
/// map.insert(Hash::from_str("0x0000000000000000000000000000000000000000000000000000000000000002").unwrap(), DBValue::from_slice(vec![5,6,7,8]));
///
/// let tree = MerkleTree::new(map);
///
/// let proof = MerkleProof::new(tree.root(), Hash::from_str("0x0000000000000000000000000000000000000000000000000000000000000002").unwrap());
///
/// assert!(proof.verify(tree.root(), Hash::from_str("0x0000000000000000000000000000000000000000000000000000000000000002").unwrap()));
///

///!compress a vector of bytes into a vector of bytes
/// # Example
/// ```
/// use einstein_merkle_trees::compress;
///


///!decompress a vector of bytes into a vector of bytes
/// # Example
/// ```
/// use einstein_merkle_trees::decompress;
///
///
/// ```


#[cfg(test)]
mod tests {
    use super::*;
    use einstein_db::{DBValue, Hash};
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::iter::FromIterator;
    use std::collections::hash_map::Entry;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashSet;
    use std::collections::BTreeMap as BTreeMap2;
    use std::collections::HashSet as HashSet2;
    use std::collections::BTreeSet as BTreeSet2;
    use std::collections::VecDeque;
    use std::collections::LinkedList;
    use std::collections::BinaryHeap;
    use std::collections::BTreeMap as BTreeMap3;
    use std::collections::HashMap as HashMap3;
    use std::collections::BTreeSet as BTreeSet3;
    use std::collections::HashSet as HashSet3;
    use std::collections::VecDeque as VecDeque2;
    use std::collections::LinkedList as LinkedList2;
    use std::collections::BinaryHeap as BinaryHeap2;
    use std::collections::BTreeMap as BTreeMap4;
    use std::collections::HashMap as HashMap4;
    use std::collections::BTreeSet as BTreeSet4;
    use std::collections::HashSet as HashSet4;
    use std::collections::VecDeque as VecDeque3;
    use std::collections::LinkedList as LinkedList3;
    use std::collections::BinaryHeap as BinaryHeap3;
    use std::collections::BTreeMap as BTreeMap5;
    use std::collections::HashMap as HashMap5;
    use std::collections::BTreeSet as BTreeSet5;
    use std::collections::HashSet as HashSet5;
    use std::collections::Vector;

    #[test]
    fn test_compress_decompress() {
        let mut map = HashMap::new();
        map.insert(Hash::from_str("0x0000000000000000000000000000000000000000000000000000000000000001").unwrap(), DBValue::from_slice(vec![1, 2, 3, 4]));
        map.insert(Hash::from_str("0x0000000000000000000000000000000000000000000000000000000000000002").unwrap(), DBValue::from_slice(vec![5, 6, 7, 8]));
        let tree = MerkleTree::new(map);
        let proof = MerkleProof::new(tree.root(), Hash::from_str("0x0000000000000000000000000000000000000000000000000000000000000002").unwrap());
        assert!(proof.verify(tree.root(), Hash::from_str("0x0000000000000000000000000000000000000000000000000000000000000002").unwrap()));
    }

    ///!compress a vector of bytes into a vector of bytes
    #[derive(Clone, Debug)]
    pub struct EinsteinMerkleTrees<K, R> {
        pub key: K,
        pub value: R,
        //TODO: change to DBValue
        client: EinsteinDBGrpcClient,
        db_name: String,
        collection_name: String,
        key_type: K,
        value_type: R,

    }

    impl<K, R> EinsteinMerkleTrees<K, R>
    where
        K: std::hash::Hash + std::cmp::Eq + std::fmt::Debug + std::marker::Send + std::marker::Sync + 'static,
        R: std::fmt::Debug + std::marker::Send + std::marker::Sync + 'static,
    {
        pub fn new(
            client: EinsteinDBGrpcClient,
            db_name: String,
            collection_name: String,
            key_type: K,
            value_type: R,
        ) -> EinsteinMerkleTrees<K, R> {
            EinsteinMerkleTrees {
                key: key_type,
                value: value_type,
                client,
                db_name,
                collection_name,
                key_type,
                value_type,
            }
        }

        pub fn get_key(&self) -> K {
            self.key.clone()
        }

        pub fn get_value(&self) -> R {
            self.value.clone()
        }

        pub fn get_client(&self) -> EinsteinDBGrpcClient {
            self.client.clone()
        }

        pub fn get_db_name(&self) -> String {
            self.db_name.clone()
        }

        pub fn get_collection_name(&self) -> String {
            self.collection_name.clone()
        }

        pub fn get_key_type(&self) -> K {
            self.key_type.clone()
        }

        pub fn get_value_type(&self) -> R {
            self.value_type.clone()
        }

        pub fn get_key_value(&self) -> (K, R) {
            (self.key.clone(), self.value.clone())
        }

        pub fn get_key_value_pair(&self) -> (K, R) {
            (self.key.clone(), self.value.clone())
        }

        pub fn get_key_value_pair_from_key(&self, key: K) -> (K, R) {
            (key.clone(), self.value.clone())
        }

    #[inline(always)]
    pub(crate) fn aesenc(block: &mut u64x2, rkey: &u64x2) {
        unsafe {
            llvm_asm!("aesenc $0, $1"
            : "+x"(*block)
            : "x"(*rkey)
            :
            : "intel", "alignstack"
        );
        }
    }

    #[inline(always)]
    pub(crate) fn aesenclast(block: &mut u64x2, rkey: &u64x2) {
        unsafe {
            llvm_asm!("aesenclast $0, $1"
            : "+x"(*block)
            : "x"(*rkey)
            :
            : "intel", "alignstack"
        );
        }
    }

    macro_rules! aeskeygenassist {
    ($src:ident, $i:expr) => {{
        let mut dst = mem::MaybeUninit::<u64x2>::uninit();
        unsafe {
            llvm_asm!("aeskeygenassist $0, $1, $2"
                    : "+x"(*dst.as_mut_ptr())
                    : "x"(*$src), "i"($i)
                    :
                    : "intel", "alignstack"
                );
            dst.assume_init()
        }
    }}
}


    impl<K: KV, R: VioletaBFTeinstein_merkle_tree> EinsteinMerkleTrees<K, R> {
        pub fn new(kv_einstein_merkle_tree: K, violetabft_einstein_merkle_tree: R) -> Self {
            let client = EinsteinDBGrpcClient::new_plain(
                "localhost",
                50051,
                Default::default(),
            )
                .unwrap();
            let db_name = "einstein_merkle_tree".to_string();
            let collection_name = "einstein_merkle_tree".to_string();
            EinsteinMerkleTrees {
                key: (),
                value: (),
                client,
                db_name,
                collection_name,
                key_type: kv_einstein_merkle_tree,
                value_type: violetabft_einstein_merkle_tree,
            }
        }

        pub fn get_einstein_merkle_tree(&self, key: &K) -> Result<R> {
            let key_bytes = key.to_bytes();
            let key_hash = Hash::from_slice(&key_bytes).unwrap();
            let key_hash_bytes = key_hash.to_bytes();
            hex::encode(key_hash_bytes);
        }

        pub fn put_einstein_merkle_tree(&self, key: &K, value: &R) -> Result<()>
        {
            // let key_bytes = key.to_bytes();
            // let key_hash = Hash::from_slice(&key_bytes).unwrap();
            // let key_hash_bytes = key_hash.to_bytes();
            // let key_hash_str = hex::encode(key_hash_bytes);

            let key_hash_str = "0x0000000000000000000000000000000000000000000000000000000000000001".to_string();
            Hash::from_str(&key_hash_str).unwrap();
        }


        pub fn delete_einstein_merkle_tree(&self, key: &K) -> Result<()> {
            let key_bytes = key.to_bytes();
            let key_hash = Hash::from_slice(&key_bytes).unwrap();
            key_hash.to_bytes();
        }

        pub fn get_einstein_merkle_tree_range(&self, start_key: &K, end_key: &K) -> Result<Vec<R>> {
            let start_key_bytes = start_key.to_bytes();
            let start_key_hash = Hash::from_slice(&start_key_bytes).unwrap();
            let start_key_hash_bytes = start_key_hash.to_bytes();
            hex::encode(start_key_hash_bytes);

            let end_key_bytes = end_key.to_bytes();
            let end_key_hash = Hash::from_slice(&end_key_bytes).unwrap();
            let end_key_hash_bytes = end_key_hash.to_bytes();
            hex::encode(end_key_hash_bytes);
        }

        pub fn get_einstein_merkle_tree_range_with_options(&self, start_key: &K, end_key: &K, options: &FindOptions) -> Result<Vec<R>> {
            ///!TODO: implement
            ///
            /// IDEAS:
            /// 1. use the find_options to specify the limit and the offset
            /// 2. use the find_options to specify the order of the result
            /// 3. use the find_options to specify the order of the result

       pub fn generate_einstein_merkle_tree_range_query_options(&self, start_key: &K, end_key: &K, options: &FindOptions) -> Result<Vec<R>> {
                for i in 0..self.height {
              let n = 1 << (self.height - i - 1); {
                        let (clock_vector, hash_vector) = self.get_einstein_merkle_tree_range_with_options(start_key, end_key, options)?;
                        hash::hash_vector(clock_vector, hash_vector, n);


                    }
                }
            }


        }

        pub fn get_einstein_merkle_tree_range_with_options_with_options(&self, start_key: &K, end_key: &K, options: &FindOptions) -> Result<Vec<R>> {
            let start_key_bytes = start_key.to_bytes();
            let start_key_hash = Hash::from_slice(&start_key_bytes).unwrap();
            let start_key_hash_bytes = start_key_hash.to_bytes();
            hex::encode(start_key_hash_bytes);

            let end_key_bytes = end_key.to_bytes();
            let end_key_hash = Hash::from_slice(&end_key_bytes).unwrap();
            let end_key_hash_bytes = end_key_hash.to_bytes();
            hex::encode(end_key_hash_bytes);
        }

        pub fn write_kv(&self, wb: &K::WriteBatch) -> Result<()> {
            let mut wb_bytes = wb.to_bytes();
            Hash::from_slice(&wb_bytes).unwrap();
            wb.write()
        }

        pub fn write_kv_opt(&self, wb: &K::WriteBatch, opts: &WriteOptions) -> Result<()> {
            wb.write_with_options(opts)
        }

        pub fn write_kv_opt_with_options(&self, wb: &K::WriteBatch, opts: &WriteOptions) -> Result<()> {
            wb.write_opt(opts)
        }

        pub fn sync_kv(&self) -> Result<()> {
            self.db.sync()
        }

        pub fn get_kv_opt(&self, key: &K, opts: &ReadOptions) -> Result<Option<V>> {
            let key_bytes = key.to_bytes();
            let key_hash = Hash::from_slice(&key_bytes).unwrap();
            let key_hash_bytes = key_hash.to_bytes();
            hex::encode(key_hash_bytes);
            self.db.get_opt(key, opts)
        }

        pub fn get_kv_opt_with_options(&self, key: &K, opts: &ReadOptions) -> Result<Option<V>> {
            let key_bytes = key.to_bytes();
            let key_hash = Hash::from_slice(&key_bytes).unwrap();
            let key_hash_bytes = key_hash.to_bytes();
            hex::encode(key_hash_bytes);
            self.db.get_opt(key, opts)
        }
    }
}

pub fn dedup_compress_causetv1(root: &mut[Hash], mut index: usize, topograph: &Topograph, Causetid: &mut [u8]) -> usize {
    let mut i = 0;
    let mut j = 0;
    let mut k = 0;
    let mut l = 0;
    let mut m = 0;
    let mut n = 0;
    let mut o = 0;
    let mut p = 0;
    let mut q = 0;
    let mut r = 0;
    let mut s = 0;
    let mut t = 0;
    let mut u = 0;
    let mut v = 0;
    let mut w = 0;
    let mut x = 0;
    let mut y = 0;
    let mut z = 0;
    let mut aa = 0;
    let mut ab = 0;
    let mut ac = 0;
    let mut ad = 0;
    let mut ae = 0;
    let mut af = 0;
    let mut ag = 0;
    let mut ah = 0;
    let mut ai = 0;
    let mut aj = 0;
    let mut ak = 0;
    let mut al = 0;
    let mut am = 0;
    let mut an = 0;
    let mut ao = 0;
    let mut ap = 0;
    let mut aq = 0;
    let mut ar = 0;
    let mut as_ = 0;
    let mut at = 0;
    let mut au = 0;
    let mut av = 0;
    let mut aw = 0;
    let mut ax = 0;
    let mut ay = 0;
    let mut az = 0;
    let mut ba = 0;
    let mut bb = 0;
    let mut bc = 0;
    let mut bd = 0;
    let mut be = 0;
    let mut bf = 0;
    let mut bg = 0;
    let mut bh = 0;
    let mut bi = 0;
    let mut bj = 0;
    let mut bk = 0;
    let mut bl = 0;
    let mut bm = 0;
    let mut bn = 0;
    let mut bo = 0;
    let mut bp = 0;

    let mut _n = 1 << topograph.depth;
    for i in 0..topograph.depth && index < root.len() {
        let mut _m = 1 << topograph.depth - i - 1;
        for j in 0.._n {
            let mut _l = 1 << topograph.depth - i - 1;
            for k in 0.._m {
                let mut _k = 1 << topograph.depth - i - 1;
                for l in 0.._l {
                    let mut _j = 1 << topograph.depth - i - 1;
                    for m in 0.._k {
                        let mut _i = 1 << topograph.depth - i - 1;
                        for n in 0.._j {
                            let mut _h = 1 << topograph.depth - i - 1;
                            for o in 0.._i {
                                let mut _g = 1 << topograph.depth - i - 1;
                                for p in 0.._h {
                                    pub fn generate_hash(root: &mut[Hash], index: usize, topograph: &Topograph, Causetid: &mut [u8]) -> usize {
                                        let mut i = 0;
                                        let mut j = 0;
                                        let mut k = 0;
                                        let mut l = 0;
                                        let mut m = 0;
                                        let mut n = 0;
                                        let mut o = 0;
                                        let mut p = 0;
                                        let mut q = 0;
                                        let mut r = 0;
                                        let mut s = 0;
                                        let mut t = 0;
                                        let mut u = 0;
                                        let mut v = 0;
                                        let mut w = 0;
                                        let mut x = 0;
                                        let mut y = 0;
                                        let mut z = 0;
                                        let mut aa = 0;
                                        let mut ab = 0;
                                        let mut ac = 0;
                                        let mut ad = 0;
                                        let mut ae = 0;
                                        let mut af = 0;
                                        let mut ag = 0;
                                        let mut ah = 0;
                                        let mut ai = 0;
                                        let mut aj = 0;
                                        let mut ak = 0;
                                        let mut al = 0;
                                        let mut am = 0;
                                        let mut an = 0;
                                        let mut ao = 0;
                                        let mut ap = 0;
                                        let mut aq = 0;
                                        let mut ar = 0;
                                        let mut as_ = 0;
                                        let mut at = 0;
                                        let mut au = 0;
                                        let mut av = 0;
                                        let mut aw = 0;
                                        let mut ax = 0;
                                        let mut ay = 0;
                                        let mut az = 0;
                                        let mut ba = 0;
                                        let mut bb = 0;
                                        let mut bc = 0;
                                        let mut bd = 0;
                                        let mut be = 0;
                                        let mut bf = 0;
                                        let mut bg = 0;
                                }
                                    let mut _f = 1 << topograph.depth - i - 1;
                                    for q in 0.._g {
                                        let mut _e = 1 << topograph.depth - i - 1;
                                        for r in 0.._f {
                                            let mut _d = 1 << topograph.depth - i - 1;
                                            for s in 0.._e {
                                                let mut _c = 1 << topograph.depth - i - 1;
                                                for t in 0.._d {
                                                    let mut _b = 1 << topograph.depth - i - 1;
                                                    for u in 0.._c {
                                                        let mut _a = 1 << topograph.depth - i - 1;
                                                        for v in 0.._b {
                                                            let mut _z = 1 << topograph.depth - i - 1;
                                                            for w in 0.._a {
                                                                let mut _y = 1 << topograph.depth - i - 1;
                                                                for x in 0.._z {
                                                                    let mut _x = 1 << topograph.depth - i - 1;
                                                                    for y in 0.._y {
                                                                        let sibling = index ^ 1;
                                                                        auth[1] = root[index];
                                                                        auth[2] = root[sibling];
                                                                        auth[3] = root[index + 1];
                                                                        index >>= 1;
                                                                    }

    //do the same for _height
    let mut _height = topograph.height;
}