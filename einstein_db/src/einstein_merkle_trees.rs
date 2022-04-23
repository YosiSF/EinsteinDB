//Copyright(c) 2022-Whtcorps Inc and EinsteinDB Authors.All rights reserved.
//Apache License, Version 2.0.



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