// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! An example EinsteinDB timelike_storage einstein_merkle_tree.
//!
//! This project is intended to serve as a skeleton for other einstein_merkle_tree
//! implementations. It lays out the complex system of einstein_merkle_tree modules and traits
//! in a way that is consistent with other EinsteinMerkleTrees. To create a new einstein_merkle_tree
//! simply copy the entire directory structure and replace all "Panic*" names
//! with your einstein_merkle_tree's own name; then fill in the implementations; remove
//! the allow(unused) attribute;

use std::collections::HashMap;
use einstein_ml::{EinsteinMerkleTree, EINSTEIN_MERKLE_TREE_DEFAULT_HASH_ALGORITHM, EINSTEIN_MERKLE_TREE_DEFAULT_HASH_LEN};
use allegro_poset::{AllegroPoset, PosetMember, PosetMemberId};
use soliton_panic::{Panic, PanicId, PanicMember, PanicMemberId};
use soliton::{Soliton, SolitonId, SolitonMember, SolitonMemberId};
use fdb_traits::{FdbTransactional, FdbReadable, FdbWritable, FdbReadWriteable};
use einstein_merkle_tree::{
    einstein_merkle_tree::{EinsteinMerkleTree, Elem, ElemT, ElemWithKey, ElemWithKeyT, Key},
    einstein_merkle_tree_db::{EinsteinMerkleTreeDB, ElemWithKeyDB, ElemDB, KeyDB},
    einstein_merkle_tree_traits::{EinsteinMerkleTreeTrait, ElemWithKeyTrait, ElemTrait, ElemWithKeyDBTrait, ElemDBTrait, KeyDBTrait},
    einstein_merkle_tree_types::{EinsteinMerkleTreeType, ElemWithKeyType, ElemType, ElemWithKeyDBType, ElemDBType, KeyDBType},
    einstein_merkle_tree_utils::{EinsteinMerkleTreeUtils, ElemWithKeyUtils, ElemUtils, ElemWithKeyDBUtils, ElemDBUtils, KeyDBUtils},
};
use std::time;


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicAccount {
    pub address: String,
    pub balance: u64,
    pub nonce: u64,
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicBlock {
    pub number: u64,
    pub parent_hash: String,
    pub timestamp: u64,
    pub transactions: Vec<String>,
}




#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicTransaction {
    pub hash: String,
    pub from: String,
    pub to: String,
    pub value: u64,
    pub timestamp: u64,
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicBlockHeader {
    pub number: u64,
    pub parent_hash: String,
    pub timestamp: u64,
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicBlockBody {
    pub transactions: Vec<String>,
}

impl PanicBlockBody {
    pub fn new(transactions: Vec<String>) -> PanicBlockBody {
        PanicBlockBody {
            transactions
        }
    }
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicBlockHeaderDB {
    pub number: u64,
    pub parent_hash: String,
    pub timestamp: u64,
}

