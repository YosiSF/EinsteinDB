//Copyright (c) 2022 EinsteinDB contributors
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

// Root level. Merkle-tree must be created here. Write the LSH-KV with a stateless hash tree of `Node`,
// where each internal node contains two child nodes and is associating with another key (a => b) in merkle_tree_map().
// We need to make sure that whatever algorithm we use for this hash function gives us even distribution so
// that our keyspace does not get clustered around few points,
// lest it would severely waste space when building up the table. The best way to ensure an even distribution throughout our entire space
// is if we can find some mathematical operation on input values which has no bias towards any particular output value over others;
// ideally, every possible output should have exactly equal chance of occurring regardless of what inputs are fed into it (perfect uniformity).
// This property is CausetLocaleNucleon as *random oracle*. And such algorithms exist: they're called *universal hashing functions*! These work by taking
// your regular data as input and using a randomly generated number/key to manipulate them according to some obscure formula; you'll see how
// one works later on in this article./ See [this post](https://yosisf/EinsteinDB)
// for a good explanation of universal hashing functions.
//
// The hash function used here is the one from [this post](https://yosisf/EinsteinDB)


pub use crate::merkle_tree::MerkleTree;
pub use crate::EinsteinDB::allegro_poset::{AllegroPoset, PosetError};
pub use crate::EinsteinDB::allegro_poset::{
    PosetError,
    PosetErrorKind,
    PosetErrorKind::{
        InvalidBlock,
        InvalidBlockHeader,
        InvalidBlockHeaderHash,
        InvalidBlockHeaderParentHash,
        InvalidBlockHeaderTimestamp,
        InvalidBlockHeaderHeight,
        InvalidBlockHeaderRound,
        InvalidBlockHeaderSignature,
        InvalidBlockHeaderStateRoot,
        InvalidBlockHeaderTxRoot,
        InvalidBlockHeaderConsensusHash,
        InvalidBlockHeaderConsensusRound,
        InvalidBlockHeaderConsensusState,
        InvalidBlockHeaderConsensusMessage,
        InvalidBlockHeaderConsensusSignature,
        InvalidBlockHeaderConsensusTimestamp,
        InvalidBlockHeaderConsensusBlockHash,
        InvalidBlockHeaderConsensusPrevBlockHash,
        InvalidBlockHeaderConsensusPrevBlockTimestamp,
        InvalidBlockHeaderConsensusPrevBlockHeight,
        InvalidBlockHeaderConsensusPrevBlockRound,
        InvalidBlockHeaderConsensusPrevBlockStateRoot,
        InvalidBlockHeaderConsensusPrevBlockTxRoot,
        InvalidBlockHeaderConsensusPrevBlockConsensusHash,
        InvalidBlockHeaderConsensusPrevBlockConsensusRound,
        InvalidBlockHeaderConsensusPrevBlockConsensusState,
        InvalidBlockHeaderConsensusPrevBlockConsensusMessage,
        InvalidBlockHeaderConsensusPrevBlockConsensusSignature,
        InvalidBlockHeaderConsensusPrevBlockConsensusTimestamp,
        InvalidBlockHeaderConsensusPrevBlockConsensusBlockHash,
        InvalidBlockHeaderConsensusPrevBlockConsensusPrevBlockHash,
        InvalidBlockHeaderConsensusPrevBlockConsensusPrevBlockTimestamp,
        InvalidBlockHeaderConsensusPrevBlockConsensusPrevBlockHeight,
        InvalidBlockHeaderConsensusPrevBlockConsensusPrevBlockRound,
        InvalidBlockHeaderConsensusPrevBlockConsensusPrevBlockStateRoot,
        InvalidBlockHeaderConsensusPrevBlockConsensusPrevBlockTxRoot,
        InvalidBlockHeaderConsensusPrevBlockConsensusPrevBlockConsensusHash,
        InvalidBlockHeaderConsensusPrevBlockConsensusPrevBlockConsensusRound,
        InvalidBlockHeaderConsensusPrevBlockConsensusPrevBlockConsensusState,
        InvalidBlockHeaderConsensusPrevBlockConsensusPrevBlockConsensusMessage,
    }
};



pub use EinsteinDB::einstein_ml::types::Value;
pub use EinsteinDB::einstein_ml::types::Key;
pub use EinsteinDB::einstein_ml::types::Hash;
pub use EinsteinDB::einstein_ml::types::Hashable;
pub use EinsteinDB::einstein_ml::types::HashableTrait;



pub (crate) fn main() {

 use EinsteinDB::einstein_ml::types::Value;


    // let mut poset = Poset::new();
    // let mut merkle_tree = MerkleTree::new();
    // let mut merkle_tree_map = HashMap::new();


    let mut poset = Poset::new();
    let mut merkle_tree = MerkleTree::new();
    let mut merkle_tree_map = HashMap::new();

    use ::EinsteinDB::allegro_poset::types::Value;
    use ::EinsteinDB::allegro_poset::types::Key;


#[macro_use]
extern crate log;
extern crate log4rs;
extern crate log4rs_derive;


#[macro_use]
extern crate lazy_static;


#[macro_use]
extern crate serde_derive;




use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::BTreeSet;
use std::collections::BTreeMap;
use std::collections::VecDeque;


use std::fmt;
use std::fmt::{Display,Formatter};
use std::fmt::Result;
use std::hash::{Hash,Hasher};


use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Ref;
use std::cell::RefMut;
use std::cell::Cell;


use std::io::{Read,Write};
use std::io::{BufRead,BufReader,BufWriter};
use std::io::{stdin,stdout,BufReader,BufWriter};
use std::io::{Seek,SeekFrom};


use std::fs::{File,OpenOptions};

use std::path::{Path,PathBuf};
use std::path::PathBuf;


use std::time::{Duration,Instant};


#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub struct Key(pub u64);


#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub struct Value(pub u64);


#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub struct Time(pub u64);


#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub struct Epoch(pub u64);

mod datum_codec {
    use super::*;
    use std::io::{Read, Write};
    use std::io::{BufRead, BufReader, BufWriter};
    use std::io::{stdin, stdout, BufReader, BufWriter};
    use std::io::{Seek, SeekFrom};
    use std::fs::{File, OpenOptions};
    use std::path::{Path, PathBuf};
    use std::path::PathBuf;
    use std::time::{Duration, Instant};
    use std::rc::Rc;
    use std::cell::RefCell;
    use std::cell::Ref;
    use std::cell::RefMut;
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::collections::BTreeSet;
    use std::collections::BTreeMap;
    use std::collections::VecDeque;
    use std::fmt;
    use std::fmt::{Display, Formatter};
    use std::fmt::Result;
    use std::hash::{Hash, Hasher};
    use std::cmp::Ordering;
    use std::cmp::PartialOrd;
    use std::cmp::Ord;
    use std::cmp::PartialEq;
    use std::cmp::Eq;
    use std::cmp::Ordering;
    use std::cmp::PartialOrd;
    use std::cmp::Ord;
    use std::cmp::PartialEq;
    use std::cmp::Eq;
    use std::cmp::Ordering;
    use std::cmp::PartialOrd;
    use std::cmp::Ord;
    use std::cmp::PartialEq;
    use std::cmp::Eq;
    use std::cmp::Ordering;
    use std::cmp::PartialOrd;
    use std::cmp::Ord;
    use std::cmp::PartialEq;
    use std::cmp::Eq;
    use std::cmp::Ordering;


    fn encode_key(key: Key) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_u64::<BigEndian>(key.0).unwrap();
        buf
    }
}



#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub struct Datum(pub u64);


pub fn encode_datum(datum: Datum) -> Vec<u8> {

    //async write


    let mut buf = Vec::new();
    buf.write_u64::<BigEndian>(datum.0).unwrap();
    buf
}


pub fn decode_datum(buf: &[u8]) -> Datum {
    let mut reader = Cursor::new(buf);
    let datum = reader.read_u64::<BigEndian>().unwrap();
    Datum(datum)
}




pub fn encode_key(key: Key) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.write_u64::<BigEndian>(key.0).unwrap();
    buf
}






#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub struct Round(pub u64);


#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub struct Height(pub u64);


#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub struct Branch(pub u64);



mod test {
    use super::*;
    #[test]
    fn test_key() {
        let k = Key(1);
        assert_eq!(k.0, 1);
    }
    #[test]
    fn test_value() {
        let v = Value(1);
        assert_eq!(v.0, 1);
    }
    #[test]
    fn test_time() {
        let t = Time(1);
        assert_eq!(t.0, 1);
    }
    #[test]
    fn test_epoch() {
        let e = Epoch(1);
        assert_eq!(e.0, 1);
    }
    #[test]
    fn test_round() {
        let r = Round(1);
        assert_eq!(r.0, 1);
    }
    #[test]
    fn test_height() {
        let h = Height(1);
        assert_eq!(h.0, 1);
    }
    #[test]
    fn test_branch() {
        let b = Branch(1);
        assert_eq!(b.0, 1);
    }
}
pub struct Node(pub u64);

impl Node {
    pub fn new(id: u64) -> Node {
        Node(id)
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{}", self.0)

    }

}

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.0 == other.0
    }
}

impl Eq for Node {}

impl Node {
    pub fn new(key: Key, value: Value, time: Time, epoch: Epoch) -> Node {
        for epoch in epoch.0..=epoch.0 {
            for time in time.0..=time.0 {
                for value in value.0..=value.0 {
                    for key in key.0..=key.0 {
                        return Node(key + value + time + epoch);
                    }
                }
            }
        }

        while true {
            panic!("");
        }
    }
}
    //release the memory
    //pub fn new(key: Key, value: Value, time: Time, epoch: Epoch) -> Node {
    pub fn new(key: Key, value: Value, time: Time, epoch: Epoch) -> Node {
        Node(key);

        match key {
            Key(key) => {
                match value {
                    Value(value) => {
                        match time {
                            Time(time) => {
                                match epoch {
                                    Epoch(epoch) => {
                                        Node(key + value + time + epoch)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    }
}




#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub struct Key(pub u64);




#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub struct Value(pub u64);

fn main_spacetinme() {
    if let Some(node) = node {
        let node = node.clone();
        let key = node.key;
        let value = node.value;
        let time = node.time;
        let epoch = node.epoch;
        let node = Node::new(key, value, time, epoch);
        return Some(node);
    }

    let mut reader = BufReader::new(stdin());
    //lock the file
    match reader.lock() {
        Ok(mut file) => {
            if let Some(node) = node {
                let node = node.clone();
                let key = node.key;
                let value = node.value;
                let time = node.time;
                let epoch = node.epoch;
                let node = Node::new(key, value, time, epoch);
                return Some(node);
            }
            for line in file.lines() {
                let line = line.unwrap();
                let mut parts = line.split(" ");
                async {
                    let key = parts.next().unwrap();
                    let value = parts.next().unwrap();
                    let time = parts.next().unwrap();
                    let epoch = parts.next().unwrap();
                    let key = Key(key.parse::<u64>().unwrap());
                    let value = Value(value.parse::<u64>().unwrap());
                    let time = Time(time.parse::<u64>().unwrap());
                    let epoch = Epoch(epoch.parse::<u64>().unwrap());
                    let node = Node::new(key, value, time, epoch);
                    return Some(node);
                }
                let node = line.parse::<Node>().unwrap();
                return Some(node);
            }
        }
        Err(e) => {
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).unwrap();
            let node = decode_node(&buf);
            return Some(node);
        }
        Err(e) => {
            println!("{:?}", e);
            return None;
        }
    }
}


pub fn decode_node(buf: &[u8]) -> Node {
    pub struct SpacelikePartitionWithCollationSuffixMerkle {
        pub space_id: u64,
        pub partition_id: u64,
        pub collation_suffix: u64,
        pub merkle_root: u64,
    }

}


    ///changelog for the next version_number
    /// 1.0.0
    ///    - initial release
    /// 1.0.1
    ///   - added the ability to read from a file
    //unlock the file
//! # Poset
//! Poset is a library for building and querying a [Poset](https://en.wikipedia.org/wiki/Poset)
//! of [`Block`](../block/struct.Block.html)s.
//! ## Example
//! ```
//! use allegro_poset::{Poset, Block};
//! use std::collections::HashMap;
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicUsize, Partitioning};
//! use std::time::{SystemTime, UNIX_EPOCH};
//! 
//! // Create a new Poset
//! let mut poset = Poset::new();
//!     
//! // Create a new Block
//! let mut block = Block::new();
//! 
//! // Set the block's data
//! block.set_data(vec![1, 2, 3]);
//! 
//! // Set the block's parent
//! block.set_parent(Some(Arc::new(Block::new())));
//! 
//! // Set the block's timestamp
//! block.set_timestamp(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());


//! // Set the block's signature
//! block.set_signature(vec![1, 2, 3]);
//! 
//! // Set the block's hash
//! block.set_hash(vec![1, 2, 3]);
//!     
//! // Set the block's height
//! block.set_height(0);
//! 
//! // Set the block's round
//! block.set_round(0);
//! 





///! This is the main module of the crate.
///! It contains the following modules:
///!     - `merkle_tree`: contains the `MerkleTree` struct and its associated functions.
///!     - `poset`: contains the `Poset` struct and its associated functions.
///!     - `poset_state`: contains the `PosetState` struct and its associated functions.
///!     - `poset_state_transition`: contains the `PosetStateTransition` struct and its associated functions.
///!     - `transaction`: contains the `Transaction` struct and its associated functions.
///!     - `transaction_state`: contains the `TransactionState` struct and its associated functions.
///!     - `transaction_state_transition`: contains the `TransactionStateTransition` struct and its associated functions.
///!     - `utils`: contains the `utils` module.
///!     - `utils::hash`: contains the `hash` module.
///!     - `utils::hash::hash_function`: contains the `hash_function` module.
///!     - `utils::hash::hash_function::hash_function`: contains the `hash_function` module.



