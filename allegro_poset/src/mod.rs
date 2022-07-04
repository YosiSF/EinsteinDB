//Copyright (c) 2022 EinsteinDB contributors
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



#[macro_use]
extern crate log;
extern crate log4rs;
extern crate log4rs_derive;


#[macro_use]
extern crate lazy_static;


#[macro_use]
extern crate serde_derive;




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

mod merkle_tree;
mod poset;


pub use crate::merkle_tree::MerkleTree;
pub use crate::poset::Poset;


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}


