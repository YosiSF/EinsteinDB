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
//
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




// Language: rust
// Path: EinsteinDB/allegro_poset/src/mod.rs
// Compare this snippet from EinsteinDB/allegro_poset/src/allegro_causet_value.rs:
pub mod alexandrov_process;
pub mod alexandrov_process_state;
pub mod causet_locale;
pub mod convert;
pub mod datum;
pub mod datum_codec;
pub mod spacetime;
pub mod sync;
pub mod types;




///! #### `merkle_tree`
/// ! This module contains the `MerkleTree` struct and its associated functions.
///


