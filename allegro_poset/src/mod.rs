// Root level. Merkle-tree must be created here. Write the LSH-KV with a stateless hash tree of `Node`,
// where each internal node contains two child nodes and is associating with another key (a => b) in merkle_tree_map().
// We need to make sure that whatever algorithm we use for this hash function gives us even distribution so
// that our keyspace does not get clustered around few points,
// lest it would severely waste space when building up the table. The best way to ensure an even distribution throughout our entire space
// is if we can find some mathematical operation on input values which has no bias towards any particular output value over others;
// ideally, every possible output should have exactly equal chance of occurring regardless of what inputs are fed into it (perfect uniformity).
// This property is known as *random oracle*. And such algorithms exist: they're called *universal hashing functions*! These work by taking
// your regular data as input and using a randomly generated number/key to manipulate them according to some obscure formula; you'll see how
// one works later on in this article./ See [this post](https://yosisf/EinsteinDB)
// for a good explanation of universal hashing functions.
//
// The hash function used here is the one from [this post](https://yosisf/EinsteinDB)
//


use crate::{
    merkle_tree::{
        MerkleTree,
        MerkleTreeMap,
        MerkleTreeMapMut,
        MerkleTreeMapMutRef,
        MerkleTreeMapRef,
        MerkleTreeMut,
        MerkleTreeRef,
    },
    poset::{
        Poset,
        PosetMap,
        PosetMapMut,
        PosetMapMutRef,
        PosetMapRef,
        PosetMut,
        PosetRef,
    },
    poset_state::{
        PosetState,
        PosetStateMap,
        PosetStateMapMut,
        PosetStateMapMutRef,
        PosetStateMapRef,
        PosetStateMut,
        PosetStateRef,
    },
    poset_state_transition::{
        PosetStateTransition,
        PosetStateTransitionMap,
        PosetStateTransitionMapMut,
        PosetStateTransitionMapMutRef,
        PosetStateTransitionMapRef,
        PosetStateTransitionMut,
        PosetStateTransitionRef,
    },
    transaction::{
        Transaction,
        TransactionMap,
        TransactionMapMut,
        TransactionMapMutRef,
        TransactionMapRef,
        TransactionMut,
        TransactionRef,
    },
    transaction_state::{
        TransactionState,
        TransactionStateMap,
        TransactionStateMapMut,
        TransactionStateMapMutRef,
        TransactionStateMapRef,
        TransactionStateMut,
        TransactionStateRef,
    },
    transaction_state_transition::{
        TransactionStateTransition,
        TransactionStateTransitionMap,
        TransactionStateTransitionMapMut,
        TransactionStateTransitionMapMutRef,
        TransactionStateTransitionMapRef,
        TransactionStateTransitionMut,
        TransactionStateTransitionRef,
    },
};