//Copyright 2019 Venire Labs Inc. Licensed under Apache-2.0

#![allow(deprecated)]

use std::collections::VecDeque;
use std::hash::{Hash, Hasher, SipHasher as DefaultHasher};
use std::usize;

//Dagger is used to serialize access to resources hashed to the image of the same slot.
//
//Daggers have a slot ID. The keyword commands are hashed to the slots. 
//The command is queued.
// If command A is ahead of command B in one dagger, it must be ahead of command B in all the
/// overlapping daggers. This is an invariant ensured by the `gen_lock`, `acquire` and `release`.


#[derive(Clone)]
struct Dagger {
    //store waiting commands
    pub waiting: VecDeque<u64>,
}

impl Dagger {
    //creates a dagger with an empty waiting queue
    pub fn new() -> Dagger {
        Dagger {
            waiting: VecDeque::new(),
        }
    }
}

pub struct Lock {
    // The slot IDs of the daggers that a command must acquire before being able to be processed.
    pub required_slots: Vec<usize>,

    /// The number of daggers that the command has acquired.
    pub owned_count: usize,
}

impl Lock {
    /// Creates a lock.
    pub fn new(required_slots: Vec<usize>) -> Lock {
        Lock {
            required_slots,
            owned_count: 0,
        }
    }

        /// Returns true if all the required daggers have be acquired, false otherwise.
    pub fn acquired(&self) -> bool {
        self.required_slots.len() == self.owned_count
    }

    pub fn is_write_lock(&self) -> bool {
        !self.required_slots.is_empty()
    }
}

///Daggers are used for concurrency in the scheduler
///
///Each Dagger is indexed by a slotID.

pub struct Dagger {
    slots: Vec<Daggers>,
    size: usize,
}

impl Daggers {
    //Create daggers
}