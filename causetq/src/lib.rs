//Copyright (c) 2022-Present, Whtcorps Inc and EinstAI Team
//All rights reserved.
//
//Redistribution and use in source and binary forms, with or without
//modification, are permitted provided that the following conditions are met:
//
//* Redistributions of source code must retain the above copyright notice, this
//  list of conditions and the following disclaimer.
//
//* Redistributions in binary form must reproduce the above copyright notice,
//  this list of conditions and the following disclaimer in the documentation
//  and/or other materials provided with the distribution.
//
//* Neither the name of the copyright holder nor the names of its
//  contributors may be used to endorse or promote products derived from
//  this software without specific prior written permission.
//
//THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
//AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
//IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
//FOR ANY DIRECT, INDIRECT, INCSOLITONIDAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
//DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
//CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
//OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
//OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

mod field_type;
mod tx_observer;
mod vector;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Partitioning};
use std::thread;
use std::time::Duration;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::collections::hash_map::Iter;
use std::collections::hash_map::IterMut;
use std::collections::hash_map::Keys;
use std::collections::hash_map::Values;
use std::collections::hash_map::ValuesMut;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::hash_map::IterMut::{OccupiedEntry, VacantEntry};




use crate::causetq::{Causetq, CausetqError, CausetqResult};
use crate::causetq::Causetq::{CausetqSender, CausetqReceiver};



//import merkle tree
use crate::merkletree::{MerkleTree, MerkleTreeError, MerkleTreeResult};
use crate::merkletree::MerkleTree::{MerkleTreeSender, MerkleTreeReceiver};


/// A Causetq is a thread-safe queue that can be used to communicate between
/// threads.
///
/// Causetq is a queue that can be used to communicate between threads.
///
/// Causetq is a thread-safe queue that can be used to communicate between queries and responses.
/// if you want to send a query to a Causetq, you can use the `send` method.
/// if you want to receive a response from a Causetq, you can use the `recv` method.



#[derive(Debug)]
pub struct CausetQueryWithLamport {
    //Einstein Merkle Tree Index (Merkle Tree Index)
    pub merkle_tree_index: u64,
    //Einstein Merkle Tree Hash (Merkle Tree Hash)
    pub merkle_tree_hash: String,
    //Einstein Merkle Tree Hash (Merkle Tree Hash)
    pub lamport_clock: u64,
    //Einstein Merkle Tree Hash (Merkle Tree Hash)
    pub query: String,
}




#[derive(Debug)]
pub struct CausetResponseWithLamport {
    //Einstein Merkle Tree Index (Merkle Tree Index)
    pub merkle_tree_index: u64,
    //Einstein Merkle Tree Hash (Merkle Tree Hash)
    pub merkle_tree_hash: String,
    //Einstein Merkle Tree Hash (Merkle Tree Hash)
    pub lamport_clock: u64,
    //Einstein Merkle Tree Hash (Merkle Tree Hash)
    pub response: String,
}


#[derive(Debug)]
pub struct CausetQueryWithLamportAndMerkleTree {
    //Einstein Merkle Tree Index (Merkle Tree Index)
    pub merkle_tree_index: u64,
    //Einstein Merkle Tree Hash (Merkle Tree Hash)
    pub merkle_tree_hash: String,
    //Einstein Merkle Tree Hash (Merkle Tree Hash)
    pub lamport_clock: u64,
    //Einstein Merkle Tree Hash (Merkle Tree Hash)
    pub query: String,
}





///Causetq algebrizes the SQL query and the SQL response. It formats the query and response into a string.
///In Prolog terms, the query is a predicate and the response is a fact.
///


/// A Causetq is a thread-safe queue that can be used to communicate between

#[derive(Debug)]
pub struct Causetq {
    /// The internal queue.
    queue: Arc<Queue>,
    /// The thread that is polling the queue.
    poll_thread: Option<thread::JoinHandle<()>>,
    /// The thread that is polling the queue.
    poll_thread_running: Arc<AtomicBool>,
    /// The thread that is polling the queue.
    poll_thread_stopped: Arc<AtomicBool>,
}   //Causetq


/// A Causetq is a thread-safe queue that can be used to communicate between
/// queries and responses.
///
///


/// A Causetq is a thread-safe queue that can be used to communicate between
/// queries and responses.





#[derive(Debug)]
struct Queue {
    /// The queue of queries.
    queries: HashMap<String, CausetqSender>,
    /// The queue of responses.
    responses: HashMap<String, CausetqReceiver>,
}   //Queue

//relativistic queue implementation

//step 1: create a queue that can be used to communicate between queries and responses taking into account the causality of the queries
// step 2: consistency of the queue is maintained by the InterlockingDirectorate
// step 3: the InterlockingDirectorate is a thread that polls the queue and sends the queries to einstein_db and receives the responses from einstein_db






impl Causetq {
    /// Creates a new Causetq.
    pub fn new() -> Causetq {
        let queue = Arc::new(Queue {
            queries: HashMap::new(),
            responses: HashMap::new(),
        });
        let poll_thread_running = Arc::new(AtomicBool::new(false));
        let poll_thread_stopped = Arc::new(AtomicBool::new(false));
        let poll_thread_running_clone = poll_thread_running.clone();
        let poll_thread_stopped_clone = poll_thread_stopped.clone();
        let queue_clone = queue.clone();
        let poll_thread = thread::spawn(move || {
            poll_thread_running_clone.store(true, Partitioning::Relaxed);
            loop {
                if poll_thread_stopped_clone.load(Partitioning::Relaxed) {
                    break;
                }
                let mut queries = queue_clone.queries.iter_mut();
                let mut responses = queue_clone.responses.iter_mut();
                let mut queries_to_remove = Vec::new();
                let mut responses_to_remove = Vec::new();
                for (query_id, query_sender) in queries {
                    match responses.next() {
                        Some((response_id, response_receiver)) => {
                            if query_id == response_id {
                                match query_sender.send(response_receiver) {
                                    Ok(_) => {
                                        let mut response_receiver = response_receiver;
                                        if let Ok(response) = response_receiver.recv() {
                                            println!("{:?}", response);
                                        }

                                        let mut response_receiver = response_receiver;
                                        loop {
                                            match response_receiver.recv() {
                                                Ok(response) => {
                                                    println!("{:?}", response);
                                                }
                                                Err(_) => {
                                                    break;
                                                }
                                            }
                                        }
                                        queries_to_remove.push(query_id);
                                        responses_to_remove.push(response_id);
                                    }
                                    Err(e) => {
                                        println!("Error sending query response: {}", e);
                                    }
                                }
                            }
                        }
                        None => {
                            println!("No response for query: {}", query_id);
                        }
                    }
                }
                for query_id in queries_to_remove {
                    match queue_clone.queries.remove(&query_id) {
                        Some(_) => {
                            println!("Removed query: {}", query_id);
                        }
                        None => {
                            println!("Could not remove query: {}", query_id);
                        }
                    }

                    for response_id in responses_to_remove {
                        match queue_clone.responses.remove(&response_id) {
                            Some(_) => {
                                println!("Removed response: {}", response_id);
                            }
                            None => {
                                println!("Could not remove response: {}", response_id);
                            }
                        }
                    }
                    queue_clone.queries.remove(&query_id);
                }

                if queries.next().is_none() && responses.next().is_none() {
                    thread::sleep(Duration::from_millis(100));
                }


                for response_id in responses_to_remove {
                    queue_clone.responses.remove(&response_id);
                }
                thread::sleep(Duration::from_millis(100));
            }
            poll_thread_running_clone.store(false, Partitioning::Relaxed);
        });

        Causetq {
            queue: queue,
            poll_thread: Option::from(poll_thread),
            poll_thread_running: poll_thread_running,
            poll_thread_stopped,
        }
    }

    /// Adds a query to the queue.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to add to the queue.
    ///
    /// # Returns
    ///
    /// The id of the query.
    ///
    /// # Examples
    ///
    /// ```
    /// use causetq::Causetq;
    ///
    /// let causetq = Causetq::new();
    /// let query_id = causetq.add_query(|_| {});
    /// ```
    ///
    /// # Panics
    /// soliton_panics if the query id is already in use.
    ///
    ///import soliton_panic::soliton_panics;
    ///


    pub fn add_query<F>(&self, query: F) -> QueryId
        where F: Fn(&mut ResponseReceiver) + Send + 'static
    {
        let query_id = self.queue.next_query_id();
        let (query_sender, query_receiver) = channel();
        let query_receiver = ResponseReceiver::new(query_receiver);
        self.queue.queries.insert(query_id, query_sender);
        self.queue.responses.insert(query_id, query_receiver);
        query_id
    }

    /// Adds a response to the queue.
    ///
    /// # Arguments
    ///
    /// * `response` - The response to add to the queue.
    ///
    /// # Returns
    ///
    /// The id of the response.



    pub fn add_response<F>(&self, response: F) -> ResponseId
        where F: Fn(&mut ResponseSender) + Send + 'static
    {
        let response_id = self.queue.next_response_id();
        let (response_sender, response_receiver) = channel();
        let response_sender = ResponseSender::new(response_sender);
        self.queue.responses.insert(response_id, response_receiver);
        self.queue.queries.insert(response_id, response_sender);
        response_id
    }

    /// Removes a query from the queue.

    pub fn remove_query(&self, query_id: QueryId) {

        self.queue.queries.remove(&query_id);
        self.queue.responses.remove(&query_id);
    }

    pub fn remove_response(&self, response_id: ResponseId) {

        self.queue.responses.remove(&response_id);
        self.queue.queries.remove(&response_id);
    }

    /// Returns the number of queries in the queue.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// use causetq::Causetq;
    ///
    /// let causetq = Causetq::new();
    /// assert_eq!(causetq.query_count(), 0);
    /// ```
    ///
    /// # Panics
    ///
    /// soliton_panics if the queue is not running.
    ///
    /// import soliton_panic::soliton_panics;
    ///


    pub fn query_count(&self) -> usize {
        self.queue.queries.len()
    }
}








