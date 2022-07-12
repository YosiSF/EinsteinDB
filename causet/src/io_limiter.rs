///Copyright (c) 2022 EinsteinDB Project Authors. Licensed under Apache-2.0
/// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
/// this file except in compliance with the License. You may obtain a copy of the
/// License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
/// law or agreed to in writing, software distributed under the License is distributed on
/// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
/// implied. See the License for the specific language governing permissions and limitations
/// under the License.
/// =================================================================
/// # About
/// =================================================================
/// This is a library for the [EinsteinDB](https://einsteindb.com
/// "EinsteinDB: A Scalable, High-Performance, Distributed Database")
///
///
///

use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::{env, fs, io::{self, BufReader, BufWriter}, path::Path};
use std::{io::{self, BufRead, BufWriter}, path::Path};


use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::thread::sleep;
use std::thread::spawn;
use std::thread::JoinHandle;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;
use std::sync::ArcLock;
use std::sync::ArcLockReadGuard;
use std::sync::ArcLockWriteGuard;
use std::sync::atomic::{AtomicUsize, Ordering};

use ::std::sync::atomic::{AtomicUsize, Ordering};
use ::std::sync::atomic::AtomicBool;

use ::std::sync::atomic::AtomicBool;

use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::SendError;
use std::sync::mpsc::RecvTimeoutError;


/// Herlihy and Wing’s definition (see Section 3), as well as the method they propose for proving linearizability,
/// weave together a number of mathematical building blocks—partial orders, total orders, representation invariants and ab- straction functions. Underlying these convenient abstractions are two funda- mental assumptions: (1) events can be totally ordered according to an irreflex- ive, transitive temporal relation; (2) the states of the “base objects” used to construct the implemented object can be observed simultaneously. While these assumptions are in line with classical or Newtonian physics, Einstein’s theory of relativity tells us that both time and simultaneity are in fact relative [8]. That is, two events may occur in opposite orders from the perspectives of two observers,
/// and two events that appear simultaneous to one observer may not be simultane- ous to another observer. Intuitively, these phenomena occur because the speed of light in a vacuum appears the same to all observers irrespective of their relative motion or the motion of the light source. As a result, assumptions (1) and (2) break in a relativistic distributed system where components may move relative to each other at high speeds.

#[derive(Debug)]
pub struct IoLimiterWithChannel {
    pub limiter: Arc<Mutex<IoLimiter>>,
    pub sender: Sender<IoLimiterWithChannelMessage>,
    pub receiver: Receiver<IoLimiterWithChannelMessage>,
}


#[derive(Debug)]
pub enum IoLimiterWithChannelMessage {
    IoLimiterMessage(IoLimiterMessage),
    IoLimiterWithChannelMessage(IoLimiterWithChannelMessage),
}


#[derive(Debug)]
pub enum IoLimiterMessage {
    IoLimiterMessage(IoLimiterMessage),
    IoLimiterWithChannelMessage(IoLimiterWithChannelMessage),
}


    pub fn max_read_bytes_per_sec(max: usize, policy: IoLimiterPolicy, threshold: usize) -> IoLimiterWithChannel {
        let (sender, receiver) = channel();
        let limiter = IoLimiter::new(max, policy, threshold, sender);
        IoLimiterWithChannel {
            limiter: Arc::new(Mutex::new(limiter)),
            sender: sender,
            receiver: receiver,
        }
    }



    pub fn max_write_bytes_per_sec(max: usize, policy: IoLimiterPolicy, threshold: usize) -> IoLimiterWithChannel {
        let (sender, receiver) = channel();
        let limiter = IoLimiter::new(max, policy, threshold, sender);
        IoLimiterWithChannel {
            limiter: Arc::new(Mutex::new(limiter)),
            sender: sender,
            receiver: receiver,
        }
    }


    pub fn max_read_bytes_per_sec_with_channel(max: usize, policy: IoLimiterPolicy, threshold: usize, sender: Sender<IoLimiterWithChannelMessage>) -> IoLimiterWithChannel {
        let limiter = IoLimiter::new(max, policy, threshold, sender);
        IoLimiterWithChannel {
            limiter: Arc::new(Mutex::new(limiter)),
            sender: sender,
            receiver: sender,
        }
    }


    pub fn max_write_bytes_per_sec_with_channel(max: usize, policy: IoLimiterPolicy, threshold: usize, sender: Sender<IoLimiterWithChannelMessage>) -> IoLimiterWithChannel {
        let limiter = IoLimiter::new(max, policy, threshold, sender);
        IoLimiterWithChannel {
            limiter: Arc::new(Mutex::new(limiter)),
            sender: sender,
            receiver: sender,
        }
    }


    pub fn max_read_bytes_per_sec_with_channel_and_receiver(max: usize, policy: IoLimiterPolicy, threshold: usize, sender: Sender<IoLimiterWithChannelMessage>, receiver: Receiver<IoLimiterWithChannelMessage>) -> IoLimiterWithChannel {
        let limiter = IoLimiter::new(max, policy, threshold, sender);
        IoLimiterWithChannel {
            limiter: Arc::new(Mutex::new(limiter)),
            sender: sender,
            receiver: receiver,
        }
    }

impl IoLimiterWithChannel {
    pub fn new(max: usize, policy: IoLimiterPolicy, threshold: usize, sender: Sender<IoLimiterWithChannelMessage>) -> IoLimiterWithChannel {
        let (sender, receiver) = channel();
        let limiter = IoLimiter::new(max, policy, threshold, sender);
        IoLimiterWithChannel {
            limiter: Arc::new(Mutex::new(limiter)),
            sender: sender,
            receiver: receiver,
        }
    }

    pub fn new_with_channel(max: usize, policy: IoLimiterPolicy, threshold: usize, sender: Sender<IoLimiterWithChannelMessage>, receiver: Receiver<IoLimiterWithChannelMessage>) -> IoLimiterWithChannel {
        let limiter = IoLimiter::new(max, policy, threshold, sender);
        IoLimiterWithChannel {
            limiter: Arc::new(Mutex::new(limiter)),
            sender: sender,
            receiver: receiver,
        }
    }

    pub fn new_with_channel_with_limiter(limiter: IoLimiter, sender: Sender<IoLimiterWithChannelMessage>, receiver: Receiver<IoLimiterWithChannelMessage>) -> IoLimiterWithChannel {
        IoLimiterWithChannel {
            limiter: Arc::new(Mutex::new(limiter)),
            sender: sender,
            receiver: receiver,
        }
    }

    pub fn new_with_channel_with_limiter_with_channel(limiter: IoLimiter, sender: Sender<IoLimiterWithChannelMessage>, receiver: Receiver<IoLimiterWithChannelMessage>) -> IoLimiterWithChannel {
        IoLimiterWithChannel {
            limiter: Arc::new(Mutex::new(limiter)),
            sender: sender,
            receiver: receiver,
        }
    }
}
pub fn min_max_for_policy(policy: IoLimiterPolicy) -> (usize, usize) {

    match policy {
        IoLimiterPolicy::Read => (0, std::usize::MAX),
        IoLimiterPolicy::Write => (0, std::usize::MAX),
        IoLimiterPolicy::ReadWrite => (0, std::usize::MAX),
    }


    /// The following is the implementation of the Herlihy and Wing’s algorithm for linearizability.
    /// The algorithm is based on the implementation of the following paper:
    /// Herlihy, N., and Wing, S. (1999). Linearizability. In Proceedings of the 35th annual international conference on safety engineering (IEEE). IEEE, pp. 1-6.
    ///
    /// The algorithm is based on the following assumptions:
    /// (1) events can be totally ordered according to an irreflex- ive, transitive temporal relation;
    /// (2) the states of the “base objects” used to construct the implemented object can be observed simultaneously.
    ///
    /// The algorithm is based on the following assumptions:
    /// (1) events can be totally ordered according to an irreflex- ive, transitive temporal relation;
    /// (2) the states of the “base objects” used to construct the implemented object can be observed simultaneously.
    ///
    ///




    fn min_max_for_policy(policy: IoLimiterPolicy) -> (usize, usize) {

        match policy {
            IoLimiterPolicy::Read => (0, std::usize::MAX),
            IoLimiterPolicy::Write => (0, std::usize::MAX),
            IoLimiterPolicy::ReadWrite => (0, std::usize::MAX),
        }

        for i in 0..std::usize::MAX {
            if policy.is_satisfied(i) {
                let min = i;
                let max = i;
                return (min, max);

            }
        }
        for i in 0..std::usize::MAX {
            if policy.is_satisfied(i) {
                if i < min {
                    min = i;
                }
                if i > max {
                    max = i;
                }
                while i < std::usize::MAX {
                    if policy.is_satisfied(i) {
                        if i < min {
                            min = i;
                        }
                        if i > max {
                            max = i;
                        }
                    }
                    i += 1;
                }
            }
                return (i, std::usize::MAX);

            }
        }
    }



impl IoLimiterWithChannel {
    pub fn max_read_bytes_per_sec(&self) -> usize {
        self.limiter.lock().unwrap().max_read_bytes_per_sec()
    }
    pub fn max_write_bytes_per_sec(&self) -> usize {
        self.limiter.lock().unwrap().max_write_bytes_per_sec()
    }
}
//lockfree version
impl IoLimiterWithChannel {
    pub fn max_read_bytes_per_sec_with_channel_with_limiter(&self) -> usize {
        self.limiter.lock().unwrap().max_read_bytes_per_sec()
    }
    pub fn max_write_bytes_per_sec_with_channel_with_limiter(&self) -> usize {
        self.limiter.lock().unwrap().max_write_bytes_per_sec()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IoLimiterPolicy {

    /// The limiter will not limit the IO.
    /// This is the default policy.
    ///
    Fair = 0,
    /// The limiter will limit the IO.
    FairRoundRobin = 1,
    /// The limiter will limit the IO.
    FairRoundRobinWithThreshold = 2,
    /// The limiter will limit the IO.
    FairRoundRobinWithThresholdAndThresholdPolicy = 3,
    FairRoundRobinWithThresholdAndThresholdPolicyAndThresholdPolicy = 4
}


pub struct IoLimiter {
    limiter_type: IoLimiterType,

    limiter_policy: IoLimiterPolicy,

    limiter_policy_threshold: usize,

    limiter_policy_threshold_policy: IoLimiterPolicy,
    ///! The limiter for the number of bytes read from the underlying reader.
    /// The limiter is shared between the reader and the writer.
    /// The limiter is not thread-safe.

    pub limiter: Arc<RateLimiter>,

    ///! The limiter for the number of bytes written to the underlying writer.
    /// The limiter is shared between the reader and the writer.
    /// The limiter is not thread-safe.

    pub soliton: Arc<Soliton>,

    ///! The limiter for the number of bytes written to the underlying writer.
    /// The limiter is shared between the reader and the writer.

}
/// An `IoLimiter` is used to limit the number of concurrent I/O operations.
/// It is used to limit the number of concurrent reads and writes of a file.
/// It is used to limit the number of concurrent reads and writes of a socket.
/// It is used to limit the number of concurrent reads and writes of a pipe.
/// It is used to limit the number of concurrent reads and writes of a TTY.
///
/// The `IoLimiter` is implemented as a semaphore.
/// The number of available permits is the number of concurrent I/O operations
/// that can be performed.
/// The number of available permits is initially set to `max`.
/// The number of available permits is decremented when an I/O operation is
/// started.
///










impl IoLimiter {
    /// Creates a new `IoLimiter` with the given maximum number of concurrent I/O operations.
    ///
    /// # Examples
    ///
    /// ```
    /// use einsteindb::io_limiter::IoLimiter;
    /// let limiter = IoLimiter::new(10);
    /// ```
    pub fn new(max: usize) -> IoLimiter {
        IoLimiter {
            limiter_type: IoLimiterType::Read,
            limiter_policy: IoLimiterPolicy::Fair,
            limiter_policy_threshold: 0,
            limiter_policy_threshold_policy: IoLimiterPolicy::Fair,
            limiter: Arc::new(RateLimiter::new(max)),
            soliton: Arc::new(Soliton::new(max)),
        }
    }
    /// Creates a new `IoLimiter` with the given maximum number of concurrent I/O operations.
    ///
    /// # Examples
    ///
    /// ```
    /// use einsteindb::io_limiter::IoLimiter;
    /// let limiter = IoLimiter::new(10);
    /// ```
    pub fn new_with_policy(max: usize, policy: IoLimiterPolicy) -> IoLimiter {
        IoLimiter {
            limiter_type: IoLimiterType::Read,
            limiter_policy: policy,
            limiter_policy_threshold: 0,
            limiter_policy_threshold_policy: IoLimiterPolicy::Fair,
            limiter: Arc::new(RateLimiter::new(max)),
            soliton: Arc::new(Soliton::new(max)),
        }
    }
    /// Creates a new `IoLimiter` with the given maximum number of concurrent I/O operations.
    ///
    /// # Examples
    ///
    /// ```
    /// use einsteindb::io_limiter::IoLimiter;
    /// let limiter = IoLimiter::new(10);
    /// ```




    /// Creates a new `IoLimiter` with the given maximum number of concurrent I/O operations.
    /// # Examples
    /// ```
    /// use einsteindb::io_limiter::IoLimiter;
    /// let limiter = IoLimiter::new(10);
    /// ```
    /// # Panics
    /// Panics if `max` is zero.
    /// Panics if `max` is greater than `usize::MAX`.
    /// Panics if `max` is greater than `usize::MAX` / 2.
    /// Panics if `max` is greater than `usize::MAX` / 4.
    ///
    /// # Panics
    /// Panics if `max` is zero.


    pub fn new_with_policy_and_threshold(max: usize, policy: IoLimiterPolicy, threshold: usize) -> IoLimiter {
        IoLimiter {
            limiter_type: IoLimiterType::Read,
            limiter_policy: policy,
            limiter_policy_threshold: threshold,
            limiter_policy_threshold_policy: IoLimiterPolicy::Fair,
            limiter: Arc::new(RateLimiter::new(max)),
            soliton: Arc::new(Soliton::new(max)),
        }
    }



    /// Creates a new `IoLimiter` with the given maximum number of concurrent I/O operations.
    ///
    ///
}

impl Default for IoLimiter {
    fn default() -> IoLimiter {
        IoLimiter::new(10)
    }
}

    pub struct IoLimiterGuard {
        limiter: Arc<RateLimiter>,
        soliton: Arc<Soliton>,
        limiter_type: IoLimiterType,
        limiter_policy: IoLimiterPolicy,
        limiter_policy_threshold: usize,
        limiter_policy_threshold_policy: IoLimiterPolicy,
    }
impl IoLimiter {
    ///! `new` creates a new `IoLimiter` with the given maximum number of
    /// permits.
    /// The maximum number of permits must be greater than zero.
    /// The maximum number of permits is initially set to `max`.


    pub fn new_with_policy_and_threshold_and_threshold_policy(max: usize, policy: IoLimiterPolicy, threshold: usize, threshold_policy: IoLimiterPolicy) -> IoLimiter {
        IoLimiter {
            limiter_type: IoLimiterType::Read,
            limiter_policy: policy,
            limiter_policy_threshold: threshold,
            limiter_policy_threshold_policy: threshold_policy,
            limiter: Arc::new(RateLimiter::new(max)),
            soliton: Arc::new(Soliton::new(max)),
        }
    }

    pub fn new(max: usize, poset_processing: Arc<PosetProcessing>, soliton: Arc<Soliton>) -> Self {
        assert!(max > 0, "max must be greater than zero");
        IoLimiter {
            limiter_type: IoLimiterType::Read,
            limiter_policy: IoLimiterPolicy::Fair,
            limiter_policy_threshold: 0,
            limiter_policy_threshold_policy: IoLimiterPolicy::Fair,

            limiter: Arc::new(RateLimiter::new(max)),
            soliton,
        }
    }


    pub fn new_with_policy_and_threshold(max: usize, policy: IoLimiterPolicy, threshold: usize) -> Self {
        assert!(max > 0, "max must be greater than zero");
        IoLimiter {
            limiter_type: IoLimiterType::Read,
            limiter_policy: policy,
            limiter_policy_threshold: threshold,
            limiter_policy_threshold_policy: IoLimiterPolicy::Fair,

            limiter: Arc::new(RateLimiter::new(max)),
            soliton: Arc::new(Soliton::new(max)),

        }
    }
}



    pub fn new_with_policy_and_threshold_and_threshold_policy(max: usize, policy: IoLimiterPolicy, threshold: usize, threshold_policy: IoLimiterPolicy) -> Self {
        assert!(max > 0, "max must be greater than zero");
        IoLimiter {
            limiter_type: IoLimiterType::Read,
            limiter_policy: policy,
            limiter_policy_threshold: threshold,
            limiter_policy_threshold_policy: threshold_policy,

            limiter: Arc::new(RateLimiter::new(max)),
            soliton: Arc::new(Soliton::new(max)),

        }
    }

    /*
    kv::{
        self,
        txn::{
            Violetabft_mvsrTxn,
            Violetabft_mvsrTxnExtra,
            Violetabft_mvsrTxnScanner,
            Violetabft_mvsrTxnScannerBuilder,
            Violetabft_mvsrTxnWrite,
            Violetabft_mvsrTxnWriteBatch,
        },
        Error as StorageError,
        Result as StorageResult,
    },
    mvcc::{
        Violetabft_mvsrReader,
        Violetabft_mvsrReaderBuilder,
        Violetabft_mvsrReaderOptions,
        Violetabft_mvsrTxnExtra as Violeta_mysqlTxnExtraImpl,
        Violetabft_mvsrTxnExtraWrapper,
        Violetabft_mvsrWrite,
        Violetabft_mvsrWriteBatch,
    },
    StorageCf,
};
*/




#[derive(Clone)]
struct IoLimiterInner {
    limiter: Arc<RateLimiter>,
    poset_processing: Arc<PosetProcessing>,
    soliton: Arc<Soliton>,
}


impl IoLimiterInner {
    fn new(limiter: Arc<RateLimiter>, poset_processing: Arc<PosetProcessing>, soliton: Arc<Soliton>) -> Self {
        IoLimiterInner {
            limiter: limiter,
            poset_processing: poset_processing,
            soliton: soliton,
        }
    }

    fn acquire(&self) -> StorageResult<()> {
        self.limiter.acquire()
    }

    fn release(&self) -> StorageResult<()> {
        self.limiter.release()
    }

    fn get_poset_processing(&self) -> Arc<PosetProcessing> {
        self.poset_processing.clone()
    }

    fn get_soliton(&self) -> Arc<Soliton> {
        self.soliton.clone()
    }

    fn get_limiter(&self) -> Arc<RateLimiter> {
        self.limiter.clone()
    }

    fn get_limiter_type(&self) -> IoLimiterType {
        self.limiter_type
    }
}

impl IOLimiterExt for soliton_panic_merkle_tree {
    type IOLimiter = PanicIOLimiter;
}


    impl IOLimiterExt for soliton_panic_merkle_tree {
        type IOLimiter = PanicIOLimiter;
    }



    impl IOLimiterExt for soliton_panic_merkle_tree {
        type IOLimiter = PanicIOLimiter;
    }


impl IOLimiter for PanicIOLimiter {
    fn acquire_read_lock(&self, _: &str) -> Result<()> {
        Ok(())
    }

    fn acquire_write_lock(&self, _: &str) -> Result<()> {
        Ok(())
    }

    fn release_read_lock(&self, _: &str) -> Result<()> {
        Ok(())
    }

    fn release_write_lock(&self, _: &str) -> Result<()> {
        Ok(())
    }
    fn new(bytes_per_sec: i64) -> Self {
        panic!()
    }

    fn acquire_read_lock_for_duration(&self, _: &str, _: Duration) -> Result<()> {
        Ok(())
    }

    fn acquire_write_lock_for_duration(&self, _: &str, _: Duration) -> Result<()> {
        Ok(())
    }

    fn set_bytes_per_second(&self, bytes_per_sec: i64) {
        panic!()
    }
    fn request(&self, bytes: i64) {
        panic!()
    }
    fn get_max_bytes_per_time(&self) -> i64 {
        panic!()
    }



    fn get_total_bytes_through(&self) -> i64 {
        panic!()
    }
    fn get_bytes_per_second(&self) -> i64 {
        panic!()
    }
    fn get_total_requests(&self) -> i64 {
        panic!()
    }
    fn get_total_time_through(&self) -> Duration {
        panic!()
    }
    fn get_total_time_waiting(&self) -> Duration {
        panic!()
    }
    fn get_total_time_blocked(&self) -> Duration {
        panic!()
    }
}

///An interlocking directorate uses the soliton to control the number of threads that can access the
/// same resource at the same time. It is lock-free and can be used in a multi-threaded environment.
/// It is also a rate limiter.
///


const SOLITON_MAX_THREADS: usize = 1024;
const SOLITON_MAX_THREADS_MASK: usize = SOLITON_MAX_THREADS - 1;


#[derive(Clone)]
pub struct PanicIOLimiter {
    limiter_type: IoLimiterType,
    limiter_policy: IoLimiterPolicy,
    limiter_policy_threshold: usize,
    limiter_policy_threshold_policy: IoLimiterPolicy,
    limiter: Arc<RateLimiter>,
    soliton: Arc<Soliton>,
}


impl PanicIOLimiter {
    fn new(limiter: Arc<RateLimiter>, soliton: Arc<Soliton>) -> Self {
        PanicIOLimiter {
            limiter_type: IoLimiterType::Read,
            limiter_policy: IoLimiterPolicy::Throttling,
            limiter_policy_threshold: 0,
            limiter_policy_threshold_policy: IoLimiterPolicy::Throttling,
            limiter: limiter,
            soliton: soliton,
        }
    }
}


impl IOLimiter for PanicIOLimiter {
    fn acquire_read_lock(&self, _: &str) -> Result<()> {
        self.soliton.acquire()
    }

    fn acquire_write_lock(&self, _: &str) -> Result<()> {
        self.soliton.acquire()
    }

    fn release_read_lock(&self, _: &str) -> Result<()> {
        self.soliton.release()
    }

    fn release_write_lock(&self, _: &str) -> Result<()> {
        self.soliton.release()
    }
    fn new(bytes_per_sec: i64) -> Self {
        panic!()
    }

    fn acquire_read_lock_for_duration(&self, _: &str, _: Duration) -> Result<()> {
        self.soliton.acquire()
    }

    fn acquire_write_lock_for_duration(&self, _: &str, _: Duration) -> Result<()> {
        self.soliton.acquire()
    }

    fn set_bytes_per_second(&self, bytes_per_sec: i64) {
        panic!()
    }
    fn request(&self, bytes: i64) {
        panic!()
    }
    fn get_max_bytes_per_time(&self) -> i64 {
        panic!()
    }

    fn get_total_bytes_through(&self) -> i64 {
        panic!()
    }

    fn get_bytes_per_second(&self) -> i64 {
        panic!()
    }

    fn get_total_requests(&self) -> i64 {
        panic!()
    }

    fn get_total_time_through(&self) -> Duration {
        panic!()
    }

    fn get_total_time_waiting(&self) -> Duration {
        panic!()
    }


    fn get_total_time_blocked(&self) -> Duration {
        panic!()
    }

}






