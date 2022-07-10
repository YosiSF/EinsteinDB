// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Partitioning};
use std::thread;
use std::time::Duration;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::collections::hash_map::Iter;
use std::collections::hash_map::IterMut;

use std::sync::atomic::
{
    AtomicUsize,
    Ordering::Relaxed,
    Ordering::SeqCst
};


use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::mpsc::TryRecvError;


use std::sync::mpsc::RecvError;
use std::sync::mpsc::RecvTimeoutError;

use super::{AllegroPoset, Poset};
use super::{PosetError, PosetErrorKind};
use super::{PosetNode, PosetNodeId, PosetNodeData};


/// A `Sync` implementation for `AllegroPoset`.
///
/// This implementation is thread-safe.
///






pub fn new_sync() -> Arc<Mutex<AllegroPoset>> {
    Arc::new(Mutex::new(AllegroPoset::new()))
}


#[derive(Debug)]


pub struct Sync {
    poset: Arc<Mutex<AllegroPoset>>,
    is_running: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    thread_id: Option<thread::ThreadId>,
    thread_name: String,
    thread_name_lock: Arc<Mutex<String>>,
    thread_name_cond: Arc<Condvar>,
    thread_name_cond_signal: Arc<AtomicBool>,
    thread_name_cond_signal_lock: Arc<Mutex<()>>,
}

pub trait Syncable {

    fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<SyncReport>;

    fn sync_with_timeout(&self, server_uri: &String, user_uuid: &String, timeout: Duration) -> Result<SyncReport>;

    fn sync_with_timeout_and_retry(&self, server_uri: &String, user_uuid: &String, timeout: Duration, retry_interval: Duration) -> Result<SyncReport>;

}

impl<'a, 'c> Syncable for InProgress<'a, 'c> {
    fn sync_with_timeout(&self, server_uri: &String, user_uuid: &String, timeout: Duration) -> Result<SyncReport> {
        self.sync.sync_with_timeout(server_uri, user_uuid, timeout)
    }



    fn sync_with_timeout_and_retry(&self, server_uri: &String, user_uuid: &String, timeout: Duration, retry_interval: Duration) -> Result<SyncReport> {
        self.sync.sync_with_timeout_and_retry(server_uri, user_uuid, timeout, retry_interval)
    }

    fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<SyncReport> {
        // Syncer behaves as if it's part of InProgress.
        // This split into a separate crate is segment synchronization functionality
        // in a single crate which can be easily disabled by consumers,
        // and to separate concerns.
        // But for all intents and purposes, Syncer operates over a "einsteindb transaction",
        // which is exactly what InProgress represents.
        let mut remote_client = RemoteClient::new(
            server_uri,
            user_uuid,
            self.sync.poset.clone(),
            server_uri.to_string(),
            user_uuid.to_string(),
            Uuid::parse_str(&user_uuid)?,
            self.sync.poset.lock().unwrap().get_node_id(),
            self.sync.poset.lock().unwrap().get_node_data().clone(),
        );
        Syncer::sync(self, &mut remote_client, self.sync.poset.clone())
    }


}





