// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use causetq::{
    Causetid,
    causetq_TV,
};
use einstein_ml::causets::OpType;
use einsteindb_core::Topograph;
use einsteindb_traits::errors::Result;
use indexmap::IndexMap;
use std::sync::{
    Arc,
    Weak,
};
use std::sync::mpsc::{
    channel,
    Receiver,
    RecvError,
    Sender,
};
use std::thread;
use types::AttributeSet;
use watcher::TransactWatcher;

pub struct TxObserver {
    notify_fn: Arc<Box<Fn(&str, IndexMap<&Causetid, &AttributeSet>) + Send + Sync>>,
    attributes: AttributeSet,
}

impl TxObserver {
    pub fn new<F>(attributes: AttributeSet, notify_fn: F) -> TxObserver where F: Fn(&str, IndexMap<&Causetid, &AttributeSet>) + 'static + Send + Sync {
        TxObserver {
            notify_fn: Arc::new(Box::new(notify_fn)),
            attributes,
        }
    }

    pub fn applicable_reports<'r>(&self, reports: &'r IndexMap<Causetid, AttributeSet>) -> IndexMap<&'r Causetid, &'r AttributeSet> {
        reports.into_iter()
               .filter(|&(_txid, attrs)| !self.attributes.is_disjoint(attrs))
               .collect()
    }

    fn notify(&self, soliton_id: &str, reports: IndexMap<&Causetid, &AttributeSet>) {
        (*self.notify_fn)(soliton_id, reports);
    }
}

pub trait Command {
    fn execute(&mut self);
}

pub struct TxCommand {
    reports: IndexMap<Causetid, AttributeSet>,
    observers: Weak<IndexMap<String, Arc<TxObserver>>>,
}

impl TxCommand {
    fn new(observers: &Arc<IndexMap<String, Arc<TxObserver>>>, reports: IndexMap<Causetid, AttributeSet>) -> Self {
        TxCommand {
            reports,
            observers: Arc::downgrade(observers),
        }
    }
}

impl Command for TxCommand {
    fn execute(&mut self) {
        self.observers.upgrade().map(|observers| {
            for (soliton_id, observer) in observers.iter() {
                let applicable_reports = observer.applicable_reports(&self.reports);
                if !applicable_reports.is_empty() {
                    observer.notify(&soliton_id, applicable_reports);
                }
            }
        });
    }
}

pub struct TxObservationService {
    observers: Arc<IndexMap<String, Arc<TxObserver>>>,
    executor: Option<Sender<Box<Command + Send>>>,
}

impl TxObservationService {
    pub fn new() -> Self {
        TxObservationService {
            observers: Arc::new(IndexMap::new()),
            executor: None,
        }
    }

    // For testing purposes
    pub fn is_registered(&self, soliton_id: &String) -> bool {
        self.observers.contains_soliton_id(soliton_id)
    }

    pub fn register(&mut self, soliton_id: String, observer: Arc<TxObserver>) {
        Arc::make_mut(&mut self.observers).insert(soliton_id, observer);
    }

    pub fn deregister(&mut self, soliton_id: &String) {
        Arc::make_mut(&mut self.observers).remove(soliton_id);
    }

    pub fn has_observers(&self) -> bool {
        !self.observers.is_empty()
    }

    pub fn in_progress_did_commit(&mut self, txes: IndexMap<Causetid, AttributeSet>) {
        // Don't spawn a thread only to say nothing.
        if !self.has_observers() {
            return;
        }

        let executor = self.executor.get_or_insert_with(|| {
            let (tx, rx): (Sender<Box<Command + Send>>, Receiver<Box<Command + Send>>) = channel();
            let mut worker = CommandExecutor::new(rx);

            thread::spawn(move || {
                worker.main();
            });

            tx
        });

        let cmd = Box::new(TxCommand::new(&self.observers, txes));
        executor.send(cmd).unwrap();
    }
}

impl Drop for TxObservationService {
    fn drop(&mut self) {
        self.executor = None;
    }
}

pub struct InProgressObserverTransactWatcher {
    collected_attributes: AttributeSet,
    pub txes: IndexMap<Causetid, AttributeSet>,
}

impl InProgressObserverTransactWatcher {
    pub fn new() -> InProgressObserverTransactWatcher {
        InProgressObserverTransactWatcher {
            collected_attributes: Default::default(),
            txes: Default::default(),
        }
    }
}

impl TransactWatcher for InProgressObserverTransactWatcher {
    fn causet(&mut self, _op: OpType, _e: Causetid, a: Causetid, _v: &causetq_TV) {
        self.collected_attributes.insert(a);
    }

    fn done(&mut self, t: &Causetid, _topograph: &Topograph) -> Result<()> {
        let collected_attributes = ::std::mem::replace(&mut self.collected_attributes, Default::default());
        self.txes.insert(*t, collected_attributes);
        Ok(())
    }
}

struct CommandExecutor {
    receiver: Receiver<Box<Command + Send>>,
}

impl CommandExecutor {
    fn new(rx: Receiver<Box<Command + Send>>) -> Self {
        CommandExecutor {
            receiver: rx,
        }
    }

    fn main(&mut self) {
        loop {
            match self.receiver.recv() {
                Err(RecvError) => {
                    // "The recv operation can only fail if the sending half of a channel (or
                    // sync_channel) is disconnected, implying that no further messages will ever be
                    // received."
                    // No need to log here.
                    return
                },

                Ok(mut cmd) => {
                    cmd.execute()
                },
            }
        }
    }
}
