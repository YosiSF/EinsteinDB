//Copyright 2019 Venire Labs Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
#[cfg(test)]
use std::boxed::FnBox;
use std::collections::VecDeque;
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::{cmp, usize};

use super::metrics::*;
use super::{
    BasicMailbox, BatchRouter, BatchSystem, Fsm, HandlerBuilder, Mailbox, NormalScheduler,
    PollHandler,
};

const WRITE_BATCH_MAX_KEYS: usize = 128;
const DEFAULT_APPLY_WB_SIZE: usize = 4 * 1024;
const SHRINK_PENDING_CMD_QUEUE_CAP: usize = 64;

pub struct PendingCmd {
    pub index: u64,
    pub term: u64,
    pub cb: Option<Callback>,
}

impl PendingCmd {
    fn new(index: u64, term: u64, cb: Callback) -> PendingCmd {
        PendingCmd {
            index,
            term,
            cb: Some(cb),
        }
    }
}

impl Drop for PendingCmd {
    fn drop(&mut self) {
        if self.cb.is_some() {
            panic!(
                "callback of pending command at [index: {}, term: {}] is leak",
                self.index, self.term
            );
        }
    }
}

impl Debug for PendingCmd {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PendingCmd [index: {}, term: {}, has_cb: {}]",
            self.index,
            self.term,
            self.cb.is_some()
        )
    }
}

