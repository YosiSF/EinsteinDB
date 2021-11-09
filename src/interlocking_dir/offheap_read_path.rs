//WHTCORPS 2021-2023 EINSTEINDB PROJECT AUTHORS. LICENSED UNDER APACHE-2.0

use std::marker::PhantomData;
use std::time::Duration;
use std::cell::RefCell;
use std::mem;
use std::sync::{Arc, Mutex};


use async_stream::try_stream;
use futures::{future, Future, Stream};
use futures03::channel::mpsc;
use futures03::prelude::*;
use rand::prelude::*;
use tokio::sync::Semaphore;

use crate::interlock::offheap_read_path::CachedRequestHandler;
use crate::interlock::prom_bench::*;

pub struct Singular<E: Engine> {

}