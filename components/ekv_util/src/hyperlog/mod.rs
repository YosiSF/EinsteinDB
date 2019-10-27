//Copyright 2019 EinsteinDB. Licensed under Apache-2.0

mod file_log;
mod formatter;

use std::env;
use std::fmt;
use std::io::{self, BufWriter};
use std::path::Path;
use std::sync::Mutex;

use chrono::{self, Duration};
use log::{self, SetLoggerError};
use slog::{self, Drain, Key, OwnedKVList, Record, KV};
use slog_async::{Async, OverflowStrategy};
use slog_term::{Decorator, PlainDecorator, RecordDecorator, TermDecorator};

use self::file_log::RotatingFileLogger;

pub use slog::Level;

//Default is 128 blocks
const SLOG_CHANNEL_SIZE: usize = 10240;

const SLOG_CHANNEL_OVERFLOW_STRATEGY: OverflowStrategy = OverflowStrategy::Block;
const TIMESTAMP_FORMAT: &str = "%Y/%m/%d %H:%M:%S.3f %:z";


