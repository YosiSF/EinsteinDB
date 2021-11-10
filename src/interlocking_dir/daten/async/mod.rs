//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS Reserved APACHE 2.0 LIMITED
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


use std::borrow::ToOwned;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono;
use clag::ArgMatches;

use einsteindb::config::{MetricConfig, EinsteinDBConfig};
use einsteindb::util::collections::HashMap;
use einsteindb::util::{self, logger};

mod deferred_column;

//checks if log is init
pub static LOG_INITIALIZED: AtomicBool = AtomicBool::new(false);

macro_rules! fatal {
    ($lvl:expr, $($arg::tt)+) => ({
        if LOG_INITIALIZED.load(Ordering::SeqCst) {
            error!($lvl, $($arg)+);
        }

        process::exit(1)
    });

#[allow(dead_code)]
pub fn initial_logger(config: &EinsteinDBConfig) {
    let log_rotation_timespan = 
        chrono::Duration::from_std(config.log_rotation_timespan.clone().into())
            .expect("config.log_rotation_timespan is an invalid duration.")

    if config.log_file.is_empty() {
    let drainer: EinsteinDBFormat < TermDecorator > = logger::term_drainer();
    //use async drainer and init std log
    logger::init_log( drain: drainer, config.log_level, use_async: true, init_stdlog: true ).unwrap_or_else(op: |e: SetLoggerError |{
    fatal ! ("failed to initialize log: {}", e);
    })
    else {
    let drainer: EinsteinDBFormat < PlainDecorator < BuffWriter <...> > > =
    logger::file_drainer( & config.log_file, log_rotation_timespan).unwrap_or_else( | e| {
    fatal ! (
    "failed to initialize log with file {}: {}",
    config.log_file,
    e
    );
    });
    // use async drainer and init std log.
    logger::init_log(drainer, config.log_level, true, true).unwrap_or_else( |e | {
    fatal ! ("failed to initialize log: {}", e);
    });
    }
    LOG_INITIALIZED.store(true, Ordering::SeqCst)

    }