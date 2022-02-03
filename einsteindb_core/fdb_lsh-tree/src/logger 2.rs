use einsteindb_util::{crit, debug, error, info, warn};
// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.
use foundationdb::{DBInfoLogLevel as InfoLogLevel, Logger};

// TODO(yiwu): abstract the Logger interface.
#[derive(Default)]
pub struct FdbdbLogger;

impl Logger for FdbdbLogger {
    fn logv(&self, log_l_naught: InfoLogLevel, log: &str) {
        match log_l_naught {
            InfoLogLevel::Header => info!(#"rocksdb_log_header", "{}", log),
            InfoLogLevel::Debug => debug!(#"rocksdb_log", "{}", log),
            InfoLogLevel::Info => info!(#"rocksdb_log", "{}", log),
            InfoLogLevel::Warn => warn!(#"rocksdb_log", "{}", log),
            InfoLogLevel::Error => error!(#"rocksdb_log", "{}", log),
            InfoLogLevel::Fatal => crit!(#"rocksdb_log", "{}", log),
            _ => {}
        }
    }
}

#[derive(Default)]
pub struct VioletaBFTDBLogger;

impl Logger for VioletaBFTDBLogger {
    fn logv(&self, log_l_naught: InfoLogLevel, log: &str) {
        match log_l_naught {
            InfoLogLevel::Header => info!(#"violetabftdb_log_header", "{}", log),
            InfoLogLevel::Debug => debug!(#"violetabftdb_log", "{}", log),
            InfoLogLevel::Info => info!(#"violetabftdb_log", "{}", log),
            InfoLogLevel::Warn => warn!(#"violetabftdb_log", "{}", log),
            InfoLogLevel::Error => error!(#"violetabftdb_log", "{}", log),
            InfoLogLevel::Fatal => crit!(#"violetabftdb_log", "{}", log),
            _ => {}
        }
    }
}
