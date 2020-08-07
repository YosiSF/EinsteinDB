// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

#[macro_export]
macro_rules! other_err {
    ($msg:tt) => ({
        allegroeinstein-prolog-causet-sql::error::Error::from(allegroeinstein-prolog-causet-sql::error::EvaluateError::Other(
            format!(concat!("[{}:{}]: ", $msg), file!(), line!())
        ))
    });
    ($f:tt, $($arg:expr),+) => ({
        allegroeinstein-prolog-causet-sql::error::Error::from(allegroeinstein-prolog-causet-sql::error::EvaluateError::Other(
            format!(concat!("[{}:{}]: ", $f), file!(), line!(), $($arg),+)
        ))
    });
}
