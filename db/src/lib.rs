//Copyright 2019 EinsteinDB Licensed Under Apache-2.0.

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate failure;
extern crate indexmap;
extern crate itertools;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;

#[cfg(feature = "syncable")]
#[macro_use] extern crate serde_derive;

extern crate petgraph;
extern crate rusqlite;
extern crate tabwriter;
extern crate time;

//! TLS support for `tokio-postgres` and `postgres` via `native-tls.
//!
//! # Examples
//!
//! ```no_run
//! use native_tls::{Certificate, TlsConnector};
//! use postgres_native_tls::MakeTlsConnector;
//! use std::fs;
//!
//! # fn main() -> Result<(), Box<std::error::Error>> {
//! let cert = fs::read("database_cert.pem")?;
//! let cert = Certificate::from_pem(&cert)?;
//! let connector = TlsConnector::builder()
//!     .add_root_certificate(cert)
//!     .build()?;
//! let connector = MakeTlsConnector::new(connector);
//!
//! let connect_future = tokio_postgres::connect(
//!     "host=localhost user=postgres sslmode=require",
//!     connector,
//! );
//!
//! // ...
//! # Ok(())
//! # }
//! ```
//!
//! ```no_run
//! use native_tls::{Certificate, TlsConnector};
//! use postgres_native_tls::MakeTlsConnector;
//! use std::fs;
//!
//! # fn main() -> Result<(), Box<std::error::Error>> {
//! let cert = fs::read("database_cert.pem")?;
//! let cert = Certificate::from_pem(&cert)?;
//! let connector = TlsConnector::builder()
//!     .add_root_certificate(cert)
//!     .build()?;
//! let connector = MakeTlsConnector::new(connector);
//!
//! let client = postgres::Client::connect(
//!     "host=localhost user=postgres sslmode=require",
//!     connector,
//! )?;
//! # Ok(())
//! # }
//! ```
#![warn(rust_2018_idioms, clippy::all, missing_docs)]

use std::future::Future;
use std::pin::Pin;
use tokio_io::{AsyncRead, AsyncWrite};
#[cfg(feature = "runtime")]
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::tls::{ChannelBinding, TlsConnect};
use tokio_tls::TlsStream;
