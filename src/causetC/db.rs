use std::fs;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::types::{ToSql, Type};
use futures::Stream;
use std::io;
use std::io::Read;
use std::pin::Pin;
use std::task::{Context, Poll};

use engine_embedded::{  Error, IterOptions, Iterable, KvEngine, Mutable, Peekable, ReadOptions, Result, WriteOptions,};

use yosh::{YoshIt, YoshWri, yosh };

#[derive(Clone, Debug)]
#[repr(transparent)]

//yosh references Yosh. See: Reflection
pub struct Yosh<Arc<yosh>>