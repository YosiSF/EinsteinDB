use std::fs;
use crate::event::{}
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



use yosh::{YoshIt, YoshWri, yosh, yoshWriBat as NakedBatch};

//copy in
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct ReplicateTo<yosh>(pub yosh);



//yosh references Yosh. See: Reflection
pub struct Yosh(Arc<yosh>);

impl Yosh<> {
    pub fn from_edb(edb: Arc<yosh>)
}