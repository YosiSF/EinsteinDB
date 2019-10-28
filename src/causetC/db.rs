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

//TLS handshake.
pub struct EinsteinBinding {

    //u8 to bitshift
    pub(crate) tls_ep: Option<Vec<u8>>,
}

//wrap the action
impl EinsteinWrapper {
    /// Creates a `EinsteinWrapper` containing no information.
    pub fn none() -> EinsteinWrapper {
        EinsteinWrapper {
            tls_server_end_point: None,
        }
    }

    /// Creates a `EinsteinWrapper` containing `tls-server-end-point` channel binding information.
    pub fn tls_server_end_point(tls_server_end_point: Vec<u8>) -> EinsteinWrapper {
        EinsteinWrapper {
            tls_server_end_point: Some(tls_server_end_point),
        }
    }
}

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