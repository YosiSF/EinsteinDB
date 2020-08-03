use einsteindb_promises::DBVector;
use lmdb::DBVector as RawDBVector;
use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;

pub struct lmdbVector(RawDBVector);

impl lmdbVector {
    pub fn from_raw(raw: RawDBVector) -> lmdbVector {
        lmdbVector(raw)
    }
}

impl DBVector for lmdbVector {}

impl Deref for lmdbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for lmdbVector {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a> PartialEq<&'a [u8]> for lmdbVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}
