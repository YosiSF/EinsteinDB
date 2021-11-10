use einsteindb_promises::DBVector;
use foundationdb::DBVector as RawDBVector;
use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;

pub struct foundationdbVector(RawDBVector);

impl foundationdbVector {
    pub fn from_raw(raw: RawDBVector) -> foundationdbVector {
        foundationdbVector(raw)
    }
}

impl DBVector for foundationdbVector {}

impl Deref for foundationdbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for foundationdbVector {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a> PartialEq<&'a [u8]> for foundationdbVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}
