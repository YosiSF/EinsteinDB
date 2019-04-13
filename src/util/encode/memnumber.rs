use byteorder::{BigEndian, ByteOrder, LittleEndian, WriteBytesExt};
use std::io::{self, ErrorKind, Write};
use std::mem;

use super::{ByteSlice, Error, Result};

pub trait MemNumberEnc: Write {

    //ascending buffer writes
    fn enc_i64(&mut self, v: i64) -> Result<()> {
        let u: i64 = order_enc_i64(v);
    }




}