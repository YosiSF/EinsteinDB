/// ```rust -e 's/^\s*//' | sed 's/^/    /'
//
//


use std::iter::Iterator;
use std::iter::FromIterator;
use std::iter::IntoIterator;
use std::iter::Map;
use std::iter::MapInto;
use std::iter::Zip;
use std::iter::ZipLongest;


pub struct ZipTuple<I1, I2> {
    iter1: I1,
    iter2: I2,
}