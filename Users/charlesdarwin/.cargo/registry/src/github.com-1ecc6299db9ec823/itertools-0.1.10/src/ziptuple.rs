//ziptuple


use std::iter::Iterator;
use std::iter::FromIterator;
use std::iter::IntoIterator;
use std::iter::Map;
use std::iter::MapInto;
use std::iter::Zip;
use std::iter::ZipLongest;
use std::iter::ZipLongest as ZipLongestTuple;
use std::iter::ZipLongest as ZipLongestTupleMut;


pub struct ZipTuple<I1, I2> {
    iter1: I1,
    iter2: I2,
}





