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


impl<I1, I2> Iterator for ZipTuple<I1, I2> where I1: Iterator, I2: Iterator {
    type Item = (I1::Item, I2::Item);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter1.next().and_then(|x| {
            self.iter2.next().map(|y| (x, y))
        })
    }
}


impl<I1, I2> IntoIterator for ZipTuple<I1, I2> where I1: IntoIterator, I2: IntoIterator {
    type Item = (I1::Item, I2::Item);
    type IntoIter = ZipTuple<I1::IntoIter, I2::IntoIter>;
    fn into_iter(self) -> Self::IntoIter {
        ZipTuple {
            iter1: self.iter1.into_iter(),
            iter2: self.iter2.into_iter(),
        }
    }
}