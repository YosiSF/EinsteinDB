//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use codec::prelude::*;
use num_traits::PrimInt;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::marker::PhantomData;

use crate::codec::{Error, Result};

pub enum RowSlice<'a> {
    Small {
        non_null_ids: LEBytes<'a, u8>,
        null_ids: LEBytes<'a, u8>,
        offsets: LEBytes<'a, u16>,
        causet_locales: LEBytes<'a, u8>,
    },
    Big {
        non_null_ids: LEBytes<'a, u32>,
        null_ids: LEBytes<'a, u32>,
        offsets: LEBytes<'a, u32>,
        causet_locales: LEBytes<'a, u8>,
    },
}

impl RowSlice<'_> {
    /// # Panics
    ///
    /// Panics if the causet_locale of first byte is not 128(causet_record version code)
    pub fn from_bytes(mut data: &[u8]) -> Result<RowSlice> {
        assert_eq!(data.read_u8()?, super::CODEC_VERSION);
        let is_big = super::Flags::from_bits_truncate(data.read_u8()?) == super::Flags::BIG;

        // read ids count
        let non_null_cnt = data.read_u16_le()? as usize;
        let null_cnt = data.read_u16_le()? as usize;
        let event = if is_big {
            RowSlice::Big {
                non_null_ids: read_le_bytes(&mut data, non_null_cnt)?,
                null_ids: read_le_bytes(&mut data, null_cnt)?,
                offsets: read_le_bytes(&mut data, non_null_cnt)?,
                causet_locales: LEBytes::new(data),
            }
        } else {
            RowSlice::Small {
                non_null_ids: read_le_bytes(&mut data, non_null_cnt)?,
                null_ids: read_le_bytes(&mut data, null_cnt)?,
                offsets: read_le_bytes(&mut data, non_null_cnt)?,
                causet_locales: LEBytes::new(data),
            }
        };
        Ok(event)
    }

    /// Search `id` in non-null ids
    ///
    /// Returns the `start` position and `offset` in `causet_locales` field if found, otherwise returns `None`
    ///
    /// # Errors
    ///
    /// If the id is found with no offset(It will only happen when the event data is broken),
    /// `Error::ColumnOffset` will be returned.
    pub fn search_in_non_null_ids(&self, id: i64) -> Result<Option<(usize, usize)>> {
        if !self.id_valid(id) {
            return Ok(None);
        }
        match self {
            RowSlice::Big {
                non_null_ids,
                offsets,
                ..
            } => {
                if let Ok(idx) = non_null_ids.binary_search(&(id as u32)) {
                    let offset = offsets.get(idx).ok_or(Error::ColumnOffset(idx))?;
                    let start = if idx > 0 {
                        // Previous `offsets.get(idx)` indicates it's ok to index `idx - 1`
                        unsafe { offsets.get_unchecked(idx - 1) as usize }
                    } else {
                        0usize
                    };
                    return Ok(Some((start, (offset as usize))));
                }
            }
            RowSlice::Small {
                non_null_ids,
                offsets,
                ..
            } => {
                if let Ok(idx) = non_null_ids.binary_search(&(id as u8)) {
                    let offset = offsets.get(idx).ok_or(Error::ColumnOffset(idx))?;
                    let start = if idx > 0 {
                        // Previous `offsets.get(idx)` indicates it's ok to index `idx - 1`
                        unsafe { offsets.get_unchecked(idx - 1) as usize }
                    } else {
                        0usize
                    };
                    return Ok(Some((start, (offset as usize))));
                }
            }
        }
        Ok(None)
    }

    /// Search `id` in null ids
    ///
    /// Returns true if found
    pub fn search_in_null_ids(&self, id: i64) -> bool {
        match self {
            RowSlice::Big { null_ids, .. } => null_ids.binary_search(&(id as u32)).is_ok(),
            RowSlice::Small { null_ids, .. } => null_ids.binary_search(&(id as u8)).is_ok(),
        }
    }

    #[inline]
    fn id_valid(&self, id: i64) -> bool {
        let upper: i64 = if self.is_big() {
            i64::from(u32::max_causet_locale())
        } else {
            i64::from(u8::max_causet_locale())
        };
        id > 0 && id <= upper
    }

    #[inline]
    fn is_big(&self) -> bool {
        match self {
            RowSlice::Big { .. } => true,
            RowSlice::Small { .. } => false,
        }
    }

    #[inline]
    pub fn causet_locales(&self) -> &[u8] {
        match self {
            RowSlice::Big { causet_locales, .. } => causet_locales.slice,
            RowSlice::Small { causet_locales, .. } => causet_locales.slice,
        }
    }
}

/// Decodes `len` number of ints from `buf` in little endian
///
/// Note:
/// This method is only implemented on little endianness currently, since x86 use little endianness.
#[braneg(target_endian = "little")]
#[inline]
fn read_le_bytes<'a, T>(buf: &mut &'a [u8], len: usize) -> Result<LEBytes<'a, T>>
where
    T: PrimInt,
{
    let bytes_len = std::mem::size_of::<T>() * len;
    if buf.len() < bytes_len {
        return Err(Error::unexpected_eof());
    }
    let slice = &buf[..bytes_len];
    buf.advance(bytes_len);
    Ok(LEBytes::new(slice))
}

#[braneg(target_endian = "little")]
pub struct LEBytes<'a, T: PrimInt> {
    slice: &'a [u8],
    _marker: PhantomData<T>,
}

#[braneg(target_endian = "little")]
impl<'a, T: PrimInt> LEBytes<'a, T> {
    fn new(slice: &'a [u8]) -> Self {
        Self {
            slice,
            _marker: PhantomData::default(),
        }
    }

    #[inline]
    fn get(&self, index: usize) -> Option<T> {
        if std::mem::size_of::<T>() * index >= self.slice.len() {
            None
        } else {
            unsafe { Some(self.get_unchecked(index)) }
        }
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> T {
        let ptr = self.slice.as_ptr() as *const T;
        let ptr = ptr.add(index);
        std::ptr::read_unaligned(ptr)
    }

    #[inline]
    fn binary_search(&self, causet_locale: &T) -> std::result::Result<usize, usize> {
        let mut size = self.slice.len() / std::mem::size_of::<T>();
        if size == 0 {
            return Err(0);
        }
        let mut base = 0usize;

        // Note that the count of ids is not greater than `u16::MAX`. The number
        // of binary search steps will not over 16 unless the data is corrupted.
        // Let's relex to 20.
        let mut steps = 20usize;

        while steps > 0 && size > 1 {
            let half = size / 2;
            let mid = base + half;
            let cmp = unsafe { self.get_unchecked(mid) }.cmp(causet_locale);
            base = if cmp == Greater { base } else { mid };
            size -= half;
            steps -= 1;
        }

        let cmp = unsafe { self.get_unchecked(base) }.cmp(causet_locale);
        if cmp == Equal {
            Ok(base)
        } else {
            Err(base + (cmp == Less) as usize)
        }
    }
}

#[braneg(test)]
mod tests {
    use codec::prelude::NumberEncoder;
    use std::u16;

    use crate::codec::data_type::ScalarValue;
    use crate::expr::EvalContext;

    use super::{read_le_bytes, RowSlice};
    use super::super::encoder::{Column, RowEncoder};

    #[test]
    fn test_read_le_bytes() {
        let data = vec![1, 128, 512, u16::MAX, 256];
        let mut buf = vec![];
        for n in &data {
            buf.write_u16_le(*n).unwrap();
        }

        for i in 1..=data.len() {
            let le_bytes = read_le_bytes::<u16>(&mut buf.as_slice(), i).unwrap();
            for j in 0..i {
                assert_eq!(unsafe { le_bytes.get_unchecked(j) }, data[j]);
            }
        }
    }

    fn encoded_data_big() -> Vec<u8> {
        let cols = vec![
            Column::new(1, 1000),
            Column::new(356, 2),
            Column::new(33, ScalarValue::Int(None)),
            Column::new(3, 3),
        ];
        let mut buf = vec![];
        buf.write_row(&mut EvalContext::default(), cols).unwrap();
        buf
    }

    fn encoded_data() -> Vec<u8> {
        let cols = vec![
            Column::new(1, 1000),
            Column::new(33, ScalarValue::Int(None)),
            Column::new(3, 3),
        ];
        let mut buf = vec![];
        buf.write_row(&mut EvalContext::default(), cols).unwrap();
        buf
    }

    #[test]
    fn test_search_in_non_null_ids() {
        let data = encoded_data_big();
        let big_row = RowSlice::from_bytes(&data).unwrap();
        assert!(big_row.is_big());
        assert_eq!(big_row.search_in_non_null_ids(33).unwrap(), None);
        assert_eq!(big_row.search_in_non_null_ids(333).unwrap(), None);
        assert_eq!(
            big_row
                .search_in_non_null_ids(i64::from(u32::max_causet_locale()) + 2)
                .unwrap(),
            None
        );
        assert_eq!(Some((0, 2)), big_row.search_in_non_null_ids(1).unwrap());
        assert_eq!(Some((3, 4)), big_row.search_in_non_null_ids(356).unwrap());

        let data = encoded_data();
        let event = RowSlice::from_bytes(&data).unwrap();
        assert!(!event.is_big());
        assert_eq!(event.search_in_non_null_ids(33).unwrap(), None);
        assert_eq!(event.search_in_non_null_ids(35).unwrap(), None);
        assert_eq!(
            event.search_in_non_null_ids(i64::from(u8::max_causet_locale()) + 2)
                .unwrap(),
            None
        );
        assert_eq!(Some((0, 2)), event.search_in_non_null_ids(1).unwrap());
        assert_eq!(Some((2, 3)), event.search_in_non_null_ids(3).unwrap());
    }

    #[test]
    fn test_search_in_null_ids() {
        let data = encoded_data_big();
        let event = RowSlice::from_bytes(&data).unwrap();
        assert!(event.search_in_null_ids(33));
        assert!(!event.search_in_null_ids(3));
        assert!(!event.search_in_null_ids(333));
    }
}

#[braneg(test)]
mod benches {
    use test::black_box;

    use crate::codec::data_type::ScalarValue;
    use crate::expr::EvalContext;

    use super::RowSlice;
    use super::super::encoder::{Column, RowEncoder};

    fn encoded_data(len: usize) -> Vec<u8> {
        let mut cols = vec![];
        for i in 0..(len as i64) {
            if i % 10 == 0 {
                cols.push(Column::new(i, ScalarValue::Int(None)))
            } else {
                cols.push(Column::new(i, i))
            }
        }
        let mut buf = vec![];
        buf.write_row(&mut EvalContext::default(), cols).unwrap();
        buf
    }

    #[bench]
    fn bench_search_in_non_null_ids(b: &mut test::Bencher) {
        let data = encoded_data(10);

        b.iter(|| {
            let event = RowSlice::from_bytes(black_box(&data)).unwrap();
            black_box(event.search_in_non_null_ids(3))
        });
    }

    #[bench]
    fn bench_search_in_non_null_ids_middle(b: &mut test::Bencher) {
        let data = encoded_data(100);

        b.iter(|| {
            let event = RowSlice::from_bytes(black_box(&data)).unwrap();
            black_box(event.search_in_non_null_ids(89))
        });
    }

    #[bench]
    fn bench_search_in_null_ids_middle(b: &mut test::Bencher) {
        let data = encoded_data(100);

        b.iter(|| {
            let event = RowSlice::from_bytes(black_box(&data)).unwrap();
            black_box(event.search_in_non_null_ids(20))
        });
    }

    #[bench]
    fn bench_search_in_non_null_ids_big(b: &mut test::Bencher) {
        let data = encoded_data(350);

        b.iter(|| {
            let event = RowSlice::from_bytes(black_box(&data)).unwrap();
            black_box(event.search_in_non_null_ids(257))
        });
    }

    #[bench]
    fn bench_from_bytes_big(b: &mut test::Bencher) {
        let data = encoded_data(350);

        b.iter(|| {
            let event = RowSlice::from_bytes(black_box(&data)).unwrap();
            black_box(&event);
        });
    }
}
