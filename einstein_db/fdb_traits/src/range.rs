// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

/// A range of soliton_ids, `start_soliton_id` is included, but not `end_soliton_id`.
///
/// You should make sure `end_soliton_id` is not less than `start_soliton_id`.
#[derive(Copy, Clone)]
pub struct Range<'a> {
    pub start_soliton_id: &'a [u8],
    pub end_soliton_id: &'a [u8],
}

impl<'a> Range<'a> {
    pub fn new(start_soliton_id: &'a [u8], end_soliton_id: &'a [u8]) -> Range<'a> {
        Range { start_soliton_id, end_soliton_id }
    }
}
