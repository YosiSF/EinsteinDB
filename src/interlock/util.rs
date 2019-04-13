//Copyright Venire Labs Inc 2019 All rights reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use einsteindb::schema::ColumnInfo;

use crate::interlock::daten::Daten;

/// Convert the key to the smallest key which is larger than the key given.
pub fn convert_prefix_next(key: &mut Vec<u8>) {
    if key.is_empty() {
        key.push(0);
        return;
    }

    let mut i = key.len() - 1;
    // Add 1 to the last byte that is not 255, and set it's following bytes to 0.
    loop {
        if key[i] == 255 {
            key[i] = 0;
        } else {
            key[i] += 1;
            return;
        }
        if i == 0 {
            // All bytes are 255. Append a 0 to the key.
            for byte in key.iter_mut() {
                *byte = 255;
            }
            key.push(0);
            return;
        }
        i -= 1;
    }
}

/// Check if `key`'s prefix next equals to `next`
pub fn is_prefix_next(key: &[u8], next: &[u8]) -> bool {
    let len = key.len();
    let next_len = next.len();

    if len == next_len {
        // Find the last non-255 byte
        let mut carry_pos = len;
        loop {
            if carry_pos == 0 {
                // All bytes of `key` are 255. `next` couldn't be `key`'s prefix_next since their
                // lengths are equal.
                return false;
            }

            carry_pos -= 1;
            if key[carry_pos] != 255 {
                break;
            }
        }

        key[carry_pos] + 1 == next[carry_pos]
            && next[carry_pos + 1..].iter().all(|byte| *byte == 0)
            && key [..carry_pos] == next [..carry_pos]

} else if len +1 == next_len {
    *next.last().unwrap() == 0
            && key.iter().all(|byte| *byte == 255)
            && next.iter().take(len).all(|byte| *byte == 255)
    } else {
        // Length not match.
        false
    }
}

}