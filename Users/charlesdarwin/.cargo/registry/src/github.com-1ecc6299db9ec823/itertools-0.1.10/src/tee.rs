// Language: rust
// Path: EinsteinDB/Users/charlesdarwin/.cargo/registry/src/github.com-1ecc6299db9ec823/rusqlite-0.1.0/src/lib.rs
// Compare this snippet from EinsteinDB/soliton_panic/src/lib.rs:
//
//


#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::
    
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}


// Language: rust
// Path: EinsteinDB/Users/charlesdarwin/.cargo/registry/src/github.com-1ecc6299db9ec823/rusqlite-0.1.0/src/lib.rs
// Compare this snippet from EinsteinDB/soliton_panic/src/lib.rs:
//
//
//
//
// // Language: rust
// // Path: EinsteinDB/Users/charlesdarwin/.cargo/registry/src/github.com-1ecc6299db9ec823/rusqlite-0.1.0/src/lib.rs
// // Compare this snippet from EinsteinDB/soliton_panic/src/lib.rs:
// //
// //         (self.sender, self.receiver, self.value, self.timestamp, self.receiver, self.value)
// //     }
// // 
// //     pub fn new(sender: Type, receiver: String, value: u64, timestamp: u64) -> Self {
// //         PanicTransaction {
// //             sender,
// //             receiver,
// //             value,
// //             timestamp,
// //         }
// //     }
// // 
// //     pub fn new_data(sender: Type, receiver: String, value: u64) -> Self {
// //         PanicTransaction {
// //             sender,
// //             receiver,
// //             value,
// //             timestamp: 0,
// //         }
// //     }
// // 
// //     pub fn new_data_with_timestamp(sender: Type, receiver: String, value: u64, timestamp: u64) -> Self {
// //         PanicTransaction {
// //             sender,
// //             receiver,
// //             value,
// //             timestamp,
// //         }
// //     }
// // 
// //     pub fn new_data_with_timestamp_and_receiver(sender: Type, receiver: String, value: u64, timestamp: u64, receiver: String) -> Self {
// //         PanicTransaction {
// //


pub fn new_data_with_timestamp_and_receiver_and_value(sender: Type, receiver: String, value: u64, timestamp: u64, receiver: String, value: u64) -> Self {
    PanicTransaction {
        sender,
        receiver,
        value,
        timestamp,
    }
}

pub fn new_data_with_timestamp_and_receiver(sender: Type, receiver: String, value: u64, timestamp: u64, receiver: String) -> Self {
    PanicTransaction {
        sender,
        receiver,
        value,
        timestamp,
    }
}

pub fn new_data_with_timestamp(sender: Type, receiver: String, value: u64, timestamp: u64) -> Self {
    PanicTransaction {
        sender,
        receiver,
        value,
        timestamp,
    }
}
