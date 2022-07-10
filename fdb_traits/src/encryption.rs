/// Copyright 2022 The EinsteinDB Authors. All rights reserved.
/// Use of this source code is governed by a MIT-style
/// license that can be found in the LICENSE file.
/// @authors   CavHack, Slushie, imcustodian, robertmc, luojunqing, dqxw, wangjiezhe
/// @license   MIT License
/// @brief     The file is used for encryption and decryption.
/// @version   0.1
/// @date      2020-03-01
/// @file      encryption.rs


use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::Path,
    str::FromStr,
};


use crate::*;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};


use std::sync::Arc;
use std::time::{Duration, SystemTime};


use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;




//fdb_opts
//             compression_strategy: String::from("default"),
//             compression_dict: Vec::new(),
//             enable_statistics: false,
//             statistics_interval: 0,
//             statistics_block_size: 0,
//             statistics_block_cache_size: 0,
//             statistics_block_cache_shard_bits: 0,
//              
//



/*
Encrypted storage. What are the tradeoffs to using SQLCipher instead of sqlite?
It's a trivial change that gives us pervasive encryption of all data, 
but we don't know the performance implications. 

If we ATTACH two databases to encrypt only some data (per-attribute encryption), 
do we over-complicate querying or handling the transaction log, and are we able to transact mixed collections of causets atomically?
*/



use std::mem;
use std::ptr::copy_nonoverlapping;

#[allow(non_camel_case_types)]
#[repr(simd)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct u64x2(pub u64, pub u64);

impl u64x2 {
    
    #[inline]
    pub(crate) fn new(a: u64, b: u64) -> Self {
        u64x2(a, b)
    }

    #[inline]
    pub(crate) fn lo(self) -> u64 {
        self.0
    }

    #[inline]
    pub(crate) fn hi(self) -> u64 {
        self.1
    }

    #[inline(always)]
    pub fn read(src: &[u8; 16]) -> Self {
        let mut tmp = mem::MaybeUninit::<Self>::uninit();
        unsafe {
            copy_nonoverlapping(src.as_ptr(), tmp.as_mut_ptr(), 16);
            let tmp = tmp.assume_init();
            tmp
        
        }


    }

     /// Write U64x2 content into array pointer (potentially unaligned)
     /// # Safety
     /// This is unsafe because it writes to an unaligned pointer.
     /// The caller must ensure that the pointer is valid.
     /// The function has no other safety requirements.
     /// # Examples
     /// ```
     /// use einstein_sqlite::U64x2;
     /// let mut dest = [0u8; 16];
     /// let src = U64x2::new(0x1122334455667788, 0x99AABBCCDDEEFF00);
     /// unsafe {
     ///    einstein_sqlite::U64x2::write(&mut dest, src);
     /// 
     ///    
#[inline(always)]
    pub unsafe fn write(dest: &mut [u8; 16], src: Self) {
        copy_nonoverlapping(src.as_ptr(), dest.as_mut_ptr(), 16);
    }

    #[inline(always)]
    pub fn write_to_slice(&self, dest: &mut [u8]) {
        unsafe {
            copy_nonoverlapping(self.as_ptr(), dest.as_mut_ptr(), 16);
        }
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *const u64 {
        self as *const Self as *const u64
    }

    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut u64 {
        self as *mut Self as *mut u64
    }

}



pub(crate) fn aesenc(block: &mut u64x2, rkey: &u64x2) {
    let mut tmp = u64x2::read(block);
    tmp.0 ^= rkey.0;

    unsafe {
        copy_nonoverlapping(tmp.as_ptr(), block.as_mut_ptr(), 16);
    }

    tmp.1 ^= rkey.1;
    unsafe {
        llvm_asm!("aesenc $0, $1"
            : "+x"(*block)
            : "x"(*rkey)
            :
            : "intel", "alignstack"

        );
        

    
    }
}

#[inline(always)]
pub(crate) fn aesenclast(block: &mut u64x2, rkey: &u64x2) {
    let mut tmp = u64x2::read(block);
    tmp.0 ^= rkey.0;

    unsafe {
        llvm_asm!("aesenclast $0, $1"
            : "+x"(*block)
            : "x"(*rkey)
            :
            : "intel", "alignstack"
        );
    }
}

macro_rules! aeskeygenassist {
    ($r:expr, $a:expr) => {
        unsafe {
            llvm_asm!("aeskeygenassist $0, $1"
                : "+x"(*$r)
                : "x"($a)
                :
                : "intel", "alignstack"
            );
        }
    };
}


#[inline(always)]
pub(crate) fn aeskeygenassist(r: &mut u64x2, a: u64) {
    aeskeygenassist!(r, a);
}





#[inline(always)]
pub(crate) fn aesroundlast(block: &mut u64x2, rkey: &u64x2) {
    unsafe {
        llvm_asm!("aesroundlast $0, $1"
            : "+x"(*block)
            : "x"(*rkey)
            :
            : "intel", "alignstack"
        );
    }
}


#[inline(always)]
pub(crate) fn aesimc(block: &mut u64x2) {
    let mut tmp = u64x2::read(block);
    tmp.0 ^= rkey.0;

        unsafe {
            llvm_asm!("aesenc $0, $1"
                : "+x"(*block)
                : "x"(*rkey)
                :
                : "intel", "alignstack"
            );
        }

    tmp.1 ^= rkey.1;
    unsafe {
        llvm_asm!("aesenc $0, $1"
            : "+x"(*block)
            : "x"(*rkey)
            :
            : "intel", "alignstack"
        );


    }


}


#[inline(always)]
pub(crate) fn aesimclast(block: &mut u64x2) {
    let mut dst = mem::MaybeUninit::<u64x2>::uninit();
        unsafe {
            llvm_asm!("aeskeygenassist $0, $1, $2"
                    : "+x"(*dst.as_mut_ptr())
                    : "x"(*$src), "i"($i)
                    :
                    : "intel", "alignstack"
                );
            dst.assume_init()
        }
    }

    #[inline(always)]
    pub(crate) fn aesimcround(block: &mut u64x2, rkey: &u64x2) {
        unsafe {
            llvm_asm!("aesround $0, $1"
                : "+x"(*block)
                : "x"(*rkey)
                :
                : "intel", "alignstack"
            );
        }

    }

    #[inline(always)]
    pub(crate) fn aesimcroundlast(block: &mut u64x2, rkey: &u64x2) {
        let mut tmp = u64x2::read(block);
        tmp.0 ^= rkey.0;
        unsafe {
            llvm_asm!("aesenclast $0, $1"
                : "+x"(*block)
                : "x"(*rkey)
                :
                : "intel", "alignstack"
            );
        }
    }


    #[inline(always)]
    pub(crate) fn aesround(block: &mut u64x2, rkey: &u64x2) {
        let mut tmp = u64x2::read(block);
        tmp.0 ^= rkey.0;
        unsafe {
            llvm_asm!("aesenc $0, $1"
                : "+x"(*block)
                : "x"(*rkey)
                :
                : "intel", "alignstack"
            );
            dst.assume_init()
        }
    }

