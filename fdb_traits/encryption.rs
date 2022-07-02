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
Encrypted storage. What are the tradeoffs to using SQLCipher instead of SQLite? 
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

     /// Write u64x2 content into array pointer (potentially unaligned)
     /// # Safety
     /// This is unsafe because it writes to an unaligned pointer.
     /// The caller must ensure that the pointer is valid.
     /// The function has no other safety requirements.
     /// # Examples
     /// ```
     /// use einstein_sqlite::u64x2;
     /// let mut dest = [0u8; 16];
     /// let src = u64x2::new(0x1122334455667788, 0x99AABBCCDDEEFF00);
     /// unsafe {
     ///    einstein_sqlite::u64x2::write(&mut dest, src);
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



#inline[(always)]
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
pub(crate) fn aesround(block: &mut u64x2, rkey: &u64x2) {
    let mut tmp = u64x2::read(block);
    tmp.0 ^= rkey.0;
    ($src:ident, $i:expr) => {{
        unsafe {
            llvm_asm!("aesenc $0, $1"
                : "+x"(*block)
                : "x"(*rkey)
                :
                : "intel", "alignstack"
            );
        }
    }};
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
    pub(crate) fn aesroundlast(block: &mut u64x2, rkey: &u64x2) {
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
        }}
    


    #[inline(always)]
    pub(crate) fn aeskeygenassist_0x00(src: &u64x2) -> u64x2 {
        aeskeygenassist!(src, 0x00)
    }
    #[inline(always)]
    pub(crate) fn aeskeygenassist_0x01(src: &u64x2) -> u64x2 {
        aeskeygenassist!(src, 0x01)
    }
    #[inline(always)]
    pub(crate) fn aeskeygenassist_0x02(src: &u64x2) -> u64x2 {
        aeskeygenassist!(src, 0x02)
    }
    #[inline(always)]
    pub(crate) fn aeskeygenassist_0x04(src: &u64x2) -> u64x2 {
        aeskeygenassist!(src, 0x04)
    }
    #[inline(always)]
    pub(crate) fn aeskeygenassist_0x08(src: &u64x2) -> u64x2 {
        aeskeygenassist!(src, 0x08)
    }
    #[inline(always)]
    pub(crate) fn aeskeygenassist_0x10(src: &u64x2) -> u64x2 {
        aeskeygenassist!(src, 0x10)
    }
    #[inline(always)]
    pub(crate) fn aeskeygenassist_0x20(src: &u64x2) -> u64x2 {
        aeskeygenassist!(src, 0x20)
    }
    #[inline(always)]
    pub(crate) fn aeskeygenassist_0x40(src: &u64x2) -> u64x2 {
        aeskeygenassist!(src, 0x40)
    }
    
    #[inline(always)]
    pub(crate) fn pxor(dst: &mut u64x2, src: &u64x2) {
        unsafe {
            llvm_asm!("pxor $0, $1"
                : "+x"(*dst)
                : "x"(*src)
                :
                : "intel", "alignstack"
            );
        }
    }
    
    macro_rules! pslldq {
        ($dst:ident, $i:expr) => {{
            unsafe {
                llvm_asm!("pslldq $0, $1"
                        : "+x"(*$dst)
                        : "i"($i)
                        :
                        : "intel", "alignstack"
                    );
            }
        }}
    }

    #[inline(always)]
    pub(crate) fn pslldq(dst: &mut u64x2, i: u32) {
        pslldq!(dst, i);
    }

    #[inline(always)]
    pub(crate) fn psrldq(dst: &mut u64x2, i: u32) {
        unsafe {
            llvm_asm!("psrldq $0, $1"
                    : "+x"(*dst)
                    : "i"(i)
                    :
                    : "intel", "alignstack"
                );
        }
    }


#[inline(always)]
pub(crate) fn pxor(dst: &mut u64x2, src: &u64x2) {
    unsafe {
        llvm_asm!("pxor $0, $1"
            : "+x"(*dst)
            : "x"(*src)
            :
            : "intel", "alignstack"
        );
    }
}

macro_rules! pslldq {
    ($dst:ident, $i:expr) => {{
        unsafe {
            llvm_asm!("pslldq $0, $1"
                    : "+x"(*$dst)
                    : "i"($i)
                    :
                    : "intel", "alignstack"
                );
        }
    }}
}

#[inline(always)]
pub(crate) fn pslldq_0x04(dst: &mut u64x2) {
    pslldq!(dst, 0x04)
}

macro_rules! pshufd {
    ($src:ident, $i:expr) => {{
        let mut dst = mem::MaybeUninit::<u64x2>::uninit();
        unsafe {
            llvm_asm!("pshufd $0, $1, $2"
                    : "+x"(*dst.as_mut_ptr())
                    : "x"(*$src), "i"($i)
                    :
                    : "intel", "alignstack"
                );
            dst.assume_init()
        }
    }}
}
#[inline(always)]
pub(crate) fn pxor(dst: &mut u64x2, src: &u64x2) {
    unsafe {
        llvm_asm!("pxor $0, $1"
            : "+x"(*dst)
            : "x"(*src)
            :
            : "intel", "alignstack"
        );
    }
}

macro_rules! pslldq {
    ($dst:ident, $i:expr) => {{
        unsafe {
            llvm_asm!("pslldq $0, $1"
                    : "+x"(*$dst)
                    : "i"($i)
                    :
                    : "intel", "alignstack"
                );
        }
    }}
}

#[inline(always)]
pub(crate) fn pslldq_0x04(dst: &mut u64x2) {
    pslldq!(dst, 0x04)
}

macro_rules! pshufd {
    ($src:ident, $i:expr) => {{
        let mut dst = mem::MaybeUninit::<u64x2>::uninit();
        unsafe {
            llvm_asm!("pshufd $0, $1, $2"
                    : "+x"(*dst.as_mut_ptr())
                    : "x"(*$src), "i"($i)
                    :
                    : "intel", "alignstack"
                );
            dst.assume_init()
        }
    }}
}


#[inline(always)]
fn assist256_1(a: &mut u64x2, mut b: u64x2) {
    b = intrinsics::pshufd_0xff(&b);
    let mut y: u64x2 = *a;
    intrinsics::pslldq_0x04(&mut y);
    intrinsics::pxor(a, &y);
    intrinsics::pslldq_0x04(&mut y);
    intrinsics::pxor(a, &y);
    intrinsics::pslldq_0x04(&mut y);
    intrinsics::pxor(a, &y);
    intrinsics::pxor(a, &b);
}

#[inline(always)]
fn assist256_2(mut a: u64x2, b: &mut u64x2) {
    a = intrinsics::pshufd_0xaa(&a);
    let mut y: u64x2 = *b;
    intrinsics::pslldq_0x04(&mut y);
    intrinsics::pxor(b, &y);
    intrinsics::pslldq_0x04(&mut y);
    intrinsics::pxor(b, &y);
    intrinsics::pslldq_0x04(&mut y);
    intrinsics::pxor(b, &y);
    intrinsics::pxor(b, &a);
}

#[inline(always)]
fn expand256(key: &[u8; 32], rkeys: &mut [u64x2; 15]) {
    let mut key0_xmm = u64x2::read(array_ref![key, 0, 16]);
    let mut key1_xmm = u64x2::read(array_ref![key, 16, 16]);

    // 0
    rkeys[0] = key0_xmm;
    rkeys[1] = key1_xmm;

    // 2
    assist256_1(&mut key0_xmm, intrinsics::aeskeygenassist_0x01(&key1_xmm));
    assist256_2(intrinsics::aeskeygenassist_0x00(&key0_xmm), &mut key1_xmm);
    rkeys[2] = key0_xmm;
    rkeys[3] = key1_xmm;

    // 4
    assist256_1(&mut key0_xmm, intrinsics::aeskeygenassist_0x02(&key1_xmm));
    assist256_2(intrinsics::aeskeygenassist_0x00(&key0_xmm), &mut key1_xmm);
    rkeys[4] = key0_xmm;
    rkeys[5] = key1_xmm;

    // 6
    assist256_1(&mut key0_xmm, intrinsics::aeskeygenassist_0x04(&key1_xmm));
    assist256_2(intrinsics::aeskeygenassist_0x00(&key0_xmm), &mut key1_xmm);
    rkeys[6] = key0_xmm;
    rkeys[7] = key1_xmm;

    // 8
    assist256_1(&mut key0_xmm, intrinsics::aeskeygenassist_0x08(&key1_xmm));
    assist256_2(intrinsics::aeskeygenassist_0x00(&key0_xmm), &mut key1_xmm);
    rkeys[8] = key0_xmm;
    rkeys[9] = key1_xmm;

    // 10
    assist256_1(&mut key0_xmm, intrinsics::aeskeygenassist_0x10(&key1_xmm));
    assist256_2(intrinsics::aeskeygenassist_0x00(&key0_xmm), &mut key1_xmm);
    rkeys[10] = key0_xmm;
    rkeys[11] = key1_xmm;

    // 12
    assist256_1(&mut key0_xmm, intrinsics::aeskeygenassist_0x20(&key1_xmm));
    assist256_2(intrinsics::aeskeygenassist_0x00(&key0_xmm), &mut key1_xmm);
    rkeys[12] = key0_xmm;
    rkeys[13] = key1_xmm;

    // 14
    assist256_1(&mut key0_xmm, intrinsics::aeskeygenassist_0x40(&key1_xmm));
    rkeys[14] = key0_xmm;
}

pub fn expand256_slice(key: &[u8; 32], rkeys: &mut [[u8; 16]; 15]) {
    let mut rkeys_xmm = [u64x2(0, 0); 15];
    expand256(key, &mut rkeys_xmm);
    for i in 0..15 {
        rkeys_xmm[i].write(&mut rkeys[i])
    }
}

fn aes256_rkeys_xmm(dst: &mut [u8; 16], src: &[u8; 16], rkeys: &[u64x2; 15]) {
    let mut state_xmm = u64x2::read(src);

    intrinsics::pxor(&mut state_xmm, &rkeys[0]);
    for i in 1..14 {
        intrinsics::aesenc(&mut state_xmm, &rkeys[i]);
    }
    intrinsics::aesenclast(&mut state_xmm, &rkeys[14]);

    state_xmm.write(dst);
}

pub fn aes256_rkeys_slice(dst: &mut [u8; 16], src: &[u8; 16], rkeys: &[[u8; 16]; 15]) {
    let mut rkeys_xmm = [u64x2(0, 0); 15];
    for i in 0..15 {
        rkeys_xmm[i] = u64x2::read(&rkeys[i]);
    }

    aes256_rkeys_xmm(dst, src, &rkeys_xmm);
}

#[cfg(test)]
pub fn aes256_ret(src: &[u8; 16], key: &[u8; 32]) -> [u8; 16] {
    let mut rkeys = [u64x2(0, 0); 15];
    expand256(key, &mut rkeys);

    let mut dst = [0u8; 16];
    aes256_rkeys_xmm(&mut dst, src, &rkeys);
    dst
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::aes::aes256_ret;
    use crate::aes::aes256_rkeys_slice;
    use crate::aes::aes256_rkeys_xmm;
    use crate::aes::expand256_slice;
    use crate::aes::expand256;
    use crate::aes::assist256_1;
    use crate::aes::assist256_2;
    use crate::aes::aes256_enc_slice;
    use crate::aes::aes256_enc_xmm;
    use crate::aes::aes256_dec_slice;
    use crate::aes::aes256_dec_xmm;
    use crate::aes::aes256_enc_slice_xmm;
    use crate::aes::aes256_enc_slice_slice;
    use crate::aes::aes256_dec_slice_xmm;
    use crate::aes::aes256_dec_slice_slice;
    use crate::aes::aes256_enc_slice_xmm_xmm;
    use crate::aes::aes256_enc_slice_slice_xmm;
    use crate::aes::aes256_dec_slice_xmm_xmm;
    use crate::aes::aes256_dec_slice_slice_xmm;
    

    #[test] 
    fn aes256_enc_slice_xmm() {
        let src = [0u8; 16];
        let key = [0u8; 32];
        let dst = aes256_enc_slice_xmm(&src, &key);
        assert_eq!(dst, aes256_ret(&src, &key));
    }

    #[test]


    //CHANGELOG: added test for aes256_enc_slice_slice_xmm
    

    



