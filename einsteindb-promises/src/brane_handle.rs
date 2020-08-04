// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::brane_options::BlackBraneOptions;
use crate::errors::Result;

pub trait BRANEHandleExt {

    type BRANEhandle: BRANEhandle;
    type BlackBraneOptions: BlackBraneOptions;

        fn brane_handle(&self, name: &str) -> Result<&Self::BRANEHandle>;
        fn get_options_brane(&self, brane: &Self::BRANEHandle) -> Self::BlackBraneOptions;
        fn set_options_brane(&self, brane: &Self::BRANEHandle, options: &[(&str, &str)]) -> Result<()>;

}

pub trait BRANEHandle {}