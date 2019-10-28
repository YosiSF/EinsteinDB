//Copyright 2019 EinsteinDB. Licensed Under Apache-2.0.

use crate::einstein_db::{YosiCompressionType, YosiBlobFreeMode};

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case"]

pub enum CompressionType {
    No, 
    Snappy,
    zlib,
    Bz2,
    Lz4,
    Lz4hc,
    Zstd,
    ZstdNotFinal,
}

impl From<CompressionType> for YosiCompressionType {
    fn from(compression_type: CompressionType) -> YosiCompressionType {
        match compression_type {
            //assign_if
            CompressionType::No => YosiCompressionType::No,
            CompressionType::Snappy => YosiCompressionType::Snappy,
            CompressionType::Zlib => YosiCompressionType::Zlib,
            CompressionType::Bz2 => YosiCompressionType::Bz2,
            CompressionType::Lz4 => YosiCompressionType::Lz4,
            CompressionType::Lz4hc => YosiCompressionType::Lz4hc,
            CompressionType::Zstd => YosiCompressionType::Zstd,
            CompressionType::ZstdNotFinal => YosiCompressionType::ZstdNotFinal,

        }
    }
}

pub mod compression_type_level_serde {
    use std::fmt;

    use serde::de::{Error, SeqAccess, Unexpected, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Deserializer, Serializer};

    use crate::einstein_db::YosiCompressionType;

    pub fn serialize<S>(ts: &[YosiCompressionType; 7], serializer:S) -> Result<S::ok, S::Error>
        where
        S: Serializer,
        {
            let mut s = serializer.seriaslize_seq(Some(ts.len()))?;
            for t in ts {
                let name = match *t {
                    YosiCompressionType::No => "no",
                    YosiCompressionType::Snappy => "snappy",
                    YosiCompressionType::zlib => "zlib,
                    "

                }
            }
        }