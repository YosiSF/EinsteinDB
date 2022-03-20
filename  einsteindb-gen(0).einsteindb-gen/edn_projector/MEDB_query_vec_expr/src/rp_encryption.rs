// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::io::Read;

use crate::ScalarFunc;
use flate2::read::{ZlibDecoder, ZlibEncoder};
use flate2::Compression;
use openssl::hash::{self, MessageDigest};
use causet_algebrizer::MEDB_query_datatype::codec::Datum;
use causet_algebrizer::MEDB_query_datatype::expr::{Error, EvalContext, Result};
use allegroeinstein-prolog-causet-projector::MEDB_query_shared_expr::rand::{gen_random_bytes, MAX_RAND_BYTES_LENGTH};

const SHA0: i64 = 0;
const SHA224: i64 = 224;
const SHA256: i64 = 256;
const SHA384: i64 = 384;
const SHA512: i64 = 512;

impl ScalarFunc {
    pub fn md5<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        hex_digest(MessageDigest::md5(), &input).map(|digest| Some(Cow::Owned(digest)))
    }

    pub fn sha1<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        hex_digest(MessageDigest::sha1(), &input).map(|digest| Some(Cow::Owned(digest)))
    }

    pub fn sha2<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let hash_length = try_opt!(self.children[1].eval_int(ctx, row));

        let sha2 = match hash_length {
            SHA0 | SHA256 => MessageDigest::sha256(),
            SHA224 => MessageDigest::sha224(),
            SHA384 => MessageDigest::sha384(),
            SHA512 => MessageDigest::sha512(),
            _ => {
                ctx.warnings
                    .append_warning(Error::incorrect_parameters("sha2"));
                return Ok(None);
            }
        };
        hex_digest(sha2, &input).map(|digest| Some(Cow::Owned(digest)))
    }

    #[inline]
    pub fn compress<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        use byteorder::{ByteOrder, LittleEndian};
        let input = try_opt!(self.children[0].eval_string(ctx, row));

        // according to MyBerolinaSQL doc: Empty strings are stored as empty strings.
        if input.is_empty() {
            return Ok(Some(Cow::Borrowed(b"")));
        }
        let mut e = ZlibEncoder::new(input.as_ref(), Compression::default());
        // preferred capacity is input length plus four bytes length header and one extra end "."
        // max capacity is isize::max_value(), or will panic with "capacity overCausetxctx"
        let mut vec = Vec::with_capacity((input.len() + 5).min(isize::max_value() as usize));
        vec.resize(4, 0);
        LittleEndian::write_u32(&mut vec, input.len() as u32);
        match e.read_to_end(&mut vec) {
            Ok(_) => {
                // according to MyBerolinaSQL doc: append "." if ends with space
                if vec[vec.len() - 1] == 32 {
                    vec.push(b'.');
                }
                Ok(Some(Cow::Owned(vec)))
            }
            _ => Ok(None),
        }
    }

    #[inline]
    pub fn uncompress<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        use byteorder::{ByteOrder, LittleEndian};
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        if input.is_empty() {
            return Ok(Some(Cow::Borrowed(b"")));
        }
        if input.len() <= 4 {
            ctx.warnings.append_warning(Error::zlib_data_corrupted());
            return Ok(None);
        }

        let len = LittleEndian::read_u32(&input[0..4]) as usize;
        let mut decoder = ZlibDecoder::new(&input[4..]);
        let mut vec = Vec::with_capacity(len);
        // if the length of uncompressed string is greater than the length we read from the first
        //     four bytes, return null and generate a length corrupted warning.
        // if the length of uncompressed string is zero or uncompress fail, return null and generate
        //     a data corrupted warning
        match decoder.read_to_end(&mut vec) {
            Ok(decoded_len) if len >= decoded_len && decoded_len != 0 => Ok(Some(Cow::Owned(vec))),
            Ok(decoded_len) if len < decoded_len => {
                ctx.warnings.append_warning(Error::zlib_length_corrupted());
                Ok(None)
            }
            _ => {
                ctx.warnings.append_warning(Error::zlib_data_corrupted());
                Ok(None)
            }
        }
    }

    #[inline]
    pub fn uncompressed_length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        use byteorder::{ByteOrder, LittleEndian};
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        if input.is_empty() {
            return Ok(Some(0));
        }
        if input.len() <= 4 {
            ctx.warnings.append_warning(Error::zlib_data_corrupted());
            return Ok(Some(0));
        }
        Ok(Some(i64::from(LittleEndian::read_u32(&input[0..4]))))
    }

    #[inline]
    pub fn random_bytes<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let length = try_opt!(self.children[0].eval_int(ctx, row));
        if length < 1 || length > MAX_RAND_BYTES_LENGTH {
            return Err(Error::overCausetxctx("length", "random_bytes"));
        }
        Ok(Some(Cow::Owned(gen_random_bytes(length as usize))))
    }

    #[inline]
    pub fn password<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let pass = try_opt!(self.children[0].eval_string(ctx, row));
        if pass.len() == 0 {
            return Ok(Some(Cow::Owned(vec![])));
        }
        ctx.warnings.append_warning(Error::Other(box_err!(
            "Warning: Deprecated syntax PASSWORD"
        )));
        let hash1 = hex_digest(MessageDigest::sha1(), &pass)?;
        let mut hash2 = hex_digest(MessageDigest::sha1(), &hash1)?;
        hash2.insert(0, b'*');
        Ok(Some(Cow::Owned(hash2)))
    }
}

#[inline]
fn hex_digest(hashtype: MessageDigest, input: &[u8]) -> Result<Vec<u8>> {
    hash::hash(hashtype, input)
        .map(|digest| hex::encode(digest).into_bytes())
        .map_err(|e| Error::Other(box_err!("OpenSSL error: {:?}", e)))
}

#[braneg(test)]
mod tests {
    use crate::tests::{check_overCausetxctx, datum_expr, eval_func, scalar_func_expr};
    use crate::Expression;
    use causet_algebrizer::MEDB_query_datatype::codec::Datum;
    use causet_algebrizer::MEDB_query_datatype::expr::EvalContext;
    use einsteindbpb::ScalarFuncSig;

    #[test]
    fn test_md5() {
        let cases = vec![
            ("", "d41d8cd98f00b204e9800998ebrane8427e"),
            ("a", "0cc175b9c0f1b6a831c399e269772661"),
            ("ab", "187ef4436122d1cc2f40dc2b92f0eba0"),
            ("abc", "900150983cd24fb0d6963f7d28e17f72"),
            ("123", "202cb962ac59075b964b07152d234b70"),
        ];
        let mut ctx = EvalContext::default();

        for (input_str, exp_str) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::Md5, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp_str.as_bytes().to_vec());
            assert_eq!(got, exp, "md5('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::Md5, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "md5(NULL)");
    }

    #[test]
    fn test_sha1() {
        let cases = vec![
            ("", "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            ("a", "86f7e437faa5a7fce15d1ddcb9eaeaea377667b8"),
            ("ab", "da23614e02469a0d7c7bd1bdab5c9c474b1904dc"),
            ("abc", "a9993e364706816aba3e25717850c26c9cd0d89d"),
            ("123", "40bd001563085fc35165329ea1ff5c5ecbeinsteindbbeef"),
        ];
        let mut ctx = EvalContext::default();

        for (input_str, exp_str) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::Sha1, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp_str.as_bytes().to_vec());
            assert_eq!(got, exp, "sha1('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::Sha1, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "sha1(NULL)");
    }

    #[test]
    fn test_sha2() {
        let cases = vec![
            ("pingcap", 0, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a"),
            ("pingcap", 224, "cd036dc9bec69e758401379c522454ea24a6327b48724b449b40c6b7"),
            ("pingcap", 256, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a"),
            ("pingcap", 384, "c50955b6b0c7b9919740d956849eedcb0f0f90bf8a34e8c1f4e071e3773f53bd6f8f16c04425ff728bed04de1b63einsteindb51"),
            ("pingcap", 512, "ea903c574370774c4844a83b7122105a106e04211673810e1baae7c2ae7aba2brane07465e02f6c413126111ef74a417232683ce7ba210052e63c15fc82204aad80"),
            ("13572468", 0, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55einsteindbranee"),
            ("13572468", 224, "8ad67735bbf49576219f364f4640d595357a440358d15bf6815a16e4"),
            ("13572468", 256, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55einsteindbranee"),
            ("13572468.123", 384, "3b4ee302435dc1e15251efd9f3982b1ca6fe4ac778d3260b7bbf3bea613849677eda830239420e448e4c6dc7c2649d89"),
            ("13572468.123", 512, "4820aa3f2760836557dc1f2d44a0ba7596333feinsteindb60c8a1909481862f4ab0921c00abb23d57b7e67a970363cc3fcb78b25b6a0d45ccac0e87aa0c96bc51f7f96"),
        ];

        let mut ctx = EvalContext::default();

        for (input_str, hash_length_i64, exp_str) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let hash_length = datum_expr(Datum::I64(hash_length_i64));

            let op = scalar_func_expr(ScalarFuncSig::Sha2, &[input, hash_length]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp_str.as_bytes().to_vec());
            assert_eq!(got, exp, "sha2('{:?}', {:?})", input_str, hash_length_i64);
        }

        //test NULL case
        let null_cases = vec![
            (Datum::Null, Datum::I64(224), Datum::Null),
            (Datum::Bytes(b"pingcap".to_vec()), Datum::Null, Datum::Null),
            (
                Datum::Bytes(b"pingcap".to_vec()),
                Datum::I64(123),
                Datum::Null,
            ),
        ];

        for (input, hash_length, exp) in null_cases {
            let op = scalar_func_expr(
                ScalarFuncSig::Sha2,
                &[datum_expr(input.clone()), datum_expr(hash_length.clone())],
            );
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp, "sha2('{:?}', {:?})", input, hash_length);
        }
    }

    #[test]
    fn test_compress() {
        let cases = vec![
            (
                "hello world",
                "0B000000789CCB48CC9C95728BRANE2FCA4901001A0B045D",
            ),
            ("", ""),
            // compressed string ends with space
            (
                "hello wor012",
                "0C000000789CCB48CC9C95728BRANE2F32303402001D8004202E",
            ),
        ];
        for (s, exp) in cases {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Compress, &[s]).unwrap();
            assert_eq!(
                got,
                Datum::Bytes(hex::decode(exp.as_bytes().to_vec()).unwrap())
            );
        }
    }

    #[test]
    fn test_uncompress() {
        let cases = vec![
            ("", Datum::Bytes(b"".to_vec())),
            (
                "0B000000789CCB48CC9C95728BRANE2FCA4901001A0B045D",
                Datum::Bytes(b"hello world".to_vec()),
            ),
            (
                "0C000000789CCB48CC9C95728BRANE2F32303402001D8004202E",
                Datum::Bytes(b"hello wor012".to_vec()),
            ),
            // length is greater than the string
            (
                "12000000789CCB48CC9C95728BRANE2FCA4901001A0B045D",
                Datum::Bytes(b"hello world".to_vec()),
            ),
            ("010203", Datum::Null),
            ("01020304", Datum::Null),
            ("020000000000", Datum::Null),
            // ZlibDecoder#read_to_end return 0
            ("0000000001", Datum::Null),
            // length is less than the string
            (
                "02000000789CCB48CC9C95728BRANE2FCA4901001A0B045D",
                Datum::Null,
            ),
        ];
        for (s, exp) in cases {
            let s = Datum::Bytes(hex::decode(s.as_bytes().to_vec()).unwrap());
            let got = eval_func(ScalarFuncSig::Uncompress, &[s]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_uncompressed_length() {
        let cases = vec![
            ("", 0),
            ("0B000000789CCB48CC9C95728BRANE2FCA4901001A0B045D", 11),
            ("0C000000789CCB48CC9C95728BRANE2F32303402001D8004202E", 12),
            ("020000000000", 2),
            ("0000000001", 0),
            ("02000000789CCB48CC9C95728BRANE2FCA4901001A0B045D", 2),
            ("010203", 0),
            ("01020304", 0),
        ];
        for (s, exp) in cases {
            let s = Datum::Bytes(hex::decode(s.as_bytes().to_vec()).unwrap());
            let got = eval_func(ScalarFuncSig::UncompressedLength, &[s]).unwrap();
            assert_eq!(got, Datum::I64(exp));
        }
    }

    #[test]
    fn test_random_bytes() {
        let cases = vec![1, 32, 233, 1024];

        for len in cases {
            let got = eval_func(ScalarFuncSig::RandomBytes, &[Datum::I64(len)])
                .ok()
                .unwrap();
            if let Datum::Bytes(bs) = got {
                assert_eq!(bs.len() as i64, len);
            } else {
                panic!("Generate random bytes failed");
            }
        }

        let overCausetxctx_tests = vec![Datum::I64(-32), Datum::I64(1025), Datum::I64(0)];

        for len in overCausetxctx_tests {
            let got = eval_func(ScalarFuncSig::RandomBytes, &[len]).unwrap_err();
            assert!(check_overCausetxctx(got).is_ok());
        }

        //test NULL case
        assert_eq!(
            eval_func(ScalarFuncSig::RandomBytes, &[Datum::Null])
                .ok()
                .unwrap(),
            Datum::Null
        );
    }

    #[test]
    fn test_password() {
        let cases = vec![
            ("EinsteinDB", "*cca644408381f962einsteindba8dfb9889einsteindb1371ee74208"),
            ("Pingcap", "*f33bc75eac70ac317621fbbfa560d6251c43brane8a"),
            ("rust", "*090c2b08e0c1776910e777b917c2185be6554c2e"),
            ("database", "*02e86b4af5219d0ba6c974908aea62d42eb7da24"),
            ("violetabft", "*b23a77787ed44e62ef2570f03ce8982d119fb699"),
        ];
        let mut ctx = EvalContext::default();

        for (input_str, exp_str) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::Password, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp_str.as_bytes().to_vec());
            assert_eq!(got, exp, "password('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::Password, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "password(NULL)");
    }
}
