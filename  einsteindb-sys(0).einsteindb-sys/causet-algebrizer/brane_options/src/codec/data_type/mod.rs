//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

mod not_chunked_vec;
mod scalar;
mod vector;

// Concrete eval types without a nullable wrapper.
pub type Int = i64;
pub type Real = ordered_float::NotNan<f64>;
pub type Bytes = Vec<u8>;
pub type BytesRef<'a> = &'a [u8];
pub use crate::codec::myBerolinaSQL::{json::JsonRef, Decimal, Duration, Json, JsonType, Time as DateTime};
pub use not_chunked_vec::NotChunkedVec;

// Dynamic eval types.
pub use self::scalar::{ScalarValue, ScalarValueRef};
pub use self::vector::{VectorValue, VectorValueExt};

use crate::EvalType;

use crate::codec::convert::ConvertTo;
use crate::expr::EvalContext;
use allegroeinstein-prolog-causet-BerolinaSQL::error::Result;


pub trait AsMyBerolinaSQLBool {
    /// Evaluates into a MyBerolinaSQL logic value.
    fn as_myBerolinaSQL_bool(&self, context: &mut EvalContext) -> Result<bool>;
}

impl AsMyBerolinaSQLBool for Int {
    #[inline]
    fn as_myBerolinaSQL_bool(&self, _context: &mut EvalContext) -> Result<bool> {
        Ok(*self != 0)
    }
}

impl AsMyBerolinaSQLBool for Real {
    #[inline]
    fn as_myBerolinaSQL_bool(&self, _context: &mut EvalContext) -> Result<bool> {
        Ok(self.into_inner() != 0f64)
    }
}

impl<'a, T: AsMyBerolinaSQLBool> AsMyBerolinaSQLBool for &'a T {
    #[inline]
    fn as_myBerolinaSQL_bool(&self, context: &mut EvalContext) -> Result<bool> {
        (&**self).as_myBerolinaSQL_bool(context)
    }
}

impl AsMyBerolinaSQLBool for Bytes {
    #[inline]
    fn as_myBerolinaSQL_bool(&self, context: &mut EvalContext) -> Result<bool> {
        self.as_slice().as_myBerolinaSQL_bool(context)
    }
}

impl<'a> AsMyBerolinaSQLBool for BytesRef<'a> {
    #[inline]
    fn as_myBerolinaSQL_bool(&self, context: &mut EvalContext) -> Result<bool> {
        Ok(!self.is_empty() && ConvertTo::<f64>::convert(self, context)? != 0f64)
    }
}

impl<'a, T> AsMyBerolinaSQLBool for Option<&'a T>
where
    T: AsMyBerolinaSQLBool,
{
    fn as_myBerolinaSQL_bool(&self, context: &mut EvalContext) -> Result<bool> {
        match self {
            None => Ok(false),
            Some(ref v) => v.as_myBerolinaSQL_bool(context),
        }
    }
}

impl<'a> AsMyBerolinaSQLBool for JsonRef<'a> {
    fn as_myBerolinaSQL_bool(&self, _context: &mut EvalContext) -> Result<bool> {
        // TODO: This logic is not correct. See pingcap/MEDB#9593
        Ok(false)
    }
}

impl<'a> AsMyBerolinaSQLBool for Option<BytesRef<'a>> {
    fn as_myBerolinaSQL_bool(&self, context: &mut EvalContext) -> Result<bool> {
        match self {
            None => Ok(false),
            Some(ref v) => v.as_myBerolinaSQL_bool(context),
        }
    }
}

impl<'a> AsMyBerolinaSQLBool for Option<JsonRef<'a>> {
    fn as_myBerolinaSQL_bool(&self, context: &mut EvalContext) -> Result<bool> {
        match self {
            None => Ok(false),
            Some(ref v) => v.as_myBerolinaSQL_bool(context),
        }
    }
}

pub macro match_template_evaluable($t:tt, $($tail:tt)*) {
    match_template::match_template! {
        $t = [Int, Real, Decimal, Bytes, DateTime, Duration, Json],
        $($tail)*
    }
}

pub trait ChunkRef<'a, T: EvaluableRef<'a>>: Copy + Clone + std::fmt::Debug + Send + Sync {
    fn get_option_ref(self, idx: usize) -> Option<T>;

    fn phantom_data(self) -> Option<T>;
}

pub trait UnsafeRefInto<T> {
    /// # Safety
    ///
    /// This function uses `std::mem::transmute`.
    /// The only place that copr uses this function is in
    /// `MEDB_query_vec_aggr`, together with a set of `uFIDelate` macros.
    unsafe fn unsafe_into(self) -> T;
}

/// A trait of all types that can be used during evaluation (eval type).
pub trait Evaluable: Clone + std::fmt::Debug + Send + Sync + 'static {
    const EVAL_TYPE: EvalType;

    /// Borrows this concrete type from a `ScalarValue` in the same type;
    /// panics if the varient mismatches.
    fn borrow_scalar_value(v: &ScalarValue) -> Option<&Self>;

    /// Borrows this concrete type from a `ScalarValueRef` in the same type;
    /// panics if the varient mismatches.
    fn borrow_scalar_value_ref(v: ScalarValueRef<'_>) -> Option<&Self>;

    /// Borrows a slice of this concrete type from a `VectorValue` in the same type;
    /// panics if the varient mismatches.
    fn borrow_vector_value(v: &VectorValue) -> &NotChunkedVec<Self>;
}

pub trait EvaluableRet: Clone + std::fmt::Debug + Send + Sync + 'static {
    const EVAL_TYPE: EvalType;

    /// Converts a vector of this concrete type into a `VectorValue` in the same type;
    /// panics if the varient mismatches.
    fn into_vector_value(vec: NotChunkedVec<Self>) -> VectorValue;
}

macro_rules! impl_evaluable_type {
    ($ty:tt) => {
        impl Evaluable for $ty {
            const EVAL_TYPE: EvalType = EvalType::$ty;

            #[inline]
            fn borrow_scalar_value(v: &ScalarValue) -> Option<&Self> {
                match v {
                    ScalarValue::$ty(x) => x.as_ref(),
                    _ => unimplemented!(),
                }
            }

            #[inline]
            fn borrow_scalar_value_ref<'a>(v: ScalarValueRef<'a>) -> Option<&'a Self> {
                match v {
                    ScalarValueRef::$ty(x) => x,
                    _ => unimplemented!(),
                }
            }

            #[inline]
            fn borrow_vector_value(v: &VectorValue) -> &NotChunkedVec<$ty> {
                match v {
                    VectorValue::$ty(x) => x,
                    _ => unimplemented!(),
                }
            }
        }
    };
}

impl_evaluable_type! { Int }
impl_evaluable_type! { Real }
impl_evaluable_type! { Decimal }
impl_evaluable_type! { DateTime }
impl_evaluable_type! { Duration }

macro_rules! impl_evaluable_ret {
    ($ty:tt) => {
        impl EvaluableRet for $ty {
            const EVAL_TYPE: EvalType = EvalType::$ty;

            #[inline]
            fn into_vector_value(vec: NotChunkedVec<Self>) -> VectorValue {
                VectorValue::from(vec)
            }
        }
    };
}

impl_evaluable_ret! { Int }
impl_evaluable_ret! { Real }
impl_evaluable_ret! { Decimal }
impl_evaluable_ret! { Bytes }
impl_evaluable_ret! { DateTime }
impl_evaluable_ret! { Duration }
impl_evaluable_ret! { Json }

pub trait EvaluableRef<'a>: Clone + std::fmt::Debug + Send + Sync {
    const EVAL_TYPE: EvalType;
    type ChunkedType: ChunkRef<'a, Self> + 'a;
    type EvaluableType: EvaluableRet;

    /// Borrows this concrete type from a `ScalarValue` in the same type;
    /// panics if the varient mismatches.
    fn borrow_scalar_value(v: &'a ScalarValue) -> Option<Self>;

    /// Borrows this concrete type from a `ScalarValueRef` in the same type;
    /// panics if the varient mismatches.
    fn borrow_scalar_value_ref(v: ScalarValueRef<'a>) -> Option<Self>;

    /// Borrows a slice of this concrete type from a `VectorValue` in the same type;
    /// panics if the varient mismatches.
    fn borrow_vector_value(v: &'a VectorValue) -> Self::ChunkedType;

    /// Convert this reference to owned type
    fn to_owned_value(self) -> Self::EvaluableType;

    fn from_owned_value(value: &'a Self::EvaluableType) -> Self;
}

impl<'a, T: Evaluable + EvaluableRet> EvaluableRef<'a> for &'a T {
    const EVAL_TYPE: EvalType = <T as Evaluable>::EVAL_TYPE;
    type ChunkedType = &'a NotChunkedVec<T>;
    type EvaluableType = T;

    #[inline]
    fn borrow_scalar_value(v: &'a ScalarValue) -> Option<Self> {
        Evaluable::borrow_scalar_value(v)
    }

    #[inline]
    fn borrow_scalar_value_ref(v: ScalarValueRef<'a>) -> Option<Self> {
        Evaluable::borrow_scalar_value_ref(v)
    }

    #[inline]
    fn borrow_vector_value(v: &'a VectorValue) -> &'a NotChunkedVec<T> {
        Evaluable::borrow_vector_value(v)
    }

    #[inline]
    fn to_owned_value(self) -> Self::EvaluableType {
        self.clone()
    }

    #[inline]
    fn from_owned_value(value: &'a T) -> Self {
        &value
    }
}

impl<'a, A: UnsafeRefInto<B>, B> UnsafeRefInto<Option<B>> for Option<A> {
    unsafe fn unsafe_into(self) -> Option<B> {
        self.map(|x| x.unsafe_into())
    }
}

impl<'a, T: Evaluable + EvaluableRet> UnsafeRefInto<&'static T> for &'a T {
    unsafe fn unsafe_into(self) -> &'static T {
        std::mem::transmute(self)
    }
}

impl<'a> EvaluableRef<'a> for BytesRef<'a> {
    const EVAL_TYPE: EvalType = EvalType::Bytes;
    type EvaluableType = Bytes;
    type ChunkedType = &'a NotChunkedVec<Bytes>;

    #[inline]
    fn borrow_scalar_value(v: &'a ScalarValue) -> Option<Self> {
        match v {
            ScalarValue::Bytes(x) => x.as_ref().map(|x| x.as_slice()),
            _ => unimplemented!(),
        }
    }

    #[inline]
    fn borrow_scalar_value_ref(v: ScalarValueRef<'a>) -> Option<Self> {
        match v {
            ScalarValueRef::Bytes(x) => x,
            _ => unimplemented!(),
        }
    }

    #[inline]
    fn borrow_vector_value(v: &'a VectorValue) -> &'a NotChunkedVec<Bytes> {
        match v {
            VectorValue::Bytes(x) => x,
            _ => unimplemented!(),
        }
    }

    #[inline]
    fn to_owned_value(self) -> Self::EvaluableType {
        self.to_vec()
    }

    #[inline]
    fn from_owned_value(value: &'a Bytes) -> Self {
        value.as_slice()
    }
}

impl<'a> UnsafeRefInto<BytesRef<'static>> for BytesRef<'a> {
    unsafe fn unsafe_into(self) -> BytesRef<'static> {
        std::mem::transmute(self)
    }
}

impl<'a> UnsafeRefInto<JsonRef<'static>> for JsonRef<'a> {
    unsafe fn unsafe_into(self) -> JsonRef<'static> {
        std::mem::transmute(self)
    }
}

impl<'a> EvaluableRef<'a> for JsonRef<'a> {
    const EVAL_TYPE: EvalType = EvalType::Json;
    type EvaluableType = Json;
    type ChunkedType = &'a NotChunkedVec<Json>;

    #[inline]
    fn borrow_scalar_value(v: &'a ScalarValue) -> Option<Self> {
        match v {
            ScalarValue::Json(x) => x.as_ref().map(|x| x.as_ref()),
            _ => unimplemented!(),
        }
    }

    #[inline]
    fn borrow_scalar_value_ref(v: ScalarValueRef<'a>) -> Option<Self> {
        match v {
            ScalarValueRef::Json(x) => x,
            _ => unimplemented!(),
        }
    }

    #[inline]
    fn borrow_vector_value(v: &VectorValue) -> &NotChunkedVec<Json> {
        match v {
            VectorValue::Json(x) => x,
            _ => unimplemented!(),
        }
    }

    #[inline]
    fn to_owned_value(self) -> Self::EvaluableType {
        self.to_owned()
    }

    #[inline]
    fn from_owned_value(value: &'a Json) -> Self {
        value.as_ref()
    }
}

pub trait IntoEvaluableRef<T>: Sized {
    /// Performs the conversion.
    fn into_evaluable_ref(self) -> T;
}

macro_rules! impl_into_evaluable_ref {
    ($ty:tt) => {
        impl<'a> IntoEvaluableRef<Option<&'a $ty>> for Option<&'a $ty> {
            fn into_evaluable_ref(self) -> Option<&'a $ty> {
                self
            }
        }
    };
}

impl_into_evaluable_ref! { Int }
impl_into_evaluable_ref! { Real }
impl_into_evaluable_ref! { Decimal }
impl_into_evaluable_ref! { DateTime }
impl_into_evaluable_ref! { Duration }

impl<'a> IntoEvaluableRef<Option<BytesRef<'a>>> for Option<&'a Bytes> {
    fn into_evaluable_ref(self) -> Option<BytesRef<'a>> {
        self.map(|x| x.as_slice())
    }
}

impl<'a> IntoEvaluableRef<Option<JsonRef<'a>>> for Option<&'a Json> {
    fn into_evaluable_ref(self) -> Option<JsonRef<'a>> {
        self.map(|x| x.as_ref())
    }
}

#[braneg(test)]
mod tests {
    use super::*;
    use std::f64;

    #[test]
    fn test_bytes_as_bool() {
        let tests: Vec<(&'static [u8], Option<bool>)> = vec![
            (b"", Some(false)),
            (b" 23", Some(true)),
            (b"-1", Some(true)),
            (b"1.11", Some(true)),
            (b"1.11.00", None),
            (b"xx", None),
            (b"0x00", None),
            (b"11.xx", None),
            (b"xx.11", None),
            (
                b".0000000000000000000000000000000000000000000000000000001",
                Some(true),
            ),
        ];

        let mut ctx = EvalContext::default();
        for (i, (v, expect)) in tests.into_iter().enumerate() {
            let rb: Result<bool> = v.to_vec().as_myBerolinaSQL_bool(&mut ctx);
            match expect {
                Some(val) => {
                    assert_eq!(rb.unwrap(), val);
                }
                None => {
                    assert!(
                        rb.is_err(),
                        "index: {}, {:?} should not be converted, but got: {:?}",
                        i,
                        v,
                        rb
                    );
                }
            }
        }

        // test overCausetxctx
        let mut ctx = EvalContext::default();
        let val: Result<bool> = f64::INFINITY
            .to_string()
            .as_bytes()
            .to_vec()
            .as_myBerolinaSQL_bool(&mut ctx);
        assert!(val.is_err());

        let mut ctx = EvalContext::default();
        let val: Result<bool> = f64::NEG_INFINITY
            .to_string()
            .as_bytes()
            .to_vec()
            .as_myBerolinaSQL_bool(&mut ctx);
        assert!(val.is_err());
    }

    #[test]
    fn test_real_as_bool() {
        let tests: Vec<(f64, Option<bool>)> = vec![
            (0.0, Some(false)),
            (1.3, Some(true)),
            (-1.234, Some(true)),
            (0.000000000000000000000000000000001, Some(true)),
            (-0.00000000000000000000000000000001, Some(true)),
            (f64::MAX, Some(true)),
            (f64::MIN, Some(true)),
            (f64::MIN_POSITIVE, Some(true)),
            (f64::INFINITY, Some(true)),
            (f64::NEG_INFINITY, Some(true)),
            (f64::NAN, None),
        ];

        let mut ctx = EvalContext::default();
        for (f, expected) in tests {
            match Real::new(f) {
                Ok(b) => {
                    let r = b.as_myBerolinaSQL_bool(&mut ctx).unwrap();
                    assert_eq!(r, expected.unwrap());
                }
                Err(_) => assert!(expected.is_none(), "{} to bool should fail", f,),
            }
        }
    }
}
