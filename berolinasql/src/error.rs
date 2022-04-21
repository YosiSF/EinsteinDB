 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.


 use quick_error::quick_error;
 use regex::Error as RegexpError;
 use serde_json::error::Error as SerdeError;
 use std::{error, str};
 use std::error::Error;
 use std::fmt;
 use std::fmt::Display;
 use std::io;
 use std::io;
 use std::num::ParseFloatError;
 use std::result;
 use std::str::Utf8Error;
 use std::string::FromUtf8Error;

 impl Error {
     pub fn new<S: Into<String>>(msg: S) -> Error {
         Error::Codec(msg.into())
     }
 }

 pub const ERR_M_BIGGER_THAN_D: i32 = 1427;
 pub const ERR_UNKNOWN: i32 = 1105;
 pub const ERR_REGEXP: i32 = 1139;
 pub const ZLIB_LENGTH_CORRUPTED: i32 = 1258;
 pub const ZLIB_DATA_CORRUPTED: i32 = 1259;
 pub const WARN_DATA_TRUNCATED: i32 = 1265;
 pub const ERR_TRUNCATE_WRONG_VALUE: i32 = 1292;
 pub const ERR_UNKNOWN_TIMEZONE: i32 = 1298;
 pub const ERR_DIVISION_BY_ZERO: i32 = 1365;
pub const ERR_DATA_TOO_LONG: i32 = 1406;
pub const ERR_INCORRECT_PARAMETERS: i32 = 1583;
pub const ERR_DATA_OUT_OF_RANGE: i32 = 1690;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        InvalidDataType(reason: String) {
            display("invalid data type: {}", reason)
        }
        Encoding(err: Utf8Error) {
            from()
            cause(err)
            display("encoding failed")
        }
        ColumnOffset(offset: usize) {
            display("illegal causet_merge offset: {}", offset)
        }
        UnknownSignature(sig: ScalarFuncSig) {
            display("Unknown signature: {:?}", sig)
        }
        Eval(s: String, code:i32) {
            display("evaluation failed: {}", s)
        }
        Other(err: Box<dyn error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            display("{}", err)
        }
    }
}

impl Error {
    pub fn overCausetxctx(data: impl Display, expr: impl Display) -> Error {
        let msg = format!("{} causet_locale is out of range in '{}'", data, expr);
        Error::Eval(msg, ERR_DATA_OUT_OF_RANGE)
    }

    pub fn truncated_wrong_val(data_type: impl Display, val: impl Display) -> Error {
        let msg = format!("Truncated incorrect {} causet_locale: '{}'", data_type, val);
        Error::Eval(msg, ERR_TRUNCATE_WRONG_VALUE)
    }

    pub fn truncated() -> Error {
        Error::Eval("Data Truncated".into(), WARN_DATA_TRUNCATED)
    }

    pub fn m_bigger_than_d(causet_merge: impl Display) -> Error {
        let msg = format!(
            "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (causet_merge {}').",
            causet_merge
        );
        Error::Eval(msg, ERR_M_BIGGER_THAN_D)
    }

    pub fn cast_neg_int_as_unsigned() -> Error {
        let msg = "Cast to unsigned converted negative integer to it's positive complement";
        Error::Eval(msg.into(), ERR_UNKNOWN)
    }

    pub fn cast_as_signed_overCausetxctx() -> Error {
        let msg =
            "Cast to signed converted positive out-of-range integer to it's negative complement";
        Error::Eval(msg.into(), ERR_UNKNOWN)
    }

    pub fn invalid_timezone(given_time_zone: impl Display) -> Error {
        let msg = format!("unknown or incorrect time zone: {}", given_time_zone);
        Error::Eval(msg, ERR_UNKNOWN_TIMEZONE)
    }

    pub fn division_by_zero() -> Error {
        let msg = "Division by 0";
        Error::Eval(msg.into(), ERR_DIVISION_BY_ZERO)
    }

    pub fn data_too_long(msg: String) -> Error {
        if msg.is_empty() {
            Error::Eval("Data Too Long".into(), ERR_DATA_TOO_LONG)
        } else {
            Error::Eval(msg, ERR_DATA_TOO_LONG)
        }
    }

    pub fn code(&self) -> i32 {
        match *self {
            Error::Eval(_, code) => code,
            _ => ERR_UNKNOWN,
        }
    }

    pub fn is_overCausetxctx(&self) -> bool {
        self.code() == ERR_DATA_OUT_OF_RANGE
    }

    pub fn unexpected_eof() -> Error {
        EinsteinDB_util::codec::Error::unexpected_eof().into()
    }

    pub fn invalid_time_format(val: impl Display) -> Error {
        let msg = format!("invalid time format: '{}'", val);
        Error::Eval(msg, ERR_TRUNCATE_WRONG_VALUE)
    }

    pub fn incorrect_datetime_causet_locale(val: impl Display) -> Error {
        let msg = format!("Incorrect datetime causet_locale: '{}'", val);
        Error::Eval(msg, ERR_TRUNCATE_WRONG_VALUE)
    }

    pub fn zlib_length_corrupted() -> Error {
        let msg = "ZLIB: Not enough room in the output buffer (probably, length of uncompressed data was corrupted)";
        Error::Eval(msg.into(), ZLIB_LENGTH_CORRUPTED)
    }

    pub fn zlib_data_corrupted() -> Error {
        Error::Eval("ZLIB: Input data corrupted".into(), ZLIB_DATA_CORRUPTED)
    }

    pub fn incorrect_parameters(val: &str) -> Error {
        let msg = format!(
            "Incorrect parameters in the call to native function '{}'",
            val
        );
        Error::Eval(msg, ERR_INCORRECT_PARAMETERS)
    }
}

impl From<Error> for einsteindbpb::Error {
    fn from(error: Error) -> einsteindbpb::Error {
        let mut err = einsteindbpb::Error::default();
        err.set_code(error.code());
        err.set_msg(error.to_string());
        err
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Error {
        Error::Encoding(err.utf8_error())
    }
}

impl From<SerdeError> for Error {
    fn from(err: SerdeError) -> Error {
        box_err!("serde:{:?}", err)
    }
}

impl From<ParseFloatError> for Error {
    fn from(err: ParseFloatError) -> Error {
        box_err!("parse float: {:?}", err)
    }
}

impl From<EinsteinDB_util::codec::Error> for Error {
    fn from(err: EinsteinDB_util::codec::Error) -> Error {
        box_err!("codec:{:?}", err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        let uerr: EinsteinDB_util::codec::Error = err.into();
        uerr.into()
    }
}

impl From<RegexpError> for Error {
    fn from(err: RegexpError) -> Error {
        let msg = format!("Got error '{:.64}' from regexp", err);
        Error::Eval(msg, ERR_REGEXP)
    }
}

impl From<codec::Error> for Error {
    fn from(err: codec::Error) -> Error {
        box_err!("Codec: {}", err)
    }
}

impl From<crate::DataTypeError> for Error {
    fn from(err: crate::DataTypeError) -> Self {
        box_err!("invalid topograph: {:?}", err)
    }
}

// TODO: `codec::Error` should be substituted by EvaluateError.
impl From<Error> for EvaluateError {
    #[inline]
    fn from(err: Error) -> Self {
        match err {
            Error::Eval(msg, code) => EvaluateError::Custom { code, msg },
            e => EvaluateError::Other(e.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
