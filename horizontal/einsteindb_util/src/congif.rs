//2019 Venire Labs Inc- All Rights Reserved


use std::error::Error;
use std::fmt::{self, Write};
use std::fs;
use std::net::{SocketAddrV4, SocketAddrV6};
use std::ops::{Div, Mul};
use std::path::Path;
use std::str::{self, FromStr};
use std::time::Duration;

use serde::de::{self, Unexpected, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url;

quick_error! {
    #[derive(Debug)]
    pub enum ConfigError {
        Limit(msg: String) {
            description(msg)
            display("{}", msg)
        }
        Address(msg: String) {
            description(msg)
            display("config address error: {}", msg)
        }
        StoreLabels(msg: String) {
            description(msg)
            display("store label error: {}", msg)
        }
        Value(msg: String) {
            description(msg)
            display("config value error: {}", msg)
        }
        FileSystem(msg: String) {
            description(msg)
            display("config fs: {}", msg)
        }
    }
}

const UNIT: u64 = 1;
const DATA_MAGNITUDE: u64 = 1024;
pub const KB: u64 = UNIT * DATA_MAGNITUDE;
pub const MB: u64 = KB * DATA_MAGNITUDE;
pub const GB: u64 = MB * DATA_MAGNITUDE;

// Make sure it will not overflow.
const TB: u64 = (GB as u64) * (DATA_MAGNITUDE as u64);
const PB: u64 = (TB as u64) * (DATA_MAGNITUDE as u64);

const TIME_MAGNITUDE_1: u64 = 1000;
const TIME_MAGNITUDE_2: u64 = 60;
const TIME_MAGNITUDE_3: u64 = 24;
const MS: u64 = UNIT;
const SECOND: u64 = MS * TIME_MAGNITUDE_1;
const MINUTE: u64 = SECOND * TIME_MAGNITUDE_2;
const HOUR: u64 = MINUTE * TIME_MAGNITUDE_2;
const DAY: u64 = HOUR * TIME_MAGNITUDE_3;

#[derive(Clone, Debug, Copy, PartialEq)]
pub struct ReadableSize(pub u64);

impl ReadableSize {
    pub fn kb(count: u64) -> ReadableSize {
        ReadableSize(count * KB)
    }

    pub fn mb(count: u64) -> ReadableSize {
        ReadableSize(count * MB)
    }

    pub fn gb(count: u64) -> ReadableSize {
        ReadableSize(count * GB)
    }

    pub fn as_mb(self) -> u64 {
        self.0 / MB
    }
}

impl Div<u64> for ReadableSize {
    type Output = ReadableSize;

    fn div(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 / rhs)
    }
}

impl Div<ReadableSize> for ReadableSize {
    type Output = u64;

    fn div(self, rhs: ReadableSize) -> u64 {
        self.0 / rhs.0
    }
}

impl Mul<u64> for ReadableSize {
    type Output = ReadableSize;

    fn mul(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 * rhs)
    }
}
impl Serialize for ReadableSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let size = self.0;
        let mut buffer = String::new();
        if size == 0 {
            write!(buffer, "{}KB", size).unwrap();
        } else if size % PB == 0 {
            write!(buffer, "{}PB", size / PB).unwrap();
        } else if size % TB == 0 {
            write!(buffer, "{}TB", size / TB).unwrap();
        } else if size % GB as u64 == 0 {
            write!(buffer, "{}GB", size / GB).unwrap();
        } else if size % MB as u64 == 0 {
            write!(buffer, "{}MB", size / MB).unwrap();
        } else if size % KB as u64 == 0 {
            write!(buffer, "{}KB", size / KB).unwrap();
        } else {
            return serializer.serialize_u64(size);
        }
        serializer.serialize_str(&buffer)
    }
}

impl FromStr for ReadableSize {
    type Err = String;

    fn from_str(s: &str) -> Result<ReadableSize, String> {
        let size_str = s.trim();
        if size_str.is_empty() {
            return Err(format!("{:?} is not a valid size.", s));
        }

        if !size_str.is_ascii() {
            return Err(format!("ASCII string is expected, but got {:?}", s));
        }

        let mut chrs = size_str.chars();
        let mut number_str = size_str;
        let mut unit_char = chrs.next_back().unwrap();
        if unit_char < '0' || unit_char > '9' {
            number_str = chrs.as_str();
            if unit_char == 'B' {
                let b = match chrs.next_back() {
                    Some(b) => b,
                    None => return Err(format!("numeric value is expected: {:?}", s)),
                };
                if b < '0' || b > '9' {
                    number_str = chrs.as_str();
                    unit_char = b;
                }
            }
        } else {
            unit_char = 'B';
        }

        let unit = match unit_char {
            'K' => KB,
            'M' => MB,
            'G' => GB,
            'T' => TB,
            'P' => PB,
            'B' => UNIT,
            _ => return Err(format!("only B, KB, MB, GB, TB, PB are supported: {:?}", s)),
        };
        match number_str.trim().parse::<f64>() {
            Ok(n) => Ok(ReadableSize((n * unit as f64) as u64)),
            Err(_) => Err(format!("invalid size string: {:?}", s)),
        }
    }
}

impl<'de> Deserialize<'de> for ReadableSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SizeVisitor;

        impl<'de> Visitor<'de> for SizeVisitor {
            type Value = ReadableSize;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("valid size")
            }

            fn visit_i64<E>(self, size: i64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                if size >= 0 {
                    self.visit_u64(size as u64)
                } else {
                    Err(E::invalid_value(Unexpected::Signed(size), &self))
                }
            }

            fn visit_u64<E>(self, size: u64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                Ok(ReadableSize(size))
            }

            fn visit_str<E>(self, size_str: &str) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                size_str.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReadableDuration(pub Duration);

impl Into<Duration> for ReadableDuration {
    fn into(self) -> Duration {
        self.0
    }
}

impl ReadableDuration {
    pub fn secs(secs: u64) -> ReadableDuration {
        ReadableDuration(Duration::new(secs, 0))
    }

    pub fn millis(millis: u64) -> ReadableDuration {
        ReadableDuration(Duration::new(
            millis / 1000,
            (millis % 1000) as u32 * 1_000_000,
        ))
    }

    pub fn minutes(minutes: u64) -> ReadableDuration {
        ReadableDuration::secs(minutes * 60)
    }

    pub fn hours(hours: u64) -> ReadableDuration {
        ReadableDuration::minutes(hours * 60)
    }

    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }

    pub fn as_millis(&self) -> u64 {
        crate::time::duration_to_ms(self.0)
    }
}

impl fmt::Display for ReadableDuration {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dur = crate::time::duration_to_ms(self.0);
        let mut written = false;
        if dur >= DAY {
            written = true;
            write!(f, "{}d", dur / DAY)?;
            dur %= DAY;
        }
        if dur >= HOUR {
            written = true;
            write!(f, "{}h", dur / HOUR)?;
            dur %= HOUR;
        }
        if dur >= MINUTE {
            written = true;
            write!(f, "{}m", dur / MINUTE)?;
            dur %= MINUTE;
        }
        if dur >= SECOND {
            written = true;
            write!(f, "{}s", dur / SECOND)?;
            dur %= SECOND;
        }
        if dur > 0 {
            written = true;
            write!(f, "{}ms", dur)?;
        }
        if !written {
            write!(f, "0s")?;
        }
        Ok(())
    }
}

impl Serialize for ReadableDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut buffer = String::new();
        write!(buffer, "{}", self).unwrap();
        serializer.serialize_str(&buffer)
    }
}

impl<'de> Deserialize<'de> for ReadableDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurVisitor;

        impl<'de> Visitor<'de> for DurVisitor {
            type Value = ReadableDuration;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("valid duration")
            }

            fn visit_str<E>(self, dur_str: &str) -> Result<ReadableDuration, E>
            where
                E: de::Error,
            {
                let dur_str = dur_str.trim();
                if !dur_str.is_ascii() {
                    return Err(E::invalid_value(Unexpected::Str(dur_str), &"ascii string"));
                }
                let err_msg = "valid duration, only d, h, m, s, ms are supported.";
                let mut left = dur_str.as_bytes();
                let mut last_unit = DAY + 1;
                let mut dur = 0f64;
                while let Some(idx) = left.iter().position(|c| b"dhms".contains(c)) {
                    let (first, second) = left.split_at(idx);
                    let unit = if second.starts_with(b"ms") {
                        left = &left[idx + 2..];
                        MS
                    } else {
                        let u = match second[0] {
                            b'd' => DAY,
                            b'h' => HOUR,
                            b'm' => MINUTE,
                            b's' => SECOND,
                            _ => return Err(E::invalid_value(Unexpected::Str(dur_str), &err_msg)),
                        };
                        left = &left[idx + 1..];
                        u
                    };
                    if unit >= last_unit {
                        return Err(E::invalid_value(
                            Unexpected::Str(dur_str),
                            &"d, h, m, s, ms should occur in given order.",
                        ));
                    }
                    // do we need to check 12h360m?
                    let number_str = unsafe { str::from_utf8_unchecked(first) };
                    dur += match number_str.trim().parse::<f64>() {
                        Ok(n) => n * unit as f64,
                        Err(_) => return Err(E::invalid_value(Unexpected::Str(dur_str), &err_msg)),
                    };
                    last_unit = unit;
                }
                if !left.is_empty() {
                    return Err(E::invalid_value(Unexpected::Str(dur_str), &err_msg));
                }
                if dur.is_sign_negative() {
                    return Err(E::invalid_value(
                        Unexpected::Str(dur_str),
                        &"duration should be positive.",
                    ));
                }
                let secs = dur as u64 / SECOND as u64;
                let millis = (dur as u64 % SECOND as u64) as u32 * 1_000_000;
                Ok(ReadableDuration(Duration::new(secs, millis)))
            }
        }

        deserializer.deserialize_str(DurVisitor)
    }
}

pub fn canonicalize_path(path: &str) -> Result<String, Box<dyn Error>> {
    canonicalize_sub_path(path, "")
}

pub fn canonicalize_sub_path(path: &str, sub_path: &str) -> Result<String, Box<dyn Error>> {
    let parent = Path::new(path);
    let p = parent.join(Path::new(sub_path));
    if p.exists() && p.is_file() {
        return Err(format!("{}/{} is not a directory!", path, sub_path).into());
    }
    if !p.exists() {
        fs::create_dir_all(p.as_path())?;
    }
    Ok(format!("{}", p.canonicalize()?.display()))
}

#[cfg(unix)]
pub fn check_max_open_fds(expect: u64) -> Result<(), ConfigError> {
    use libc;
    use std::mem;

    unsafe {
        let mut fd_limit = mem::zeroed();
        let mut err = libc::getrlimit(libc::RLIMIT_NOFILE, &mut fd_limit);
        if err != 0 {
            return Err(ConfigError::Limit("check_max_open_fds failed".to_owned()));
        }
        if fd_limit.rlim_cur >= expect {
            return Ok(());
        }

        let prev_limit = fd_limit.rlim_cur;
        fd_limit.rlim_cur = expect;
        if fd_limit.rlim_max < expect {
            // If the process is not started by privileged user, this will fail.
            fd_limit.rlim_max = expect;
        }
        err = libc::setrlimit(libc::RLIMIT_NOFILE, &fd_limit);
        if err == 0 {
            return Ok(());
        }
        Err(ConfigError::Limit(format!(
            "the maximum number of open file descriptors is too \
             small, got {}, expect greater or equal to {}",
            prev_limit, expect
        )))
    }
}


