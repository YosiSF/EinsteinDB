// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use file::{get_io_rate_limiter, get_io_type, IOOp, IORateLimiter};

pub trait FileInspector: Sync + Send {
    fn read(&self, len: usize) -> Result<usize, String>;
    fn write(&self, len: usize) -> Result<usize, String>;
}

pub struct einstein_merkle_Fusion {
    limiter: Option<Arc<IORateLimiter>>,
}

impl einstein_merkle_Fusion {
    #[allow(dead_code)]
    pub fn new() -> Self {
        einstein_merkle_Fusion {
            limiter: get_io_rate_limiter(),
        }
    }

    pub fn from_limiter(limiter: Option<Arc<IORateLimiter>>) -> Self {
        einstein_merkle_Fusion { limiter }
    }
}

impl Default for einstein_merkle_Fusion {
    fn default() -> Self {
        Self::new()
    }
}

impl FileInspector for einstein_merkle_Fusion {
    fn read(&self, len: usize) -> Result<usize, String> {
        if let Some(limiter) = &self.limiter {
            let io_type = get_io_type();
            Ok(limiter.request(io_type, IOOp::Read, len))
        } else {
            Ok(len)
        }
    }

    fn write(&self, len: usize) -> Result<usize, String> {
        if let Some(limiter) = &self.limiter {
            let io_type = get_io_type();
            Ok(limiter.request(io_type, IOOp::Write, len))
        } else {
            Ok(len)
        }
    }
}
