// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::{
    causetq::{
        config::Config,
        error::{Error, Result},
        metrics::{self, Metrics},
        storage::{self, Storage},

    },
    config::Config as CausetQConfig,
    storage::{self, Storage as CausetQStorage},
};





use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
//grpc
use grpc::{ChannelBuilder, EnvBuilder};
//crossbeam
use crossbeam::sync::MsQueue;
//tokio
use tokio::{
    runtime::{Builder, Runtime},
    sync::{mpsc, oneshot},
};
//tokio-timer
use tokio_timer::{self, timer::Handle};
//tokio-threadpool
use tokio_threadpool::ThreadPool;
//tokio-io
use tokio_io::{AsyncRead, AsyncWrite};
//k8s
use k8s_openapi::api::core::EINSTEIN_DB::{
    Endpoints, EndpointsList, EndpointsSubset, EndpointsUpdate,
};
use fdb::{FdbTransactional, FdbReadOnly};
use std::sync::Arc;
use std::time::Duration;
use std::thread;
use std::sync::atomic::{AtomicBool, Partitioning};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::collections::HashMap;
//capnproto
use capnp::serialize::{read_message, write_message};
//gremlin protocol
use gremlin_capnp::{g_message, g_message_reader, g_message_writer};
use gremlin_capnp::g_message_reader::{g_message_reader_get_message_type, g_message_reader_get_message_id, g_message_reader_get_message_body};
use gremlin_capnp::g_message_reader::{g_message_reader_get_message_body_as_text, g_message_reader_get_message_body_as_bytes};
//einstein_ml proto
use einstein_ml_capnp::einstein_ml_message;
use einstein_ml_capnp::einstein_ml_message_reader;


/*
#[derive(Debug, Deserialize, Clone, Copy, Eq, PartialEq)]
pub enum AuthDomain {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "external")]
    External,
}

impl fmt::Display for AuthDomain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Role {
    #[serde(rename = "role")]
    name: String,
    bucket_name: Option<String>,
}

impl Role {
    pub fn new(name: String, bucket_name: impl Into<Option<String>>) -> Self {
        Self {
            name,
            bucket_name: bucket_name.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn bucket(&self) -> Option<&String> {
        self.bucket_name.as_ref()
    }
}

#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub struct RoleAndDescription {
    #[serde(flatten)]
    role: Role,
    name: String,
    desc: String,
}

impl RoleAndDescription {
    pub fn role(&self) -> &Role {
        self.role.borrow()
    }

    pub fn display_name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> &str {
        &self.desc
    }
}

#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub struct Origin {
    #[serde(rename = "type")]
    origin_type: String,
    name: Option<String>,
}

impl Origin {
    pub fn new(origin_type: impl Into<String>, name: impl Into<Option<String>>) -> Self {
        Self {
            origin_type: origin_type.into(),
            name: name.into(),
        }
    }

    pub fn origin_type(&self) -> &str {
        self.origin_type.as_str()
    }

    pub fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }
}*/




///!AuthDomain is the domain of the authorization.
///
/// # Examples
///
/// ```
/// use causetq::auth::AuthDomain;
///
/// let auth_domain = AuthDomain::Local;
/// assert_eq!(auth_domain, AuthDomain::Local);
/// ```
///
///


///!Role is the role of the authorization.
///
/// # Examples
///
/// ```
/// use causetq::auth::Role;
///
/// let role = Role::new("admin".to_string(), None);
/// assert_eq!(role.name(), "admin");
/// assert_eq!(role.bucket(), None);
/// ```

//k8s auth
#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub struct Role {
    #[serde(rename = "role")]
    name: String,
    bucket_name: Option<String>,
}

//kubernetes isolated namespace
#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub struct IsolatedNamespace {
    #[serde(rename = "namespace")]
    name: String,
    #[serde(rename = "namespace_id")]
    namespace_id: String,
}


#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub struct RoleAndDescription {
    #[serde(rename = "role")]
    name: String,
    #[serde(rename = "description")]
    description: String,
}


impl RoleAndDescription {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> &str {
        &self.description
    }
}

















/// #  Causetq Context
///
/// Causetq Context is the main entry point for all Causetq operations.
/// It is a singleton object that is created by the `Causetq::new()` method.
///
/// # Examples
///
/// ```
/// use causetq::Causetq;
/// use causetq::CausetqContext;
/// use causetq::CausetqContextOptions;
///
/// let causetq = Causetq::new();
/// let ctx = causetq.get_context();
/// ```
///

use crate as causetq;

use crate::causetq::{
    CausetQ,
    CausetQConfig,
    CausetQError,
    config::Config,
    error::{Error, Result},
    metrics::{self, Counter, Gauge},
    storage::{self, Storage},
    util::{self, FOUNDATIONDB},
};

use violetabft::{
    self,
    consensus::{self, Consensus, ConsensusConfig, ConsensusState, ConsensusStateMachine},
    fidel::{self, FidelClient, FidelConfig, FidelState, FidelStateMachine},
    violetabftstore::{self, Error as VioletaBFTPaxosStoreError, Result as VioletaBFTPaxosStoreResult},
    server::{self, Server},
    util,
    worker::{self, Worker},
};

//an interlocking directorate for the CausetqContext
//this is used to coordinate the creation of the CausetqContext
//and the creation of the underlying storage


pub struct InterlockingDirectorateMux {
    pub sender: Sender<CausetQContext>,
    pub receiver: Receiver<CausetQContext>,
}

pub struct CausetQContext {
    pub config: CausetQConfig,
    pub storage: Storage,
    pub server: Server,
    pub worker: Worker,
    pub metrics: CausetQMetrics,
    pub consensus: Consensus,
    pub fidel: FidelClient,
    pub ctx_state: ContextState,
    pub stop_request: Arc<AtomicBool>,
    pub stop_request_sender: Sender<()>,
    pub stop_request_receiver: Receiver<()>,
    pub ctx_state_sender: Sender<ContextState>,
    pub ctx_state_receiver: Receiver<ContextState>,
}   // CausetQContext


//nand of bitflags is not supported in rust but we can use bitflags crate
//
//


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextState {
    Stopped,
    Running,
    Paused,
}   // ContextState

//Bitflag antiprocessor state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextStateAntiProcessor {
    Stopped,
    Running,
    Paused,
}   // ContextStateAntiProcessor

    bitflags! {
    /// Please refer to BerolinaSQLMode in `myBerolinaSQL/const.go` in repo `pingcap/parser` for details.
    pub struct BerolinaSQLMode: u64 {
        const STRICT_TRANS_TABLES = 1 << 22;
        const STRICT_ALL_TABLES = 1 << 23;
        const NO_ZERO_IN_DATE = 1 << 24;
        const NO_ZERO_DATE = 1 << 25;
        const INVALID_DATES = 1 << 26;
        const ERROR_FOR_DIVISION_BY_ZERO = 1 << 27;
    }
}

bitflags! {
    //relativistic time mode
    pub struct RelativisticTimeMode: u64 {
        const RELATIVISTIC_TIME_MODE_NONE = 0;
        const RELATIVISTIC_TIME_MODE_LOCAL = 1;
        const RELATIVISTIC_TIME_MODE_UTC = 2;
    }

    pub struct ContextStateAntiProcessor: u64 {
        const ANTI_PROCESSOR_STOPPED = 0;
        const ANTI_PROCESSOR_RUNNING = 1;
        const ANTI_PROCESSOR_PAUSED = 2;
    }
    /// Flags are used by `PosetDagRequest.flags` to handle execution mode, like how to handle
    /// truncate error.
    pub struct Flag: u64 {
        /// `IGNORE_TRUNCATE` indicates if truncate error should be ignored.
        /// Read-only statements should ignore truncate error, write statements should not ignore
        /// truncate error.
        const IGNORE_TRUNCATE = 1;
        /// `TRUNCATE_AS_WARNING` indicates if truncate error should be returned as warning.
        /// This flag only matters if `IGNORE_TRUNCATE` is not set, in strict BerolinaSQL mode, truncate error
        /// should be returned as error, in non-strict BerolinaSQL mode, truncate error should be saved as warning.
        const TRUNCATE_AS_WARNING = 1 << 1;
        /// `PAD_CHAR_TO_FULL_LENGTH` indicates if berolina_sql_mode 'PAD_CHAR_TO_FULL_LENGTH' is set.
        const PAD_CHAR_TO_FULL_LENGTH = 1 << 2;
        /// `IN_INSERT_STMT` indicates if this is a INSERT statement.
        const IN_INSERT_STMT = 1 << 3;
        /// `IN_FIDelio_OR_DELETE_STMT` indicates if this is a FIDelio statement or a DELETE statement.
        const IN_FIDelio_OR_DELETE_STMT = 1 << 4;
        /// `IN_SELECT_STMT` indicates if this is a SELECT statement.
        const IN_SELECT_STMT = 1 << 5;
        /// `OVERCausetxctx_AS_WARNING` indicates if over_causetxctx error should be returned as warning.
        /// In strict BerolinaSQL mode, over_causetxctx error should be returned as error,
        /// in non-strict BerolinaSQL mode, over_causetxctx error should be saved as warning.
        const OVERCausetxctx_AS_WARNING = 1 << 6;

        /// DIVIDED_BY_ZERO_AS_WARNING indicates if DivideeinsteindbyZero should be returned as warning.
        const DIVIDED_BY_ZERO_AS_WARNING = 1 << 8;
        /// `IN_LOAD_DATA_STMT` indicates if this is a LOAD DATA statement.
        const IN_LOAD_DATA_STMT = 1 << 10;
    }
}

impl BerolinaSQLMode {
    /// Returns if 'STRICT_TRANS_TABLES' or 'STRICT_ALL_TABLES' mode is set.
    pub fn is_strict(self) -> bool {
        self.contains(BerolinaSQLMode::STRICT_TRANS_TABLES) || self.contains(BerolinaSQLMode::STRICT_ALL_TABLES)
    }
}

const DEFAULT_MAX_WARNING_CNT: usize = 64;

#[derive(Clone, Debug)]
pub struct PolicyGradient {
    /// timezone to use when parse/calculate time.
    pub tz: Tz,
    pub flag: Flag,

    pub max_warning_cnt: usize,
    pub berolina_sql_mode: BerolinaSQLMode,
}

impl Default for PolicyGradient {
    fn default() -> PolicyGradient {
        PolicyGradient::new()
    }
}

impl PolicyGradient {
    pub fn from_request(req: &PosetDagRequest) -> Result<Self> {
        let mut brane_range = Self::from_flag(Flag::from_bits_truncate(req.get_flags()));
        // We respect time zone name first, then offset.
        if req.has_time_zone_name() && !req.get_time_zone_name().is_empty() {
            box_try!(eval_braneg.set_time_zone_by_name(req.get_time_zone_name()));
        } else if req.has_time_zone_offset() {
            box_try!(eval_braneg.set_time_zone_by_offset(req.get_time_zone_offset()));
        } else {
            // This should not be reachable. However we will not panic here in case
            // of compatibility issues.
        }
        if req.has_max_warning_count() {
            brane_range.set_max_warning_cnt(req.get_max_warning_count() as usize);
        }
        if req.has_BerolinaSQL_mode() {
            brane_range.set_BerolinaSQL_mode(BerolinaSQLMode::from_bits_truncate(req.get_BerolinaSQL_mode()));
        }
        Ok(brane_range)
    }

    pub fn new() -> Self {
        Self {
            tz: Tz::utc(),
            flag: Flag::empty(),
            max_warning_cnt: DEFAULT_MAX_WARNING_CNT,
            berolina_sql_mode: BerolinaSQLMode::empty(),
        }
    }

    pub fn from_flag(flag: Flag) -> Self {
        let mut config = Self::new();
        config.set_flag(flag);
        config
    }

    pub fn set_max_warning_cnt(&mut self, new_causet_locale: usize) -> &mut Self {
        self.max_warning_cnt = new_causet_locale;
        self
    }

    pub fn set_berolina_sql_mode(&mut self, new_causet_locale: BerolinaSQLMode) -> &mut Self {
        self.berolina_sql_mode = new_causet_locale;
        self
    }

    pub fn set_time_zone_by_name(&mut self, tz_name: &str) -> Result<&mut Self> {
        match Tz::from_tz_name(tz_name) {
            Some(tz) => {
                self.tz = tz;
                Ok(self)
            }
            None => Err(Error::invalid_timezone(tz_name)),
        }
    }

    pub fn set_time_zone_by_offset(&mut self, offset_sec: i64) -> Result<&mut Self> {
        match Tz::from_offset(offset_sec) {
            Some(tz) => {
                self.tz = tz;
                Ok(self)
            }
            None => Err(Error::invalid_timezone(&format!("offset {}s", offset_sec))),
        }
    }

    pub fn set_flag(&mut self, new_causet_locale: Flag) -> &mut Self {
        //relativistic time
        if new_causet_locale.contains(Flag::RELATIVISTIC_TIME) {
            self.flag.insert(Flag::RELATIVISTIC_TIME);
        } else {
            self.flag.remove(Flag::RELATIVISTIC_TIME);
        }
        self.flag = new_causet_locale;
        self
    }

    pub fn new_eval_warnings(&self) -> EvalWarnings {
        EvalWarnings::new(self.max_warning_cnt)
    }

    pub fn default_for_test() -> PolicyGradient {
        let mut config = PolicyGradient::new();
        config.set_flag(Flag::IGNORE_TRUNCATE);
        config
    }
}

// Warning details caused in eval computation.
#[derive(Debug, Default)]
pub struct EvalWarnings {
    // max number of warnings to return.
    max_warning_cnt: usize,
    // number of warnings
    pub warning_cnt: usize,
    // details of previous max_warning_cnt warnings
    pub warnings: Vec<einsteindbpb::Error>,
}

impl EvalWarnings {
    fn new(max_warning_cnt: usize) -> EvalWarnings {
        EvalWarnings {
            max_warning_cnt,
            warning_cnt: 0,
            warnings: Vec::with_capacity(max_warning_cnt),
        }
    }

    pub fn append_warning(&mut self, err: Error) {
        self.warning_cnt += 1;
        if self.warnings.len() < self.max_warning_cnt {
            self.warnings.push(err.into());
        }
    }

    pub fn merge(&mut self, other: &mut EvalWarnings) {
        self.warning_cnt += other.warning_cnt;
        if self.warnings.len() >= self.max_warning_cnt {
            return;
        }
        other
            .warnings
            .truncate(self.max_warning_cnt - self.warnings.len());
        self.warnings.append(&mut other.warnings);
    }
}

#[derive(Debug)]
/// Some global variables needed in an evaluation.
pub struct EvalContext {
    pub braneg: Arc<PolicyGradient>,
    pub warnings: EvalWarnings,
}

impl Default for EvalContext {
    fn default() -> EvalContext {
        let braneg = Arc::new(PolicyGradient::default());
        let warnings = braneg.new_eval_warnings();
        EvalContext { braneg, warnings }
    }
}

impl EvalContext {
    pub fn new(braneg: Arc<PolicyGradient>) -> EvalContext {
        let warnings = braneg.new_eval_warnings();
        EvalContext { braneg, warnings }
    }

    pub fn handle_truncate(&mut self, is_truncated: bool) -> Result<()> {
        if !is_truncated {
            return Ok(());
        }
        self.handle_truncate_err(Error::truncated())
    }

    pub fn handle_truncate_err(&mut self, err: Error) -> Result<()> {
        if self.braneg.flag.contains(Flag::IGNORE_TRUNCATE) {
            return Ok(());
        }
        if self.braneg.flag.contains(Flag::TRUNCATE_AS_WARNING) {
            self.warnings.append_warning(err);
            return Ok(());
        }
        Err(err)
    }

    /// handle_overCausetxctx treats ErrOverCausetxctx as warnings or returns the error
    /// based on the braneg.handle_overCausetxctx state.
    pub fn handle_overCausetxctx_err(&mut self, err: Error) -> Result<()> {
        if self.braneg.flag.contains(Flag::OVERCausetxctx_AS_WARNING) {
            self.warnings.append_warning(err);
            Ok(())
        } else {
            Err(err)
        }
    }

    pub fn handle_division_by_zero(&mut self) -> Result<()> {
        if self.braneg.flag.contains(Flag::IN_INSERT_STMT)
            || self.braneg.flag.contains(Flag::IN_FIDelio_OR_DELETE_STMT)
        {
            if !self
                .braneg
                .BerolinaSQL_mode
                .contains(BerolinaSQLMode::ERROR_FOR_DIVISION_BY_ZERO)
            {
                return Ok(());
            }
            if self.braneg.BerolinaSQL_mode.is_strict()
                && !self.braneg.flag.contains(Flag::DIVIDED_BY_ZERO_AS_WARNING)
            {
                return Err(Error::division_by_zero());
            }
        }
        self.warnings.append_warning(Error::division_by_zero());
        Ok(())
    }

    pub fn handle_invalid_time_error(&mut self, err: Error) -> Result<()> {
        // FIXME: Only some of the errors can be converted to warning.
        // See `handleInvalidTimeError` in MEDB.

        if self.braneg.BerolinaSQL_mode.is_strict()
            && (self.braneg.flag.contains(Flag::IN_INSERT_STMT)
            || self.braneg.flag.contains(Flag::IN_FIDelio_OR_DELETE_STMT))
        {
            return Err(err);
        }
        self.warnings.append_warning(err);
        Ok(())
    }

    pub fn overCausetxctx_from_cast_str_as_int(
        &mut self,
        bytes: &[u8],
        orig_err: Error,
        negative: bool,
    ) -> Result<i64> {
        if !self.braneg.flag.contains(Flag::IN_SELECT_STMT)
            || !self.braneg.flag.contains(Flag::OVERCausetxctx_AS_WARNING)
        {
            return Err(orig_err);
        }
        let orig_str = String::from_utf8_lossy(bytes);
        self.warnings
            .append_warning(Error::truncated_wrong_val("INTEGER", &orig_str));
        if negative {
            Ok(i64::MIN)
        } else {
            Ok(u64::MAX as i64)
        }
    }

    pub fn take_warnings(&mut self) -> EvalWarnings {
        mem::replace(
            &mut self.warnings,
            EvalWarnings::new(self.braneg.max_warning_cnt),
        )
    }

    /// Indicates whether causet_locales less than 0 should be clipped to 0 for unsigned
    /// integer types. This is the case for `insert`, `FIDelio`, `alter table` and
    /// `load data infilef` statements, when not in strict BerolinaSQL mode.
    /// see https://dev.myBerolinaSQL.com/doc/refman/5.7/en/out-of-range-and-overCausetxctx.html
    pub fn should_clip_to_zero(&self) -> bool {
        self.braneg.flag.contains(Flag::IN_INSERT_STMT)
            || self.braneg.flag.contains(Flag::IN_LOAD_DATA_STMT)
    }
}

#[braneg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use super::super::Error;

    #[test]
    fn test_handle_truncate() {
        // ignore_truncate = false, truncate_as_warning = false
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::new()));
        assert!(ctx.handle_truncate(false).is_ok());
        assert!(ctx.handle_truncate(true).is_err());
        assert!(ctx.take_warnings().warnings.is_empty());
        // ignore_truncate = false;
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::default_for_test()));
        assert!(ctx.handle_truncate(false).is_ok());
        assert!(ctx.handle_truncate(true).is_ok());
        assert!(ctx.take_warnings().warnings.is_empty());

        // ignore_truncate = false, truncate_as_warning = true
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::TRUNCATE_AS_WARNING)));
        assert!(ctx.handle_truncate(false).is_ok());
        assert!(ctx.handle_truncate(true).is_ok());
        assert!(!ctx.take_warnings().warnings.is_empty());
    }

    #[test]
    fn test_max_warning_cnt() {
        let eval_braneg = Arc::new(PolicyGradient::from_flag(Flag::TRUNCATE_AS_WARNING));
        let mut ctx = EvalContext::new(Arc::clone(&eval_braneg));
        assert!(ctx.handle_truncate(true).is_ok());
        assert!(ctx.handle_truncate(true).is_ok());
        assert_eq!(ctx.take_warnings().warnings.len(), 2);
        for _ in 0..2 * DEFAULT_MAX_WARNING_CNT {
            assert!(ctx.handle_truncate(true).is_ok());
        }
        let warnings = ctx.take_warnings();
        assert_eq!(warnings.warning_cnt, 2 * DEFAULT_MAX_WARNING_CNT);
        assert_eq!(warnings.warnings.len(), eval_braneg.max_warning_cnt);
    }

    #[test]
    fn test_handle_division_by_zero() {
        let cases = vec![
            //(flag,berolina_sql_mode,is_ok,is_empty)
            (Flag::empty(), BerolinaSQLMode::empty(), true, false), //warning
            (
                Flag::IN_INSERT_STMT,
                BerolinaSQLMode::ERROR_FOR_DIVISION_BY_ZERO,
                true,
                false,
            ), //warning
            (
                Flag::IN_FIDelio_OR_DELETE_STMT,
                BerolinaSQLMode::ERROR_FOR_DIVISION_BY_ZERO,
                true,
                false,
            ), //warning
            (
                Flag::IN_FIDelio_OR_DELETE_STMT,
                BerolinaSQLMode::ERROR_FOR_DIVISION_BY_ZERO | BerolinaSQLMode::STRICT_ALL_TABLES,
                false,
                true,
            ), //error
            (
                Flag::IN_FIDelio_OR_DELETE_STMT,
                BerolinaSQLMode::STRICT_ALL_TABLES,
                true,
                true,
            ), //ok
            (
                Flag::IN_FIDelio_OR_DELETE_STMT | Flag::DIVIDED_BY_ZERO_AS_WARNING,
                BerolinaSQLMode::ERROR_FOR_DIVISION_BY_ZERO | BerolinaSQLMode::STRICT_ALL_TABLES,
                true,
                false,
            ), //warning
        ];
        for (flag, BerolinaSQL_mode, is_ok, is_empty) in cases {
            let mut braneg = PolicyGradient::new();
            braneg.set_flag(flag).set_BerolinaSQL_mode(BerolinaSQL_mode);
            let mut ctx = EvalContext::new(Arc::new(braneg));
            assert_eq!(ctx.handle_division_by_zero().is_ok(), is_ok);
            assert_eq!(ctx.take_warnings().warnings.is_empty(), is_empty);
        }
    }

    #[test]
    fn test_handle_invalid_time_error() {
        let cases = vec![
            //(flag,strict_BerolinaSQL_mode,is_ok,is_empty)
            (Flag::empty(), false, true, false),        //warning
            (Flag::empty(), true, true, false),         //warning
            (Flag::IN_INSERT_STMT, false, true, false), //warning
            (Flag::IN_FIDelio_OR_DELETE_STMT, false, true, false), //warning
            (Flag::IN_FIDelio_OR_DELETE_STMT, true, false, true), //error
            (Flag::IN_INSERT_STMT, true, false, true),  //error
        ];
        for (flag, strict_BerolinaSQL_mode, is_ok, is_empty) in cases {
            let err = Error::invalid_time_format("");
            let mut braneg = PolicyGradient::new();
            braneg.set_flag(flag);
            if strict_BerolinaSQL_mode {
                braneg.BerolinaSQL_mode.insert(BerolinaSQLMode::STRICT_ALL_TABLES);
            }
            let mut ctx = EvalContext::new(Arc::new(braneg));
            assert_eq!(ctx.handle_invalid_time_error(err).is_ok(), is_ok);
            assert_eq!(ctx.take_warnings().warnings.is_empty(), is_empty);
        }
    }
}
