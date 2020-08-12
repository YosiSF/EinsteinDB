mod storage_impl;

pub use self::storage_impl::EinsteinDBStorage;

use async_trait::async_trait;
use ekvproto::interlock::{KeyRange, Response};
use protobuf::Message;
use allegroeinstein-prolog-causet-sql::storage::IntervalRange;
use fidelpb::{PosetDagRequest, SelectResponse, StreamResponse};

use crate::interlock::metrics::*;
use crate::interlock::{Deadline, RequestHandler, Result};
use crate::storage::{Statistics, Store};

pub struct PosetDagHandlerBuilder<S: Store + 'static> {
    req: PosetDagRequest,
    ranges: Vec<KeyRange>,
    store: S,
    data_version: Option<u64>,
    deadline: Deadline,
    batch_row_limit: usize,
    is_streaming: bool,
    is_cache_enabled: bool,
    enable_batch_if_possible: bool,
}

impl<S: Store + 'static> PosetDagHandlerBuilder<S> {
    pub fn new(
        req: PosetDagRequest,
        ranges: Vec<KeyRange>,
        store: S,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
        is_cache_enabled: bool,
    ) -> Self {
        PosetDagHandlerBuilder {
            req,
            ranges,
            store,
            data_version: None,
            deadline,
            batch_row_limit,
            is_streaming,
            is_cache_enabled,
            enable_batch_if_possible: true,
        }
    }

    pub fn data_version(mut self, data_version: Option<u64>) -> Self {
        self.data_version = data_version;
        self
    }

    pub fn enable_batch_if_possible(mut self, enable_batch_if_possible: bool) -> Self {
        self.enable_batch_if_possible = enable_batch_if_possible;
        self
    }

    pub fn build(self) -> Result<Box<dyn RequestHandler>> {
        // TODO: support batch executor while handling server-side streaming requests
        // https://github.com/EinsteinDB/EinsteinDB/pull/5945
        if self.enable_batch_if_possible && !self.is_streaming {
            MilevaDB_query_vec_executors::runner::BatchExecutorsRunner::check_supported(
                self.req.get_executors(),
            )?;
            INTERLOCK_POSETDAG_REQ_COUNT.with_label_values(&["batch"]).inc();
            Ok(BatchPOSETDAGHandler::new(
                self.req,
                self.ranges,
                self.store,
                self.data_version,
                self.deadline,
                self.is_cache_enabled,
            )?
            .into_boxed())
        } else {
            INTERLOCK_POSETDAG_REQ_COUNT.with_label_values(&["normal"]).inc();
            Ok(POSETDAGHandler::new(
                self.req,
                self.ranges,
                self.store,
                self.data_version,
                self.deadline,
                self.batch_row_limit,
                self.is_streaming,
                self.is_cache_enabled,
            )?
            .into_boxed())
        }
    }
}

pub struct POSETDAGHandler {
    runner: MilevaDB_query_normal_executors::ExecutorsRunner<Statistics>,
    data_version: Option<u64>,
}

impl POSETDAGHandler {
    pub fn new<S: Store + 'static>(
        req: PosetDagRequest,
        ranges: Vec<KeyRange>,
        store: S,
        data_version: Option<u64>,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
        is_cache_enabled: bool,
    ) -> Result<Self> {
        Ok(Self {
            runner: MilevaDB_query_normal_executors::ExecutorsRunner::from_request(
                req,
                ranges,
                EinsteinDBStorage::new(store, is_cache_enabled),
                deadline,
                batch_row_limit,
                is_streaming,
            )?,
            data_version,
        })
    }
}

#[async_trait]
impl RequestHandler for POSETDAGHandler {
    #[minitrace::trace_async(fidelpb::Event::EinsteinDBCoprExecutePosetDagRunner as u32)]
    async fn handle_request(&mut self) -> Result<Response> {
        let result = self.runner.handle_request();
        handle_qe_response(result, self.runner.can_be_cached(), self.data_version)
    }

    fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        handle_qe_stream_response(self.runner.handle_streaming_request())
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.runner.collect_storage_stats(dest);
    }
}

pub struct BatchPOSETDAGHandler {
    runner: MilevaDB_query_vec_executors::runner::BatchExecutorsRunner<Statistics>,
    data_version: Option<u64>,
}

impl BatchPOSETDAGHandler {
    pub fn new<S: Store + 'static>(
        req: PosetDagRequest,
        ranges: Vec<KeyRange>,
        store: S,
        data_version: Option<u64>,
        deadline: Deadline,
        is_cache_enabled: bool,
    ) -> Result<Self> {
        Ok(Self {
            runner: MilevaDB_query_vec_executors::runner::BatchExecutorsRunner::from_request(
                req,
                ranges,
                EinsteinDBStorage::new(store, is_cache_enabled),
                deadline,
            )?,
            data_version,
        })
    }
}

#[async_trait]
impl RequestHandler for BatchPOSETDAGHandler {
    #[minitrace::trace_async(fidelpb::Event::EinsteinDBCoprExecuteBatchPosetDagRunner as u32)]
    async fn handle_request(&mut self) -> Result<Response> {
        let result = self.runner.handle_request().await;
        handle_qe_response(result, self.runner.can_be_cached(), self.data_version)
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.runner.collect_storage_stats(dest);
    }
}

fn handle_qe_response(
    result: allegroeinstein-prolog-causet-sql::Result<SelectResponse>,
    can_be_cached: bool,
    data_version: Option<u64>,
) -> Result<Response> {
    use allegroeinstein-prolog-causet-sql::error::ErrorInner;

    match result {
        Ok(sel_resp) => {
            let mut resp = Response::default();
            resp.set_data(box_try!(sel_resp.write_to_bytes()));
            resp.set_can_be_cached(can_be_cached);
            resp.set_is_cache_hit(false);
            if let Some(v) = data_version {
                resp.set_cache_last_version(v);
            }
            Ok(resp)
        }
        Err(err) => match *err.0 {
            ErrorInner::Storage(err) => Err(err.into()),
            ErrorInner::Evaluate(err) => {
                let mut resp = Response::default();
                let mut sel_resp = SelectResponse::default();
                sel_resp.mut_error().set_code(err.code());
                sel_resp.mut_error().set_msg(err.to_string());
                resp.set_data(box_try!(sel_resp.write_to_bytes()));
                resp.set_can_be_cached(can_be_cached);
                resp.set_is_cache_hit(false);
                Ok(resp)
            }
        },
    }
}

fn handle_qe_stream_response(
    result: allegroeinstein-prolog-causet-sql::Result<(Option<(StreamResponse, IntervalRange)>, bool)>,
) -> Result<(Option<Response>, bool)> {
    use allegroeinstein-prolog-causet-sql::error::ErrorInner;

    match result {
        Ok((Some((s_resp, range)), finished)) => {
            let mut resp = Response::default();
            resp.set_data(box_try!(s_resp.write_to_bytes()));
            resp.mut_range().set_start(range.lower_inclusive);
            resp.mut_range().set_end(range.upper_exclusive);
            Ok((Some(resp), finished))
        }
        Ok((None, finished)) => Ok((None, finished)),
        Err(err) => match *err.0 {
            ErrorInner::Storage(err) => Err(err.into()),
            ErrorInner::Evaluate(err) => {
                let mut resp = Response::default();
                let mut s_resp = StreamResponse::default();
                s_resp.mut_error().set_code(err.code());
                s_resp.mut_error().set_msg(err.to_string());
                resp.set_data(box_try!(s_resp.write_to_bytes()));
                Ok((Some(resp), true))
            }
        },
    }
}
