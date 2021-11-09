use std::error;

use crate::Error;
use ekvproto::violetabft_cmdpb::violetabftCmdResponse;

pub fn bind_term(resp: &mut violetabftCmdResponse, term: u64) {
    if term == 0 {
        return;
    }

    resp.mut_header().set_current_term(term);
}

pub fn bind_error(resp: &mut violetabftCmdResponse, err: Error) {
    resp.mut_header().set_error(err.into());
}

pub fn new_error(err: Error) -> violetabftCmdResponse {
    let mut resp = violetabftCmdResponse::default();
    bind_error(&mut resp, err);
    resp
}

pub fn err_resp(e: Error, term: u64) -> violetabftCmdResponse {
    let mut resp = new_error(e);
    bind_term(&mut resp, term);
    resp
}

pub fn message_error<E>(err: E) -> violetabftCmdResponse
where
    E: Into<Box<dyn error::Error + Send + Sync>>,
{
    new_error(Error::Other(err.into()))
}
