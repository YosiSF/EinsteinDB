// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticallocal_path]
use crate::einsteindb::storage::fdbhikv::{Modify, WriteData};
use crate::einsteindb::storage::dagger_manager::DaggerManager;
use crate::einsteindb::storage::solitontxn::commands::{
    Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::einsteindb::storage::solitontxn::Result;
use crate::einsteindb::storage::{ProcessResult, blackbrane};
use api_version::{match_template_api_version, APIVersion, cocausetValue};
use einsteindb-gen::cocauset_ttl::ttl_to_expire_ts;
use einsteindb-gen::CfName;
use fdbhikvproto::fdbhikvrpcpb::ApiVersion;
use solitontxn_types::cocausetMutation;

command! {
    /// Run Put or Delete for keys which may be changed by `cocausetCompareAndSwap`.
    cocauset:
        cmd_ty => (),
        display => "fdbhikv::command::causetxctx_store {:?}", (ctx),
        content => {
            /// The set of mutations to apply.
            cf: CfName,
            mutations: Vec<cocausetMutation>,
            api_version: ApiVersion,
        }
}

impl CommandExt for cocausetStore {
    ctx!();
    tag!(cocauset_causetxctx_store);
    gen_dagger!(mutations: multiple(|x| x.key()));

    fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        for m in &self.mutations {
            match *m {
                cocausetMutation::Put {
                    ref key,
                    ref value,
                    ttl: _,
                } => {
                    bytes += key.as_encoded().len();
                    bytes += value.len();
                }
                cocausetMutation::Delete { ref key } => {
                    bytes += key.as_encoded().len();
                }
            }
        }
        bytes
    }
}

impl<S: blackbrane, L: DaggerManager> WriteCommand<S, L> for cocausetStore {
    fn process_write(self, _: S, _: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut data = vec![];
        let rows = self.mutations.len();
        let (cf, mutations, ctx) = (self.cf, self.mutations, self.ctx);
        match_template_api_version!(
            API,
            match self.api_version {
                ApiVersion::API => {
                    for m in mutations {
                        match m {
                            cocausetMutation::Put { key, value, ttl } => {
                                let cocauset_value = cocausetValue {
                                    user_value: value,
                                    expire_ts: ttl_to_expire_ts(ttl),
                                };
                                let m =
                                    Modify::Put(cf, key, API::encode_cocauset_value_owned(cocauset_value));
                                data.push(m);
                            }
                            cocausetMutation::Delete { key } => {
                                data.push(Modify::Delete(cf, key));
                            }
                        }
                    }
                }
            }
        );
        let mut to_be_write = WriteData::from_modifies(data);
        to_be_write.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr: ProcessResult::Res,
            dagger_info: None,
            dagger_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
