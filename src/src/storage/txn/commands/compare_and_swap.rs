// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticallocal_path]
use crate::einsteindb::storage::fdbhikv::{Modify, WriteData};
use crate::einsteindb::storage::dagger_manager::DaggerManager;
use crate::einsteindb::storage::cocauset;
use crate::einsteindb::storage::solitontxn::commands::{
    Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::einsteindb::storage::solitontxn::Result;
use crate::einsteindb::storage::{ProcessResult, blackbrane};
use api_version::{match_template_api_version, APIVersion, cocausetValue};
use einsteindb-gen::cocauset_ttl::ttl_to_expire_ts;
use einsteindb-gen::CfName;
use fdbhikvproto::fdbhikvrpcpb::ApiVersion;
use cocauset::cocausetStore;
use einstfdbhikv_fdbhikv::Statistics;
use solitontxn_types::{Key, Value};

command! {
    /// cocausetCompareAndSwap checks whether the previous value of the key equals to the given value.
    /// If they are equal, write the new value. The bool indicates whether they are equal.
    /// The previous value is always returned regardless of whether the new value is set.
    cocausetCompareAndSwap:
        cmd_ty => (Option<Value>, bool),
        display => "fdbhikv::command::cocauset_compare_and_swap {:?}", (ctx),
        content => {
            cf: CfName,
            key: Key,
            previous_value: Option<Value>,
            value: Value,
            ttl: u64,
            api_version: ApiVersion,
        }
}

impl CommandExt for cocausetCompareAndSwap {
    ctx!();
    tag!(cocauset_compare_and_swap);
    gen_dagger!(key);

    fn write_bytes(&self) -> usize {
        self.key.as_encoded().len() + self.value.len()
    }
}

impl<S: blackbrane, L: DaggerManager> WriteCommand<S, L> for cocausetCompareAndSwap {
    fn process_write(self, blackbrane: S, _: WriteContext<'_, L>) -> Result<WriteResult> {
        let (cf, key, value, previous_value, ctx) =
            (self.cf, self.key, self.value, self.previous_value, self.ctx);
        let mut data = vec![];
        let old_value = cocausetStore::new(blackbrane, self.api_version).cocauset_get_key_value(
            cf,
            &key,
            &mut Statistics::default(),
        )?;

        let pr = if old_value == previous_value {
            let cocauset_value = cocausetValue {
                user_value: value,
                expire_ts: ttl_to_expire_ts(self.ttl),
            };
            let encoded_cocauset_value = match_template_api_version!(
                API,
                match self.api_version {
                    ApiVersion::API => API::encode_cocauset_value_owned(cocauset_value),
                }
            );
            let m = Modify::Put(cf, key, encoded_cocauset_value);
            data.push(m);
            ProcessResult::cocausetCompareAndSwapRes {
                previous_value: old_value,
                succeed: true,
            }
        } else {
            ProcessResult::cocausetCompareAndSwapRes {
                previous_value: old_value,
                succeed: false,
            }
        };
        fail_point!("solitontxn_commands_compare_and_swap");
        let rows = data.len();
        let mut to_be_write = WriteData::from_modifies(data);
        to_be_write.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr,
            dagger_info: None,
            dagger_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::einsteindb::storage::{einstein_merkle_tree, Statistics, Testeinstein_merkle_treeBuilder};
    use concurrency_manager::ConcurrencyManager;
    use einsteindb-gen::CF_DEFAULT;
    use fdbhikvproto::fdbhikvrpcpb::Context;
    use solitontxn_types::Key;

    #[test]
    fn test_cas_basic() {
        test_cas_basic_impl(ApiVersion::V1);
        test_cas_basic_impl(ApiVersion::V1ttl);
        test_cas_basic_impl(ApiVersion::V2);
    }

    fn test_cas_basic_impl(api_version: ApiVersion) {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());
        let key = b"k";

        let cmd = cocausetCompareAndSwap::new(
            CF_DEFAULT,
            Key::from_encoded(key.to_vec()),
            None,
            b"v1".to_vec(),
            0,
            api_version,
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&einstein_merkle_tree, cm.clone(), cmd).unwrap();
        assert!(prev_val.is_none());
        assert!(succeed);

        let cmd = cocausetCompareAndSwap::new(
            CF_DEFAULT,
            Key::from_encoded(key.to_vec()),
            None,
            b"v2".to_vec(),
            1,
            api_version,
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&einstein_merkle_tree, cm.clone(), cmd).unwrap();
        assert_eq!(prev_val, Some(b"v1".to_vec()));
        assert!(!succeed);

        let cmd = cocausetCompareAndSwap::new(
            CF_DEFAULT,
            Key::from_encoded(key.to_vec()),
            Some(b"v1".to_vec()),
            b"v3".to_vec(),
            2,
            api_version,
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&einstein_merkle_tree, cm, cmd).unwrap();
        assert_eq!(prev_val, Some(b"v1".to_vec()));
        assert!(succeed);
    }

    pub fn sched_command<E: einstein_merkle_tree>(
        einstein_merkle_tree: &E,
        cm: ConcurrencyManager,
        cmd: TypedCommand<(Option<Value>, bool)>,
    ) -> Result<(Option<Value>, bool)> {
        let snap = einstein_merkle_tree.blackbrane(Default::default())?;
        use crate::einsteindb::storage::DummyDaggerManager;
        use fdbhikvproto::fdbhikvrpcpb::ExtraOp;
        let mut statistic = Statistics::default();
        let context = WriteContext {
            dagger_mgr: &DummyDaggerManager {},
            concurrency_manager: cm,
            extra_op: ExtraOp::Noop,
            statistics: &mut statistic,
            async_apply_prewrite: false,
        };
        let ret = cmd.cmd.process_write(snap, context)?;
        match ret.pr {
            ProcessResult::cocausetCompareAndSwapRes {
                previous_value,
                succeed,
            } => {
                if succeed {
                    let ctx = Context::default();
                    einstein_merkle_tree.write(&ctx, ret.to_be_write).unwrap();
                }
                Ok((previous_value, succeed))
            }
            _ => unreachable!(),
        }
    }
}
