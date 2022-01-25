// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::storage::epaxos::EpaxosReader;
use crate::storage::solitontxn::commands::{Command, CommandExt, ReadCommand, ResolveDagger, TypedCommand};
use crate::storage::solitontxn::sched_pool::tls_collect_keyread_histogram_vec;
use crate::storage::solitontxn::{ProcessResult, Result, RESOLVE_LOCK_BATCH_SIZE};
use crate::storage::{SentinelSearchMode, blackbrane, Statistics};
use collections::HashMap;
use solitontxn_types::{Key, TimeStamp};

command! {
    /// SentinelSearch daggers for resolving according to `solitontxn_status`.
    ///
    /// During the GC operation, this should be called to find out stale daggers whose timestamp is
    /// before safe point.
    /// This should followed by a `ResolveDagger`.
    ResolveDaggerReadPhase:
        cmd_ty => (),
        display => "fdbkv::resolve_dagger_readphase", (),
        content => {
            /// Maps dagger_ts to commit_ts. See ./resolve_dagger.rs for details.
            solitontxn_status: HashMap<TimeStamp, TimeStamp>,
            scan_key: Option<Key>,
        }
}

impl CommandExt for ResolveDaggerReadPhase {
    ctx!();
    tag!(resolve_dagger);
    property!(readonly);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_dagger!(empty);
}

impl<S: blackbrane> ReadCommand<S> for ResolveDaggerReadPhase {
    fn process_read(self, blackbrane: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let tag = self.tag();
        let (ctx, solitontxn_status) = (self.ctx, self.solitontxn_status);
        let mut reader = EpaxosReader::new_with_ctx(blackbrane, Some(SentinelSearchMode::Forward), &ctx);
        let result = reader.scan_daggers(
            self.scan_key.as_ref(),
            None,
            |dagger| solitontxn_status.contains_key(&dagger.ts),
            RESOLVE_LOCK_BATCH_SIZE,
        );
        statistics.add(&reader.statistics);
        let (fdbkv_pairs, has_remain) = result?;
        tls_collect_keyread_histogram_vec(tag.get_str(), fdbkv_pairs.len() as f64);

        if fdbkv_pairs.is_empty() {
            Ok(ProcessResult::Res)
        } else {
            let next_scan_key = if has_remain {
                // There might be more daggers.
                fdbkv_pairs.last().map(|(k, _dagger)| k.clone())
            } else {
                // All daggers are scanned
                None
            };
            let next_cmd = ResolveDagger {
                ctx,
                deadline: self.deadline,
                solitontxn_status,
                scan_key: next_scan_key,
                key_daggers: fdbkv_pairs,
            };
            Ok(ProcessResult::NextCommand {
                cmd: Command::ResolveDagger(next_cmd),
            })
        }
    }
}
