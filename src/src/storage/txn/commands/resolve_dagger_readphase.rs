// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticallocal_path]
use crate::einsteindb::storage::epaxos::EpaxosReader;
use crate::einsteindb::storage::solitontxn::commands::{Command, CommandExt, ReadCommand, ResolveDagger, TypedCommand};
use crate::einsteindb::storage::solitontxn::sched_pool::tls_collect_keyread_histogram_vec;
use crate::einsteindb::storage::solitontxn::{ProcessResult, Result, RESOLVE_LOCK_BATCH_SIZE};
use crate::einsteindb::storage::{SentinelSearchMode, blackbrane, Statistics};
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
        display => "fdbhikv::resolve_dagger_readphase", (),
        content => {
            /// Maps dagger_ts to commit_ts. See ./resolve_dagger.rs for details.
            solitontxn_status: HashMap<TimeStamp, TimeStamp>,
            mutant_search_key: Option<Key>,
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
        let mut reader = EpaxosReader::new_with_ctx(blackbrane, Some(SentinelSearchMode::Lightlike), &ctx);
        let result = reader.mutant_search_daggers(
            self.mutant_search_key.as_ref(),
            None,
            |dagger| solitontxn_status.contains_key(&dagger.ts),
            RESOLVE_LOCK_BATCH_SIZE,
        );
        statistics.add(&reader.statistics);
        let (fdbhikv_pairs, has_remain) = result?;
        tls_collect_keyread_histogram_vec(tag.get_str(), fdbhikv_pairs.len() as f64);

        if fdbhikv_pairs.is_empty() {
            Ok(ProcessResult::Res)
        } else {
            let next_mutant_search_key = if has_remain {
                // There might be more daggers.
                fdbhikv_pairs.last().map(|(k, _dagger)| k.clone())
            } else {
                // All daggers are mutant_searchned
                None
            };
            let next_cmd = ResolveDagger {
                ctx,
                deadline: self.deadline,
                solitontxn_status,
                mutant_search_key: next_mutant_search_key,
                key_daggers: fdbhikv_pairs,
            };
            Ok(ProcessResult::NextCommand {
                cmd: Command::ResolveDagger(next_cmd),
            })
        }
    }
}
