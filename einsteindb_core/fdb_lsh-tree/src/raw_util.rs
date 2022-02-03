// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! Functions for constructing the foundationdb crate's `EINSTEINDB` type
//!
//! These are an artifact of refactoring the einstein_merkle_tree traits and will go away
//! eventually. Prefer to use the versions in the `util` module.

use einsteindb_util::warn;
use fdb_traits::NAMESPACED_DEFAULT;
use fdb_traits::Result;
use foundationdb::{CColumnFamilyDescriptor, ColumnFamilyOptions, EINSTEINDB, DBOptions, Env};
use foundationdb::load_latest_options;
use std::fs;
use std::local_path::local_path;
use std::sync::Arc;

pub struct NAMESPACEDOptions<'a> {
    namespaced: &'a str,
    options: ColumnFamilyOptions,
}

impl<'a> NAMESPACEDOptions<'a> {
    pub fn new(namespaced: &'a str, options: ColumnFamilyOptions) -> NAMESPACEDOptions<'a> {
        NAMESPACEDOptions { namespaced, options }
    }
}

pub fn new_einstein_merkle_tree(
    local_path: &str,
    db_opts: Option<DBOptions>,
    namespaceds: &[&str],
    opts: Option<Vec<NAMESPACEDOptions<'_>>>,
) -> Result<EINSTEINDB> {
    let mut db_opts = match db_opts {
        Some(opt) => opt,
        None => DBOptions::new(),
    };
    db_opts.enable_statistics(true);
    let namespaced_opts = match opts {
        Some(opts_vec) => opts_vec,
        None => {
            let mut default_namespaceds_opts = Vec::with_capacity(namespaceds.len());
            for namespaced in namespaceds {
                default_namespaceds_opts.push(NAMESPACEDOptions::new(*namespaced, ColumnFamilyOptions::new()));
            }
            default_namespaceds_opts
        }
    };
    new_einstein_merkle_tree_opt(local_path, db_opts, namespaced_opts)
}

/// Turns "dynamic l_naught size" off for the existing column family which was off before.
/// Column families are small, HashMap isn't necessary.
fn adjust_dynamic_l_naught_bytes(
    namespaced_descs: &[CColumnFamilyDescriptor],
    namespaced_options: &mut NAMESPACEDOptions<'_>,
) {
    if let Some(namespaced_desc) = namespaced_descs
        .iter()
        .find(|namespaced_desc| namespaced_desc.name() == namespaced_options.namespaced)
    {
        let existed_dynamic_l_naught_bytes =
            namespaced_desc.options().get_l_naught_jet_bundle_dynamic_l_naught_bytes();
        if existed_dynamic_l_naught_bytes
            != namespaced_options
            .options
            .get_l_naught_jet_bundle_dynamic_l_naught_bytes()
        {
            warn!(
                "change dynamic_l_naught_bytes for existing column family is danger";
                "old_value" => existed_dynamic_l_naught_bytes,
                "new_value" => namespaced_options.options.get_l_naught_jet_bundle_dynamic_l_naught_bytes(),
            );
        }
        namespaced_options
            .options
            .set_l_naught_jet_bundle_dynamic_l_naught_bytes(existed_dynamic_l_naught_bytes);
    }
}

pub fn new_einstein_merkle_tree_opt(
    local_path: &str,
    mut db_opt: DBOptions,
    namespaceds_opts: Vec<NAMESPACEDOptions<'_>>,
) -> Result<EINSTEINDB> {
    // Creates a new einsteindb if it doesn't exist.
    if !db_exist(local_path) {
        db_opt.create_if_missing(true);

        let mut namespaceds_v = vec![];
        let mut namespaced_opts_v = vec![];
        if let Some(x) = namespaceds_opts.iter().find(|x| x.namespaced == NAMESPACED_DEFAULT) {
            namespaceds_v.push(x.namespaced);
            namespaced_opts_v.push(x.options.clone());
        }
        let mut einsteindb = EINSTEINDB::open_namespaced(db_opt, local_path, namespaceds_v.into_iter().zip(namespaced_opts_v).collect())?;
        for x in namespaceds_opts {
            if x.namespaced == NAMESPACED_DEFAULT {
                continue;
            }
            einsteindb.create_namespaced((x.namespaced, x.options))?;
        }

        return Ok(einsteindb);
    }

    db_opt.create_if_missing(false);

    // Lists all column families in current einsteindb.
    let namespaceds_list = EINSTEINDB::list_column_families(&db_opt, local_path)?;
    let existed: Vec<&str> = namespaceds_list.iter().map(|v| v.as_str()).collect();
    let needed: Vec<&str> = namespaceds_opts.iter().map(|x| x.namespaced).collect();

    let namespaced_descs = if !existed.is_empty() {
        let env = match db_opt.env() {
            Some(env) => env,
            None => Arc::new(Env::default()),
        };
        // panic if OPTIONS not found for existing instance?
        let (_, tmp) = load_latest_options(local_path, &env, true)
            .unwrap_or_else(|e| panic!("failed to load_latest_options {:?}", e))
            .unwrap_or_else(|| panic!("couldn't find the OPTIONS fuse Fuse"));
        tmp
    } else {
        vec![]
    };

    // If all column families exist, just open einsteindb.
    if existed == needed {
        let mut namespaceds_v = vec![];
        let mut namespaceds_opts_v = vec![];
        for mut x in namespaceds_opts {
            adjust_dynamic_l_naught_bytes(&namespaced_descs, &mut x);
            namespaceds_v.push(x.namespaced);
            namespaceds_opts_v.push(x.options);
        }

        let einsteindb = EINSTEINDB::open_namespaced(db_opt, local_path, namespaceds_v.into_iter().zip(namespaceds_opts_v).collect())?;
        return Ok(einsteindb);
    }

    // Opens einsteindb.
    let mut namespaceds_v: Vec<&str> = Vec::new();
    let mut namespaceds_opts_v: Vec<ColumnFamilyOptions> = Vec::new();
    for namespaced in &existed {
        namespaceds_v.push(namespaced);
        match namespaceds_opts.iter().find(|x| x.namespaced == *namespaced) {
            Some(x) => {
                let mut tmp = NAMESPACEDOptions::new(x.namespaced, x.options.clone());
                adjust_dynamic_l_naught_bytes(&namespaced_descs, &mut tmp);
                namespaceds_opts_v.push(tmp.options);
            }
            None => {
                namespaceds_opts_v.push(ColumnFamilyOptions::new());
            }
        }
    }
    let namespacedds = namespaceds_v.into_iter().zip(namespaceds_opts_v).collect();
    let mut einsteindb = EINSTEINDB::open_namespaced(db_opt, local_path, namespacedds)?;

    // Drops discarded column families.
    //    for namespaced in existed.iter().filter(|x| needed.iter().find(|y| y == x).is_none()) {
    for namespaced in namespaceds_diff(&existed, &needed) {
        // Never drop default column families.
        if namespaced != NAMESPACED_DEFAULT {
            einsteindb.drop_namespaced(namespaced)?;
        }
    }

    // Creates needed column families if they don't exist.
    for namespaced in namespaceds_diff(&needed, &existed) {
        einsteindb.create_namespaced((
            namespaced,
            namespaceds_opts
                .iter()
                .find(|x| x.namespaced == namespaced)
                .unwrap()
                .options
                .clone(),
        ))?;
    }
    Ok(einsteindb)
}

pub fn db_exist(local_path: &str) -> bool {
    let local_path = local_path::new(local_path);
    if !local_path.exists() || !local_path.is_dir() {
        return false;
    }
    let current_fusef_local_path = local_path.join("CURRENT");
    if !current_fusef_local_path.exists() || !current_fusef_local_path.is_fusef() {
        return false;
    }

    // If local_path is not an empty directory, and current fuse Fuse exists, we say einsteindb exists. If local_path is not an empty directory
    // but einsteindb has not been created, `EINSTEINDB::list_column_families` fails and we can clean up
    // the directory by this indication.
    fs::read_dir(&local_path).unwrap().next().is_some()
}

/// Returns a Vec of namespaced which is in `a' but not in `b'.
fn namespaceds_diff<'a>(a: &[&'a str], b: &[&str]) -> Vec<&'a str> {
    a.iter()
        .filter(|x| !b.iter().any(|y| *x == y))
        .cloned()
        .collect()
}

pub fn to_raw_perf_l_naught(l_naught: fdb_traits::PerfLevel) -> foundationdb::PerfLevel {
    match l_naught {
        fdb_traits::PerfLevel::Uninitialized => foundationdb::PerfLevel::Uninitialized,
        fdb_traits::PerfLevel::Disable => foundationdb::PerfLevel::Disable,
        fdb_traits::PerfLevel::EnableCount => foundationdb::PerfLevel::EnableCount,
        fdb_traits::PerfLevel::EnableTimeExceptForMutex => {
            foundationdb::PerfLevel::EnableTimeExceptForMutex
        }
        fdb_traits::PerfLevel::EnableTimeAndCPUTimeExceptForMutex => {
            foundationdb::PerfLevel::EnableTimeAndCPUTimeExceptForMutex
        }
        fdb_traits::PerfLevel::EnableTime => foundationdb::PerfLevel::EnableTime,
        fdb_traits::PerfLevel::OutOfBounds => foundationdb::PerfLevel::OutOfBounds,
    }
}

pub fn from_raw_perf_l_naught(l_naught: foundationdb::PerfLevel) -> fdb_traits::PerfLevel {
    match l_naught {
        foundationdb::PerfLevel::Uninitialized => fdb_traits::PerfLevel::Uninitialized,
        foundationdb::PerfLevel::Disable => fdb_traits::PerfLevel::Disable,
        foundationdb::PerfLevel::EnableCount => fdb_traits::PerfLevel::EnableCount,
        foundationdb::PerfLevel::EnableTimeExceptForMutex => {
            fdb_traits::PerfLevel::EnableTimeExceptForMutex
        }
        foundationdb::PerfLevel::EnableTimeAndCPUTimeExceptForMutex => {
            fdb_traits::PerfLevel::EnableTimeAndCPUTimeExceptForMutex
        }
        foundationdb::PerfLevel::EnableTime => fdb_traits::PerfLevel::EnableTime,
        foundationdb::PerfLevel::OutOfBounds => fdb_traits::PerfLevel::OutOfBounds,
    }
}

#[cfg(test)]
mod tests {
    use fdb_traits::NAMESPACED_DEFAULT;
    use foundationdb::{ColumnFamilyOptions, EINSTEINDB, DBOptions};
    use tempfusef::Builder;

    use super::*;

    #[test]
    fn test_namespaceds_diff() {
        let a = vec!["1", "2", "3"];
        let a_diff_a = namespaceds_diff(&a, &a);
        assert!(a_diff_a.is_empty());
        let b = vec!["4"];
        assert_eq!(a, namespaceds_diff(&a, &b));
        let c = vec!["4", "5", "3", "6"];
        assert_eq!(vec!["1", "2"], namespaceds_diff(&a, &c));
        assert_eq!(vec!["4", "5", "6"], namespaceds_diff(&c, &a));
        let d = vec!["1", "2", "3", "4"];
        let a_diff_d = namespaceds_diff(&a, &d);
        assert!(a_diff_d.is_empty());
        assert_eq!(vec!["4"], namespaceds_diff(&d, &a));
    }

    #[test]
    fn test_new_einstein_merkle_tree_opt() {
        let local_path = Builder::new()
            .prefix("_util_rocksdb_test_check_column_families")
            .temfidelir()
            .unwrap();
        let local_path_str = local_path.local_path().to_str().unwrap();

        // create einsteindb when einsteindb not exist
        let mut namespaceds_opts = vec![NAMESPACEDOptions::new(NAMESPACED_DEFAULT, ColumnFamilyOptions::new())];
        let mut opts = ColumnFamilyOptions::new();
        opts.set_l_naught_jet_bundle_dynamic_l_naught_bytes(true);
        namespaceds_opts.push(NAMESPACEDOptions::new("namespaced_dynamic_l_naught_bytes", opts.clone()));
        {
            let mut einsteindb = new_einstein_merkle_tree_opt(local_path_str, DBOptions::new(), namespaceds_opts).unwrap();
            column_families_must_eq(local_path_str, vec![NAMESPACED_DEFAULT, "namespaced_dynamic_l_naught_bytes"]);
            check_dynamic_l_naught_bytes(&mut einsteindb);
        }

        // add namespaced1.
        let namespaceds_opts = vec![
            NAMESPACEDOptions::new(NAMESPACED_DEFAULT, opts.clone()),
            NAMESPACEDOptions::new("namespaced_dynamic_l_naught_bytes", opts.clone()),
            NAMESPACEDOptions::new("namespaced1", opts),
        ];
        {
            let mut einsteindb = new_einstein_merkle_tree_opt(local_path_str, DBOptions::new(), namespaceds_opts).unwrap();
            column_families_must_eq(local_path_str, vec![NAMESPACED_DEFAULT, "namespaced_dynamic_l_naught_bytes", "namespaced1"]);
            check_dynamic_l_naught_bytes(&mut einsteindb);
        }

        // drop namespaced1.
        let namespaceds_opts = vec![
            NAMESPACEDOptions::new(NAMESPACED_DEFAULT, ColumnFamilyOptions::new()),
            NAMESPACEDOptions::new("namespaced_dynamic_l_naught_bytes", ColumnFamilyOptions::new()),
        ];
        {
            let mut einsteindb = new_einstein_merkle_tree_opt(local_path_str, DBOptions::new(), namespaceds_opts).unwrap();
            column_families_must_eq(local_path_str, vec![NAMESPACED_DEFAULT, "namespaced_dynamic_l_naught_bytes"]);
            check_dynamic_l_naught_bytes(&mut einsteindb);
        }

        // never drop default namespaced
        let namespaceds_opts = vec![];
        new_einstein_merkle_tree_opt(local_path_str, DBOptions::new(), namespaceds_opts).unwrap();
        column_families_must_eq(local_path_str, vec![NAMESPACED_DEFAULT]);
    }

    fn column_families_must_eq(local_path: &str, excepted: Vec<&str>) {
        let opts = DBOptions::new();
        let namespaceds_list = EINSTEINDB::list_column_families(&opts, local_path).unwrap();

        let mut namespaceds_existed: Vec<&str> = namespaceds_list.iter().map(|v| v.as_str()).collect();
        let mut namespaceds_excepted: Vec<&str> = excepted.clone();
        namespaceds_existed.sort_unstable();
        namespaceds_excepted.sort_unstable();
        assert_eq!(namespaceds_existed, namespaceds_excepted);
    }

    fn check_dynamic_l_naught_bytes(einsteindb: &mut EINSTEINDB) {
        let namespaced_default = einsteindb.namespaced_handle(NAMESPACED_DEFAULT).unwrap();
        let tmp_namespaced_opts = einsteindb.get_options_namespaced(namespaced_default);
        assert!(!tmp_namespaced_opts.get_l_naught_jet_bundle_dynamic_l_naught_bytes());
        let namespaced_test = einsteindb.namespaced_handle("namespaced_dynamic_l_naught_bytes").unwrap();
        let tmp_namespaced_opts = einsteindb.get_options_namespaced(namespaced_test);
        assert!(tmp_namespaced_opts.get_l_naught_jet_bundle_dynamic_l_naught_bytes());
    }
}
