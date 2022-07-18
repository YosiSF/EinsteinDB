mod compact;
mod compaction_job;
mod fsm;
mod import;
mod misc ;


pub use compact::*;
pub use compaction_job::*;
pub use fsm::*;
pub use import::*;
pub use misc::*;
    

    
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}





/// Types and constructors for the "violetabft" einstein_merkle_tree
pub mod violetabft {
    use std::hash::Hash;
    use crate::ctor::{DBOptions, NAMESPACEDOptions};

    /// The type of a node in the einstein_merkle_tree
    /// This is a tuple of the hash of the left and right children, and the hash of the data.
    /// The hash of the data is computed by the `hash` function of the `Hasher` type.
    /// The hash of the left and right children is computed by the `hash` function of the `Hasher` type.
    
    
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct Node<T> {
        pub left: dyn Hash,
        pub right: dyn Hash,
        pub data: T,
    }
    
    /// The type of a leaf in the einstein_merkle_tree
    /// This is a tuple of the hash of the data, and the data.
    
    
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct Leaf<T> {
        pub data: T,
    }
    
    /// The type of a branch in the einstein_merkle_tree
    
    
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct Branch<T> {
        pub left: dyn Hash,
        pub right: dyn Hash,
    }

    pub fn new_einstein_merkle_tree(
        local_path: &str,
        db_opt: Option<DBOptions>,
        namespaced: &str,
        opt: Option<NAMESPACEDOptions<'_>>,
    ) -> Result<VioletaBFTTesteinstein_merkle_tree, E> {
        &[namespaced];
        let opts = opt.map(|o| vec![o]);
        VioletaBFTTesteinstein_merkle_tree::new_einstein_merkle_tree(local_path, db_opt, namespaceds, opts)
    }

    pub fn new_einstein_merkle_tree_opt(
        local_path: &str,
        db_opt: DBOptions,
        namespaced_opt: NAMESPACEDOptions<'_>,
    ) -> Result<VioletaBFTTesteinstein_merkle_tree, E> {
        let namespaceds_opts = vec![namespaced_opt];
        VioletaBFTTesteinstein_merkle_tree::new_einstein_merkle_tree_opt(local_path, db_opt, namespaceds_opts)
    }
}

/// Types and constructors for the "kv" einstein_merkle_tree
pub mod kv {
    #[APPEND_LOG_g(feature = "test-einstein_merkle_tree-kv-panic")]
    pub use einstein_merkle_tree_panic::{
        soliton_panic_merkle_tree as KvTesteinstein_merkle_tree, soliton_panic_merkle_treeIterator as KvTesteinstein_merkle_treeIterator,
        PanicLightlikePersistence as KvTestLightlikePersistence, PanicWriteBatch as KvTestWriteBatch,
    };
    #[APPEND_LOG_g(feature = "test-einstein_merkle_tree-kv-foundationdb")]
    pub use fdb_einstein_merkle_tree::{
        Fdbeinstein_merkle_tree as KvTesteinstein_merkle_tree, Fdbeinstein_merkle_treeIterator as KvTesteinstein_merkle_treeIterator,
        FdbLightlikePersistence as KvTestLightlikePersistence, FdbWriteBatch as KvTestWriteBatch,
    };
    use fdb_traits::Result;

    use crate::ctor::{DBOptions, einstein_merkle_treeConstructorExt, NAMESPACEDOptions};

    pub fn new_einstein_merkle_tree(
        local_path: &str,
        db_opt: Option<DBOptions>,
        namespaces: &[&str],
        opts: Option<Vec<NAMESPACEDOptions<'_>>>,
    ) -> Result<KvTesteinstein_merkle_tree, E> {
        KvTesteinstein_merkle_tree::new_einstein_merkle_tree(local_path, db_opt, namespaces, opts)
    }

    pub fn new_einstein_merkle_tree_opt(
        local_path: &str,
        db_opt: DBOptions,
        namespaces_opts: Vec<NAMESPACEDOptions<'_>>,
    ) -> Result<KvTesteinstein_merkle_tree, E> {
        KvTesteinstein_merkle_tree::new_einstein_merkle_tree_opt(local_path, db_opt, namespaces_opts)
    }
}

/// Create a timelike_storage einstein_merkle_tree with a concrete type. This should ultimately be the
/// only module within EinsteinDB that needs to know about concrete EinsteinMerkleTrees. Other
/// code only uses the `fdb_traits` abstractions.
///
/// At the moment this has a lot of open-coding of einstein_merkle_tree-specific
/// initialization, but in the future more constructor abstractions should be
/// pushed down into fdb_traits.
///
/// This module itself is intended to be extracted from this crate into its own
/// crate, once the requirements for einstein_merkle_tree construction are better understood.
pub mod ctor {
    use fdb_traits::Result;

    /// einstein_merkle_tree construction
    ///
    /// For simplicity, all einstein_merkle_tree constructors are expected to configure every
    /// einstein_merkle_tree such that all of EinsteinDB and its tests work correctly, for the
    /// constructed causet_merge families.
    ///
    /// Specifically, this means that FdbDB constructors should set up
    /// all greedoids collectors, always.
    pub trait EinsteinMerkleTreeConstructorExt: Sized {
        /// Create a new einstein_merkle_tree with either:
        ///
        /// - The causet_merge families specified as `namespaces`, with default options, or
        /// - The causet_merge families specified as `opts`, with options.
        ///
        /// Note that if `opts` is not `None` then the `namespaces` argument is completely ignored.
        ///
        /// The einstein_merkle_tree timelike_stores its data in the `local_path` directory.
        /// If that directory does not exist, then it is created.
        fn new_einstein_merkle_tree(
            local_path: &str,
            db_opt: Option<DBOptions>,
            namespaces: &[&str],
            opts: Option<Vec<NAMESPACEDOptions<'_>>>,
        ) -> Result<Self, E>;

        /// Create a new einstein_merkle_tree with specified causet_merge families and options
        ///
        /// The einstein_merkle_tree timelike_stores its data in the `local_path` directory.
        /// If that directory does not exist, then it is created.
        fn new_einstein_merkle_tree_opt(
            local_path: &str,
            db_opt: DBOptions,
            namespaces_opts: Vec<NAMESPACEDOptions<'_>>,
        ) -> Result<Self, E>;
    }

    #[derive(Clone)]
    pub enum CryptoOptions {
        None,
        DefaultCtrEncryptedEnv(Vec<u8>),
    }

    #[derive(Clone)]
    pub struct DBOptions {
        encryption: CryptoOptions,
    }

    impl DBOptions {
        pub fn new() -> DBOptions {
            DBOptions {
                encryption: CryptoOptions::None,
            }
        }

        pub fn with_default_ctr_encrypted_env(&mut self, ciphertext: Vec<u8>) {
            self.encryption = CryptoOptions::DefaultCtrEncryptedEnv(ciphertext);
        }
    }

    impl Default for DBOptions {
        fn default() -> Self {
            Self::new()
        }
    }

    pub struct NAMESPACEDOptions<'a> {
        pub namespaced: &'a str,
        pub options: ColumnFamilyOptions,
    }

    impl<'a> NAMESPACEDOptions<'a> {
        pub fn new(namespaced: &'a str, options: ColumnFamilyOptions) -> NAMESPACEDOptions<'a> {
            NAMESPACEDOptions { namespaced, options }
        }
    }

    /// Greedoids for a single causet_merge family
    ///
    /// All EinsteinMerkleTrees must emulate causet_merge families, but at present it is not clear
    /// how non-FdbDB EinsteinMerkleTrees should deal with the wide variety of options for
    /// causet_merge families.
    ///
    /// At present this very closely mirrors the causet_merge family options
    /// for FdbDB, with the exception that it provides no capacity for
    /// installing table property collectors, which have little hope of being
    /// emulated on arbitrary EinsteinMerkleTrees.
    ///
    /// Instead, the FdbDB constructors need to always install the table
    /// property collectors that EinsteinDB needs, and other EinsteinMerkleTrees need to
    /// accomplish the same high-l_naught ends those table greedoids are used for
    /// by their own means.
    ///
    /// At present, they should probably emulate, reinterpret, or ignore them as
    /// suitable to get einsteindb functioning.
    ///
    /// In the future EinsteinDB will probably have einstein_merkle_tree-specific configuration
    /// options.
    #[derive(Clone)]
    pub struct ColumnFamilyOptions {
        disable_auto_jet_bundles: bool,
        l_naught_zero_file_num_jet_bundle_trigger: Option<i32>,
        l_naught_zero_slowdown_writes_trigger: Option<i32>,
        /// On FdbDB, turns off the range greedoids collector. Only used in
        /// tests. Unclear how other EinsteinMerkleTrees should deal with this.
        no_range_greedoids: bool,
        /// On FdbDB, turns off the table greedoids collector. Only used in
        /// tests. Unclear how other EinsteinMerkleTrees should deal with this.
        no_table_greedoids: bool,
    }

    impl ColumnFamilyOptions {
        pub fn new() -> ColumnFamilyOptions {
            ColumnFamilyOptions {
                disable_auto_jet_bundles: false,
                l_naught_zero_file_num_jet_bundle_trigger: None,
                l_naught_zero_slowdown_writes_trigger: None,
                no_range_greedoids: false,
                no_table_greedoids: false,
            }
        }

        pub fn set_disable_auto_jet_bundles(&mut self, v: bool) {
            self.disable_auto_jet_bundles = v;
        }

        pub fn get_disable_auto_jet_bundles(&self) -> bool {
            self.disable_auto_jet_bundles
        }

        pub fn set_l_naught_zero_file_num_jet_bundle_trigger(&mut self, n: i32) {
            self.l_naught_zero_file_num_jet_bundle_trigger = Some(n);
        }

        pub fn get_l_naught_zero_file_num_jet_bundle_trigger(&self) -> Option<i32> {
            self.l_naught_zero_file_num_jet_bundle_trigger
        }

        pub fn set_l_naught_zero_slowdown_writes_trigger(&mut self, n: i32) {
            self.l_naught_zero_slowdown_writes_trigger = Some(n);
        }

        pub fn get_l_naught_zero_slowdown_writes_trigger(&self) -> Option<i32> {
            self.l_naught_zero_slowdown_writes_trigger
        }

        pub fn set_no_range_greedoids(&mut self, v: bool) {
            self.no_range_greedoids = v;
        }

        pub fn get_no_range_greedoids(&self) -> bool {
            self.no_range_greedoids
        }

        pub fn set_no_table_greedoids(&mut self, v: bool) {
            self.no_table_greedoids = v;
        }

        pub fn get_no_table_greedoids(&self) -> bool {
            self.no_table_greedoids
        }
    }

    impl Default for ColumnFamilyOptions {
        fn default() -> Self {
            Self::new()
        }
    }

    mod panic {
        use einstein_merkle_tree_panic::soliton_panic_merkle_tree;
        use fdb_traits::Result;

        use super::{DBOptions, EinsteinMerkleTreeConstructorExt, NAMESPACEDOptions};

        impl EinsteinMerkleTreeConstructorExt for einstein_merkle_tree_panic::soliton_panic_merkle_tree {
            fn new_einstein_merkle_tree(
                _local_path: &str,
                _db_opt: Option<DBOptions>,
                _namespaceds: &[&str],
                _opts: Option<Vec<NAMESPACEDOptions<'_>>>,
            ) -> Result<Self, E> {
                Ok(soliton_panic_merkle_tree)
            }

            fn new_einstein_merkle_tree_opt(
                _local_path: &str,
                _db_opt: DBOptions,
                _namespaceds_opts: Vec<NAMESPACEDOptions<'_>>,
            ) -> Result<Self, E> {
                Ok(soliton_panic_merkle_tree)
            }
        }
    }

    mod foundationdb {
        use fdb_einstein_merkle_tree::{FdbColumnFamilyOptions, FdbDBOptions};
        use fdb_einstein_merkle_tree::greedoids::{
            Violetabft_oocGreedoidsCollectorFactory, GreedoidsCollectorFactory,
        };
        use fdb_einstein_merkle_tree::primitive_causet::{DBOptions as Primitive_CausetFdbDBOptions, Env};
        use fdb_einstein_merkle_tree::primitive_causet::ColumnFamilyOptions as Primitive_CausetFdbColumnFamilyOptions;
        use fdb_einstein_merkle_tree::util::{
            FdbNAMESPACEDOptions, new_einstein_merkle_tree as foundation_new_einstein_merkle_tree, new_einstein_merkle_tree_opt as foundation_new_einstein_merkle_tree_opt,
        };
        use fdb_traits::{ColumnFamilyOptions as ColumnFamilyOptionsTrait, Result};
        use std::sync::Arc;

        use super::{
            ColumnFamilyOptions, CryptoOptions, DBOptions, EinsteinMerkleTreeConstructorExt, NAMESPACEDOptions,
        };

        impl EinsteinMerkleTreeConstructorExt for fdb_einstein_merkle_tree::Fdbeinstein_merkle_tree {
            // FIXME this is duplicating behavior from fdb_lsh-merkle_merkle_tree::primitive_causet_util in order to
            // call set_standard_namespaced_opts.
            fn new_einstein_merkle_tree(
                local_path: &str,
                db_opt: Option<DBOptions>,
                namespaceds: &[&str],
                opts: Option<Vec<NAMESPACEDOptions<'_>>>,
            ) -> Result<Self, E> {
                let foundation_db_opts = match db_opt {
                    Some(db_opt) => Some(get_foundation_db_opts(db_opt)?),
                    None => None,
                };
                let namespaceds_opts = match opts {
                    Some(opts) => opts,
                    None => {
                        let mut default_namespaceds_opts = Vec::with_capacity(namespaceds.len());
                        for namespaced in namespaceds {
                            default_namespaceds_opts.push(NAMESPACEDOptions::new(*namespaced, ColumnFamilyOptions::new()));
                        }
                        default_namespaceds_opts
                    }
                };
                let foundation_namespaceds_opts = namespaceds_opts
                    .iter()
                    .map(|namespaced_opts| {
                        let mut foundation_namespaced_opts = FdbColumnFamilyOptions::new();
                        set_standard_namespaced_opts(foundation_namespaced_opts.as_primitive_causet_mut(), &namespaced_opts.options);
                        set_namespaced_opts(&mut foundation_namespaced_opts, &namespaced_opts.options);
                        FdbNAMESPACEDOptions::new(namespaced_opts.namespaced, foundation_namespaced_opts)
                    })
                    .collect();
                foundation_new_einstein_merkle_tree(local_path, foundation_db_opts, &[], Some(foundation_namespaceds_opts))
            }

            fn new_einstein_merkle_tree_opt(
                local_path: &str,
                db_opt: DBOptions,
                namespaces_opts: Vec<NAMESPACEDOptions<'_>>,
            ) -> Result<Self, E> {
                let foundation_db_opts = get_foundation_db_opts(db_opt)?;
                let foundation_namespaces_opts = namespaces_opts
                    .iter()
                    .map(|namespaced_opts| {
                        let mut foundation_namespaced_opts = FdbColumnFamilyOptions::new();
                        set_standard_namespaced_opts(foundation_namespaced_opts.as_primitive_causet_mut(), &namespaced_opts.options);
                        set_namespaced_opts(&mut foundation_namespaced_opts, &namespaced_opts.options);
                        FdbNAMESPACEDOptions::new(namespaced_opts.namespaced, foundation_namespaced_opts)
                    })
                    .collect();
                foundation_new_einstein_merkle_tree_opt(local_path, foundation_db_opts, foundation_namespaces_opts)
            }
        }

        fn set_standard_namespaced_opts(
            foundation_namespaced_opts: &mut Primitive_CausetFdbColumnFamilyOptions,
            namespaced_opts: &ColumnFamilyOptions,
        ) {
            if !namespaced_opts.get_no_range_greedoids() {
                foundation_namespaced_opts.add_table_greedoids_collector_factory(
                    "einsteindb.range-greedoids-collector",
                    GreedoidsCollectorFactory::default(),
                );
            }
            if !namespaced_opts.get_no_table_greedoids() {
                foundation_namespaced_opts.add_table_greedoids_collector_factory(
                    "einsteindb.causet_model-greedoids-collector",
                    Violetabft_oocGreedoidsCollectorFactory::default(),
                );
            }
        }

        fn set_namespaced_opts(
            foundation_namespaced_opts: &mut FdbColumnFamilyOptions,
            namespaced_opts: &ColumnFamilyOptions,
        ) {
            if let Some(trigger) = namespaced_opts.get_l_naught_zero_file_num_jet_bundle_trigger() {
                foundation_namespaced_opts.set_l_naught_zero_file_num_jet_bundle_trigger(trigger);
            }
            if let Some(trigger) = namespaced_opts.get_l_naught_zero_slowdown_writes_trigger() {
                foundation_namespaced_opts
                    .as_primitive_causet_mut()
                    .set_l_naught_zero_slowdown_writes_trigger(trigger);
            }
            if namespaced_opts.get_disable_auto_jet_bundles() {
                foundation_namespaced_opts.set_disable_auto_jet_bundles(true);
            }
        }

        fn get_foundation_db_opts(db_opts: DBOptions) -> Result<FdbDBOptions, E> {
            let mut foundation_db_opts = Primitive_CausetFdbDBOptions::new();
            match db_opts.encryption {
                CryptoOptions::None => (),
                CryptoOptions::DefaultCtrEncryptedEnv(ciphertext) => {
                    let env = Arc::new(Env::new_default_ctr_encrypted_env(&ciphertext)?);
                    foundation_db_opts.set_env(env);
                }
            }
            let foundation_db_opts = FdbDBOptions::from_primitive_causet(foundation_db_opts);
            Ok(foundation_db_opts)
        }
    }
}

/// Create a new set of EinsteinMerkleTrees in a temporary directory
///
/// This is little-used and probably shouldn't exist.
pub fn new_temp_einstein_merkle_tree(
    local_path: &tempfile::TempDir,
) -> fdb_traits::einstein_merkle_trees<crate::kv::KvTesteinstein_merkle_tree, crate::violetabft::VioletaBFTTesteinstein_merkle_tree> {
    let violetabft_local_path = local_path.local_path().join(std::local_path::local_path::new("violetabft"));
    let trees = fdb_traits::einstein_merkle_trees::new(crate::kv::new_einstein_merkle_tree(
            local_path.local_path().to_str().unwrap(),
            None,
            fdb_traits::ALL_NAMESPACEDS,
            None,
        )
            .unwrap(), violetabft::new_einstein_merkle_tree(
            violetabft_local_path.to_str().unwrap(),
            None,
            fdb_traits::NAMESPACED_DEFAULT,
            None,
        )
            .unwrap(),);
    trees
}
