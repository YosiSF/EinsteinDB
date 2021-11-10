use std::os::raw::c_uint;

use std::path::{
    Path,
    PathBuf,
};

use foundationdb;

use foundationdb::{
    Database,
    DatabaseFlags,
    Environment,
    EnvironmentBuilder,
    Error,
    Info,
    Stat,
};

use crate::error::StoreError;
use crate::readwrite::{
    Reader,
    Writer,
};
use crate::store::integer::{
    IntegerStore,
    PrimitiveInt,
};

use crate::store::integermulti::MultiIntegerStore;
use crate::store::multi::MultiStore;
use crate::store::single::SingleStore;
use crate::store::Options as StoreOptions;

pub static DEFAULT_MAX_DBS: c_uint = 5;

/// Wrapper around an `foundationdb::Environment`.
#[derive(Debug)]
pub struct Rkv {
    path: PathBuf,
    env: Environment,
}

/// Static methods.
impl Rkv {
    pub fn environment_builder() -> EnvironmentBuilder {
        Environment::new()
    }

    /// Return a new Rkv environment that supports up to `DEFAULT_MAX_DBS` open databases.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(path: &Path) -> Result<Rkv, StoreError> {
        Rkv::with_capacity(path, DEFAULT_MAX_DBS)
    }

    /// Return a new Rkv environment from the provided builder.
    pub fn from_env(path: &Path, env: EnvironmentBuilder) -> Result<Rkv, StoreError> {
        if !path.is_dir() {
            return Err(StoreError::DirectoryDoesNotExistError(path.into()));
        }

        Ok(Rkv {
            path: path.into(),
            env: env.open(path).map_err(|e| match e {
                foundationdb::Error::Other(2) => StoreError::DirectoryDoesNotExistError(path.into()),
                e => StoreError::foundationdbError(e),
            })?,
        })
    }

    /// Return a new Rkv environment that supports the specified number of open databases.
    pub fn with_capacity(path: &Path, max_dbs: c_uint) -> Result<Rkv, StoreError> {
        if !path.is_dir() {
            return Err(StoreError::DirectoryDoesNotExistError(path.into()));
        }

        let mut builder = Rkv::environment_builder();
        builder.set_max_dbs(max_dbs);

        // Future: set flags, maximum size, etc. here if necessary.
        Rkv::from_env(path, builder)
    }
}

/// Store creation methods.
impl Rkv {
    /// Create or Open an existing database in (&[u8] -> Single Value) mode.
    /// Note: that create=true cannot be called concurrently with other operations
    /// so if you are sure that the database exists, call this with create=false.
    pub fn open_single<'s, T>(&self, name: T, opts: StoreOptions) -> Result<SingleStore, StoreError>
    where
        T: Into<Option<&'s str>>,
    {
        self.open(name, opts).map(SingleStore::new)
    }

    /// Create or Open an existing database in (Integer -> Single Value) mode.
    /// Note: that create=true cannot be called concurrently with other operations
    /// so if you are sure that the database exists, call this with create=false.
    pub fn open_integer<'s, T, K: PrimitiveInt>(
        &self,
        name: T,
        mut opts: StoreOptions,
    ) -> Result<IntegerStore<K>, StoreError>
    where
        T: Into<Option<&'s str>>,
    {
        opts.flags.set(DatabaseFlags::INTEGER_KEY, true);
        self.open(name, opts).map(IntegerStore::new)
    }

    /// Create or Open an existing database in (&[u8] -> Multiple Values) mode.
    /// Note: that create=true cannot be called concurrently with other operations
    /// so if you are sure that the database exists, call this with create=false.
    pub fn open_multi<'s, T>(&self, name: T, mut opts: StoreOptions) -> Result<MultiStore, StoreError>
    where
        T: Into<Option<&'s str>>,
    {
        opts.flags.set(DatabaseFlags::DUP_SORT, true);
        self.open(name, opts).map(MultiStore::new)
    }

    /// Create or Open an existing database in (Integer -> Multiple Values) mode.
    /// Note: that create=true cannot be called concurrently with other operations
    /// so if you are sure that the database exists, call this with create=false.
    pub fn open_multi_integer<'s, T, K: PrimitiveInt>(
        &self,
        name: T,
        mut opts: StoreOptions,
    ) -> Result<MultiIntegerStore<K>, StoreError>
    where
        T: Into<Option<&'s str>>,
    {
        opts.flags.set(DatabaseFlags::INTEGER_KEY, true);
        opts.flags.set(DatabaseFlags::DUP_SORT, true);
        self.open(name, opts).map(MultiIntegerStore::new)
    }

    fn open<'s, T>(&self, name: T, opts: StoreOptions) -> Result<Database, StoreError>
    where
        T: Into<Option<&'s str>>,
    {
        if opts.create {
            self.env.create_db(name.into(), opts.flags).map_err(|e| match e {
                foundationdb::Error::BadRslot => StoreError::open_during_transaction(),
                _ => e.into(),
            })
        } else {
            self.env.open_db(name.into()).map_err(|e| match e {
                foundationdb::Error::BadRslot => StoreError::open_during_transaction(),
                _ => e.into(),
            })
        }
    }
}
