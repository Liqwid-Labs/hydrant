use std::sync::{Arc, Mutex, RwLock};

use heed::{Database, WithTls};
use tracing::debug;

/// Wrapper around LMDB to provide safe resizing, error on duplicate database names, and snapshotting
#[derive(Debug, Clone)]
pub struct Env {
    env: heed::Env<WithTls>,
    db_names: Arc<Mutex<Vec<String>>>,
    resize_lock: Arc<RwLock<()>>,
    page_size: usize,
}

impl From<heed::Env> for Env {
    fn from(env: heed::Env) -> Self {
        Self {
            env,
            db_names: Arc::new(Mutex::new(vec![])),
            resize_lock: Arc::new(RwLock::new(())),
            page_size: page_size::get(),
        }
    }
}

impl Env {
    pub fn create_database<KC, DC>(
        &self,
        wtxn: &mut heed::RwTxn,
        name: &str,
    ) -> Result<Database<KC, DC>>
    where
        KC: 'static,
        DC: 'static,
    {
        let mut db_names = self.db_names.lock().expect("db_names mutex poisoned");
        if db_names.contains(&name.to_string()) {
            return Err(Error::DatabaseExists(name.to_string()));
        }
        db_names.push(name.to_string());
        Ok(self.env.create_database(wtxn, Some(name))?)
    }

    pub fn create_database_with_flags<KC, DC>(
        &self,
        wtxn: &mut heed::RwTxn,
        name: &str,
        flags: heed::DatabaseFlags,
    ) -> Result<Database<KC, DC>>
    where
        KC: 'static,
        DC: 'static,
    {
        let mut db_names = self.db_names.lock().expect("db_names mutex poisoned");
        if db_names.contains(&name.to_string()) {
            return Err(Error::DatabaseExists(name.to_string()));
        }
        db_names.push(name.to_string());
        Ok(self
            .env
            .database_options()
            .types::<KC, DC>()
            .name(name)
            .flags(flags)
            .create(wtxn)?)
    }

    pub fn write_txn(&self) -> Result<RwTxn<'_>> {
        let _guard = self.resize_lock.read().expect("resize lock poisoned");
        let txn = self.env.write_txn()?;
        Ok(RwTxn { txn, _guard })
    }
    pub fn read_txn(&self) -> Result<RoTxn<'_>> {
        let _guard = self.resize_lock.read().expect("resize lock poisoned");
        let txn = self.env.read_txn()?;
        Ok(RoTxn { txn, _guard })
    }
    pub fn persist(&self) -> Result<()> {
        Ok(self.env.force_sync()?)
    }

    pub(crate) fn resize(&self) -> Result<()> {
        let info = self.env.info();

        let used_size = self.page_size * info.last_page_number;
        let current_size = info.map_size;
        let free_size = current_size - used_size;
        let minimum_free_space = 1024 * 1024 * 1024; // 1GB

        if free_size < minimum_free_space || free_size > minimum_free_space * 2 {
            let new_size = current_size + minimum_free_space;
            let new_size = new_size + new_size % self.page_size; // Round up to next page

            let lock = self.resize_lock.write().unwrap();
            self.env.clear_stale_readers()?;
            if self.env.info().number_of_readers != 0 {
                return Err(Error::ActiveReadersOnResize(
                    self.env.info().number_of_readers,
                ));
            }
            unsafe { self.env.resize(new_size)? }
            debug!(?current_size, ?new_size, "Resized database");
            drop(lock)
        }

        Ok(())
    }

    pub(crate) fn snapshot(
        &self,
        path: impl AsRef<std::path::Path>,
        overwrite: bool,
    ) -> Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = if overwrite {
            std::fs::File::create(path)
        } else {
            std::fs::File::create_new(path)
        }?;

        Ok(self
            .env
            .copy_to_file(&mut file, heed::CompactionOption::Enabled)?)
    }
}

pub struct RoTxn<'env> {
    txn: heed::RoTxn<'env, WithTls>,
    _guard: std::sync::RwLockReadGuard<'env, ()>,
}
impl<'env> std::ops::Deref for RoTxn<'env> {
    type Target = heed::RoTxn<'env, WithTls>;
    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
impl<'env> std::ops::DerefMut for RoTxn<'env> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.txn
    }
}

pub struct RwTxn<'env> {
    txn: heed::RwTxn<'env>,
    _guard: std::sync::RwLockReadGuard<'env, ()>,
}

impl<'env> std::ops::Deref for RwTxn<'env> {
    type Target = heed::RwTxn<'env>;
    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
impl<'env> std::ops::DerefMut for RwTxn<'env> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.txn
    }
}
impl<'env> RwTxn<'env> {
    pub fn commit(self) -> heed::Result<()> {
        self.txn.commit()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Database name already in use
    #[error("database name already in use: {0}")]
    DatabaseExists(String),

    /// Readers were active while resizing the environment. This usually means someone is holding a
    /// read transaction in a separate process.
    #[error("cannot resize while readers are active; is another process accessing the database?")]
    ActiveReadersOnResize(u32),

    /// I/O error: can come from the standard library or be a rewrapped [`MdbError`].
    #[error("{0}")]
    Io(#[from] std::io::Error),

    /// LMDB error
    #[error("{0}")]
    Mdb(heed::MdbError),

    /// Encoding error
    #[error("error while encoding: {0}")]
    Encoding(heed::BoxedError),
    /// Decoding error
    #[error("error while decoding: {0}")]
    Decoding(heed::BoxedError),

    /// The environment is already open in this program;
    /// close it to be able to open it again with different options.
    #[error(
        "the environment is already open in this program; close it to be able to open it again with different options"
    )]
    EnvAlreadyOpened,
}
pub type Result<T> = std::result::Result<T, Error>;

impl From<heed::Error> for Error {
    fn from(error: heed::Error) -> Self {
        match error {
            heed::Error::Io(error) => Error::Io(error),
            heed::Error::Mdb(error) => Error::Mdb(error),
            heed::Error::Encoding(error) => Error::Encoding(error),
            heed::Error::Decoding(error) => Error::Decoding(error),
            heed::Error::EnvAlreadyOpened => Error::EnvAlreadyOpened,
        }
    }
}
