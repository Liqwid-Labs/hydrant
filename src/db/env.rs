use std::sync::{Arc, RwLock};

use anyhow::{Context, ensure};
use heed::{Database, WithTls};
use tracing::debug;

/// Wrapper around LMDB to provide safe resizing of the database
#[derive(Debug, Clone)]
pub struct Env {
    env: heed::Env<WithTls>,
    resize_lock: Arc<RwLock<()>>,
    page_size: usize,
}

impl From<heed::Env> for Env {
    fn from(env: heed::Env) -> Self {
        Self {
            env,
            resize_lock: Arc::new(RwLock::new(())),
            page_size: page_size::get(),
        }
    }
}

impl Env {
    pub fn create_database<KC, DC>(
        &self,
        wtxn: &mut heed::RwTxn,
        name: Option<&str>,
    ) -> heed::Result<Database<KC, DC>>
    where
        KC: 'static,
        DC: 'static,
    {
        self.env.create_database(wtxn, name)
    }

    pub fn write_txn(&self) -> heed::Result<RwTxn<'_>> {
        let _guard = self.resize_lock.read().expect("resize lock poisoned");
        let txn = self.env.write_txn()?;
        Ok(RwTxn { txn, _guard })
    }
    pub fn read_txn(&self) -> heed::Result<RoTxn<'_>> {
        let _guard = self.resize_lock.read().expect("resize lock poisoned");
        let txn = self.env.read_txn()?;
        Ok(RoTxn { txn, _guard })
    }
    pub fn force_sync(&self) -> heed::Result<()> {
        self.env.force_sync()
    }

    pub fn resize(&self) -> anyhow::Result<()> {
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
            ensure!(
                self.env.info().number_of_readers == 0,
                "cannot resize while readers are active. is another process accessing the database?"
            );
            unsafe {
                self.env
                    .resize(new_size)
                    .context("failed to resize database")?;
            }
            debug!(?current_size, ?new_size, "Resized database");
            drop(lock)
        }

        Ok(())
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
