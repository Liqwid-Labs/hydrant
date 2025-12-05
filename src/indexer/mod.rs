use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::db::Db;
use crate::primitives::{Datum, DatumHash, Script, ScriptHash, Tx};

pub mod utxo;

pub trait Indexer {
    fn id(&self) -> &str;

    #[allow(unused_variables)]
    fn insert_tx(&self, db: &Db, wtxn: &mut heed::RwTxn, tx: &Tx) -> Result<bool> {
        Ok(false)
    }
    #[allow(unused_variables)]
    fn delete_tx(&self, db: &Db, wtxn: &mut heed::RwTxn, tx: &Tx) -> Result<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn insert_datum(
        &self,
        db: &Db,
        wtxn: &mut heed::RwTxn,
        hash: &DatumHash,
        datum: &Datum,
    ) -> Result<bool> {
        Ok(false)
    }
    #[allow(unused_variables)]
    fn delete_datum(&self, db: &Db, wtxn: &mut heed::RwTxn, hash: &DatumHash) -> Result<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn insert_script(
        &self,
        db: &Db,
        wtxn: &mut heed::RwTxn,
        hash: &ScriptHash,
        script: &Script,
    ) -> Result<bool> {
        Ok(false)
    }
    #[allow(unused_variables)]
    fn delete_script(&self, db: &Db, wtxn: &mut heed::RwTxn, hash: &ScriptHash) -> Result<()> {
        Ok(())
    }

    fn clear(&self, wtxn: &mut heed::RwTxn) -> Result<()>;
}

pub(crate) type IndexerList = Vec<Arc<Mutex<dyn Indexer + Send + 'static>>>;
