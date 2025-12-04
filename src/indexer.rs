use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::primitives::{Block, Datum, DatumHash, Script, ScriptHash, Tx};

pub mod oracle;

#[allow(dead_code)]
pub trait Indexer {
    fn id(&self) -> String;

    #[allow(unused_variables)]
    fn insert_block(&self, wtxn: &mut heed::RwTxn, block: &Block) -> Result<bool> {
        Ok(true)
    }
    #[allow(unused_variables)]
    fn delete_block(&self, wtxn: &mut heed::RwTxn, block: &Block) -> Result<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn insert_tx(&self, wtxn: &mut heed::RwTxn, tx: &Tx) -> Result<bool> {
        Ok(false)
    }
    #[allow(unused_variables)]
    fn delete_tx(&self, wtxn: &mut heed::RwTxn, tx: &Tx) -> Result<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn insert_datum(
        &self,
        wtxn: &mut heed::RwTxn,
        hash: &DatumHash,
        datum: &Datum,
    ) -> Result<bool> {
        Ok(false)
    }
    #[allow(unused_variables)]
    fn delete_datum(&self, wtxn: &mut heed::RwTxn, hash: &DatumHash) -> Result<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn insert_script(
        &self,
        wtxn: &mut heed::RwTxn,
        hash: &ScriptHash,
        script: &Script,
    ) -> Result<bool> {
        Ok(false)
    }
    #[allow(unused_variables)]
    fn delete_script(&self, wtxn: &mut heed::RwTxn, hash: &ScriptHash) -> Result<()> {
        Ok(())
    }

    fn clear(&self, wtxn: &mut heed::RwTxn) -> Result<()>;
}

pub(crate) type IndexerList = Vec<Arc<Mutex<dyn Indexer + Send + 'static>>>;
