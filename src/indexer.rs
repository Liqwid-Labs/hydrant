use anyhow::Result;

use crate::tx::{Datum, DatumHash, Tx};

pub mod oracle;

pub trait Indexer {
    fn insert_tx(&self, wtxn: &mut heed::RwTxn, tx: &Tx) -> Result<bool>;
    fn delete_tx(&self, wtxn: &mut heed::RwTxn, tx: &Tx) -> Result<()>;

    fn insert_datum(
        &self,
        wtxn: &mut heed::RwTxn,
        datum_hash: &DatumHash,
        datum: &Datum,
    ) -> Result<bool>;
    fn delete_datum(&self, wtxn: &mut heed::RwTxn, datum_hash: &DatumHash) -> Result<()>;

    fn clear(&self, wtxn: &mut heed::RwTxn) -> Result<()>;
}
