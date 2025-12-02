use anyhow::{Context, Result};
use heed::types::Bytes;
use heed::{Database, RwTxn};

use crate::codec::RkyvCodec;
use crate::env::Env;
use crate::indexer::Indexer;
use crate::indexer::oracle::datum::OracleDatum;
use crate::tx::{Datum, DatumHash, Hash, Policy, Tx, TxOutput, TxOutputPointer};

mod datum;
mod primitives;

#[derive(Clone)]
pub struct OracleIndexer {
    env: Env,
    txos: Database<RkyvCodec<TxOutputPointer>, RkyvCodec<TxOutput>>,
    utxos: Database<RkyvCodec<TxOutputPointer>, RkyvCodec<()>>,
    datums: Database<RkyvCodec<DatumHash>, Bytes>,
}

const POLICY_ID: Policy = Hash([
    0x0f, 0xde, 0x77, 0xa0, 0xea, 0x08, 0x33, 0x50, 0x2b, 0x38, 0x6d, 0x34, 0xe3, 0x3d, 0x78, 0xf8,
    0x6c, 0x75, 0x4b, 0xad, 0x30, 0x9e, 0xe8, 0xbf, 0x00, 0x8d, 0x7a, 0x9d,
]);

impl OracleIndexer {
    pub fn new(env: &Env) -> Result<Self> {
        let env = env.clone();

        let mut wtxn = env.write_txn()?;
        let txos = env.create_database(&mut wtxn, Some("txos"))?;
        let utxos = env.create_database(&mut wtxn, Some("utxos"))?;
        let datums = env.create_database(&mut wtxn, Some("datums"))?;
        wtxn.commit()?;

        Ok(Self {
            env,
            txos,
            utxos,
            datums,
        })
    }

    pub fn utxos(&self) -> Result<Vec<(TxOutputPointer, TxOutput)>> {
        let txn = self.env.read_txn()?;
        self.utxos
            .iter(&txn)?
            .map(|res| {
                let pointer = rkyv::deserialize::<TxOutputPointer, rkyv::rancor::Error>(res?.0)?;
                let txo = self.txos.get(&txn, &pointer)?.context("missing txo")?;
                let txo = rkyv::deserialize::<TxOutput, rkyv::rancor::Error>(txo)?;
                Ok((pointer, txo))
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn datum(&self, hash: &DatumHash) -> Result<Option<OracleDatum>> {
        let rtxn = self.env.read_txn()?;
        let Some(datum) = self.datums.get(&rtxn, hash)? else {
            return Ok(None);
        };
        let oracle_datum = minicbor::decode::<OracleDatum>(datum)?;
        Ok(Some(oracle_datum))
    }
}

impl Indexer for OracleIndexer {
    fn insert_tx(&self, wtxn: &mut RwTxn, tx: &Tx) -> anyhow::Result<bool> {
        let mut added_some = false;

        // Mark consumed UTxOs as spent
        for input in tx.spent() {
            added_some |= self.utxos.delete(wtxn, input)?;
        }

        // Add UTxOs
        for (index, output) in tx
            .unspent()
            .enumerate()
            .filter(|(_, output)| output.assets.iter().any(|a| a.policy == POLICY_ID))
        {
            let pointer = TxOutputPointer::new(tx.hash.clone(), index);
            self.txos.put(wtxn, &pointer, output)?;
            self.utxos.put(wtxn, &pointer, &())?;

            added_some = true;
        }

        Ok(added_some)
    }

    fn delete_tx(&self, wtxn: &mut RwTxn, tx: &Tx) -> anyhow::Result<()> {
        self.txos
            .delete_range(wtxn, &TxOutputPointer::range(&tx.hash))?;
        self.utxos
            .delete_range(wtxn, &TxOutputPointer::range(&tx.hash))?;

        for input in tx.spent() {
            if self.txos.get(wtxn, input)?.is_some() {
                self.utxos.put(wtxn, input, &())?;
            }
        }

        Ok(())
    }

    fn insert_datum(
        &self,
        wtxn: &mut heed::RwTxn,
        datum_hash: &DatumHash,
        datum: &Datum,
    ) -> Result<bool> {
        if minicbor::decode::<OracleDatum>(datum.as_slice()).is_err() {
            return Ok(false);
        }
        if self.datums.get(wtxn, datum_hash)?.is_some() {
            return Ok(false);
        }
        self.datums.put(wtxn, datum_hash, datum)?;
        Ok(true)
    }

    fn delete_datum(&self, wtxn: &mut heed::RwTxn, datum_hash: &DatumHash) -> Result<()> {
        self.datums.delete(wtxn, datum_hash)?;
        Ok(())
    }

    fn clear(&self, wtxn: &mut RwTxn) -> anyhow::Result<()> {
        self.txos.clear(wtxn)?;
        self.utxos.clear(wtxn)?;
        self.datums.clear(wtxn)?;
        Ok(())
    }
}
