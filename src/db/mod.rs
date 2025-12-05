use anyhow::{Context, Result};
use heed::byteorder::BigEndian;
use heed::types::{Str, U64, Unit};
use heed::{Database, EnvOpenOptions};
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::Point;
use tracing::info;

use crate::indexer::IndexerList;
use crate::primitives::{
    BlockHash, DatumHash, Tx, TxHash, TxOutput, TxOutputPointer, VolatileBlock,
};

mod codec;
mod env;

pub use codec::RkyvCodec;
pub use env::Env;

#[derive(Clone)]
pub struct Db {
    pub max_rollback_blocks: usize,
    pub env: Env,

    // big endian ints are lexicographically ordered
    slots: Database<U64<BigEndian>, RkyvCodec<BlockHash>>,
    volatile_tx: Database<RkyvCodec<TxHash>, RkyvCodec<Tx>>,
    volatile_block: Database<RkyvCodec<BlockHash>, RkyvCodec<VolatileBlock>>,
    indexer_ids: Database<Str, Unit>,
}

impl Db {
    pub fn new(path: &str, max_rollback_blocks: usize) -> Result<Self> {
        info!(?path, "Creating/opening database...");
        std::fs::create_dir_all(path)?;
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(64)
                .flags(
                    heed::EnvFlags::NO_SYNC // manually fsync data
                    | heed::EnvFlags::NO_META_SYNC // manually fsync metadata
                    | heed::EnvFlags::WRITE_MAP, // assume no memory unsafety in this program
                )
                .map_size(1024 * 1024 * 1024 * 2) // 2GB
                .open(path)?
        };

        let mut wtxn = env.write_txn()?;
        let slots = env.create_database(&mut wtxn, Some("slots"))?;
        let volatile_tx = env.create_database(&mut wtxn, Some("volatile_tx"))?;
        let volatile_block = env.create_database(&mut wtxn, Some("volatile_block"))?;
        let indexer_ids = env.create_database(&mut wtxn, Some("indexer_ids"))?;
        wtxn.commit()?;

        Ok(Self {
            max_rollback_blocks,
            env: env.into(),
            slots,
            volatile_tx,
            volatile_block,
            indexer_ids,
        })
    }

    pub fn get_volatile_block(
        &self,
        rtxn: &heed::RoTxn,
        block_hash: &BlockHash,
    ) -> Result<Option<VolatileBlock>> {
        self.volatile_block
            .get(rtxn, block_hash)?
            .map(|res| {
                Ok(rkyv::deserialize::<VolatileBlock, rkyv::rancor::Error>(
                    res,
                )?)
            })
            .transpose()
    }

    pub fn get_volatile_tx(&self, rtxn: &heed::RoTxn, tx_hash: &TxHash) -> Result<Option<Tx>> {
        self.volatile_tx
            .get(rtxn, tx_hash)?
            .map(|res| Ok(rkyv::deserialize::<Tx, rkyv::rancor::Error>(res)?))
            .transpose()
    }

    pub fn get_volatile_tx_output(
        &self,
        rtxn: &heed::RoTxn,
        pointer: &TxOutputPointer,
    ) -> Result<Option<TxOutput>> {
        self.volatile_tx
            .get(rtxn, &pointer.hash)?
            .map(rkyv::deserialize::<Tx, rkyv::rancor::Error>)
            .transpose()?
            .map(|tx| {
                tx.outputs
                    .get(pointer.index as usize)
                    .cloned()
                    .context("missing output")
            })
            .transpose()
    }

    pub fn assert_indexer_ids(&self, rtxn: &heed::RoTxn, indexer_ids: &[&str]) -> Result<()> {
        // Insert indexer ids if they don't exist
        if self.indexer_ids.len(rtxn)? == 0 {
            let mut wtxn = self.env.write_txn()?;
            for id in indexer_ids.iter() {
                self.indexer_ids.put(&mut wtxn, id, &())?;
            }
            if indexer_ids.is_empty() {
                self.indexer_ids.put(&mut wtxn, "empty", &())?;
            }
            wtxn.commit()?;
            return Ok(());
        }

        // Check indexer ids
        let expected_indexer_ids = self
            .indexer_ids
            .iter(rtxn)?
            .map(|res| -> Result<String> { Ok(res?.0.to_string()) })
            .collect::<Result<Vec<_>>>()?;
        anyhow::ensure!(
            expected_indexer_ids.as_slice() == indexer_ids,
            "indexer ids don't match. expected: {expected_indexer_ids:?}, got: {indexer_ids:?}"
        );
        Ok(())
    }

    pub fn tip(&self) -> Result<Point> {
        let rtxn = self.env.read_txn()?;
        if let Some((slot, block_hash)) = self.slots.rev_range(&rtxn, &(0..))?.next().transpose()? {
            let block_hash = rkyv::deserialize::<BlockHash, rkyv::rancor::Error>(block_hash)?;
            Ok(Point::Specific(slot, block_hash.to_vec()))
        } else {
            Ok(Point::Origin)
        }
    }

    pub(crate) fn roll_forward(&self, indexers: &IndexerList, block: &MultiEraBlock) -> Result<()> {
        let indexers = indexers
            .iter()
            .map(|i| i.lock().expect("indexer mutex poisoned"))
            .collect::<Vec<_>>();
        let mut wtxn = self.env.write_txn()?;

        // Ensure the indexers didn't change
        let indexer_ids = indexers.iter().map(|i| i.id()).collect::<Vec<_>>();
        self.assert_indexer_ids(&wtxn, &indexer_ids)?;

        // Pass datums + txs to each indexer, storing the hashes of those that got inserted
        let mut tx_hashes = vec![];
        let mut datum_hashes = vec![];
        for raw_tx in block.txs().iter() {
            let (tx, datums) = Tx::parse(raw_tx);

            let did_insert_tx = indexers.iter().try_fold(false, |acc, i| {
                i.insert_tx(self, &mut wtxn, &tx).map(|b| acc || b)
            })?;
            if did_insert_tx {
                tx_hashes.push(tx.hash.clone());
                self.volatile_tx.put(&mut wtxn, &tx.hash, &tx)?;
            }

            for (datum_hash, datum) in datums.iter() {
                let did_insert_datum = indexers.iter().try_fold(false, |acc, i| {
                    i.insert_datum(self, &mut wtxn, datum_hash, datum)
                        .map(|b| acc || b)
                })?;
                if did_insert_datum {
                    datum_hashes.push(datum_hash.clone());
                }
            }
        }

        // Block Hash -> Block
        let block = VolatileBlock::parse(block, tx_hashes, datum_hashes);
        self.volatile_block.put(&mut wtxn, &block.hash, &block)?;

        // Slot -> Block Hash
        self.slots.put(&mut wtxn, &block.slot, &block.hash)?;

        wtxn.commit()?;
        Ok(self.env.resize()?)
    }

    pub(crate) fn roll_backward(&self, indexers: &IndexerList, point: &Point) -> Result<()> {
        // TODO: error when rolling back too far
        let slot = match point {
            Point::Origin => return self.clear(indexers),
            Point::Specific(slot, _) => *slot + 1,
        };

        let indexers = indexers
            .iter()
            .map(|i| i.lock().expect("indexer mutex poisoned"))
            .collect::<Vec<_>>();
        let rtxn = self.env.read_txn()?;

        // Ensure the indexers didn't change
        let indexer_ids = indexers.iter().map(|i| i.id()).collect::<Vec<_>>();
        self.assert_indexer_ids(&rtxn, &indexer_ids)?;

        for res in self.slots.rev_range(&rtxn, &(slot..))? {
            let (slot, block_hash) = res?;
            let block_hash = rkyv::deserialize::<BlockHash, rkyv::rancor::Error>(block_hash)?;

            let block = self
                .volatile_block
                .get(&rtxn, &block_hash)?
                .with_context(|| {
                    format!("block not found while rolling back, the db could be corrupt or rolled back further than max_rollback_blocks: {}", block_hash)
                })?;

            // NOTE: reverse order because a tx may spend outputs from a previous tx
            // in the same block
            let mut wtxn = self.env.write_txn()?;
            for tx_hash in block.txs.iter().rev() {
                let tx_hash = rkyv::deserialize::<TxHash, rkyv::rancor::Error>(tx_hash)?;
                let tx = self.volatile_tx.get(&rtxn, &tx_hash)?.with_context(|| {
                    format!(
                        "tx not found while rolling back, the db could be corrupt: {}",
                        tx_hash
                    )
                })?;
                let tx = rkyv::deserialize::<Tx, rkyv::rancor::Error>(tx)?;
                for indexer in indexers.iter() {
                    indexer.delete_tx(self, &mut wtxn, &tx)?;
                }
            }
            for datum_hash in block.datums.iter().rev() {
                let datum_hash = rkyv::deserialize::<DatumHash, rkyv::rancor::Error>(datum_hash)?;
                for indexer in indexers.iter() {
                    indexer.delete_datum(self, &mut wtxn, &datum_hash)?;
                }
            }

            self.slots.delete(&mut wtxn, &slot)?;
            self.volatile_block.delete(&mut wtxn, &block_hash)?;
            wtxn.commit()?;
        }

        Ok(self.env.resize()?)
    }

    pub(crate) fn trim_volatile(&self) -> Result<()> {
        let rtxn = self.env.read_txn()?;
        let mut wtxn = self.env.write_txn()?;

        for slot in self
            .slots
            .rev_range(&rtxn, &(0..))?
            .skip(self.max_rollback_blocks)
        {
            let (_, block_hash) = slot?;
            let block_hash = rkyv::deserialize::<BlockHash, rkyv::rancor::Error>(block_hash)?;

            // If we can't find the block, we've already trimmed it
            let Some(block) = self.volatile_block.get(&rtxn, &block_hash)? else {
                break;
            };
            let block = rkyv::deserialize::<VolatileBlock, rkyv::rancor::Error>(block)?;

            // Drop all the txs in the block
            for tx_hash in block.txs.iter().rev() {
                self.volatile_tx.delete(&mut wtxn, tx_hash)?;
            }

            // Drop the block
            self.volatile_block.delete(&mut wtxn, &block_hash)?;
        }

        Ok(wtxn.commit()?)
    }

    pub(crate) fn clear(&self, indexers: &IndexerList) -> Result<()> {
        let indexers = indexers
            .iter()
            .map(|i| i.lock().expect("indexer mutex poisoned"))
            .collect::<Vec<_>>();
        let mut wtxn = self.env.write_txn()?;

        self.slots.clear(&mut wtxn)?;
        self.volatile_block.clear(&mut wtxn)?;
        self.volatile_tx.clear(&mut wtxn)?;
        self.indexer_ids.clear(&mut wtxn)?;
        for indexer in indexers.iter() {
            indexer.clear(&mut wtxn)?;
        }

        wtxn.commit()?;
        Ok(self.env.resize()?)
    }

    pub fn persist(&self) -> Result<()> {
        Ok(self.env.persist()?)
    }

    pub fn snapshot(&self, path: impl AsRef<std::path::Path>, overwrite: bool) -> Result<()> {
        Ok(self.env.snapshot(path, overwrite)?)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_max_rollback_blocks() {}
}
