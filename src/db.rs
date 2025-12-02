use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use heed::byteorder::BigEndian;
use heed::types::U64;
use heed::{Database, EnvOpenOptions};
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::Point;
use tracing::info;

use crate::codec::RkyvCodec;
use crate::env::Env;
use crate::indexer::Indexer;
use crate::tx::{Block, BlockHash, DatumHash, Tx, TxHash};

#[derive(Clone)]
pub struct Db {
    pub max_rollback_blocks: usize,
    pub env: Env,
    pub slots: Database<U64<BigEndian>, RkyvCodec<BlockHash>>,
    pub volatile_tx: Database<RkyvCodec<TxHash>, RkyvCodec<Tx>>,
    pub volatile_block: Database<RkyvCodec<BlockHash>, RkyvCodec<Block>>,
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
        wtxn.commit()?;

        Ok(Self {
            max_rollback_blocks,
            env: env.into(),
            slots,
            volatile_tx,
            volatile_block,
        })
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

    pub fn roll_forward(
        &self,
        indexer: &Arc<Mutex<impl Indexer>>,
        block: &MultiEraBlock,
    ) -> Result<()> {
        let indexer = indexer.lock().expect("indexer mutex poisoned");
        let mut wtxn = self.env.write_txn()?;

        let mut tx_hashes = vec![];
        let mut datum_hashes = vec![];
        for raw_tx in block.txs().iter() {
            let (tx, datums) = Tx::parse(raw_tx);
            if indexer.insert_tx(&mut wtxn, &tx)? {
                tx_hashes.push(tx.hash.clone());
                self.volatile_tx.put(&mut wtxn, &tx.hash, &tx)?;
            }
            for (datum_hash, datum) in datums.iter() {
                if indexer.insert_datum(&mut wtxn, datum_hash, datum)? {
                    datum_hashes.push(datum_hash.clone());
                }
            }
        }

        // Block Hash -> Block
        let block = Block::parse(block, tx_hashes, datum_hashes);
        self.volatile_block.put(&mut wtxn, &block.hash, &block)?;

        // Slot -> Block Hash
        self.slots.put(&mut wtxn, &block.slot, &block.hash)?;

        wtxn.commit()?;
        self.env.resize()
    }

    pub fn roll_backward(&self, indexer: &Arc<Mutex<impl Indexer>>, point: &Point) -> Result<()> {
        let slot = match point {
            Point::Origin => return self.clear(indexer),
            Point::Specific(slot, _) => *slot + 1,
        };

        let indexer = indexer.lock().expect("indexer mutex poisoned");
        let rtxn = self.env.read_txn()?;
        for res in self.slots.rev_range(&rtxn, &(slot..))? {
            let (slot, block_hash) = res?;
            let block_hash = rkyv::deserialize::<BlockHash, rkyv::rancor::Error>(block_hash)?;

            let block = self
                .volatile_block
                .get(&rtxn, &block_hash)?
                .with_context(|| {
                    format!("Block not found, the db could be corrupted: {}", block_hash)
                })?;

            // NOTE: reverse order because a tx may spend outputs from a previous tx
            // in the same block
            let mut wtxn = self.env.write_txn()?;
            for tx_hash in block.txs.iter().rev() {
                let tx_hash = rkyv::deserialize::<TxHash, rkyv::rancor::Error>(tx_hash)?;
                let tx = self
                    .volatile_tx
                    .get(&rtxn, &tx_hash)?
                    .context("missing tx")?;
                let tx = rkyv::deserialize::<Tx, rkyv::rancor::Error>(tx)?;
                indexer.delete_tx(&mut wtxn, &tx)?;
            }
            for datum_hash in block.datums.iter().rev() {
                let datum_hash = rkyv::deserialize::<DatumHash, rkyv::rancor::Error>(datum_hash)?;
                indexer.delete_datum(&mut wtxn, &datum_hash)?;
            }

            self.slots.delete(&mut wtxn, &slot)?;
            self.volatile_block.delete(&mut wtxn, &block_hash)?;
            wtxn.commit()?;
        }

        self.env.resize()
    }

    pub fn trim_volatile(&self) -> Result<()> {
        let rtxn = self.env.read_txn()?;
        let mut wtxn = self.env.write_txn()?;

        for slot in self
            .slots
            .rev_range(&rtxn, &(0..))?
            .skip(self.max_rollback_blocks)
        {
            let (_, block_hash) = slot?;
            let block_hash = rkyv::deserialize::<BlockHash, rkyv::rancor::Error>(block_hash)?;
            let exists = self.volatile_block.delete(&mut wtxn, &block_hash)?;
            if !exists {
                break;
            }
        }

        Ok(wtxn.commit()?)
    }

    pub fn clear(&self, indexer: &Arc<Mutex<impl Indexer>>) -> Result<()> {
        let indexer = indexer.lock().expect("indexer mutex poisoned");
        let mut wtxn = self.env.write_txn()?;

        self.slots.clear(&mut wtxn)?;
        self.volatile_block.clear(&mut wtxn)?;
        self.volatile_tx.clear(&mut wtxn)?;
        indexer.clear(&mut wtxn)?;

        wtxn.commit()?;
        self.env.resize()
    }

    pub fn persist(&self) -> Result<()> {
        Ok(self.env.force_sync()?)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_max_rollback_blocks() {}
}
